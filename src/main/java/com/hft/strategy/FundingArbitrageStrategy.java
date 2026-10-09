package com.hft.strategy;

import com.hft.perp.PerpAccount;
import com.hft.config.GlobalParams;
import com.hft.engine.OrderService;
import com.hft.exchange.ExchangeGateway;
import com.hft.model.OrderEnums.Side;
import com.hft.model.OrderResult;
import com.hft.store.BalanceStore;
import com.hft.store.FundingStore.Funding;
import com.hft.store.OrderBook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

/**
 * Funding-арбитраж между биржами (только перпы, market=perp).
 *
 * <p>Ставка funding у одной и той же монеты на разных биржах разная. Если на бирже A она заметно выше,
 * чем на B, открываются две равные по объёму позиции: <b>шорт на A</b> (шорт получает funding при
 * положительной ставке) и <b>лонг на B</b>. Цена монеты на результат почти не влияет — позиции
 * компенсируют друг друга; доход — разница ставок каждый период funding.
 *
 * <ul>
 *   <li>Ставки сравниваются приведёнными к 8 часам (у Hyperliquid период — 1 час).</li>
 *   <li>Монеты сопоставляются по базовой валюте: BTCUSDT на Binance и BTCUSDC на Hyperliquid — одна монета.</li>
 *   <li>Вход: разница ≥ {@code fundingArbMinDiffPercent} и окупает комиссии полного круга (4 сделки тейкера
 *       по takerFeePercent обеих бирж) не дольше чем за {@code fundingArbPaybackPeriods} периодов по 8 ч, цены на двух биржах отличаются не больше
 *       {@code fundingArbMaxBasisPercent}, торговля на обеих биржах разрешена, пар меньше {@code fundingArbMaxPositions}.</li>
 *   <li>Сначала шорт, затем лонг на исполненный объём; если вторая нога не исполнилась — первая закрывается.</li>
 *   <li>Выход: разница упала ниже {@code fundingArbExitDiffPercent}, истёк {@code fundingArbMaxHoldHours},
 *       одна из бирж остановлена — обе ноги закрываются reduceOnly-ордерами.</li>
 * </ul>
 *
 * Риски: разница ставок может смениться раньше, чем окупятся 4 комиссии тейкера; при сильном движении
 * цены нога с плечом может быть ликвидирована раньше, чем вы вмешаетесь, — держите плечо низким.
 */
public final class FundingArbitrageStrategy {

    /** Логгер. */
    private static final Logger log = LoggerFactory.getLogger(FundingArbitrageStrategy.class);

    /** Нога биржи: шлюз и символ на нём. */
    record Venue(ExchangeGateway gw, String symbol) {
        /** Сервис ордеров. */
        OrderService orders() { return gw.orders(); }
        /** Ставка funding (null — нет). */
        Funding funding() { PerpAccount p = gw.perp(); return p == null ? null : p.funding().get(symbol); }
        /** Середина стакана (NaN — нет). */
        double mid() { OrderBook b = gw.marketData().book(symbol); return b == null || !b.isReady() ? Double.NaN : b.midPrice(); }
        /** Имя для логов. */
        String name() { return gw.id() + ":" + symbol; }
    }

    /** Открытая пара: шорт там, где ставка выше, лонг — где ниже. */
    record Pair(String coin, Venue shortLeg, Venue longLeg, double qty, double shortEntry, double longEntry,
                double entryDiffPercent, long openedAtMs) {}

    /** Возможность: монета, где шортить и где лонговать, разница ставок за 8 ч, %. */
    record Opportunity(String coin, Venue shortLeg, Venue longLeg, double diffPercent) {}

    /** Настройки процесса. */
    private final Supplier<GlobalParams> params;
    /** Работающие биржи. */
    private final Supplier<Collection<ExchangeGateway>> gateways;
    /** Торговля на бирже включена (параметр tradingEnabled). */
    private final java.util.function.Predicate<String> tradingEnabled;
    /** Комиссия тейкера биржи, % (параметр takerFeePercent). */
    private final java.util.function.ToDoubleFunction<String> takerFeePercent;
    /** Открытые пары по монете. */
    private final Map<String, Pair> pairs = new ConcurrentHashMap<>();
    /** Последние найденные возможности (для админки). */
    private volatile List<Opportunity> lastOpportunities = List.of();
    /** Цикл решений. */
    private ScheduledExecutorService scheduler;
    /** Счётчики. */
    private final AtomicLong opened = new AtomicLong(), closed = new AtomicLong(), failed = new AtomicLong();
    /** Результат закрытых пар по ценам после комиссий всех сделок (без funding). */
    private volatile double pricePnl;

    /**
     * @param params настройки процесса (читаются на каждом цикле)
     * @param gateways работающие биржи
     * @param tradingEnabled включена ли торговля на бирже (по id)
     * @param takerFeePercent комиссия тейкера биржи, % (по id)
     */
    public FundingArbitrageStrategy(Supplier<GlobalParams> params, Supplier<Collection<ExchangeGateway>> gateways,
                            java.util.function.Predicate<String> tradingEnabled,
                            java.util.function.ToDoubleFunction<String> takerFeePercent) {
        this.params = params;
        this.gateways = gateways;
        this.tradingEnabled = tradingEnabled;
        this.takerFeePercent = takerFeePercent;
    }

    /** Запустить цикл (раз в 5 с). */
    public synchronized void start() {
        if (scheduler != null) return;
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "funding-arb");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(() -> {
            try { tick(System.currentTimeMillis()); }
            catch (Exception e) { log.error("[funding-arb] ошибка цикла", e); }
        }, 5, 5, TimeUnit.SECONDS);
    }

    /** Остановить цикл и закрыть открытые пары (биржи ещё работают). */
    public synchronized void stop() {
        ScheduledExecutorService s = scheduler;
        scheduler = null;
        if (s != null) {
            s.shutdown();
            try { s.awaitTermination(15, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        if (!pairs.isEmpty()) log.info("[funding-arb] остановка бота — закрываю {} пар", pairs.size());
        for (Pair p : List.copyOf(pairs.values())) close(p, "остановка бота");
    }

    /** Один цикл: выходы по открытым парам, затем входы. */
    public void tick(long now) {
        GlobalParams g = params.get();
        List<Opportunity> ops = opportunities(g, now);
        lastOpportunities = ops;
        Map<String, Opportunity> byCoin = new java.util.HashMap<>();
        for (Opportunity o : ops) byCoin.put(o.coin(), o);

        for (Pair p : List.copyOf(pairs.values())) {
            String why = exitReason(p, g, now);
            if (why != null) close(p, why);
        }
        if (!g.fundingArbEnabled()) return;
        for (Opportunity o : ops) {
            if (pairs.size() >= g.fundingArbMaxPositions()) break;
            if (pairs.containsKey(o.coin()) || !enough(o, g)) continue;
            if (!tradable(o.shortLeg()) || !tradable(o.longLeg())) continue;
            if (busy(o.shortLeg()) || busy(o.longLeg())) continue;
            double ms = o.shortLeg().mid(), ml = o.longLeg().mid();
            if (!(ms > 0) || !(ml > 0)) continue;
            double basis = Math.abs(ms - ml) / ((ms + ml) / 2) * 100;
            if (basis > g.fundingArbMaxBasisPercent()) continue;
            open(o, g, (ms + ml) / 2);
        }
    }

    /**
     * Возможности по всем монетам, которые торгуются перпами минимум на двух биржах:
     * для каждой — биржа с максимальной и с минимальной ставкой. По убыванию разницы.
     */
    List<Opportunity> opportunities(GlobalParams g, long now) {
        Set<String> filter = g.fundingArbSymbols().isBlank() ? Set.of() : Set.of(g.fundingArbSymbols().split(","));
        long maxAge = Math.max(120_000, 4_000L * g.fundingPollSec());
        Map<String, List<Venue>> byCoin = new TreeMap<>();
        for (ExchangeGateway gw : gateways.get()) {
            if (gw.perp() == null) continue;
            for (String s : gw.marketData().symbols()) {
                String coin = coin(s);
                if (!filter.isEmpty() && !filter.contains(s) && !filter.contains(coin)) continue;
                Venue v = new Venue(gw, s);
                Funding f = v.funding();
                if (f == null || now - f.updatedMs() > maxAge) continue;           // старая ставка — не сравниваем
                byCoin.computeIfAbsent(coin, k -> new ArrayList<>()).add(v);
            }
        }
        List<Opportunity> out = new ArrayList<>();
        byCoin.forEach((coin, vs) -> {
            if (vs.size() < 2) return;
            Venue hi = vs.get(0), lo = vs.get(0);
            for (Venue v : vs) {
                if (v.funding().ratePer8h() > hi.funding().ratePer8h()) hi = v;
                if (v.funding().ratePer8h() < lo.funding().ratePer8h()) lo = v;
            }
            if (hi.gw() == lo.gw()) return;
            out.add(new Opportunity(coin, hi, lo, (hi.funding().ratePer8h() - lo.funding().ratePer8h()) * 100));
        });
        out.sort((a, b) -> Double.compare(b.diffPercent(), a.diffPercent()));
        return out;
    }

    /** Почему пару пора закрыть; null — держим. */
    String exitReason(Pair p, GlobalParams g, long now) {
        Collection<ExchangeGateway> active = gateways.get();
        if (!active.contains(p.shortLeg().gw()) || !active.contains(p.longLeg().gw())) return "биржа остановлена";
        if (p.shortLeg().gw().risk().isStopped() || p.longLeg().gw().risk().isStopped()) return "торговля на бирже остановлена";
        if (now - p.openedAtMs() > g.fundingArbMaxHoldHours() * 3_600_000) return "таймаут";
        Funding fs = p.shortLeg().funding(), fl = p.longLeg().funding();
        if (fs == null || fl == null) return null;
        double diff = (fs.ratePer8h() - fl.ratePer8h()) * 100;
        if (diff < g.fundingArbExitDiffPercent()) return String.format("разница ставок упала до %.4f%%", diff);
        return null;
    }

    /** Открыть пару: шорт на бирже с высокой ставкой, затем лонг на исполненный объём. */
    private void open(Opportunity o, GlobalParams g, double mid) {
        double qty = commonQty(o.shortLeg(), o.longLeg(), g.fundingArbOrderQuote() / mid);
        if (qty <= 0) return;
        log.info("[funding-arb] {}: шорт {} ({}%/8ч), лонг {} ({}%/8ч), разница {}%, объём {}", o.coin(),
                o.shortLeg().name(), pct(o.shortLeg().funding().ratePer8h()), o.longLeg().name(), pct(o.longLeg().funding().ratePer8h()),
                String.format("%.4f", o.diffPercent()), qty);
        OrderResult s = o.shortLeg().orders().sellMarket(o.shortLeg().symbol(), qty);
        if (s.executedQty() <= 0) { failed.incrementAndGet(); return; }
        double lq = commonQty(o.shortLeg(), o.longLeg(), s.executedQty());
        OrderResult l = lq > 0 ? o.longLeg().orders().buyMarket(o.longLeg().symbol(), lq) : null;
        if (l == null || l.executedQty() <= 0) {
            failed.incrementAndGet();
            log.warn("[funding-arb] {}: лонг на {} не исполнился — закрываю шорт на {}", o.coin(), o.longLeg().name(), o.shortLeg().name());
            OrderResult u = o.shortLeg().orders().reduceMarket(o.shortLeg().symbol(), Side.BUY, s.executedQty());
            if (u.executedQty() > 0) {                       // откат — тоже две сделки с комиссией
                double r = (s.avgPrice() - u.avgPrice()) * u.executedQty()
                        - fee(o.shortLeg(), s.avgPrice(), u.executedQty()) - fee(o.shortLeg(), u.avgPrice(), u.executedQty());
                o.shortLeg().gw().risk().recordPnl(r);
                pricePnl += r;
            }
            return;
        }
        double extra = s.executedQty() - l.executedQty();                 // объёмы ног разошлись — лишнее закрыть
        if (extra > 1e-12) {
            OrderResult u = o.shortLeg().orders().reduceMarket(o.shortLeg().symbol(), Side.BUY, extra);
            if (u.executedQty() > 0) {
                double r = (s.avgPrice() - u.avgPrice()) * u.executedQty()
                        - fee(o.shortLeg(), s.avgPrice(), u.executedQty()) - fee(o.shortLeg(), u.avgPrice(), u.executedQty());
                o.shortLeg().gw().risk().recordPnl(r);
                pricePnl += r;
            }
        }
        pairs.put(o.coin(), new Pair(o.coin(), o.shortLeg(), o.longLeg(), l.executedQty(), s.avgPrice(), l.avgPrice(),
                o.diffPercent(), System.currentTimeMillis()));
        opened.incrementAndGet();
    }

    /** Закрыть обе ноги reduceOnly-ордерами; результат по ценам — в дневной PnL бирж. */
    private void close(Pair p, String why) {
        log.info("[funding-arb] {}: закрываю ({}) — шорт {}, лонг {}", p.coin(), why, p.shortLeg().name(), p.longLeg().name());
        OrderResult s = p.shortLeg().orders().reduceMarket(p.shortLeg().symbol(), Side.BUY, p.qty());
        OrderResult l = p.longLeg().orders().reduceMarket(p.longLeg().symbol(), Side.SELL, p.qty());
        double pnl = 0;
        if (s.executedQty() > 0) {                       // комиссии входа и выхода этой ноги
            double r = (p.shortEntry() - s.avgPrice()) * s.executedQty()
                    - fee(p.shortLeg(), p.shortEntry(), s.executedQty()) - fee(p.shortLeg(), s.avgPrice(), s.executedQty());
            p.shortLeg().gw().risk().recordPnl(r);
            pnl += r;
        }
        if (l.executedQty() > 0) {
            double r = (l.avgPrice() - p.longEntry()) * l.executedQty()
                    - fee(p.longLeg(), p.longEntry(), l.executedQty()) - fee(p.longLeg(), l.avgPrice(), l.executedQty());
            p.longLeg().gw().risk().recordPnl(r);
            pnl += r;
        }
        double leftS = p.qty() - s.executedQty(), leftL = p.qty() - l.executedQty();
        if (leftS > p.qty() * 1e-6 || leftL > p.qty() * 1e-6) {
            log.error("[funding-arb] {}: не всё закрылось (шорт осталось {}, лонг {}) — повторю на следующем цикле", p.coin(), leftS, leftL);
            pairs.put(p.coin(), new Pair(p.coin(), p.shortLeg(), p.longLeg(), Math.max(leftS, leftL), p.shortEntry(), p.longEntry(),
                    p.entryDiffPercent(), p.openedAtMs()));
        } else {
            pairs.remove(p.coin());
            closed.incrementAndGet();
        }
        pricePnl += pnl;
    }

    /**
     * Комиссии полного круга пары, %: вход и выход на обеих биржах — 4 сделки тейкера
     * (2 × комиссия биржи шорта + 2 × комиссия биржи лонга).
     */
    double roundTripFeePercent(Venue shortLeg, Venue longLeg) {
        return 2 * takerFeePercent.applyAsDouble(shortLeg.gw().id()) + 2 * takerFeePercent.applyAsDouble(longLeg.gw().id());
    }

    /**
     * Хватает ли разницы ставок: она больше fundingArbMinDiffPercent и окупает комиссии полного круга
     * не больше чем за fundingArbPaybackPeriods периодов по 8 ч.
     */
    boolean enough(Opportunity o, GlobalParams g) {
        return o.diffPercent() >= g.fundingArbMinDiffPercent()
                && o.diffPercent() * g.fundingArbPaybackPeriods() >= roundTripFeePercent(o.shortLeg(), o.longLeg());
    }

    /**
     * Комиссия сделки для учёта результата. В бумажном режиме она уже заложена в цену исполнения
     * (движок ухудшает цену на комиссию), поэтому 0 — иначе комиссия считалась бы дважды.
     * На бирже цена исполнения без комиссии — считаем по takerFeePercent.
     */
    private double fee(Venue v, double price, double qty) {
        boolean live = v.gw() instanceof com.hft.exchange.generic.RequestStatsSource r && r.isLive();
        return live ? takerFeePercent.applyAsDouble(v.gw().id()) / 100 * price * qty : 0;
    }

    /** Торговля на бирже разрешена (kill switch не взведён, tradingEnabled=true). */
    private boolean tradable(Venue v) {
        return !v.gw().risk().isStopped() && tradingEnabled.test(v.gw().id());
    }

    /** Символ уже занят позицией другой стратегии на этой бирже — не трогаем. */
    private boolean busy(Venue v) {
        return Math.abs(v.orders().positions().qty(v.symbol())) > 1e-12;
    }

    /** Объём, который проходит по шагам обеих бирж (округление вниз до общего кратного). */
    static double commonQty(Venue a, Venue b, double qty) {
        double q = qty;
        for (int i = 0; i < 4; i++) {
            double qa = a.orders().filters().roundQuantity(a.symbol(), q);
            double qb = b.orders().filters().roundQuantity(b.symbol(), qa);
            if (Math.abs(qb - q) < 1e-12) break;
            q = qb;
        }
        return q;
    }

    /** Монета символа: базовая валюта (BTCUSDT и BTCUSDC -> BTC). */
    static String coin(String symbol) {
        try { return BalanceStore.baseAsset(symbol); } catch (IllegalArgumentException e) { return symbol; }
    }

    /** Ставка в процентах для логов. */
    private static String pct(double rate) { return String.format("%.4f", rate * 100); }

    /** Пары и возможности — для админки. */
    public Map<String, Object> stats() {
        GlobalParams g = params.get();
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("enabled", g.fundingArbEnabled());
        m.put("opened", opened.get());
        m.put("closed", closed.get());
        m.put("failed", failed.get());
        m.put("pricePnl", pricePnl);
        List<Map<String, Object>> ps = new ArrayList<>();
        for (Pair p : pairs.values()) {
            Map<String, Object> x = new LinkedHashMap<>();
            x.put("coin", p.coin());
            x.put("short", p.shortLeg().name());
            x.put("long", p.longLeg().name());
            x.put("qty", p.qty());
            x.put("entryDiffPercent", p.entryDiffPercent());
            x.put("openedAtMs", p.openedAtMs());
            ps.add(x);
        }
        m.put("pairs", ps);
        List<Map<String, Object>> os = new ArrayList<>();
        for (Opportunity o : lastOpportunities) {
            Map<String, Object> x = new LinkedHashMap<>();
            x.put("coin", o.coin());
            x.put("short", o.shortLeg().name());
            x.put("shortPer8hPercent", o.shortLeg().funding().ratePer8h() * 100);
            x.put("long", o.longLeg().name());
            x.put("longPer8hPercent", o.longLeg().funding().ratePer8h() * 100);
            x.put("diffPercent", o.diffPercent());
            x.put("aprPercent", o.diffPercent() * 3 * 365);
            double fees = roundTripFeePercent(o.shortLeg(), o.longLeg());
            x.put("roundTripFeePercent", fees);
            x.put("paybackPeriods", o.diffPercent() > 0 ? fees / o.diffPercent() : null);   // сколько 8-часовых периодов окупают комиссии
            x.put("enough", enough(o, g));
            os.add(x);
        }
        m.put("opportunities", os);
        return m;
    }

    /** Открытых пар. */
    public int openPairs() { return pairs.size(); }
}
