package com.hft.perp;

import com.hft.config.GlobalParams;
import com.hft.exchange.ExchangeGateway;
import com.hft.model.OrderEnums.Side;
import com.hft.model.OrderResult;
import com.hft.perp.FundingArbitrage.Venue;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;

/**
 * Ценовой арбитраж перпов между биржами (только market=perp).
 *
 * <p>Цена одного и того же перпа на разных биржах на короткое время расходится. Если лучший бид на бирже A
 * выше лучшего аска на бирже B, бот одновременно <b>продаёт на A</b> (шорт) и <b>покупает на B</b> (лонг).
 * Когда цены сходятся — обе позиции закрываются.
 *
 * <pre>
 * вход:   спред входа = (бид A − аск B) / середина, %
 * выход:  спред выхода = (аск A − бид B) / середина, % — сколько стоит закрыть пару сейчас
 * результат ≈ спред входа − спред выхода − 4 комиссии тейкера (по takerFeePercent обеих бирж)
 * </pre>
 *
 * <ul>
 *   <li>Вход: спред входа − {@code perpArbExitSpreadPercent} − комиссии круга ≥ {@code perpArbMinProfitPercent};
 *       оба стакана не старше {@code perpArbMaxBookAgeMs}; объём не больше {@code perpArbDepthUsage} лучших уровней.</li>
 *   <li>Обе ноги отправляются параллельно; если одна не исполнилась — вторая сразу закрывается.</li>
 *   <li>Выход: спред выхода ≤ {@code perpArbExitSpreadPercent} (цены сошлись), убыток пары по текущим ценам больше
 *       {@code perpArbStopLossPercent}, таймаут {@code perpArbMaxHoldMinutes}, остановка торговли на бирже.</li>
 * </ul>
 *
 * Пока пара открыта, по позициям начисляется funding (у шорта и лонга на разных биржах — разные ставки); обычно это
 * мелочь на фоне минут удержания. Монеты сопоставляются по базовой валюте (USDT- и USDC-перпы — одна монета).
 * Такие расхождения выбирают участники с меньшей задержкой: на ликвидных монетах сделок будет мало.
 */
public final class PerpPriceArbitrage {

    /** Логгер. */
    private static final Logger log = LoggerFactory.getLogger(PerpPriceArbitrage.class);

    /** Открытая пара: шорт на дорогой бирже, лонг на дешёвой. */
    record Pair(String coin, Venue shortLeg, Venue longLeg, double qty, double shortEntry, double longEntry,
                double entrySpreadPercent, long openedAtMs) {}

    /** Возможность: где продавать, где покупать, спред входа и ожидаемая прибыль после комиссий, %. */
    record Opportunity(String coin, Venue shortLeg, Venue longLeg, double spreadPercent, double feesPercent,
                       double expectedPercent, double maxQty, double mid) {}

    /** Настройки процесса. */
    private final Supplier<GlobalParams> params;
    /** Работающие биржи. */
    private final Supplier<Collection<ExchangeGateway>> gateways;
    /** Торговля на бирже включена. */
    private final Predicate<String> tradingEnabled;
    /** Комиссия тейкера биржи, %. */
    private final ToDoubleFunction<String> takerFeePercent;
    /** Открытые пары по монете. */
    private final Map<String, Pair> pairs = new ConcurrentHashMap<>();
    /** Последние возможности (для админки). */
    private volatile List<Opportunity> lastOpportunities = List.of();
    /** Цикл решений и параллельная отправка ног. */
    private ScheduledExecutorService scheduler;
    private final ExecutorService legs = Executors.newFixedThreadPool(2, r -> {
        Thread t = new Thread(r, "perp-arb-leg");
        t.setDaemon(true);
        return t;
    });
    /** Счётчики. */
    private final AtomicLong opened = new AtomicLong(), closed = new AtomicLong(), failed = new AtomicLong(), stops = new AtomicLong();
    /** Результат закрытых пар после комиссий. */
    private volatile double pnl;
    /** Буферы верха стакана (только поток цикла). */
    private final double[] ta = new double[4], tb = new double[4];

    /**
     * @param params настройки процесса
     * @param gateways работающие биржи
     * @param tradingEnabled включена ли торговля на бирже (по id)
     * @param takerFeePercent комиссия тейкера биржи, % (по id)
     */
    public PerpPriceArbitrage(Supplier<GlobalParams> params, Supplier<Collection<ExchangeGateway>> gateways,
                              Predicate<String> tradingEnabled, ToDoubleFunction<String> takerFeePercent) {
        this.params = params;
        this.gateways = gateways;
        this.tradingEnabled = tradingEnabled;
        this.takerFeePercent = takerFeePercent;
    }

    /** Запустить цикл (раз в perpArbCheckMs). */
    public synchronized void start() {
        if (scheduler != null) return;
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "perp-arb");
            t.setDaemon(true);
            return t;
        });
        long every = Math.max(50, params.get().perpArbCheckMs());
        scheduler.scheduleWithFixedDelay(() -> {
            try { tick(System.currentTimeMillis()); }
            catch (Exception e) { log.error("[perp-arb] ошибка цикла", e); }
        }, every, every, TimeUnit.MILLISECONDS);
    }

    /** Остановить цикл и закрыть открытые пары (биржи ещё работают). */
    public synchronized void stop() {
        ScheduledExecutorService s = scheduler;
        scheduler = null;
        if (s != null) {
            s.shutdown();
            try { s.awaitTermination(15, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        if (!pairs.isEmpty()) log.info("[perp-arb] остановка бота — закрываю {} пар", pairs.size());
        for (Pair p : List.copyOf(pairs.values())) close(p, "остановка бота");
        legs.shutdown();
    }

    /** Один цикл: выходы, затем входы. */
    public void tick(long now) {
        GlobalParams g = params.get();
        for (Pair p : List.copyOf(pairs.values())) {
            String why = exitReason(p, g, now);
            if (why != null) close(p, why);
        }
        List<Opportunity> ops = opportunities(g);
        lastOpportunities = ops;
        if (!g.perpArbEnabled()) return;
        for (Opportunity o : ops) {
            if (pairs.size() >= g.perpArbMaxPositions()) break;
            if (o.expectedPercent() < g.perpArbMinProfitPercent() || pairs.containsKey(o.coin())) continue;
            if (!tradable(o.shortLeg()) || !tradable(o.longLeg()) || busy(o.shortLeg()) || busy(o.longLeg())) continue;
            open(o, g);
        }
    }

    /** Комиссии полного круга пары, %: вход и выход на обеих биржах. */
    double roundTripFeePercent(Venue a, Venue b) {
        return 2 * takerFeePercent.applyAsDouble(a.gw().id()) + 2 * takerFeePercent.applyAsDouble(b.gw().id());
    }

    /** Для каждой монеты на ≥ 2 биржах — лучшая пара «продать на A, купить на B». По убыванию ожидаемой прибыли. */
    List<Opportunity> opportunities(GlobalParams g) {
        Set<String> filter = g.perpArbSymbols().isBlank() ? Set.of() : Set.of(g.perpArbSymbols().split(","));
        Map<String, List<Venue>> byCoin = new TreeMap<>();
        for (ExchangeGateway gw : gateways.get()) {
            if (gw.perp() == null) continue;
            for (String s : gw.marketData().symbols()) {
                String coin = FundingArbitrage.coin(s);
                if (!filter.isEmpty() && !filter.contains(s) && !filter.contains(coin)) continue;
                OrderBook b = gw.marketData().book(s);
                if (b == null || !b.isReady() || b.ageMs() > g.perpArbMaxBookAgeMs()) continue;   // старый стакан — не сравниваем
                byCoin.computeIfAbsent(coin, k -> new ArrayList<>()).add(new Venue(gw, s));
            }
        }
        List<Opportunity> out = new ArrayList<>();
        byCoin.forEach((coin, vs) -> {
            Opportunity best = null;
            for (Venue a : vs) for (Venue b : vs) {
                if (a.gw() == b.gw()) continue;
                if (!a.gw().marketData().book(a.symbol()).readTop(ta) || !b.gw().marketData().book(b.symbol()).readTop(tb)) continue;
                double bidA = ta[0], askB = tb[2];
                double mid = (ta[0] + ta[2] + tb[0] + tb[2]) / 4;
                double spread = (bidA - askB) / mid * 100;
                double fees = roundTripFeePercent(a, b);
                double expected = spread - g.perpArbExitSpreadPercent() - fees;
                double maxQty = Math.min(ta[1], tb[3]) * g.perpArbDepthUsage();
                if (best == null || expected > best.expectedPercent())
                    best = new Opportunity(coin, a, b, spread, fees, expected, maxQty, mid);
            }
            if (best != null) out.add(best);
        });
        out.sort((x, y) -> Double.compare(y.expectedPercent(), x.expectedPercent()));
        return out;
    }

    /** Спред выхода пары сейчас: (аск биржи шорта − бид биржи лонга) / середина, %. NaN — нет данных. */
    private double exitSpread(Pair p) {
        OrderBook a = p.shortLeg().gw().marketData().book(p.shortLeg().symbol());
        OrderBook b = p.longLeg().gw().marketData().book(p.longLeg().symbol());
        double[] x = new double[4], y = new double[4];                 // свои буферы: вызывается и из админки
        if (a == null || b == null || !a.readTop(x) || !b.readTop(y)) return Double.NaN;
        double mid = (x[0] + x[2] + y[0] + y[2]) / 4;
        return (x[2] - y[0]) / mid * 100;
    }

    /** Почему пару пора закрыть; null — держим. */
    String exitReason(Pair p, GlobalParams g, long now) {
        Collection<ExchangeGateway> active = gateways.get();
        if (!active.contains(p.shortLeg().gw()) || !active.contains(p.longLeg().gw())) return "биржа остановлена";
        if (p.shortLeg().gw().risk().isStopped() || p.longLeg().gw().risk().isStopped()) return "торговля на бирже остановлена";
        if (now - p.openedAtMs() > g.perpArbMaxHoldMinutes() * 60_000) return "таймаут";
        double exit = exitSpread(p);
        if (Double.isNaN(exit)) return null;
        if (exit <= g.perpArbExitSpreadPercent()) return String.format("цены сошлись (спред выхода %.4f%%)", exit);
        double unrealized = p.entrySpreadPercent() - exit;                   // результат пары по ценам, % (без комиссий)
        if (unrealized < -g.perpArbStopLossPercent()) { stops.incrementAndGet(); return String.format("стоп: расхождение выросло, %.4f%%", unrealized); }
        return null;
    }

    /** Открыть пару: обе ноги параллельно; одна не исполнилась — вторая закрывается. */
    private void open(Opportunity o, GlobalParams g) {
        double qty = Math.min(g.perpArbOrderQuote() / o.mid(), o.maxQty());
        qty = FundingArbitrage.commonQty(o.shortLeg(), o.longLeg(), qty);
        if (qty <= 0) return;
        final double q = qty;
        log.info("[perp-arb] {}: продаю {} / покупаю {}, спред {}%, ожидаемо {}% после комиссий, объём {}", o.coin(),
                o.shortLeg().name(), o.longLeg().name(), fmt(o.spreadPercent()), fmt(o.expectedPercent()), q);
        var fs = CompletableFuture.supplyAsync(() -> o.shortLeg().orders().sellMarket(o.shortLeg().symbol(), q), legs);
        var fl = CompletableFuture.supplyAsync(() -> o.longLeg().orders().buyMarket(o.longLeg().symbol(), q), legs);
        OrderResult s = fs.join(), l = fl.join();
        double sq = s.executedQty(), lq = l.executedQty();
        if (sq <= 0 || lq <= 0) {                                      // одна нога не встала — без хеджа не держим
            failed.incrementAndGet();
            if (sq > 0) unwind(o.shortLeg(), Side.BUY, sq, s.avgPrice(), true);
            if (lq > 0) unwind(o.longLeg(), Side.SELL, lq, l.avgPrice(), false);
            log.warn("[perp-arb] {}: нога не исполнилась (шорт {}, лонг {}) — откатил", o.coin(), sq, lq);
            return;
        }
        double common = Math.min(sq, lq);                               // объёмы разошлись — лишнее закрыть
        if (sq - common > 1e-12) unwind(o.shortLeg(), Side.BUY, sq - common, s.avgPrice(), true);
        if (lq - common > 1e-12) unwind(o.longLeg(), Side.SELL, lq - common, l.avgPrice(), false);
        double mid = (s.avgPrice() + l.avgPrice()) / 2;
        pairs.put(o.coin(), new Pair(o.coin(), o.shortLeg(), o.longLeg(), common, s.avgPrice(), l.avgPrice(),
                (s.avgPrice() - l.avgPrice()) / mid * 100, System.currentTimeMillis()));
        opened.incrementAndGet();
    }

    /** Закрыть часть одной ноги и учесть результат с комиссиями. */
    private void unwind(Venue v, Side side, double qty, double entry, boolean wasShort) {
        OrderResult u = v.orders().reduceMarket(v.symbol(), side, qty);
        if (u.executedQty() <= 0) return;
        double r = (wasShort ? entry - u.avgPrice() : u.avgPrice() - entry) * u.executedQty()
                - fee(v, entry, u.executedQty()) - fee(v, u.avgPrice(), u.executedQty());
        v.gw().risk().recordPnl(r);
        pnl += r;
    }

    /** Закрыть обе ноги параллельно reduceOnly-ордерами; результат после комиссий — в дневной PnL бирж. */
    private void close(Pair p, String why) {
        log.info("[perp-arb] {}: закрываю ({})", p.coin(), why);
        var fs = CompletableFuture.supplyAsync(() -> p.shortLeg().orders().reduceMarket(p.shortLeg().symbol(), Side.BUY, p.qty()), legs);
        var fl = CompletableFuture.supplyAsync(() -> p.longLeg().orders().reduceMarket(p.longLeg().symbol(), Side.SELL, p.qty()), legs);
        OrderResult s = fs.join(), l = fl.join();
        double r = 0;
        if (s.executedQty() > 0) {
            double x = (p.shortEntry() - s.avgPrice()) * s.executedQty()
                    - fee(p.shortLeg(), p.shortEntry(), s.executedQty()) - fee(p.shortLeg(), s.avgPrice(), s.executedQty());
            p.shortLeg().gw().risk().recordPnl(x);
            r += x;
        }
        if (l.executedQty() > 0) {
            double x = (l.avgPrice() - p.longEntry()) * l.executedQty()
                    - fee(p.longLeg(), p.longEntry(), l.executedQty()) - fee(p.longLeg(), l.avgPrice(), l.executedQty());
            p.longLeg().gw().risk().recordPnl(x);
            r += x;
        }
        pnl += r;
        double leftS = p.qty() - s.executedQty(), leftL = p.qty() - l.executedQty();
        if (leftS > p.qty() * 1e-6 || leftL > p.qty() * 1e-6) {
            log.error("[perp-arb] {}: не всё закрылось (шорт {}, лонг {}) — повторю на следующем цикле", p.coin(), leftS, leftL);
            pairs.put(p.coin(), new Pair(p.coin(), p.shortLeg(), p.longLeg(), Math.max(leftS, leftL), p.shortEntry(), p.longEntry(),
                    p.entrySpreadPercent(), p.openedAtMs()));
        } else {
            pairs.remove(p.coin());
            closed.incrementAndGet();
        }
    }

    /** Комиссия сделки для результата: на бирже — по takerFeePercent, в бумаге уже в цене исполнения. */
    private double fee(Venue v, double price, double qty) {
        boolean live = v.gw() instanceof com.hft.exchange.generic.RequestStatsSource r && r.isLive();
        return live ? takerFeePercent.applyAsDouble(v.gw().id()) / 100 * price * qty : 0;
    }

    /** Торговля на бирже разрешена. */
    private boolean tradable(Venue v) { return !v.gw().risk().isStopped() && tradingEnabled.test(v.gw().id()); }

    /** На символе уже есть позиция (другой стратегии или пары) — не трогаем. */
    private static boolean busy(Venue v) { return Math.abs(v.orders().positions().qty(v.symbol())) > 1e-12; }

    /** Число для логов. */
    private static String fmt(double v) { return String.format("%.4f", v); }

    /** Пары и возможности — для админки. */
    public Map<String, Object> stats() {
        GlobalParams g = params.get();
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("enabled", g.perpArbEnabled());
        m.put("opened", opened.get());
        m.put("closed", closed.get());
        m.put("failed", failed.get());
        m.put("stops", stops.get());
        m.put("pnl", pnl);
        List<Map<String, Object>> ps = new ArrayList<>();
        for (Pair p : pairs.values()) {
            Map<String, Object> x = new LinkedHashMap<>();
            x.put("coin", p.coin());
            x.put("short", p.shortLeg().name());
            x.put("long", p.longLeg().name());
            x.put("qty", p.qty());
            x.put("entrySpreadPercent", p.entrySpreadPercent());
            double e = exitSpread(p);
            x.put("exitSpreadPercent", Double.isNaN(e) ? null : e);
            x.put("openedAtMs", p.openedAtMs());
            ps.add(x);
        }
        m.put("pairs", ps);
        List<Map<String, Object>> os = new ArrayList<>();
        for (Opportunity o : lastOpportunities) {
            Map<String, Object> x = new LinkedHashMap<>();
            x.put("coin", o.coin());
            x.put("sell", o.shortLeg().name());
            x.put("buy", o.longLeg().name());
            x.put("spreadPercent", o.spreadPercent());
            x.put("roundTripFeePercent", o.feesPercent());
            x.put("expectedPercent", o.expectedPercent());
            x.put("enough", o.expectedPercent() >= g.perpArbMinProfitPercent());
            os.add(x);
        }
        m.put("opportunities", os);
        return m;
    }

    /** Открытых пар. */
    public int openPairs() { return pairs.size(); }
}
