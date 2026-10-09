package com.hft.strategy;

import com.hft.config.GlobalParams;
import com.hft.exchange.ExchangeGateway;
import com.hft.model.OrderEnums.Side;
import com.hft.model.OrderResult;
import com.hft.strategy.FundingArbitrageStrategy.Venue;
import com.hft.store.BalanceStore;
import com.hft.store.FundingStore.Funding;
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
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;

/**
 * Cash-and-carry: <b>лонг спота</b> на одной бирже и <b>шорт перпа</b> на той же монете на другой — при
 * положительной ставке funding шорт получает выплаты, а цена монеты на результат почти не влияет
 * (спот и перп двигаются вместе).
 *
 * <p>У каждой биржи в боте один рынок (параметр market), поэтому спот и перп берутся с разных бирж:
 * например, BTCUSDT на Gate (spot) и BTCUSDT на Binance (perp). Монеты сопоставляются по базовой валюте.
 *
 * <ul>
 *   <li>Вход: ставка перпа за 8 ч ≥ {@code carryMinRatePercent} и окупает комиссии полного круга
 *       (2 сделки спота + 2 перпа по takerFeePercent бирж) за {@code carryPaybackPeriods} периодов;
 *       цены спота и перпа отличаются не больше {@code carryMaxBasisPercent}.</li>
 *   <li>Сначала покупается спот, затем шортится перп на полученный объём (за вычетом комиссии, которую биржа
 *       берёт монетой); не встал шорт — спот продаётся обратно.</li>
 *   <li>Выход: ставка упала ниже {@code carryExitRatePercent}, таймаут {@code carryMaxHoldHours}, остановка
 *       торговли — продаётся спот, закрывается шорт.</li>
 * </ul>
 *
 * Нужны деньги на обеих биржах: на споте — вся сумма покупки, на перпе — маржа шорта (сумма / плечо).
 */
public final class FundingCarryStrategy {

    /** Логгер. */
    private static final Logger log = LoggerFactory.getLogger(FundingCarryStrategy.class);

    /** Открытая позиция: спот куплен, перп зашорчен. */
    record Carry(String coin, Venue spot, Venue perp, double spotQty, double perpQty, double spotEntry, double perpEntry,
                 double entryRatePercent, long openedAtMs) {}

    /** Возможность: спот, перп, ставка за 8 ч, комиссии круга и базис, %. */
    record Opportunity(String coin, Venue spot, Venue perp, double ratePercent, double feesPercent, double basisPercent) {}

    /** Настройки процесса. */
    private final Supplier<GlobalParams> params;
    /** Работающие биржи. */
    private final Supplier<Collection<ExchangeGateway>> gateways;
    /** Торговля на бирже включена. */
    private final Predicate<String> tradingEnabled;
    /** Комиссия тейкера биржи, %. */
    private final ToDoubleFunction<String> takerFeePercent;
    /** Открытые позиции по монете. */
    private final Map<String, Carry> carries = new ConcurrentHashMap<>();
    /** Последние возможности. */
    private volatile List<Opportunity> lastOpportunities = List.of();
    /** Цикл решений. */
    private ScheduledExecutorService scheduler;
    /** Счётчики. */
    private final AtomicLong opened = new AtomicLong(), closed = new AtomicLong(), failed = new AtomicLong();
    /** Результат закрытых позиций по ценам после комиссий (без funding). */
    private volatile double pricePnl;

    /**
     * @param params настройки процесса
     * @param gateways работающие биржи
     * @param tradingEnabled включена ли торговля на бирже (по id)
     * @param takerFeePercent комиссия тейкера биржи, % (по id)
     */
    public FundingCarryStrategy(Supplier<GlobalParams> params, Supplier<Collection<ExchangeGateway>> gateways,
                        Predicate<String> tradingEnabled, ToDoubleFunction<String> takerFeePercent) {
        this.params = params;
        this.gateways = gateways;
        this.tradingEnabled = tradingEnabled;
        this.takerFeePercent = takerFeePercent;
    }

    /** Запустить цикл (раз в 5 с). */
    public synchronized void start() {
        if (scheduler != null) return;
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "funding-carry");
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(() -> {
            try { tick(System.currentTimeMillis()); }
            catch (Exception e) { log.error("[carry] ошибка цикла", e); }
        }, 5, 5, TimeUnit.SECONDS);
    }

    /** Остановить цикл и закрыть позиции (биржи ещё работают). */
    public synchronized void stop() {
        ScheduledExecutorService s = scheduler;
        scheduler = null;
        if (s != null) {
            s.shutdown();
            try { s.awaitTermination(15, TimeUnit.SECONDS); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
        }
        if (!carries.isEmpty()) log.info("[carry] остановка бота — закрываю {} позиций", carries.size());
        for (Carry c : List.copyOf(carries.values())) close(c, "остановка бота");
    }

    /** Один цикл: выходы, затем входы. */
    public void tick(long now) {
        GlobalParams g = params.get();
        for (Carry c : List.copyOf(carries.values())) {
            String why = exitReason(c, g, now);
            if (why != null) close(c, why);
        }
        List<Opportunity> ops = opportunities(g, now);
        lastOpportunities = ops;
        if (!g.carryEnabled()) return;
        for (Opportunity o : ops) {
            if (carries.size() >= g.carryMaxPositions()) break;
            if (carries.containsKey(o.coin()) || !enough(o, g) || Math.abs(o.basisPercent()) > g.carryMaxBasisPercent()) continue;
            if (!tradable(o.spot()) || !tradable(o.perp()) || busyPerp(o.perp())) continue;
            open(o, g);
        }
    }

    /** Комиссии полного круга, %: покупка и продажа спота + открытие и закрытие шорта. */
    double roundTripFeePercent(Venue spot, Venue perp) {
        return 2 * takerFeePercent.applyAsDouble(spot.gw().id()) + 2 * takerFeePercent.applyAsDouble(perp.gw().id());
    }

    /** Ставка достаточна и окупает комиссии за carryPaybackPeriods периодов по 8 ч. */
    boolean enough(Opportunity o, GlobalParams g) {
        return o.ratePercent() >= g.carryMinRatePercent() && o.ratePercent() * g.carryPaybackPeriods() >= o.feesPercent();
    }

    /** Для каждой монеты: перп с наибольшей ставкой и любой спот той же монеты на другой бирже. */
    List<Opportunity> opportunities(GlobalParams g, long now) {
        Set<String> filter = g.carrySymbols().isBlank() ? Set.of() : Set.of(g.carrySymbols().split(","));
        long maxAge = Math.max(120_000, 4_000L * g.fundingPollSec());
        Map<String, Venue> bestPerp = new TreeMap<>();
        Map<String, Venue> spot = new TreeMap<>();
        for (ExchangeGateway gw : gateways.get()) {
            for (String s : gw.marketData().symbols()) {
                String coin = FundingArbitrageStrategy.coin(s);
                if (!filter.isEmpty() && !filter.contains(s) && !filter.contains(coin)) continue;
                Venue v = new Venue(gw, s);
                if (gw.perp() == null) {
                    if (!(v.mid() > 0)) continue;
                    spot.putIfAbsent(coin, v);
                } else {
                    Funding f = v.funding();
                    if (f == null || now - f.updatedMs() > maxAge || !(v.mid() > 0)) continue;
                    Venue cur = bestPerp.get(coin);
                    if (cur == null || f.ratePer8h() > cur.funding().ratePer8h()) bestPerp.put(coin, v);
                }
            }
        }
        List<Opportunity> out = new ArrayList<>();
        bestPerp.forEach((coin, p) -> {
            Venue s = spot.get(coin);
            if (s == null || s.gw() == p.gw()) return;
            double ms = s.mid(), mp = p.mid();
            out.add(new Opportunity(coin, s, p, p.funding().ratePer8h() * 100, roundTripFeePercent(s, p), (mp - ms) / ms * 100));
        });
        out.sort((a, b) -> Double.compare(b.ratePercent(), a.ratePercent()));
        return out;
    }

    /** Почему пора закрыть; null — держим. */
    String exitReason(Carry c, GlobalParams g, long now) {
        Collection<ExchangeGateway> active = gateways.get();
        if (!active.contains(c.spot().gw()) || !active.contains(c.perp().gw())) return "биржа остановлена";
        if (c.spot().gw().risk().isStopped() || c.perp().gw().risk().isStopped()) return "торговля на бирже остановлена";
        if (now - c.openedAtMs() > g.carryMaxHoldHours() * 3_600_000) return "таймаут";
        Funding f = c.perp().funding();
        if (f != null && f.ratePer8h() * 100 < g.carryExitRatePercent())
            return String.format("ставка упала до %.4f%%", f.ratePer8h() * 100);
        return null;
    }

    /** Купить спот, затем зашортить перп на полученный объём; шорт не встал — продать спот обратно. */
    private void open(Opportunity o, GlobalParams g) {
        double qty = o.spot().orders().filters().roundQuantity(o.spot().symbol(), g.carryOrderQuote() / o.spot().mid());
        if (qty <= 0 || !Venue.claimBoth(o.spot(), o.perp(), OWNER)) return;
        boolean ok = false;
        try { ok = openClaimed(o, qty); }
        finally { if (!ok) { o.spot().release(OWNER); o.perp().release(OWNER); } }
    }

    /** Имя владельца позиций в OrderService. */
    private static final String OWNER = "carry";

    /** Открыть (ноги уже взяты под управление); true — позиция открыта. */
    private boolean openClaimed(Opportunity o, double qty) {
        log.info("[carry] {}: покупаю спот {} и шорчу перп {} (ставка {}%/8ч, комиссии круга {}%)", o.coin(),
                o.spot().name(), o.perp().name(), fmt(o.ratePercent()), fmt(o.feesPercent()));
        OrderResult b = o.spot().orders().buyMarket(o.spot().symbol(), qty);
        if (b.executedQty() <= 0) { failed.incrementAndGet(); return false; }
        // на бирже комиссия покупки списывается монетой — на руках чуть меньше купленного
        double held = o.spot().orders().feesInPrice() ? b.executedQty()
                : b.executedQty() * (1 - takerFeePercent.applyAsDouble(o.spot().gw().id()) / 100);
        double perpQty = o.perp().orders().filters().roundQuantity(o.perp().symbol(), held);
        OrderResult s = perpQty > 0 ? o.perp().orders().sellMarket(o.perp().symbol(), perpQty) : null;
        if (s == null || s.executedQty() <= 0) {
            failed.incrementAndGet();
            log.warn("[carry] {}: шорт {} не исполнился — продаю спот обратно", o.coin(), o.perp().name());
            OrderResult back = o.spot().orders().sellMarket(o.spot().symbol(), spotSellable(o.spot(), held));
            if (back.executedQty() > 0) {
                double r = (back.avgPrice() - b.avgPrice()) * back.executedQty()
                        - fee(o.spot(), b.avgPrice(), b.executedQty()) - fee(o.spot(), back.avgPrice(), back.executedQty());
                o.spot().gw().risk().recordPnl(r);
                pricePnl += r;
            }
            return false;
        }
        carries.put(o.coin(), new Carry(o.coin(), o.spot(), o.perp(), held, s.executedQty(), b.avgPrice(), s.avgPrice(),
                o.ratePercent(), System.currentTimeMillis()));
        opened.incrementAndGet();
        return true;
    }

    /** Сколько спота можно продать: не больше купленного и не больше свободного остатка. */
    private static double spotSellable(Venue v, double qty) {
        double free = v.orders().balances().free(BalanceStore.baseAsset(v.symbol()));
        return v.orders().filters().roundQuantity(v.symbol(), Math.min(qty, free > 0 ? free : qty));
    }

    /** Закрыть: закрыть шорт (reduceOnly) и продать спот; результат после комиссий — в дневной PnL бирж. */
    private void close(Carry c, String why) {
        log.info("[carry] {}: закрываю ({})", c.coin(), why);
        double pq = Math.min(c.perpQty(), c.perp().shortQty());   // шорт могли ликвидировать или закрыть стопом на бирже
        OrderResult s = pq > 0 ? c.perp().orders().reduceMarket(c.perp().symbol(), Side.BUY, pq) : FundingArbitrageStrategy.none(c.perp().symbol(), Side.BUY);
        double sellQty = spotSellable(c.spot(), c.spotQty());
        OrderResult b = sellQty > 0 ? c.spot().orders().sellMarket(c.spot().symbol(), sellQty) : null;
        double r = 0;
        if (s.executedQty() > 0) {
            double x = (c.perpEntry() - s.avgPrice()) * s.executedQty()
                    - fee(c.perp(), c.perpEntry(), s.executedQty()) - fee(c.perp(), s.avgPrice(), s.executedQty());
            c.perp().gw().risk().recordPnl(x);
            r += x;
        }
        if (b != null && b.executedQty() > 0) {
            double x = (b.avgPrice() - c.spotEntry()) * b.executedQty()
                    - fee(c.spot(), c.spotEntry(), b.executedQty()) - fee(c.spot(), b.avgPrice(), b.executedQty());
            c.spot().gw().risk().recordPnl(x);
            r += x;
        }
        pricePnl += r;
        double leftPerp = Math.min(c.perpQty(), c.perp().shortQty());
        double leftSpot = c.spotQty() - (b == null ? 0 : b.executedQty());
        boolean spotDust = c.spot().orders().filters().roundQuantity(c.spot().symbol(), leftSpot) <= 0;   // остаток меньше шага — не продать
        if (leftPerp > c.perpQty() * 1e-6 || !spotDust) {
            log.error("[carry] {}: не всё закрылось (перп {}, спот {}) — повторю на следующем цикле", c.coin(), leftPerp, leftSpot);
            carries.put(c.coin(), new Carry(c.coin(), c.spot(), c.perp(), Math.max(0, leftSpot), Math.max(0, leftPerp),
                    c.spotEntry(), c.perpEntry(), c.entryRatePercent(), c.openedAtMs()));
        } else {
            carries.remove(c.coin());
            c.spot().release(OWNER);
            c.perp().release(OWNER);
            closed.incrementAndGet();
        }
    }

    /** Комиссия сделки для результата: на бирже — по takerFeePercent, в бумаге уже в цене. */
    private double fee(Venue v, double price, double qty) {
        boolean live = v.gw() instanceof com.hft.exchange.generic.RequestStatsSource r && r.isLive();
        return live ? takerFeePercent.applyAsDouble(v.gw().id()) / 100 * price * qty : 0;
    }

    /** Торговля на бирже разрешена. */
    private boolean tradable(Venue v) { return !v.gw().risk().isStopped() && tradingEnabled.test(v.gw().id()); }

    /** На перпе уже есть позиция другой стратегии. */
    private static boolean busyPerp(Venue v) { return Math.abs(v.orders().positions().qty(v.symbol())) > 1e-12; }

    /** Число для логов. */
    private static String fmt(double v) { return String.format("%.4f", v); }

    /** Позиции и возможности — для админки. */
    public Map<String, Object> stats() {
        GlobalParams g = params.get();
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("enabled", g.carryEnabled());
        m.put("opened", opened.get());
        m.put("closed", closed.get());
        m.put("failed", failed.get());
        m.put("pricePnl", pricePnl);
        List<Map<String, Object>> cs = new ArrayList<>();
        for (Carry c : carries.values()) {
            Map<String, Object> x = new LinkedHashMap<>();
            x.put("coin", c.coin());
            x.put("spot", c.spot().name());
            x.put("perp", c.perp().name());
            x.put("spotQty", c.spotQty());
            x.put("perpQty", c.perpQty());
            x.put("entryRatePercent", c.entryRatePercent());
            x.put("openedAtMs", c.openedAtMs());
            cs.add(x);
        }
        m.put("positions", cs);
        List<Map<String, Object>> os = new ArrayList<>();
        for (Opportunity o : lastOpportunities) {
            Map<String, Object> x = new LinkedHashMap<>();
            x.put("coin", o.coin());
            x.put("spot", o.spot().name());
            x.put("perp", o.perp().name());
            x.put("per8hPercent", o.ratePercent());
            x.put("aprPercent", o.ratePercent() * 3 * 365);
            x.put("roundTripFeePercent", o.feesPercent());
            x.put("paybackPeriods", o.ratePercent() > 0 ? o.feesPercent() / o.ratePercent() : null);
            x.put("basisPercent", o.basisPercent());
            x.put("enough", enough(o, g) && Math.abs(o.basisPercent()) <= g.carryMaxBasisPercent());
            os.add(x);
        }
        m.put("opportunities", os);
        return m;
    }

    /** Открытых позиций. */
    public int openPositions() { return carries.size(); }
}
