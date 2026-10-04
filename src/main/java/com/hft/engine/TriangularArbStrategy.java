package com.hft.engine;

import com.hft.config.TradingParams;
import com.hft.config.TradingSettings;
import com.hft.model.OrderResult;
import com.hft.model.Tick;
import com.hft.store.BalanceStore;
import com.hft.store.MarketDataStore;
import com.hft.store.OrderBook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

/**
 * Треугольный арбитраж внутри одной биржи.
 *
 * Из выбранных символов строятся треугольники вокруг базовой валюты H (triHomeAsset, обычно USDT):
 * H → A → B → H, например USDT → BTC (BTCUSDT) → ETH (ETHBTC) → USDT (ETHUSDT), и тот же круг в обратную
 * сторону. Чтобы бот видел треугольник, в выборе должны быть все три символа.
 *
 * На каждом тике по символу треугольника круг пересчитывается по лучшим ценам стаканов: покупка по ask,
 * продажа по bid, на каждой ноге — комиссия тейкера (takerFeePercent). Размер ограничен triOrderQuote и
 * половиной объёма на лучших уровнях всех трёх стаканов. Если чистая прибыль ≥ triMinProfitPercent —
 * круг исполняется тремя рыночными ордерами подряд вне потока конвейера.
 *
 * Риски: ноги идут последовательно (между ними цена может уйти), а не атомарно; если нога не исполнилась,
 * остаток при triUnwindOnFail продаётся обратно в H. Результат круга (после комиссий) идёт в дневной PnL.
 */
public final class TriangularArbStrategy extends Strategy {

    private static final Logger log = LoggerFactory.getLogger(TriangularArbStrategy.class);

    /** Нога: символ и направление (buy — получаем базовую валюту символа за котируемую). */
    record Leg(String symbol, boolean buy, String from, String to) {}

    /** Круг из трёх ног; name — для логов и cooldown. */
    record Cycle(String name, Leg[] legs) {}

    /** Результат оценки круга по текущим стаканам. */
    record Quote(double profitPct, double maxStart) {}

    private final TradingSettings settings;
    private final OrderExecutor executor;
    private volatile BooleanSupplier realtime = () -> true;

    private volatile String builtFor = null;
    private volatile List<Cycle> cycles = List.of();
    private volatile Map<String, List<Cycle>> bySymbol = Map.of();
    private final Map<String, Long> lastRun = new HashMap<>();   // только поток конвейера
    private final double[] top = new double[4];                  // только поток конвейера

    private final AtomicLong opportunities = new AtomicLong(), executed = new AtomicLong(), failed = new AtomicLong();
    private volatile double lastProfitPct = Double.NaN, totalPnl;

    public TriangularArbStrategy(MarketDataStore market, OrderService orders, String exchangeId, TradingSettings settings) {
        super("triangular-arb", market, orders);
        this.settings = settings;
        this.executor = new OrderExecutor(exchangeId + "-tri", 1);
    }

    public void setRealtimeSource(BooleanSupplier s) { this.realtime = s; }

    // ------------------------------------------------------------ построение треугольников

    /** Все треугольники H → A → B → H среди символов (обе стороны). */
    static List<Cycle> buildCycles(Set<String> symbols, String home) {
        Map<String, String[]> pairs = new LinkedHashMap<>();              // символ -> {base, quote}
        for (String s : symbols) {
            try { pairs.put(s, BalanceStore.splitSymbol(s)); }
            catch (IllegalArgumentException e) { /* котируемая валюта не из списка — в треугольники не входит */ }
        }
        List<Cycle> out = new ArrayList<>();
        for (var e1 : pairs.entrySet()) {
            String a = other(e1.getValue(), home);
            if (a == null) continue;                                      // первая нога должна касаться H
            for (var e2 : pairs.entrySet()) {
                if (e2.getKey().equals(e1.getKey())) continue;
                String b = other(e2.getValue(), a);
                if (b == null || b.equals(home)) continue;                // вторая нога: A ↔ B
                for (var e3 : pairs.entrySet()) {
                    if (e3.getKey().equals(e1.getKey()) || e3.getKey().equals(e2.getKey())) continue;
                    String back = other(e3.getValue(), b);
                    if (!home.equals(back)) continue;                     // третья нога: B ↔ H
                    Leg[] legs = {leg(e1.getKey(), e1.getValue(), home), leg(e2.getKey(), e2.getValue(), a), leg(e3.getKey(), e3.getValue(), b)};
                    out.add(new Cycle(home + "→" + a + "→" + b + "→" + home, legs));
                }
            }
        }
        return out;
    }

    private static String other(String[] bq, String cur) {
        return bq[0].equals(cur) ? bq[1] : bq[1].equals(cur) ? bq[0] : null;
    }

    private static Leg leg(String symbol, String[] bq, String from) {
        boolean buy = bq[1].equals(from);                                 // отдаём котируемую — покупаем базовую
        return new Leg(symbol, buy, from, buy ? bq[0] : bq[1]);
    }

    private void ensureBuilt(String home) {
        if (home.equals(builtFor)) return;
        List<Cycle> cs = buildCycles(market.symbols(), home);
        Map<String, List<Cycle>> idx = new HashMap<>();
        for (Cycle c : cs) for (Leg l : c.legs()) idx.computeIfAbsent(l.symbol(), k -> new ArrayList<>()).add(c);
        cycles = cs;
        bySymbol = idx;
        builtFor = home;
        log.info("[{}] треугольников вокруг {}: {} {}", name(), home, cs.size(), cs.stream().map(Cycle::name).toList());
    }

    // ------------------------------------------------------------ оценка

    /** Прибыль круга (%) по лучшим ценам с комиссией и наибольший старт в H, который выдерживают лучшие уровни. */
    Quote evaluate(Cycle c, double feePct, long maxAgeMs) {
        double f = 1 - feePct / 100.0;
        double amount = 1.0, maxStart = Double.MAX_VALUE;
        for (Leg l : c.legs()) {
            OrderBook b = market.book(l.symbol());
            if (b == null || !b.isReady() || b.ageMs() > maxAgeMs || !b.readTop(top)) return null;
            double bid = top[0], bidQty = top[1], ask = top[2], askQty = top[3];
            if (!(bid > 0) || !(ask > 0)) return null;
            double capacityFrom = l.buy() ? askQty * ask : bidQty;           // сколько «from» примет лучший уровень
            maxStart = Math.min(maxStart, capacityFrom / amount);
            amount = l.buy() ? amount / ask * f : amount * bid * f;
        }
        return new Quote((amount - 1) * 100, maxStart);
    }

    @Override
    protected void onTick(Tick tick) {
        TradingParams p = settings.get();
        if (!p.triangularEnabled()) return;
        ensureBuilt(p.triHomeAsset());
        List<Cycle> touched = bySymbol.get(tick.symbol());
        if (touched == null || executor.isBusy("tri") || !realtime.getAsBoolean()) return;
        long now = System.currentTimeMillis();
        for (Cycle c : touched) {
            Quote q = evaluate(c, p.takerFeePercent(), p.triMaxBookAgeMs());
            if (q == null || q.profitPct() < p.triMinProfitPercent()) continue;
            Long last = lastRun.get(c.name());
            if (last != null && now - last < p.triCooldownMs()) continue;
            double start = Math.min(p.triOrderQuote(), q.maxStart() * 0.5);
            if (start < p.triOrderQuote() * 0.1) continue;               // лучшие уровни слишком тонкие
            opportunities.incrementAndGet();
            lastRun.put(c.name(), now);
            log.info("[{}] круг {}: ожидаемая прибыль {}% на {} {}", name(), c.name(),
                    String.format("%.3f", q.profitPct()), String.format("%.2f", start), p.triHomeAsset());
            executor.submit("tri", () -> run(c, start, p));
            return;                                                       // один круг за раз
        }
    }

    // ------------------------------------------------------------ исполнение

    private void run(Cycle c, double start, TradingParams p) {
        double f = 1 - p.takerFeePercent() / 100.0;
        double amount = start;
        for (int i = 0; i < 3; i++) {
            Leg l = c.legs()[i];
            BalanceStore bal = orders.balances();
            double before = bal.free(l.to());
            OrderResult r;
            if (l.buy()) {
                OrderBook b = market.book(l.symbol());
                double ask = b == null ? Double.NaN : b.bestAsk();
                if (!(ask > 0)) { abort(c, i, amount, p, "нет цены " + l.symbol()); return; }
                r = orders.buyMarket(l.symbol(), amount / ask);
            } else {
                r = orders.sellMarket(l.symbol(), amount);
            }
            // не исполнилось — на руках по-прежнему «from» этой ноги в количестве amount
            if (r.executedQty() <= 0) { abort(c, i, amount, p, "нога " + l.symbol() + ": " + r.status()); return; }
            // сколько реально пришло — по балансу (комиссия у бирж списывается по-разному);
            // если баланс не обновился, оценка по цене исполнения с комиссией тейкера
            double received = bal.free(l.to()) - before;
            double estimate = l.buy() ? r.executedQty() * f : r.executedQty() * r.avgPrice() * f;
            amount = received > 0 && received <= estimate * 1.01 ? received : estimate;
        }
        double pnl = amount - start;
        orders.risk().recordPnl(pnl);
        executed.incrementAndGet();
        lastProfitPct = pnl / start * 100;
        totalPnl += pnl;
        log.info("[{}] круг {} завершён: {} → {} {} ({}%)", name(), c.name(), String.format("%.4f", start),
                String.format("%.4f", amount), p.triHomeAsset(), String.format("%.3f", lastProfitPct));
    }

    /** Нога не прошла: если на руках промежуточная валюта — продать её обратно в H. */
    private void abort(Cycle c, int legIndex, double amountHeld, TradingParams p, String why) {
        failed.incrementAndGet();
        log.warn("[{}] круг {} прерван на ноге {}: {}", name(), c.name(), legIndex + 1, why);
        if (legIndex == 0 || amountHeld <= 0 || !p.triUnwindOnFail()) return;
        String held = c.legs()[legIndex].from();
        String home = p.triHomeAsset();
        for (String s : market.symbols()) {
            if (s.equals(held + home)) {
                OrderResult r = orders.sellMarket(s, amountHeld);
                log.warn("[{}] остаток {} {} продан обратно в {}: {}", name(), String.format("%.8f", amountHeld), held, home, r.status());
                return;
            }
        }
        log.error("[{}] остаток {} {} не вернуть в {}: нет пары {}{} в выборе", name(), amountHeld, held, home, held, home);
    }

    public Map<String, Object> stats() {
        Map<String, Object> m = new LinkedHashMap<>();
        // до первого тика треугольники ещё не построены — показываем, какие будут
        List<Cycle> cs = builtFor != null ? cycles : buildCycles(market.symbols(), settings.get().triHomeAsset());
        m.put("enabled", settings.get().triangularEnabled());
        m.put("triangles", cs.stream().map(Cycle::name).toList());
        m.put("opportunities", opportunities.get());
        m.put("executed", executed.get());
        m.put("failed", failed.get());
        m.put("lastProfitPct", Double.isNaN(lastProfitPct) ? null : lastProfitPct);
        m.put("totalPnl", totalPnl);
        return m;
    }

    public long opportunities() { return opportunities.get(); }
    public long executedCount() { return executed.get(); }
    public long failedCount() { return failed.get(); }
    public double totalPnl() { return totalPnl; }
    public int triangles() { return cycles.size(); }

    public boolean drain(long ms) { return executor.drain(ms); }
}
