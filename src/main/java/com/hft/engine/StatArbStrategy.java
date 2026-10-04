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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;

/**
 * Статистический арбитраж (торговля парами) внутри одной биржи.
 *
 * Для пары A/B раз в statArbSampleMs берутся середины стаканов, в окне из statArbWindow отсчётов
 * оценивается коэффициент хеджирования β = cov(ln A, ln B) / var(ln B) и спред s = ln A − β·ln B.
 * z = (s − среднее) / σ показывает, насколько A сейчас дёшев или дорог относительно B.
 * Пары берутся только с корреляцией доходностей ≥ statArbMinCorrelation.
 *
 * Спот без шорта, поэтому торгуется дешёвая нога: z ≤ −entry — A дёшев, покупаем A; z ≥ entry — B дёшев,
 * покупаем B. Выход: спред вернулся (|z| ≤ exit), разошёлся ещё сильнее (|z| ≥ stop — стоп) или вышло
 * время statArbMaxHoldMs. Полноценная рыночно-нейтральная версия (шорт дорогой ноги) требует фьючерсов
 * или маржи и здесь не реализована: открытая позиция несёт риск движения рынка в целом.
 */
public final class StatArbStrategy extends Strategy {

    private static final Logger log = LoggerFactory.getLogger(StatArbStrategy.class);

    /** Окно отсчётов одной пары и её позиция. Отсчёты пишет только поток конвейера. */
    static final class Pair {
        final String a, b;
        final double[] la, lb;
        int n, head;
        volatile double z = Double.NaN, beta = Double.NaN, corr = Double.NaN;
        Pair(String a, String b, int window) { this.a = a; this.b = b; la = new double[window]; lb = new double[window]; }
        String name() { return a + "/" + b; }
        void add(double pa, double pb) {
            la[head] = Math.log(pa); lb[head] = Math.log(pb);
            head = (head + 1) % la.length;
            if (n < la.length) n++;
        }
        /** Пересчитать β, z и корреляцию доходностей по окну; false — окно ещё не заполнено или σ = 0. */
        boolean compute() {
            int w = la.length;
            if (n < w) return false;
            double ma = 0, mb = 0;
            for (int i = 0; i < w; i++) { ma += la[i]; mb += lb[i]; }
            ma /= w; mb /= w;
            double cov = 0, vb = 0;
            for (int i = 0; i < w; i++) { cov += (la[i] - ma) * (lb[i] - mb); vb += (lb[i] - mb) * (lb[i] - mb); }
            if (vb <= 0) return false;
            double bt = cov / vb;
            double ms = 0;
            for (int i = 0; i < w; i++) ms += la[i] - bt * lb[i];
            ms /= w;
            double vs = 0;
            for (int i = 0; i < w; i++) { double d = la[i] - bt * lb[i] - ms; vs += d * d; }
            double sd = Math.sqrt(vs / (w - 1));
            if (sd <= 0) return false;
            int last = (head - 1 + w) % w;
            // корреляция доходностей (а не уровней: у уровней корреляция бывает высокой и без связи)
            double sra = 0, srb = 0, saa = 0, sbb = 0, sab = 0;
            int m = 0;
            for (int k = 1; k < w; k++) {
                int i = (head + k) % w, j = (head + k - 1) % w;
                double ra = la[i] - la[j], rb = lb[i] - lb[j];
                sra += ra; srb += rb; saa += ra * ra; sbb += rb * rb; sab += ra * rb; m++;
            }
            double cva = saa - sra * sra / m, cvb = sbb - srb * srb / m, cab = sab - sra * srb / m;
            corr = cva > 0 && cvb > 0 ? cab / Math.sqrt(cva * cvb) : 0;
            beta = bt;
            z = (la[last] - bt * lb[last] - ms) / sd;
            return true;
        }
    }

    /** Открытая позиция по паре: какая нога куплена, сколько и почём. */
    record Position(String symbol, boolean longA, double qty, double entryPrice, long openedAtMs) {}

    private final TradingSettings settings;
    private final OrderExecutor executor;
    private volatile BooleanSupplier realtime = () -> true;
    private volatile List<Pair> pairs = List.of();
    private volatile String builtFor = null;
    private int builtWindow;
    private long lastSampleMs;
    private final Map<String, Position> positions = new ConcurrentHashMap<>();   // имя пары -> позиция
    private final AtomicLong trades = new AtomicLong(), stops = new AtomicLong();
    private volatile double totalPnl;

    public StatArbStrategy(MarketDataStore market, OrderService orders, String exchangeId, TradingSettings settings) {
        super("stat-arb", market, orders);
        this.settings = settings;
        this.executor = new OrderExecutor(exchangeId + "-statarb", 2);
    }

    public void setRealtimeSource(BooleanSupplier s) { this.realtime = s; }

    /** Пары из параметра "A/B,C/D" или все пары выбранных символов с одной котируемой валютой. */
    static List<String[]> pairList(String spec, java.util.Collection<String> symbols, int maxAutoPairs) {
        List<String[]> out = new ArrayList<>();
        if (spec != null && !spec.isBlank()) {
            for (String p : spec.split(",")) {
                String[] ab = p.trim().split("/");
                if (ab.length == 2 && symbols.contains(ab[0]) && symbols.contains(ab[1])) out.add(ab);
            }
            return out;
        }
        List<String> syms = new ArrayList<>(symbols);
        java.util.Collections.sort(syms);
        for (int i = 0; i < syms.size(); i++)
            for (int j = i + 1; j < syms.size() && out.size() < maxAutoPairs; j++)
                if (quote(syms.get(i)).equals(quote(syms.get(j)))) out.add(new String[]{syms.get(i), syms.get(j)});
        return out;
    }

    private static String quote(String s) {
        try { return BalanceStore.quoteAsset(s); } catch (IllegalArgumentException e) { return "?" + s; }
    }

    private void ensureBuilt(TradingParams p) {
        if (p.statArbPairs().equals(builtFor) && p.statArbWindow() == builtWindow) return;
        List<Pair> ps = new ArrayList<>();
        for (String[] ab : pairList(p.statArbPairs(), market.symbols(), p.statArbMaxAutoPairs())) ps.add(new Pair(ab[0], ab[1], p.statArbWindow()));
        pairs = ps;
        builtFor = p.statArbPairs();
        builtWindow = p.statArbWindow();
        log.info("[{}] пары: {} (окно {} × {} мс)", name(), ps.stream().map(Pair::name).toList(), p.statArbWindow(), p.statArbSampleMs());
    }

    @Override
    protected void onTick(Tick tick) {
        TradingParams p = settings.get();
        if (!p.statArbEnabled()) return;
        ensureBuilt(p);
        long now = System.currentTimeMillis();
        if (now - lastSampleMs < p.statArbSampleMs()) return;
        lastSampleMs = now;
        long maxAge = Math.max(p.maxDataAgeMs(), 2 * p.statArbSampleMs());
        for (Pair pr : pairs) {
            double ma = mid(pr.a, maxAge), mb = mid(pr.b, maxAge);
            if (!(ma > 0) || !(mb > 0)) continue;                         // нет свежих данных — отсчёт пропускаем
            pr.add(ma, mb);
            if (pr.compute()) decide(pr, p, now);
        }
    }

    private double mid(String symbol, long maxAgeMs) {
        OrderBook b = market.book(symbol);
        if (b == null || !b.isReady() || b.ageMs() > maxAgeMs) return Double.NaN;
        return b.midPrice();
    }

    private void decide(Pair pr, TradingParams p, long now) {
        Position pos = positions.get(pr.name());
        double z = pr.z;
        if (pos == null) {
            if (!realtime.getAsBoolean() || pr.corr < p.statArbMinCorrelation() || Math.abs(z) < p.statArbEntryZ()) return;
            if (Math.abs(z) >= p.statArbStopZ()) return;                  // уже за стопом — не входим
            boolean longA = z < 0;
            String sym = longA ? pr.a : pr.b;
            if (executor.isBusy(sym) || heldByOtherPair(sym)) return;
            OrderBook b = market.book(sym);
            double ask = b == null ? Double.NaN : b.bestAsk();
            if (!(ask > 0)) return;
            double qty = p.statArbOrderQuote() / ask;
            log.info("[{}] вход {}: z={} β={} корр={} — покупаю {}", name(), pr.name(), fmt(z), fmt(pr.beta), fmt(pr.corr), sym);
            executor.submit(sym, () -> {
                OrderResult r = orders.buyMarket(sym, qty);
                if (r.executedQty() > 0) positions.put(pr.name(), new Position(sym, longA, r.executedQty(), r.avgPrice(), System.currentTimeMillis()));
            });
            return;
        }
        boolean reverted = pos.longA() ? z >= -p.statArbExitZ() : z <= p.statArbExitZ();
        boolean stop = pos.longA() ? z <= -p.statArbStopZ() : z >= p.statArbStopZ();
        boolean timeout = now - pos.openedAtMs() > p.statArbMaxHoldMs();
        if (!reverted && !stop && !timeout || executor.isBusy(pos.symbol())) return;
        String why = stop ? "стоп (спред разошёлся)" : reverted ? "спред вернулся" : "таймаут";
        log.info("[{}] выход {} ({}): z={}", name(), pr.name(), why, fmt(z));
        if (stop) stops.incrementAndGet();
        executor.submit(pos.symbol(), () -> close(pr.name(), pos));
    }

    private boolean heldByOtherPair(String symbol) {
        for (Position x : positions.values()) if (x.symbol().equals(symbol)) return true;
        return false;
    }

    private void close(String pairName, Position pos) {
        OrderResult r = orders.sellMarket(pos.symbol(), pos.qty());
        if (r.executedQty() <= 0) return;
        double fee = settings.get().takerFeePercent() / 100.0 * (r.avgPrice() + pos.entryPrice()) * r.executedQty();
        double pnl = (r.avgPrice() - pos.entryPrice()) * r.executedQty() - fee;
        orders.risk().recordPnl(pnl);
        trades.incrementAndGet();
        totalPnl += pnl;
        double left = pos.qty() - r.executedQty();
        if (left > pos.qty() * 1e-6) positions.put(pairName, new Position(pos.symbol(), pos.longA(), left, pos.entryPrice(), pos.openedAtMs()));
        else positions.remove(pairName);
        log.info("[{}] {} закрыта: {} {} @ {}, результат {}", name(), pairName, r.executedQty(), pos.symbol(), r.avgPrice(), String.format("%.4f", pnl));
    }

    /** Закрыть все позиции — при остановке бота. */
    public void closeAll() {
        if (!executor.drain(15_000)) log.warn("[{}] не все ордера завершились за 15 с", name());
        positions.forEach((pairName, pos) -> close(pairName, pos));
    }

    private static String fmt(double v) { return String.format("%.2f", v); }

    public Map<String, Object> stats() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("enabled", settings.get().statArbEnabled());
        List<Map<String, Object>> ps = new ArrayList<>();
        if (builtFor == null) {                       // до первого тика — какие пары будут
            for (String[] ab : pairList(settings.get().statArbPairs(), market.symbols(), settings.get().statArbMaxAutoPairs())) ps.add(Map.of("pair", ab[0] + "/" + ab[1], "samples", 0));
        }
        for (Pair pr : pairs) {
            Map<String, Object> x = new LinkedHashMap<>();
            x.put("pair", pr.name());
            x.put("samples", pr.n);
            x.put("z", Double.isNaN(pr.z) ? null : pr.z);
            x.put("beta", Double.isNaN(pr.beta) ? null : pr.beta);
            x.put("correlation", Double.isNaN(pr.corr) ? null : pr.corr);
            x.put("position", positions.containsKey(pr.name()) ? positions.get(pr.name()).symbol() : null);
            ps.add(x);
        }
        m.put("pairs", ps);
        m.put("trades", trades.get());
        m.put("stops", stops.get());
        m.put("totalPnl", totalPnl);
        return m;
    }

    List<Pair> pairs() { return pairs; }

    /** z-score каждой пары (NaN — окно ещё не заполнено). */
    public Map<String, Double> zScores() {
        Map<String, Double> m = new LinkedHashMap<>();
        for (Pair pr : pairs) m.put(pr.name(), pr.z);
        return m;
    }
    public int openPositions() { return positions.size(); }
    public long tradesCount() { return trades.get(); }
    public double totalPnl() { return totalPnl; }
}
