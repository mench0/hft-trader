package com.hft.strategy;

import com.hft.engine.OrderService;
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
 * <p>Для пары {@code A/B} раз в {@code statArbSampleMs} берутся середины
 * стаканов. На окне из {@code statArbWindow} отсчётов оцениваются:
 * <ul>
 *   <li>
 *     коэффициент хеджирования
 *     {@code β = cov(ln A, ln B) / var(ln B)};
 *   </li>
 *   <li>
 *     спред {@code s = ln A − β · ln B};
 *   </li>
 *   <li>
 *     z-score {@code z = (s − mean) / σ}, показывающий, насколько
 *     {@code A} сейчас дёшев или дорог относительно {@code B}.
 *   </li>
 * </ul>
 *
 * <p>В торговлю допускаются только пары с корреляцией доходностей
 * не ниже {@code statArbMinCorrelation}.
 *
 * <p>На фьючерсах (market=perp) позиция рыночно-нейтральная: дешёвая нога покупается,
 * дорогая шортится на сумму × |β|. Если вторая нога не исполнилась — первая закрывается.
 *
 * <p>Спот не поддерживает шорт, поэтому там торгуется только дешёвая нога:
 * <ul>
 *   <li>
 *     {@code z ≤ −entry} — {@code A} дёшев относительно {@code B},
 *     покупаем {@code A};
 *   </li>
 *   <li>
 *     {@code z ≥ entry} — {@code B} дёшев относительно {@code A},
 *     покупаем {@code B}.
 *   </li>
 * </ul>
 *
 * <p>Выход из позиции происходит при одном из условий:
 * <ul>
 *   <li>спред вернулся к равновесию: {@code |z| ≤ exit};</li>
 *   <li>спред продолжил расходиться: {@code |z| ≥ stop} — стоп;</li>
 *   <li>истёк максимальный срок удержания {@code statArbMaxHoldMs}.</li>
 * </ul>
 *
 * <p>На споте открытая позиция дополнительно несёт риск движения рынка в целом.
 */
public final class StatArbStrategy extends Strategy {

    /** Логгер. */
    private static final Logger log = LoggerFactory.getLogger(StatArbStrategy.class);

    /** Окно отсчётов одной пары и её позиция. Отсчёты пишет только поток конвейера. */
    static final class Pair {
        /** Символы пары: A и B. */
        final String a, b;
        /** Окна ln-цен A и B (кольцевые). */
        final double[] la, lb;
        /** Сколько отсчётов в окне и куда пишется следующий. */
        int n, head;
        /** Последние z-score спреда, β и корреляция доходностей (читает админка). */
        volatile double z = Double.NaN, beta = Double.NaN, corr = Double.NaN;
        /** σ спреда ln A − β·ln B (для оценки ожидаемого хода). */
        volatile double sd = Double.NaN;
        Pair(String a, String b, int window) { this.a = a; this.b = b; la = new double[window]; lb = new double[window]; }
        /** Имя пары A/B. */
        String name() { return a + "/" + b; }
        /** Добавить отсчёт: логарифмы середин стаканов. */
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
            this.sd = sd;
            beta = bt;
            z = (la[last] - bt * lb[last] - ms) / sd;
            return true;
        }
    }

    /**
     * Открытая позиция по паре: какая нога куплена, сколько и почём; на перпах ещё шорт второй ноги
     * (hedgeSymbol, hedgeQty, hedgeEntry), на споте hedgeSymbol = null.
     */
    record Position(String symbol, boolean longA, double qty, double entryPrice, long openedAtMs,
                    String hedgeSymbol, double hedgeQty, double hedgeEntry) {
        Position(String symbol, boolean longA, double qty, double entryPrice, long openedAtMs) {
            this(symbol, longA, qty, entryPrice, openedAtMs, null, 0, 0);
        }
    }

    /** Параметры биржи. */
    private final TradingSettings settings;
    /** Отправка ордеров вне потока конвейера. */
    private final OrderExecutor executor;
    /** Данные в реальном времени. */
    private volatile BooleanSupplier realtime = () -> true;
    /** Пары с окнами отсчётов. */
    private volatile List<Pair> pairs = List.of();
    /** Для какого statArbPairs построены пары (null — ещё не строились). */
    private volatile String builtFor = null;
    /** Для какого окна построены пары. */
    private int builtWindow;
    /** Время последнего отсчёта (поток конвейера). */
    private long lastSampleMs;
    private final Map<String, Position> positions = new ConcurrentHashMap<>();   // имя пары -> позиция
    /** Счётчики: закрыто сделок, из них по стопу. */
    private final AtomicLong trades = new AtomicLong(), stops = new AtomicLong();
    /** Результат с запуска. */
    private volatile double totalPnl;

    /**
     * @param market рыночные данные
     * @param orders сервис ордеров
     * @param exchangeId биржа (имя потоков)
     * @param settings параметры биржи
     */
    public StatArbStrategy(MarketDataStore market, OrderService orders, String exchangeId, TradingSettings settings) {
        super("stat-arb", market, orders);
        this.settings = settings;
        this.executor = new OrderExecutor(exchangeId + "-statarb", 2);
    }

    /** Источник признака «данные в реальном времени»: на REST-запасе новых входов нет. */
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

    /** Котируемая валюта символа; для неразборчивого — уникальная строка (пару не образует). */
    private static String quote(String s) {
        try { return BalanceStore.quoteAsset(s); } catch (IllegalArgumentException e) { return "?" + s; }
    }

    /** Построить пары (при первом тике и смене statArbPairs/окна). */
    private void ensureBuilt(TradingParams p) {
        if (p.statArbPairs().equals(builtFor) && p.statArbWindow() == builtWindow) return;
        List<Pair> ps = new ArrayList<>();
        for (String[] ab : pairList(p.statArbPairs(), market.symbols(), p.statArbMaxAutoPairs())) ps.add(new Pair(ab[0], ab[1], p.statArbWindow()));
        pairs = ps;
        builtFor = p.statArbPairs();
        builtWindow = p.statArbWindow();
        log.info("[{}] пары: {} (окно {} × {} мс)", name(), ps.stream().map(Pair::name).toList(), p.statArbWindow(), p.statArbSampleMs());
    }

    /** Раз в statArbSampleMs снять отсчёты по всем парам и принять решения. */
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

    /** Середина стакана, если он свежий; иначе NaN. */
    private double mid(String symbol, long maxAgeMs) {
        OrderBook b = market.book(symbol);
        if (b == null || !b.isReady() || b.ageMs() > maxAgeMs) return Double.NaN;
        return b.midPrice();
    }

    /** Вход или выход по паре по z-score и корреляции. */
    private void decide(Pair pr, TradingParams p, long now) {
        Position pos = positions.get(pr.name());
        double z = pr.z;
        if (pos == null) {
            if (!realtime.getAsBoolean() || pr.corr < p.statArbMinCorrelation() || Math.abs(z) < p.statArbEntryZ()) return;
            if (Math.abs(z) >= p.statArbStopZ()) return;                  // уже за стопом — не входим
            // ожидаемый возврат спреда (|z| − exit)·σ — доля от суммы ноги; должен окупать комиссии всех сделок:
            // на споте одна нога (2 сделки), на перпах две (4 сделки)
            double expectedPct = (Math.abs(z) - p.statArbExitZ()) * pr.sd * 100.0;
            double costPct = orders.roundTripCostPercent(p.statArbOrderQuote()) * (orders.isPerp() ? 2 : 1);
            if (!(expectedPct > costPct)) return;
            boolean longA = z < 0;
            String sym = longA ? pr.a : pr.b;
            String hedge = orders.isPerp() ? (longA ? pr.b : pr.a) : null;     // на перпах — шорт дорогой ноги
            if (executor.isBusy(sym) || heldByOtherPair(sym)) return;
            if (hedge != null && (executor.isBusy(hedge) || heldByOtherPair(hedge))) return;
            OrderBook b = market.book(sym);
            double ask = b == null ? Double.NaN : b.bestAsk();
            if (!(ask > 0)) return;
            double qty = p.statArbOrderQuote() / ask;
            double hedgeBid = hedge == null ? Double.NaN : bid(hedge);
            if (hedge != null && !(hedgeBid > 0)) return;
            double beta = Math.min(5, Math.max(0.2, Math.abs(pr.beta)));        // защита от вырожденного β
            double hedgeQty = hedge == null ? 0 : p.statArbOrderQuote() * beta / hedgeBid;
            log.info("[{}] вход {}: z={} β={} корр={} — покупаю {}{}", name(), pr.name(), fmt(z), fmt(pr.beta), fmt(pr.corr), sym,
                    hedge == null ? "" : ", шорт " + hedge);
            executor.submit(sym, () -> {
                OrderResult r = orders.buyMarket(sym, qty);
                if (r.executedQty() <= 0) return;
                long at = System.currentTimeMillis();
                if (hedge == null) {
                    positions.put(pr.name(), new Position(sym, longA, r.executedQty(), r.avgPrice(), at));
                    return;
                }
                OrderResult h = orders.sellMarket(hedge, hedgeQty);
                if (h.executedQty() <= 0) {                                     // вторая нога не встала — без хеджа не держим
                    log.warn("[{}] {}: шорт {} не исполнился — закрываю лонг {}", name(), pr.name(), hedge, sym);
                    orders.reduceMarket(sym, com.hft.model.OrderEnums.Side.SELL, r.executedQty());
                    return;
                }
                positions.put(pr.name(), new Position(sym, longA, r.executedQty(), r.avgPrice(), at, hedge, h.executedQty(), h.avgPrice()));
            });
            return;
        }
        boolean reverted = pos.longA() ? z >= -p.statArbExitZ() : z <= p.statArbExitZ();
        boolean stop = pos.longA() ? z <= -p.statArbStopZ() : z >= p.statArbStopZ();
        boolean timeout = now - pos.openedAtMs() > p.statArbMaxHoldMs();
        if (!reverted && !stop && !timeout || executor.isBusy(pos.symbol())) return;
        if (pos.hedgeSymbol() != null && executor.isBusy(pos.hedgeSymbol())) return;
        String why = stop ? "стоп (спред разошёлся)" : reverted ? "спред вернулся" : "таймаут";
        log.info("[{}] выход {} ({}): z={}", name(), pr.name(), why, fmt(z));
        if (stop) stops.incrementAndGet();
        executor.submit(pos.symbol(), () -> close(pr.name(), pos));
    }

    /** Символ уже занят другой парой — не входим, чтобы не путать позиции. */
    private boolean heldByOtherPair(String symbol) {
        for (Position x : positions.values()) if (x.symbol().equals(symbol) || symbol.equals(x.hedgeSymbol())) return true;
        return false;
    }

    /** Лучший бид символа или NaN. */
    private double bid(String symbol) {
        OrderBook b = market.book(symbol);
        return b == null ? Double.NaN : b.bestBid();
    }

    /** Закрыть позицию пары (лонг — продажей, шорт-хедж — покупкой, reduceOnly); результат после комиссий — в дневной PnL. */
    private void close(String pairName, Position pos) {
        double hedgePnl = 0;
        if (pos.hedgeSymbol() != null && pos.hedgeQty() > 0) {
            OrderResult h = orders.reduceMarket(pos.hedgeSymbol(), com.hft.model.OrderEnums.Side.BUY, pos.hedgeQty());
            if (h.executedQty() > 0) {
                hedgePnl = (pos.hedgeEntry() - h.avgPrice()) * h.executedQty()
                        - orders.costOf(pos.hedgeEntry() * h.executedQty()) - orders.costOf(h.avgPrice() * h.executedQty());
                double hl = pos.hedgeQty() - h.executedQty();
                pos = new Position(pos.symbol(), pos.longA(), pos.qty(), pos.entryPrice(), pos.openedAtMs(),
                        hl > pos.hedgeQty() * 1e-6 ? pos.hedgeSymbol() : null, Math.max(0, hl), pos.hedgeEntry());
            }
        }
        if (pos.qty() <= 0) {                                       // лонг уже закрыт раньше, оставался только хедж
            orders.risk().recordPnl(hedgePnl);
            totalPnl += hedgePnl;
            if (pos.hedgeSymbol() == null) { positions.remove(pairName); trades.incrementAndGet(); }
            else positions.put(pairName, pos);
            return;
        }
        OrderResult r = orders.reduceMarket(pos.symbol(), com.hft.model.OrderEnums.Side.SELL, pos.qty());
        if (r.executedQty() <= 0) {
            if (hedgePnl != 0) { orders.risk().recordPnl(hedgePnl); totalPnl += hedgePnl; positions.put(pairName, pos); }
            return;
        }
        double fee = orders.costOf(pos.entryPrice() * r.executedQty()) + orders.costOf(r.avgPrice() * r.executedQty());
        double pnl = (r.avgPrice() - pos.entryPrice()) * r.executedQty() - fee + hedgePnl;
        orders.risk().recordPnl(pnl);
        trades.incrementAndGet();
        totalPnl += pnl;
        double left = pos.qty() - r.executedQty();
        if (left > pos.qty() * 1e-6 || pos.hedgeSymbol() != null)
            positions.put(pairName, new Position(pos.symbol(), pos.longA(), Math.max(0, left), pos.entryPrice(), pos.openedAtMs(),
                    pos.hedgeSymbol(), pos.hedgeQty(), pos.hedgeEntry()));
        else positions.remove(pairName);
        log.info("[{}] {} закрыта: {} {} @ {}, результат {}", name(), pairName, r.executedQty(), pos.symbol(), r.avgPrice(), String.format("%.4f", pnl));
    }

    /** Закрыть все позиции — при остановке бота. */
    public void closeAll() {
        if (!executor.drain(15_000)) log.warn("[{}] не все ордера завершились за 15 с", name());
        positions.forEach((pairName, pos) -> close(pairName, pos));
    }

    /** Число с двумя знаками для логов. */
    private static String fmt(double v) { return String.format("%.2f", v); }

    /** Состояние для GET /strategies. */
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
            Position pp = positions.get(pr.name());
            x.put("position", pp == null ? null : pp.hedgeSymbol() == null ? pp.symbol() : pp.symbol() + " лонг / " + pp.hedgeSymbol() + " шорт");
            ps.add(x);
        }
        m.put("pairs", ps);
        m.put("trades", trades.get());
        m.put("stops", stops.get());
        m.put("totalPnl", totalPnl);
        return m;
    }

    /** Пары (для тестов). */
    List<Pair> pairs() { return pairs; }

    /** z-score каждой пары (NaN — окно ещё не заполнено). */
    public Map<String, Double> zScores() {
        Map<String, Double> m = new LinkedHashMap<>();
        for (Pair pr : pairs) m.put(pr.name(), pr.z);
        return m;
    }
    /** Открытых позиций. */
    public int openPositions() { return positions.size(); }
    /** Закрыто сделок. */
    public long tradesCount() { return trades.get(); }
    /** Результат с запуска. */
    public double totalPnl() { return totalPnl; }
}
