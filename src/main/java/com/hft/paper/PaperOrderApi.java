package com.hft.paper;

import com.hft.model.OrderEnums.Side;
import com.hft.model.OrderEnums.TimeInForce;
import com.hft.model.OrderResult;
import com.hft.rest.ExchangeOrderApi;
import com.hft.store.BalanceStore;
import com.hft.store.MarketDataStore;
import com.hft.store.OrderBook;
import com.hft.store.PositionStore;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Бумажная биржа: исполняет ордера против живого стакана, ничего не отправляя.
 * <p>
 * Правила (сознательно пессимистичные):
 * <pre>
 * - рыночные и пересекающие стакан заявки идут по уровням как тейкер, цена
 *   ухудшена на комиссию тейкера;
 * - GTC-остаток встаёт в очередь и исполняется только когда рынок ПРОШЁЛ
 *   через нашу цену (строго), по цене заявки, с комиссией мейкера;
 * - замороженные под заявки средства не учитываются, очередь внутри уровня
 *   не моделируется, задержки сети нет — реальные результаты будут хуже.
 * </pre>
 * Баланс при мгновенных исполнениях корректирует OrderService (по результату),
 * поэтому здесь баланс меняется только для отложенных заполнений GTC.
 * <p>
 * Режим перпов ({@link #perp(PositionStore)}): те же правила исполнения, но сделка меняет позицию
 * (лонг/шорт), в баланс котируемой валюты идёт только реализованный результат; reduceOnly-ордер
 * не больше открытой позиции. Funding начисляет {@link com.hft.perp.PerpAccount} по реальным ставкам биржи.
 * Ликвидация не моделируется — держите плечо низким.
 */
public final class PaperOrderApi implements ExchangeOrderApi {

    /** Лимитный ордер, ждущий цены: остаток, цена, запрошено, исполнено, сумма в котируемой. */
    private record Resting(long id, String symbol, Side side, double qtyLeft, double price, double requested, double filled, double quoteSum) {}

    /** Стакан, по которому исполняются ордера. */
    private final MarketDataStore market;
    /** Бумажные балансы. */
    private final BalanceStore balances;
    /** Комиссия мейкера, доля. */
    private final double makerFee;
    /** Комиссия тейкера, доля. */
    private final double takerFee;
    /** Номера ордеров. */
    private final AtomicLong ids = new AtomicLong(1_000_000);
    /** Висящие лимитные ордера. */
    private final Map<Long, Resting> resting = new ConcurrentHashMap<>();
    /** Позиции в режиме перпов; null — спот. */
    private volatile PositionStore positions;

    /**
     * @param market стаканы биржи
     * @param balances бумажные балансы
     * @param makerFeePct комиссия мейкера, %
     * @param takerFeePct комиссия тейкера, %
     */
    public PaperOrderApi(MarketDataStore market, BalanceStore balances, double makerFeePct, double takerFeePct) {
        this.market = market;
        this.balances = balances;
        this.makerFee = makerFeePct / 100.0;
        this.takerFee = takerFeePct / 100.0;
    }

    /** Снимок стакана для прохода по уровням: один согласованный снимок, буфер на поток. */
    private static final ThreadLocal<OrderBook.Levels> LEVELS = ThreadLocal.withInitial(OrderBook.Levels::new);
    /** Буфер лучших цен на поток (без аллокаций). */
    private static final ThreadLocal<double[]> TOP = ThreadLocal.withInitial(() -> new double[4]);

    /** Снимок всех уровней стакана символа. */
    private OrderBook.Levels levels(String symbol) {
        OrderBook.Levels l = LEVELS.get();
        market.book(symbol).copyTo(l);
        return l;
    }

    /** Включить режим перпов: исполнения меняют позиции. */
    public PaperOrderApi perp(PositionStore positions) { this.positions = positions; return this; }

    // ------------------------------------------------------------ ExchangeOrderApi

    @Override public boolean isPerp() { return positions != null; }

    /** Закрытие: не больше открытой позиции нужного знака. */
    @Override
    public OrderResult reduceMarket(String symbol, Side side, double qty) {
        PositionStore ps = positions;
        if (ps != null) {
            double pos = ps.qty(symbol);
            double max = side == Side.SELL ? Math.max(0, pos) : Math.max(0, -pos);
            if (max <= 1e-12) return result(0, symbol, side, "REJECTED", qty, 0, 0, System.nanoTime());
            qty = Math.min(qty, max);
        }
        return takeFromBook(symbol, side, qty, Double.NaN, false, "MARKET");
    }

    @Override public OrderResult buyLimit(String s, double q, double p, TimeInForce t) { return limit(s, Side.BUY, q, p, t); }
    @Override public OrderResult sellLimit(String s, double q, double p, TimeInForce t) { return limit(s, Side.SELL, q, p, t); }
    @Override public OrderResult buyMarket(String s, double q) { return takeFromBook(s, Side.BUY, q, Double.NaN, false, "MARKET"); }
    @Override public OrderResult sellMarket(String s, double q) { return takeFromBook(s, Side.SELL, q, Double.NaN, false, "MARKET"); }

    /** Рыночная покупка на сумму в котируемой валюте. */
    @Override
    public OrderResult buyMarketForQuote(String symbol, double quoteAmount) {
        long t0 = System.nanoTime();
        OrderBook.Levels b = levels(symbol);
        double left = quoteAmount, qty = 0, spent = 0;
        for (int i = 0; i < b.askCount && left > 1e-12; i++) {
            double px = b.askPrices[i];
            double take = Math.min(b.askQtys[i], left / px);
            qty += take; spent += take * px; left -= take * px;
        }
        if (qty <= 0) return result(0, symbol, Side.BUY, "REJECTED", 0, 0, 0, t0);
        double avg = spent / qty * (1 + takerFee);
        return result(ids.incrementAndGet(), symbol, Side.BUY, "FILLED", qty, qty, avg, t0);
    }

    /** Отменить висящий ордер. */
    @Override
    public void cancelOrder(String symbol, long orderId) {
        resting.remove(orderId);
    }

    /** Отменить все висящие ордера символа. */
    @Override
    public int cancelAll(String symbol) {
        int n = 0;
        for (Iterator<Resting> it = resting.values().iterator(); it.hasNext(); ) {
            if (it.next().symbol().equals(symbol)) { it.remove(); n++; }
        }
        return n;
    }

    /** Сколько висит лимитных ордеров. */
    public int openOrders() { return resting.size(); }

    // ------------------------------------------------------------------ internals

    /** Лимитный ордер: исполнить то, что пересекает стакан, остаток (GTC) — оставить висеть. */
    private OrderResult limit(String symbol, Side side, double qty, double price, TimeInForce tif) {
        if (tif == TimeInForce.FOK) {
            double available = depthUpTo(symbol, side, price);
            if (available + 1e-12 < qty) return result(0, symbol, side, "EXPIRED", qty, 0, 0, System.nanoTime());
            return takeFromBook(symbol, side, qty, price, true, "LIMIT");
        }
        OrderResult taker = takeFromBook(symbol, side, qty, price, true, "LIMIT");
        if (tif == TimeInForce.IOC) {
            return taker.executedQty() > 0 ? taker : result(taker.orderId(), symbol, side, "EXPIRED", qty, 0, 0, System.nanoTime());
        }
        // GTC: остаток становится заявкой в очереди
        double left = qty - taker.executedQty();
        if (left <= 1e-12) return taker;
        long id = taker.orderId() != 0 ? taker.orderId() : ids.incrementAndGet();
        resting.put(id, new Resting(id, symbol, side, left, price, qty, taker.executedQty(), taker.executedQty() * taker.avgPrice()));
        String status = taker.executedQty() > 0 ? "PARTIALLY_FILLED" : "NEW";
        return new OrderResult(id, "paper-" + id, symbol, side, status, qty, taker.executedQty(), taker.avgPrice(), 0);
    }

    /** Объём на стороне стакана до цены limitPrice включительно. */
    private double depthUpTo(String symbol, Side side, double limitPrice) {
        OrderBook.Levels b = levels(symbol);
        double sum = 0;
        if (side == Side.BUY) {
            for (int i = 0; i < b.askCount && b.askPrices[i] <= limitPrice; i++) sum += b.askQtys[i];
        } else {
            for (int i = 0; i < b.bidCount && b.bidPrices[i] >= limitPrice; i++) sum += b.bidQtys[i];
        }
        return sum;
    }

    /** Проход по стакану; limited=true — не дальше лимитной цены. */
    private OrderResult takeFromBook(String symbol, Side side, double qty, double limitPrice, boolean limited, String type) {
        long t0 = System.nanoTime();
        OrderBook.Levels b = levels(symbol);
        double left = qty, filled = 0, notional = 0;
        if (side == Side.BUY) {
            for (int i = 0; i < b.askCount && left > 1e-12; i++) {
                double px = b.askPrices[i];
                if (limited && px > limitPrice) break;
                double take = Math.min(left, b.askQtys[i]);
                filled += take; notional += take * px; left -= take;
            }
        } else {
            for (int i = 0; i < b.bidCount && left > 1e-12; i++) {
                double px = b.bidPrices[i];
                if (limited && px < limitPrice) break;
                double take = Math.min(left, b.bidQtys[i]);
                filled += take; notional += take * px; left -= take;
            }
        }
        if (filled <= 0) return result(0, symbol, side, limited ? "NEW" : "REJECTED", qty, 0, 0, t0);
        double avg = notional / filled;
        avg = side == Side.BUY ? avg * (1 + takerFee) : avg * (1 - takerFee);
        String status = left <= 1e-12 ? "FILLED" : "PARTIALLY_FILLED";
        return result(ids.incrementAndGet(), symbol, side, status, qty, filled, avg, t0);
    }

    /**
     * Вызывается после каждого обновления стакана: исполняет лимитные заявки,
     * через цену которых рынок строго прошёл. Баланс правим сами —
     * OrderService об этих сделках не узнает.
     */
    public void settle(String symbol) {
        double[] top = TOP.get();
        if (!market.book(symbol).readTop(top)) return;
        for (Resting r : resting.values()) {
            if (!r.symbol().equals(symbol)) continue;
            boolean crossed = r.side() == Side.BUY ? top[2] < r.price() : top[0] > r.price();
            if (!crossed) continue;
            if (resting.remove(r.id()) == null) continue;
            PositionStore ps = positions;
            if (ps != null) {                              // перп: позиция и реализованный результат
                double px = r.side() == Side.BUY ? r.price() * (1 + makerFee) : r.price() * (1 - makerFee);
                double pnl = ps.apply(symbol, r.side() == Side.BUY, r.qtyLeft(), px);
                if (pnl != 0) balances.adjust(BalanceStore.quoteAsset(symbol), pnl);
                continue;
            }
            String base = BalanceStore.baseAsset(symbol), quote = BalanceStore.quoteAsset(symbol);
            double quoteAmount = r.qtyLeft() * r.price();
            if (r.side() == Side.BUY) {
                balances.adjust(base, r.qtyLeft() * (1 - makerFee));
                balances.adjust(quote, -quoteAmount);
            } else {
                balances.adjust(base, -r.qtyLeft());
                balances.adjust(quote, quoteAmount * (1 - makerFee));
            }
        }
    }

    private static OrderResult result(long id, String symbol, Side side, String status,
                                      double req, double exec, double avg, long t0) {
        return new OrderResult(id, "paper-" + id, symbol, side, status, req, exec, avg, System.nanoTime() - t0);
    }
}
