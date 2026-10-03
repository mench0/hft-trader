package com.hft.model;

import com.hft.model.OrderEnums.Side;
import com.hft.model.OrderEnums.TimeInForce;
import com.hft.model.OrderEnums.Type;

/**
 * Описание ордера, который мы собираемся отправить на биржу.
 *
 * Покрывает все комбинации, которые вам нужны:
 *   LIMIT  + конкретный объём   -> limit(...).quantity(0.5)
 *   LIMIT  + весь баланс        -> limit(...).fullBalance()
 *   MARKET + конкретный объём   -> market(...).quantity(0.5)
 *   MARKET + весь баланс        -> market(...).fullBalance()
 *
 * "Весь баланс" разрешается в конкретное число прямо перед отправкой:
 * OrderService смотрит в BalanceStore и подставляет доступное количество
 * с учётом резерва под комиссию.
 */
public final class OrderRequest {

    private final String symbol;
    private final Side side;
    private final Type type;

    private double quantity;        // -1 означает "весь доступный баланс"
    private double price;           // только для LIMIT
    private TimeInForce timeInForce = TimeInForce.GTC;
    private double quotePortion = 1.0;  // доля баланса при fullBalance(), 1.0 = 100%

    private OrderRequest(String symbol, Side side, Type type) {
        this.symbol = symbol.toUpperCase();
        this.side = side;
        this.type = type;
    }

    // ---------- Фабрики ----------

    /** Лимитный ордер по указанной цене. */
    public static OrderRequest limit(String symbol, Side side, double price) {
        OrderRequest r = new OrderRequest(symbol, side, Type.LIMIT);
        r.price = price;
        return r;
    }

    /** Рыночный ордер — цена определяется стаканом. */
    public static OrderRequest market(String symbol, Side side) {
        return new OrderRequest(symbol, side, Type.MARKET);
    }

    // ---------- Объём ----------

    /** Конкретный объём в базовой валюте (для BTCUSDT — количество BTC). */
    public OrderRequest quantity(double qty) {
        if (qty <= 0) {
            throw new IllegalArgumentException("Объём должен быть положительным: " + qty);
        }
        this.quantity = qty;
        return this;
    }

    /**
     * Весь доступный баланс. Для BUY — потратить все котируемые средства (USDT),
     * для SELL — продать всю базовую валюту (BTC).
     * Реальное число подставляется в OrderService перед отправкой.
     */
    public OrderRequest fullBalance() {
        this.quantity = -1;
        return this;
    }

    /**
     * Часть доступного баланса: 0.25 = четверть, 0.5 = половина.
     * Удобно, когда не хотите заходить на весь депозит сразу.
     */
    public OrderRequest balancePortion(double portion) {
        if (portion <= 0 || portion > 1.0) {
            throw new IllegalArgumentException("Доля должна быть в (0, 1]: " + portion);
        }
        this.quantity = -1;
        this.quotePortion = portion;
        return this;
    }

    // ---------- TIF ----------

    /** Только для LIMIT. Для MARKET биржа игнорирует этот параметр. */
    public OrderRequest timeInForce(TimeInForce tif) {
        this.timeInForce = tif;
        return this;
    }

    // ---------- Геттеры ----------

    public String symbol() { return symbol; }
    public Side side() { return side; }
    public Type type() { return type; }
    public double price() { return price; }
    public TimeInForce timeInForce() { return timeInForce; }
    public double quotePortion() { return quotePortion; }

    /** Заданный объём, либо -1 если нужно взять из баланса. */
    public double rawQuantity() { return quantity; }

    public boolean isFullBalance() { return quantity < 0; }

    @Override
    public String toString() {
        String qty = isFullBalance()
                ? (quotePortion == 1.0 ? "весь баланс" : (int) (quotePortion * 100) + "% баланса")
                : String.valueOf(quantity);
        return type + " " + side + " " + symbol + " qty=" + qty
                + (type == Type.LIMIT ? " @ " + price + " " + timeInForce : "");
    }
}
