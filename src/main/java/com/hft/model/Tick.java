package com.hft.model;

/**
 * Одна сделка с биржи. Mutable-объект: Disruptor выделяет их один раз
 * при старте и переиспользует, поэтому в горячем пути нет аллокаций.
 */
public final class Tick {

    /** Символ (строка из подписки, без копий на каждый тик). */
    private String symbol;
    /** Цена сделки или середина стакана. */
    private double price;
    /** Объём сделки (0 для тика по стакану). */
    private double quantity;
    /** true — агрессор продавец (покупатель стоял в стакане). */
    private boolean buyerIsMaker;
    /** Время события по часам биржи, мс. */
    private long exchangeTimeMs;
    /** Время получения в процессе (System.nanoTime) — для замеров задержки. */
    private long receivedNanos;

    public void set(String symbol, double price, double quantity,
                    boolean buyerIsMaker, long exchangeTimeMs, long receivedNanos) {
        this.symbol = symbol;
        this.price = price;
        this.quantity = quantity;
        this.buyerIsMaker = buyerIsMaker;
        this.exchangeTimeMs = exchangeTimeMs;
        this.receivedNanos = receivedNanos;
    }

    /** Символ. */
    public String symbol() { return symbol; }
    /** Цена. */
    public double price() { return price; }
    /** Объём. */
    public double quantity() { return quantity; }

    /** true = сделка инициирована продавцом (агрессивная продажа). */
    public boolean buyerIsMaker() { return buyerIsMaker; }

    /** Время события по часам биржи, мс. */
    public long exchangeTimeMs() { return exchangeTimeMs; }
    /** Время получения (System.nanoTime). */
    public long receivedNanos() { return receivedNanos; }
}
