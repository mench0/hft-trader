package com.hft.model;

/**
 * Одна сделка с биржи. Mutable-объект: Disruptor выделяет их один раз
 * при старте и переиспользует, поэтому в горячем пути нет аллокаций.
 */
public final class Tick {

    private String symbol;
    private double price;
    private double quantity;
    private boolean buyerIsMaker;
    private long exchangeTimeMs;
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

    public String symbol() { return symbol; }
    public double price() { return price; }
    public double quantity() { return quantity; }

    /** true = сделка инициирована продавцом (агрессивная продажа). */
    public boolean buyerIsMaker() { return buyerIsMaker; }

    public long exchangeTimeMs() { return exchangeTimeMs; }
    public long receivedNanos() { return receivedNanos; }
}
