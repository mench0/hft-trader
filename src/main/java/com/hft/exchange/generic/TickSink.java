package com.hft.exchange.generic;

/** Куда фид отдаёт тик (цена середины после обновления стакана). Без объекта Tick — без аллокаций. */
@FunctionalInterface
public interface TickSink {
    /** Принять тик: символ, цена, объём, сторона агрессора, время биржи, время получения (nanoTime). */
    void onTick(String symbol, double price, double qty, boolean buyerIsMaker, long exchangeTimeMs, long receivedNanos);
}
