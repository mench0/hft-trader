package com.hft.exchange.generic;

/** Куда фид отдаёт тик (цена середины после обновления стакана). Без объекта Tick — без аллокаций. */
@FunctionalInterface
public interface TickSink {
    void onTick(String symbol, double price, double qty, boolean buyerIsMaker, long exchangeTimeMs, long receivedNanos);
}
