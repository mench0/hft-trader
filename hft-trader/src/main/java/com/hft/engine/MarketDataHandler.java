package com.hft.engine;

import com.hft.metrics.Latency;
import com.hft.model.Tick;
import com.hft.store.MarketDataStore;
import com.lmax.disruptor.EventHandler;

/**
 * Первая стадия конвейера: записать тик в память и замерить,
 * сколько прошло от получения пакета до обработки.
 *
 * Работает в одном потоке Disruptor, поэтому в MarketDataStore
 * пишет ровно один поток — синхронизация не нужна.
 */
public final class MarketDataHandler implements EventHandler<Tick> {

    private final MarketDataStore store;
    private final Latency latency = new Latency("Обработка тика");

    public MarketDataHandler(MarketDataStore store) {
        this.store = store;
    }

    @Override
    public void onEvent(Tick tick, long sequence, boolean endOfBatch) {
        store.onTick(tick);
        latency.recordSince(tick.receivedNanos());
    }

    public Latency latency() { return latency; }
}
