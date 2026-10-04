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

    /** Куда записываются тики. */
    private final MarketDataStore store;
    /** Время обработки тика. */
    private final Latency latency = new Latency("Обработка тика");

    /** @param store хранилище рыночных данных биржи */
    public MarketDataHandler(MarketDataStore store) {
        this.store = store;
    }

    /** Записать тик в окно цен и статистику символа (первая стадия конвейера, до стратегий). */
    @Override
    public void onEvent(Tick tick, long sequence, boolean endOfBatch) {
        store.onTick(tick);
        latency.recordSince(tick.receivedNanos());
    }

    /** Время обработки тика. */
    public Latency latency() { return latency; }
}
