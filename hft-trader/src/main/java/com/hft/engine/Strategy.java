package com.hft.engine;

import com.hft.model.Tick;
import com.hft.store.MarketDataStore;
import com.lmax.disruptor.EventHandler;

/**
 * Вторая стадия конвейера: собственно торговая логика.
 *
 * Disruptor вызывает onEvent для каждого тика уже после того,
 * как MarketDataHandler записал его в память. Значит, внутри
 * можно пользоваться store и видеть актуальное состояние.
 *
 * Важно: этот метод выполняется в потоке конвейера. Всё, что здесь
 * происходит, задерживает обработку следующих тиков. Блокирующие
 * вызовы (в том числе отправка ордера по HTTP) в идеале должны уходить
 * в отдельный поток. Для начала можно оставить синхронно — при 5 мс
 * бюджете и нечастых сделках это приемлемо.
 */
public abstract class Strategy implements EventHandler<Tick> {

    protected final MarketDataStore market;
    protected final OrderService orders;
    private final String name;
    private volatile boolean enabled = false;

    protected Strategy(String name, MarketDataStore market, OrderService orders) {
        this.name = name;
        this.market = market;
        this.orders = orders;
    }

    @Override
    public final void onEvent(Tick tick, long sequence, boolean endOfBatch) {
        if (!enabled) return;
        try {
            onTick(tick);
        } catch (Exception e) {
            org.slf4j.LoggerFactory.getLogger(getClass())
                    .error("Ошибка в стратегии {}", name, e);
        }
    }

    /** Здесь пишется торговая логика. */
    protected abstract void onTick(Tick tick);

    public String name() { return name; }
    public boolean isEnabled() { return enabled; }
    public void enable() { this.enabled = true; }
    public void disable() { this.enabled = false; }
}
