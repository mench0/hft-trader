package com.hft.engine;

import com.hft.model.Tick;
import com.hft.store.MarketDataStore;
import com.lmax.disruptor.EventHandler;

/**
 * Вторая стадия конвейера: собственно торговая логика.
 *
 * <p>Disruptor вызывает {@code onEvent} для каждого тика после того,
 * как {@code MarketDataHandler} записал его в память. Поэтому внутри
 * метода можно обращаться к {@code store} и видеть актуальное состояние
 * рыночных данных.
 *
 * <p><b>Важно:</b> метод выполняется непосредственно в потоке конвейера.
 * Любая операция внутри него задерживает обработку следующих тиков.
 * Блокирующие вызовы, в том числе отправку ордера по HTTP, в идеале следует
 * выполнять в отдельном потоке.
 *
 * <p>На начальном этапе синхронное выполнение допустимо при бюджете
 * около {@code 5 ms} и нечастых сделках.
 */
public abstract class Strategy implements EventHandler<Tick> {

    /** Рыночные данные биржи. */
    protected final MarketDataStore market;
    /** Отправка ордеров через проверки риска. */
    protected final OrderService orders;
    /** Имя стратегии для логов и метрик. */
    private final String name;
    /** Общий выключатель торговли (включается /trading/start). */
    private volatile boolean enabled = false;

    /**
     * @param name имя для логов
     * @param market рыночные данные биржи
     * @param orders сервис ордеров биржи
     */
    protected Strategy(String name, MarketDataStore market, OrderService orders) {
        this.name = name;
        this.market = market;
        this.orders = orders;
    }

    /** Вызывается конвейером на каждый тик; ошибки стратегии логируются и не останавливают конвейер. */
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

    /** Имя стратегии. */
    public String name() { return name; }
    /** Торговля разрешена. */
    public boolean isEnabled() { return enabled; }
    /** Разрешить торговлю. */
    public void enable() { this.enabled = true; }
    /** Запретить торговлю (позиции не закрываются). */
    public void disable() { this.enabled = false; }
}
