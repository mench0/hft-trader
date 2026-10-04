package com.hft.engine;

import com.hft.model.Tick;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

import java.util.concurrent.ThreadFactory;

/**
 * Кольцевой буфер между сетевым потоком и обработчиками.
 *
 * Зачем не BlockingQueue: там на каждую операцию берётся блокировка,
 * а объекты аллоцируются заново. Disruptor выделяет все Tick один раз
 * при старте и переиспользует их по кругу — в горячем пути нет ни
 * блокировок, ни работы для сборщика мусора.
 *
 * Стратегия ожидания:
 *   BusySpinWaitStrategy — минимальная задержка, но ядро загружено на 100%.
 *     Имеет смысл только на выделенном железе с изолированными ядрами.
 *   YieldingWaitStrategy — почти та же задержка, но поток отдаёт квант
 *     времени. На обычном VPS это правильный выбор, он тут и стоит.
 */
public final class TickPipeline {

    private static final int RING_SIZE = 4096; // степень двойки

    /** Disruptor: кольцо тиков и потоки обработчиков. */
    private final Disruptor<Tick> disruptor;
    /** Кольцевой буфер, в который публикуются тики. */
    private final RingBuffer<Tick> ring;

    /**
     * first обрабатывает тик раньше всех (запись в MarketDataStore), остальные — после него и параллельно
     * между собой: стратегия не должна увидеть тик, который ещё не записан в окно цен.
     */
    @SafeVarargs
    public TickPipeline(EventHandler<Tick> first, EventHandler<Tick>... after) {
        ThreadFactory tf = r -> {
            Thread t = new Thread(r, "tick-pipeline");
            t.setDaemon(true);
            return t;
        };

        this.disruptor = new Disruptor<>(
                Tick::new,
                RING_SIZE,
                tf,
                ProducerType.MULTI,          // несколько WebSocket-соединений пишут параллельно
                new YieldingWaitStrategy()
        );

        var group = disruptor.handleEventsWith(first);
        if (after.length > 0) group.then(after);
        this.ring = disruptor.getRingBuffer();
    }

    /** Запустить потоки обработчиков. */
    public void start() {
        disruptor.start();
    }

    /** Дождаться обработки опубликованных тиков и остановить потоки. */
    public void shutdown() {
        disruptor.shutdown();
    }

    /**
     * Публикация тика. Объект не создаётся — берётся из кольца
     * и перезаписывается на месте.
     */
    public void publish(String symbol, double price, double qty,
                        boolean buyerIsMaker, long exchangeTimeMs, long receivedNanos) {
        long seq = ring.next();
        try {
            Tick tick = ring.get(seq);
            tick.set(symbol, price, qty, buyerIsMaker, exchangeTimeMs, receivedNanos);
        } finally {
            ring.publish(seq);
        }
    }

    /** Сколько места осталось в кольце. Если стремится к нулю — обработчик не успевает. */
    public long remainingCapacity() {
        return ring.remainingCapacity();
    }
}
