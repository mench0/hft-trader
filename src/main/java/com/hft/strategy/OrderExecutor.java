package com.hft.strategy;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Отправка ордеров вне потока конвейера тиков.
 *
 * <p>Стратегия работает в потоке Disruptor. Если ждать в нём ответа биржи
 * (REST до 10 с, WebSocket до 5 с, паузы лимитера, дочитывание статуса),
 * тики всех символов биржи встанут в очередь, и стратегия начнёт торговать
 * по устаревшим ценам.
 *
 * <p>Поэтому стратегия только ставит задачу в этот исполнитель и сразу
 * возвращается.
 *
 * <p>Правила:
 * <ul>
 *   <li>
 *     <b>Один символ — одна задача одновременно.</b>
 *     Не более одного «ордера в полёте» на символ. Пока ответ не получен,
 *     стратегия не принимает по этому символу новых торговых решений.
 *   </li>
 *   <li>
 *     <b>Разные символы выполняются параллельно.</b>
 *     Используется небольшой пул потоков, чтобы медленный ответ по одному
 *     символу не блокировал обработку других.
 *   </li>
 *   <li>
 *     <b>Потоки завершаются после простоя.</b>
 *     После перезапуска биржи пул не накапливает неиспользуемые потоки.
 *   </li>
 * </ul>
 */
public final class OrderExecutor {

    /** Логгер исполнителя. */
    private static final Logger log = LoggerFactory.getLogger(OrderExecutor.class);

    /** Имя для потоков и логов. */
    private final String name;
    /** Пул потоков с ограниченной очередью. */
    private final ThreadPoolExecutor pool;
    /** Символы с ордером «в полёте». */
    private final Set<String> inFlight = ConcurrentHashMap.newKeySet();
    /** Счётчики: отправлено, отклонено (символ занят или очередь полна), упало с ошибкой. */
    private final AtomicLong submitted = new AtomicLong(), rejected = new AtomicLong(), failed = new AtomicLong();

    /**
     * @param name имя (биржа и стратегия)
     * @param threads сколько ордеров могут идти одновременно
     */
    public OrderExecutor(String name, int threads) {
        this.name = name;
        AtomicLong n = new AtomicLong();
        this.pool = new ThreadPoolExecutor(threads, threads, 30, TimeUnit.SECONDS, new LinkedBlockingQueue<>(1024), r -> {
            Thread t = new Thread(r, "orders-" + name + "-" + n.incrementAndGet());
            t.setDaemon(true);
            return t;
        });
        this.pool.allowCoreThreadTimeOut(true);
    }

    /** Поставить задачу по символу. false — по символу уже идёт ордер (или очередь переполнена), задача не принята. */
    public boolean submit(String symbol, Runnable task) {
        if (!inFlight.add(symbol)) { rejected.incrementAndGet(); return false; }
        try {
            pool.execute(() -> {
                try { task.run(); }
                catch (Throwable e) { failed.incrementAndGet(); log.error("[{}] ошибка задачи по {}: {}", name, symbol, e.toString()); }
                finally { inFlight.remove(symbol); }
            });
            submitted.incrementAndGet();
            return true;
        } catch (RejectedExecutionException e) {
            inFlight.remove(symbol);
            rejected.incrementAndGet();
            log.warn("[{}] очередь ордеров переполнена — {} пропущен", name, symbol);
            return false;
        }
    }

    /** По символу уже идёт ордер — новые решения по нему не принимаются. */
    public boolean isBusy(String symbol) { return inFlight.contains(symbol); }

    /** Дождаться, пока все поставленные задачи завершатся (не дольше timeoutMs). */
    public boolean drain(long timeoutMs) {
        long until = System.currentTimeMillis() + timeoutMs;
        while (!inFlight.isEmpty() && System.currentTimeMillis() < until) {
            try { Thread.sleep(10); } catch (InterruptedException e) { Thread.currentThread().interrupt(); return false; }
        }
        return inFlight.isEmpty();
    }

    /** Счётчики и загрузка пула для админки. */
    public java.util.Map<String, Object> stats() {
        return java.util.Map.of("submitted", submitted.get(), "rejectedBusy", rejected.get(), "failed", failed.get(),
                "inFlight", inFlight.size(), "queued", pool.getQueue().size());
    }
}
