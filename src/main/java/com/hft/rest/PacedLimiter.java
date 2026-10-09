package com.hft.rest;

/**
 * Ограничитель частоты запросов: между запросами не меньше 1/rps секунды,
 * а после ответа «слишком часто» все запросы блокируются на заданное время.
 * Если ждать пришлось бы дольше maxWaitMs — бросает ApiException вместо ожидания,
 * чтобы поток стратегии не завис.
 */
public final class PacedLimiter {
    /** Минимальный интервал между запросами, нс. */
    private final long gapNanos;
    /** Дольше ждать нельзя — отказ. */
    private final long maxWaitMs;
    /** Момент, когда можно отправить следующий запрос (nanoTime). */
    private long nextSlot;
    /** До какого момента запросы запрещены после ответа «слишком часто». */
    private volatile long blockedUntilMs;

    /**
     * @param requestsPerSec сколько запросов в секунду
     * @param maxWaitMs предел ожидания в очереди
     */
    public PacedLimiter(double requestsPerSec, long maxWaitMs) {
        this.gapNanos = (long) (1_000_000_000d / Math.max(0.1, requestsPerSec));
        this.maxWaitMs = maxWaitMs;
    }

    /** Запретить запросы на ms (после ответа «слишком часто»). */
    public void blockFor(long ms) { blockedUntilMs = Math.max(blockedUntilMs, System.currentTimeMillis() + ms); }

    /** Сколько ещё мс запросы запрещены. */
    public long blockedForMs() { return Math.max(0, blockedUntilMs - System.currentTimeMillis()); }

    /** Дождаться своего слота; LocalThrottleException, если запросы приостановлены или ждать дольше maxWaitMs. */
    public void acquire() throws InterruptedException {
        long blocked = blockedForMs();
        if (blocked > 0) throw new LocalThrottleException(LocalThrottleException.Reason.PAUSED, blocked, "запросы приостановлены ещё на " + blocked + " мс");
        long waitNs;
        synchronized (this) {
            long now = System.nanoTime();
            long slot = Math.max(now, nextSlot);
            waitNs = slot - now;
            // слот занимаем только если реально будем ждать — отказ не должен съедать пропускную способность
            if (waitNs / 1_000_000 > maxWaitMs)
                throw new LocalThrottleException(LocalThrottleException.Reason.QUEUE_FULL, waitNs / 1_000_000, "очередь запросов переполнена");
            nextSlot = slot + gapNanos;
        }
        if (waitNs > 0) Thread.sleep(waitNs / 1_000_000, (int) (waitNs % 1_000_000));
    }
}
