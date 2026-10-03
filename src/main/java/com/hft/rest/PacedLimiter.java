package com.hft.rest;

/**
 * Ограничитель частоты запросов: между запросами не меньше 1/rps секунды,
 * а после ответа «слишком часто» все запросы блокируются на заданное время.
 * Если ждать пришлось бы дольше maxWaitMs — бросает ApiException вместо ожидания,
 * чтобы поток стратегии не завис.
 */
public final class PacedLimiter {
    private final long gapNanos;
    private final long maxWaitMs;
    private long nextSlot;
    private volatile long blockedUntilMs;

    public PacedLimiter(double requestsPerSec, long maxWaitMs) {
        this.gapNanos = (long) (1_000_000_000d / Math.max(0.1, requestsPerSec));
        this.maxWaitMs = maxWaitMs;
    }

    public void blockFor(long ms) { blockedUntilMs = Math.max(blockedUntilMs, System.currentTimeMillis() + ms); }

    public long blockedForMs() { return Math.max(0, blockedUntilMs - System.currentTimeMillis()); }

    public void acquire() throws InterruptedException {
        long blocked = blockedForMs();
        if (blocked > 0) throw new ApiException(429, "LOCAL", "запросы приостановлены ещё на " + blocked + " мс", true);
        long waitNs;
        synchronized (this) {
            long now = System.nanoTime();
            long slot = Math.max(now, nextSlot);
            waitNs = slot - now;
            // слот занимаем только если реально будем ждать — отказ не должен съедать пропускную способность
            if (waitNs / 1_000_000 > maxWaitMs) throw new ApiException(429, "LOCAL", "очередь запросов переполнена", true);
            nextSlot = slot + gapNanos;
        }
        if (waitNs > 0) Thread.sleep(waitNs / 1_000_000, (int) (waitNs % 1_000_000));
    }
}
