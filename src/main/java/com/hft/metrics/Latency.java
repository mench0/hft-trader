package com.hft.metrics;

import org.HdrHistogram.Histogram;

/**
 * Замер латентности по перцентилям.
 *
 * Среднее время бесполезно: если 99 запросов заняли 1 мс, а один — 500 мс,
 * среднее покажет 6 мс и скроет проблему. Смотреть нужно p99 и p999.
 *
 * Histogram не аллоцирует память при записи, поэтому его безопасно
 * вызывать в горячем пути.
 */
public final class Latency {

    /** Название замера для логов. */
    private final String name;
    /** Гистограмма от 1 нс до 60 с с точностью 3 значащих цифры. */
    private final Histogram histogram = new Histogram(1, 60_000_000_000L, 3);

    /** @param name название замера */
    public Latency(String name) {
        this.name = name;
    }

    /** Учесть длительность в наносекундах. */
    public void record(long nanos) {
        if (nanos > 0 && nanos < 60_000_000_000L) {
            histogram.recordValue(nanos);
        }
    }

    /** Учесть время от startNanos (System.nanoTime) до сейчас. */
    public void recordSince(long startNanos) {
        record(System.nanoTime() - startNanos);
    }

    /** Медиана, мкс. */
    public long p50Micros() { return histogram.getValueAtPercentile(50) / 1000; }
    /** 99-й перцентиль, мкс. */
    public long p99Micros() { return histogram.getValueAtPercentile(99) / 1000; }
    /** Сколько замеров. */
    public long count() { return histogram.getTotalCount(); }

    /** Начать замер заново. */
    public void reset() {
        histogram.reset();
    }
}
