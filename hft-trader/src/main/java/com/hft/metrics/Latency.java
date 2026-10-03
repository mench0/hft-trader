package com.hft.metrics;

import org.HdrHistogram.Histogram;
import org.slf4j.Logger;

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

    private final String name;
    private final Histogram histogram = new Histogram(1, 60_000_000_000L, 3);

    public Latency(String name) {
        this.name = name;
    }

    public void record(long nanos) {
        if (nanos > 0 && nanos < 60_000_000_000L) {
            histogram.recordValue(nanos);
        }
    }

    public void recordSince(long startNanos) {
        record(System.nanoTime() - startNanos);
    }

    public void log(Logger logger) {
        if (histogram.getTotalCount() == 0) return;
        logger.info("{} (мкс): p50={} p99={} p999={} max={} n={}",
                name,
                histogram.getValueAtPercentile(50) / 1000,
                histogram.getValueAtPercentile(99) / 1000,
                histogram.getValueAtPercentile(99.9) / 1000,
                histogram.getMaxValue() / 1000,
                histogram.getTotalCount());
    }

    public long p50Micros() { return histogram.getValueAtPercentile(50) / 1000; }
    public long p99Micros() { return histogram.getValueAtPercentile(99) / 1000; }
    public long p999Micros() { return histogram.getValueAtPercentile(99.9) / 1000; }
    public long maxMicros() { return histogram.getMaxValue() / 1000; }
    public long count() { return histogram.getTotalCount(); }

    public void reset() {
        histogram.reset();
    }
}
