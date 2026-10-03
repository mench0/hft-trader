package com.hft.store;

/**
 * Скользящее окно последних N цен для одного инструмента.
 *
 * Реализовано кольцевым буфером на double[]: при добавлении новой цены
 * старая затирается, аллокаций нет вообще.
 *
 * Среднее и дисперсия считаются инкрементально (алгоритм Уэлфорда),
 * а не пересчитываются по всему окну на каждом тике — иначе на окне
 * в 5000 элементов это 5000 операций на каждую сделку.
 */
public final class PriceWindow {

    private final double[] buffer;
    private final int capacity;
    private int writeIndex;
    private int size;

    // Инкрементальная статистика
    private double sum;
    private double sumOfSquares;

    public PriceWindow(int capacity) {
        this.capacity = capacity;
        this.buffer = new double[capacity];
    }

    /** Добавить цену. Если окно заполнено, самая старая вытесняется. */
    public void add(double value) {
        if (size == capacity) {
            double evicted = buffer[writeIndex];
            sum -= evicted;
            sumOfSquares -= evicted * evicted;
        } else {
            size++;
        }
        buffer[writeIndex] = value;
        sum += value;
        sumOfSquares += value * value;
        writeIndex = (writeIndex + 1) % capacity;
    }

    public double mean() {
        return size == 0 ? Double.NaN : sum / size;
    }

    /** Стандартное отклонение по выборке. */
    public double stdDev() {
        if (size < 2) return Double.NaN;
        double m = sum / size;
        double variance = (sumOfSquares - size * m * m) / (size - 1);
        return variance > 0 ? Math.sqrt(variance) : 0;
    }

    /**
     * Z-score текущего значения: на сколько сигм оно отклонилось от среднего.
     * |z| > 2 — заметное отклонение, |z| > 3 — экстремальное.
     * Это основа mean-reversion и статистического арбитража.
     */
    public double zScore(double value) {
        double sd = stdDev();
        if (Double.isNaN(sd) || sd == 0) return 0;
        return (value - mean()) / sd;
    }

    /** Z-score последнего добавленного значения. */
    public double currentZScore() {
        if (size == 0) return 0;
        int lastIdx = (writeIndex - 1 + capacity) % capacity;
        return zScore(buffer[lastIdx]);
    }

    public double last() {
        if (size == 0) return Double.NaN;
        return buffer[(writeIndex - 1 + capacity) % capacity];
    }

    public double min() {
        if (size == 0) return Double.NaN;
        double m = Double.MAX_VALUE;
        for (int i = 0; i < size; i++) m = Math.min(m, buffer[i]);
        return m;
    }

    public double max() {
        if (size == 0) return Double.NaN;
        double m = -Double.MAX_VALUE;
        for (int i = 0; i < size; i++) m = Math.max(m, buffer[i]);
        return m;
    }

    /** Изменение от самого старого значения к последнему, в процентах. */
    public double changePercent() {
        if (size < 2) return 0;
        int oldestIdx = size == capacity ? writeIndex : 0;
        double oldest = buffer[oldestIdx];
        if (oldest == 0) return 0;
        return (last() - oldest) / oldest * 100.0;
    }

    public int size() { return size; }

    /** Окно должно заполниться хотя бы наполовину, иначе статистика недостоверна. */
    public boolean isWarmedUp() { return size >= capacity / 2; }
}
