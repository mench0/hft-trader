package com.hft.exchange.generic;

import java.util.Arrays;

/**
 * Локальный стакан одного символа для бирж, которые шлют изменения уровней.
 * Отсортированные массивы примитивов + двоичный поиск: без TreeMap, без упаковки double в объекты,
 * без аллокаций в установившемся режиме. Не потокобезопасен — живёт в потоке WS.
 *
 * Размер ограничен maxLevels на сторону: биржа присылает изменения далёких уровней, но не всегда
 * их удаление, и без предела массивы росли бы всё время работы. Уровни дальше предела от лучшей
 * цены для торговли не нужны и отбрасываются.
 */
public final class LocalBook {
    private final int maxLevels;
    private double[] bp, bq;   // биды по убыванию
    private double[] ap, aq;   // аски по возрастанию
    private int bn, an;

    public LocalBook(int maxLevels) {
        this.maxLevels = Math.max(1, maxLevels);
        int cap = Math.min(64, this.maxLevels);
        bp = new double[cap]; bq = new double[cap]; ap = new double[cap]; aq = new double[cap];
    }

    /** Предел уровней для стакана, из которого берутся верхние depth уровней. */
    public static int levelsFor(int depth) { return Math.max(4 * depth, 200); }

    public void clear() { bn = 0; an = 0; }

    public void applyBid(double p, double q) {
        int i = find(bp, bn, p, true);
        if (i >= 0) {
            if (q <= 0) { System.arraycopy(bp, i + 1, bp, i, bn - i - 1); System.arraycopy(bq, i + 1, bq, i, bn - i - 1); bn--; }
            else bq[i] = q;
            return;
        }
        if (q <= 0) return;
        int at = -i - 1;
        if (at >= maxLevels) return;                                 // дальше предела — не нужен
        if (bn == maxLevels) bn--;                                   // вытесняем самый дальний
        else if (bn == bp.length) { int n = Math.min(bn * 2, maxLevels); bp = Arrays.copyOf(bp, n); bq = Arrays.copyOf(bq, n); }
        System.arraycopy(bp, at, bp, at + 1, bn - at); System.arraycopy(bq, at, bq, at + 1, bn - at);
        bp[at] = p; bq[at] = q; bn++;
    }

    public void applyAsk(double p, double q) {
        int i = find(ap, an, p, false);
        if (i >= 0) {
            if (q <= 0) { System.arraycopy(ap, i + 1, ap, i, an - i - 1); System.arraycopy(aq, i + 1, aq, i, an - i - 1); an--; }
            else aq[i] = q;
            return;
        }
        if (q <= 0) return;
        int at = -i - 1;
        if (at >= maxLevels) return;
        if (an == maxLevels) an--;
        else if (an == ap.length) { int n = Math.min(an * 2, maxLevels); ap = Arrays.copyOf(ap, n); aq = Arrays.copyOf(aq, n); }
        System.arraycopy(ap, at, ap, at + 1, an - at); System.arraycopy(aq, at, aq, at + 1, an - at);
        ap[at] = p; aq[at] = q; an++;
    }

    /** Двоичный поиск: индекс или -(точка вставки)-1. desc — массив по убыванию. */
    private static int find(double[] a, int n, double p, boolean desc) {
        int lo = 0, hi = n - 1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            double v = a[mid];
            if (v == p) return mid;
            boolean goRight = desc ? v > p : v < p;
            if (goRight) lo = mid + 1; else hi = mid - 1;
        }
        return -(lo + 1);
    }

    void apply(BookBatch b) {
        for (int i = 0; i < b.bn; i++) applyBid(b.bp[i], b.bq[i]);
        for (int i = 0; i < b.an; i++) applyAsk(b.ap[i], b.aq[i]);
    }

    public boolean isReady() { return bn > 0 && an > 0; }
    boolean isCrossed() { return isReady() && bp[0] >= ap[0]; }
    double bestBid() { return bp[0]; }
    double bestAsk() { return ap[0]; }

    int bidLevels() { return bn; }
    int askLevels() { return an; }

    /** Верхние уровни в переданные массивы; возвращает число скопированных уровней. */
    public int topBids(double[] px, double[] qty) { int k = Math.min(px.length, bn); System.arraycopy(bp, 0, px, 0, k); System.arraycopy(bq, 0, qty, 0, k); return k; }
    public int topAsks(double[] px, double[] qty) { int k = Math.min(px.length, an); System.arraycopy(ap, 0, px, 0, k); System.arraycopy(aq, 0, qty, 0, k); return k; }
}
