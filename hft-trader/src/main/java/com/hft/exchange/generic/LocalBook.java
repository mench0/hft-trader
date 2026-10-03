package com.hft.exchange.generic;

import java.util.Arrays;

/**
 * Локальный стакан одного символа для бирж, которые шлют изменения уровней.
 * Отсортированные массивы примитивов + двоичный поиск: без TreeMap, без упаковки double в объекты,
 * без аллокаций в установившемся режиме. Не потокобезопасен — живёт в потоке WS.
 */
final class LocalBook {
    private double[] bp = new double[64], bq = new double[64];   // биды по убыванию
    private double[] ap = new double[64], aq = new double[64];   // аски по возрастанию
    private int bn, an;

    void clear() { bn = 0; an = 0; }

    void applyBid(double p, double q) {
        int i = find(bp, bn, p, true);
        if (i >= 0) {
            if (q <= 0) { System.arraycopy(bp, i + 1, bp, i, bn - i - 1); System.arraycopy(bq, i + 1, bq, i, bn - i - 1); bn--; }
            else bq[i] = q;
            return;
        }
        if (q <= 0) return;
        int at = -i - 1;
        if (bn == bp.length) { bp = Arrays.copyOf(bp, bn * 2); bq = Arrays.copyOf(bq, bn * 2); }
        System.arraycopy(bp, at, bp, at + 1, bn - at); System.arraycopy(bq, at, bq, at + 1, bn - at);
        bp[at] = p; bq[at] = q; bn++;
    }

    void applyAsk(double p, double q) {
        int i = find(ap, an, p, false);
        if (i >= 0) {
            if (q <= 0) { System.arraycopy(ap, i + 1, ap, i, an - i - 1); System.arraycopy(aq, i + 1, aq, i, an - i - 1); an--; }
            else aq[i] = q;
            return;
        }
        if (q <= 0) return;
        int at = -i - 1;
        if (an == ap.length) { ap = Arrays.copyOf(ap, an * 2); aq = Arrays.copyOf(aq, an * 2); }
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

    boolean isReady() { return bn > 0 && an > 0; }
    boolean isCrossed() { return isReady() && bp[0] >= ap[0]; }
    double bestBid() { return bp[0]; }
    double bestAsk() { return ap[0]; }

    /** Верхние уровни в переданные массивы; возвращает число скопированных уровней. */
    int topBids(double[] px, double[] qty) { int k = Math.min(px.length, bn); System.arraycopy(bp, 0, px, 0, k); System.arraycopy(bq, 0, qty, 0, k); return k; }
    int topAsks(double[] px, double[] qty) { int k = Math.min(px.length, an); System.arraycopy(ap, 0, px, 0, k); System.arraycopy(aq, 0, qty, 0, k); return k; }
}
