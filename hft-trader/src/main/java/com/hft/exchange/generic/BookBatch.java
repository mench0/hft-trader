package com.hft.exchange.generic;

/**
 * Переиспользуемый буфер одного сообщения стакана: диалект заполняет его при разборе,
 * фид применяет к локальному стакану. Массивы растут один раз и дальше переиспользуются —
 * на каждое сообщение ничего не создаётся.
 */
public final class BookBatch {
    String venue;           // null — сообщение не про стакан
    boolean snapshot;
    long tsMs;
    double[] bp = new double[64], bq = new double[64], ap = new double[64], aq = new double[64];
    int bn, an;

    /** Известные имена символов биржи: сравниваем с символами в буфере, не создавая строк. */
    private volatile String[] known = new String[0];

    void setKnown(java.util.Collection<String> venues) { known = venues.toArray(new String[0]); }

    void reset() { venue = null; snapshot = false; tsMs = 0; bn = 0; an = 0; }

    void bid(double p, double q) {
        if (bn == bp.length) { bp = java.util.Arrays.copyOf(bp, bn * 2); bq = java.util.Arrays.copyOf(bq, bn * 2); }
        bp[bn] = p; bq[bn++] = q;
    }

    void ask(double p, double q) {
        if (an == ap.length) { ap = java.util.Arrays.copyOf(ap, an * 2); aq = java.util.Arrays.copyOf(aq, an * 2); }
        ap[an] = p; aq[an++] = q;
    }

    /** Строка символа из символов буфера: известный — тот же объект String, неизвестный — новая строка. */
    String resolve(char[] c, int off, int len) {
        for (String k : known) {
            if (k.length() != len) continue;
            boolean eq = true;
            for (int i = 0; i < len; i++) if (k.charAt(i) != c[off + i]) { eq = false; break; }
            if (eq) return k;
        }
        return new String(c, off, len);
    }

    String resolve(String s) {
        for (String k : known) if (k.equals(s)) return k;
        return s;
    }
}
