package com.hft.store;

import java.util.concurrent.locks.StampedLock;

/**
 * Стакан одного инструмента, целиком в памяти.
 *
 * Хранение: два плоских массива double[] на сторону (цены и объёмы), без TreeMap и аллокаций
 * на обновление. Биды по убыванию (лучший первый), аски по возрастанию.
 *
 * Потоки: пишет сетевой поток фида, читают стратегия, риск, бумажный движок и админка.
 * Чтобы читатель никогда не увидел полуобновлённый стакан (новые биды со старыми асками,
 * счётчик от одного снимка, цены от другого), запись идёт под {@link StampedLock}, а чтение —
 * оптимистичное: без блокировки, с проверкой версии; только если писатель вмешался,
 * чтение повторяется под read-lock. В обычном случае это пара volatile-чтений — дешевле мьютекса.
 *
 * Каждый метод согласован сам по себе. Если нужно несколько значений из ОДНОГО снимка
 * (bid и ask вместе, весь стакан для симуляции) — {@link #readTop(double[])} и {@link #copyTo(Levels)}.
 */
public final class OrderBook {

    private final String symbol;
    private final int depth;
    private final StampedLock lock = new StampedLock();

    private final double[] bidPrices;
    private final double[] bidQtys;
    private final double[] askPrices;
    private final double[] askQtys;

    private int bidCount;
    private int askCount;

    private long lastUpdateMs;
    private long lastUpdateId;

    public OrderBook(String symbol, int depth) {
        this.symbol = symbol;
        this.depth = depth;
        this.bidPrices = new double[depth];
        this.bidQtys = new double[depth];
        this.askPrices = new double[depth];
        this.askQtys = new double[depth];
    }

    /** Полная замена снимка стакана. Массивы уже отсортированы, просто копируем — без аллокаций. */
    public void applySnapshot(double[] bp, double[] bq, int bn,
                              double[] ap, double[] aq, int an,
                              long updateId, long timeMs) {
        long st = lock.writeLock();
        try {
            bidCount = Math.min(bn, depth);
            askCount = Math.min(an, depth);
            System.arraycopy(bp, 0, bidPrices, 0, bidCount);
            System.arraycopy(bq, 0, bidQtys, 0, bidCount);
            System.arraycopy(ap, 0, askPrices, 0, askCount);
            System.arraycopy(aq, 0, askQtys, 0, askCount);
            this.lastUpdateId = updateId;
            this.lastUpdateMs = timeMs;
        } finally {
            lock.unlockWrite(st);
        }
    }

    // ---------- согласованные снимки ----------

    /** out = {bid, bidQty, ask, askQty} из одного снимка. false — стакан пуст с какой-то стороны. */
    public boolean readTop(double[] out) {
        long st = lock.tryOptimisticRead();
        boolean ok = topRaw(out);
        if (lock.validate(st)) return ok;
        st = lock.readLock();
        try { return topRaw(out); } finally { lock.unlockRead(st); }
    }

    private boolean topRaw(double[] out) {
        int bn = bidCount, an = askCount;
        out[0] = bn > 0 ? bidPrices[0] : Double.NaN;
        out[1] = bn > 0 ? bidQtys[0] : 0;
        out[2] = an > 0 ? askPrices[0] : Double.NaN;
        out[3] = an > 0 ? askQtys[0] : 0;
        return bn > 0 && an > 0;
    }

    /** Копия всех уровней одного снимка в переиспользуемый буфер (для бумажного движка, симуляций). */
    public void copyTo(Levels into) {
        into.ensure(depth);
        long st = lock.tryOptimisticRead();
        copyRaw(into);
        if (lock.validate(st)) return;
        st = lock.readLock();
        try { copyRaw(into); } finally { lock.unlockRead(st); }
    }

    private void copyRaw(Levels into) {
        int bn = Math.min(bidCount, depth), an = Math.min(askCount, depth);
        System.arraycopy(bidPrices, 0, into.bidPrices, 0, bn);
        System.arraycopy(bidQtys, 0, into.bidQtys, 0, bn);
        System.arraycopy(askPrices, 0, into.askPrices, 0, an);
        System.arraycopy(askQtys, 0, into.askQtys, 0, an);
        into.bidCount = bn;
        into.askCount = an;
    }

    /** Переиспользуемый буфер уровней. */
    public static final class Levels {
        public double[] bidPrices = new double[0], bidQtys = new double[0], askPrices = new double[0], askQtys = new double[0];
        public int bidCount, askCount;

        void ensure(int n) {
            if (bidPrices.length >= n) return;
            bidPrices = new double[n]; bidQtys = new double[n]; askPrices = new double[n]; askQtys = new double[n];
        }
    }

    // ---------- чтение (каждый метод — из одного согласованного снимка) ----------

    public double bestBid() {
        long st = lock.tryOptimisticRead();
        double r = bidCount > 0 ? bidPrices[0] : Double.NaN;
        if (lock.validate(st)) return r;
        st = lock.readLock();
        try { return bidCount > 0 ? bidPrices[0] : Double.NaN; } finally { lock.unlockRead(st); }
    }

    public double bestBidQty() {
        long st = lock.tryOptimisticRead();
        double r = bidCount > 0 ? bidQtys[0] : 0;
        if (lock.validate(st)) return r;
        st = lock.readLock();
        try { return bidCount > 0 ? bidQtys[0] : 0; } finally { lock.unlockRead(st); }
    }

    public double bestAsk() {
        long st = lock.tryOptimisticRead();
        double r = askCount > 0 ? askPrices[0] : Double.NaN;
        if (lock.validate(st)) return r;
        st = lock.readLock();
        try { return askCount > 0 ? askPrices[0] : Double.NaN; } finally { lock.unlockRead(st); }
    }

    public double bestAskQty() {
        long st = lock.tryOptimisticRead();
        double r = askCount > 0 ? askQtys[0] : 0;
        if (lock.validate(st)) return r;
        st = lock.readLock();
        try { return askCount > 0 ? askQtys[0] : 0; } finally { lock.unlockRead(st); }
    }

    /** Середина спреда — обычно используется как "справедливая" цена. */
    public double midPrice() {
        long st = lock.tryOptimisticRead();
        double r = midRaw();
        if (lock.validate(st)) return r;
        st = lock.readLock();
        try { return midRaw(); } finally { lock.unlockRead(st); }
    }

    /** Спред в процентах от mid — так удобнее сравнивать разные инструменты. */
    public double spreadPercent() {
        long st = lock.tryOptimisticRead();
        double r = spreadPercentRaw();
        if (lock.validate(st)) return r;
        st = lock.readLock();
        try { return spreadPercentRaw(); } finally { lock.unlockRead(st); }
    }

    /** Взвешенная середина: учитывает объёмы на лучших уровнях. */
    public double microPrice() {
        long st = lock.tryOptimisticRead();
        double r = microRaw();
        if (lock.validate(st)) return r;
        st = lock.readLock();
        try { return microRaw(); } finally { lock.unlockRead(st); }
    }

    /** Дисбаланс сторон в первых levels уровнях: от -1 (одни продавцы) до +1 (одни покупатели). */
    public double imbalance(int levels) {
        long st = lock.tryOptimisticRead();
        double r = imbalanceRaw(levels);
        if (lock.validate(st)) return r;
        st = lock.readLock();
        try { return imbalanceRaw(levels); } finally { lock.unlockRead(st); }
    }

    /** Средняя цена рыночной покупки qty — проход по аскам. NaN, если стакана не хватило. */
    public double estimateBuyPrice(double qty) {
        long st = lock.tryOptimisticRead();
        double r = walkRaw(askPrices, askQtys, askCount, qty);
        if (lock.validate(st)) return r;
        st = lock.readLock();
        try { return walkRaw(askPrices, askQtys, askCount, qty); } finally { lock.unlockRead(st); }
    }

    /** Ожидаемое проскальзывание рыночного ордера в процентах от mid. */
    public double estimateSlippagePercent(double qty, boolean isBuy) {
        long st = lock.tryOptimisticRead();
        double r = slippageRaw(qty, isBuy);
        if (lock.validate(st)) return r;
        st = lock.readLock();
        try { return slippageRaw(qty, isBuy); } finally { lock.unlockRead(st); }
    }

    public int bidCount() {
        long st = lock.tryOptimisticRead();
        int r = bidCount;
        if (lock.validate(st)) return r;
        st = lock.readLock();
        try { return bidCount; } finally { lock.unlockRead(st); }
    }

    public int askCount() {
        long st = lock.tryOptimisticRead();
        int r = askCount;
        if (lock.validate(st)) return r;
        st = lock.readLock();
        try { return askCount; } finally { lock.unlockRead(st); }
    }

    public long lastUpdateMs() {
        long st = lock.tryOptimisticRead();
        long r = lastUpdateMs;
        if (lock.validate(st)) return r;
        st = lock.readLock();
        try { return lastUpdateMs; } finally { lock.unlockRead(st); }
    }

    public boolean isReady() {
        long st = lock.tryOptimisticRead();
        boolean r = bidCount > 0 && askCount > 0;
        if (lock.validate(st)) return r;
        st = lock.readLock();
        try { return bidCount > 0 && askCount > 0; } finally { lock.unlockRead(st); }
    }

    public double bidPriceAt(int level) {
        long st = lock.tryOptimisticRead();
        double r = level >= 0 && level < bidCount ? bidPrices[level] : Double.NaN;
        if (lock.validate(st)) return r;
        st = lock.readLock();
        try { return level >= 0 && level < bidCount ? bidPrices[level] : Double.NaN; } finally { lock.unlockRead(st); }
    }

    public double bidQtyAt(int level) {
        long st = lock.tryOptimisticRead();
        double r = level >= 0 && level < bidCount ? bidQtys[level] : 0;
        if (lock.validate(st)) return r;
        st = lock.readLock();
        try { return level >= 0 && level < bidCount ? bidQtys[level] : 0; } finally { lock.unlockRead(st); }
    }

    public double askPriceAt(int level) {
        long st = lock.tryOptimisticRead();
        double r = level >= 0 && level < askCount ? askPrices[level] : Double.NaN;
        if (lock.validate(st)) return r;
        st = lock.readLock();
        try { return level >= 0 && level < askCount ? askPrices[level] : Double.NaN; } finally { lock.unlockRead(st); }
    }

    public String symbol() { return symbol; }

    /** Возраст данных. Если больше пары секунд — стакан протух. */
    public long ageMs() { return System.currentTimeMillis() - lastUpdateMs(); }

    // ---------- «сырые» вычисления: вызываются только из оптимистичного/заблокированного чтения ----------
    // Внутри возможны мусорные значения при гонке с писателем, но индексы всегда < depth, а результат
    // отбрасывается, если validate() не прошёл.

    private double midRaw() {
        if (bidCount == 0 || askCount == 0) return Double.NaN;
        return (bidPrices[0] + askPrices[0]) / 2.0;
    }

    private double spreadRaw() {
        if (bidCount == 0 || askCount == 0) return Double.NaN;
        return askPrices[0] - bidPrices[0];
    }

    private double spreadPercentRaw() {
        double mid = midRaw();
        if (Double.isNaN(mid) || mid == 0) return Double.NaN;
        return spreadRaw() / mid * 100.0;
    }

    private double microRaw() {
        if (bidCount == 0 || askCount == 0) return Double.NaN;
        double bq = bidQtys[0], aq = askQtys[0];
        double total = bq + aq;
        if (total == 0) return midRaw();
        return (bidPrices[0] * aq + askPrices[0] * bq) / total;
    }

    private double imbalanceRaw(int levels) {
        int n = Math.min(Math.min(levels, depth), Math.min(bidCount, askCount));
        double bidVol = 0, askVol = 0;
        for (int i = 0; i < n; i++) { bidVol += bidQtys[i]; askVol += askQtys[i]; }
        double total = bidVol + askVol;
        return total == 0 ? 0 : (bidVol - askVol) / total;
    }

    private double walkRaw(double[] prices, double[] qtys, int count, double qty) {
        double remaining = qty, cost = 0;
        for (int i = 0, n = Math.min(count, depth); i < n && remaining > 0; i++) {
            double take = Math.min(remaining, qtys[i]);
            cost += take * prices[i];
            remaining -= take;
        }
        if (remaining > 0 || qty <= 0) return Double.NaN;
        return cost / qty;
    }

    private double slippageRaw(double qty, boolean isBuy) {
        double mid = midRaw();
        if (Double.isNaN(mid)) return Double.NaN;
        double exec = isBuy ? walkRaw(askPrices, askQtys, askCount, qty) : walkRaw(bidPrices, bidQtys, bidCount, qty);
        if (Double.isNaN(exec)) return Double.NaN;
        return Math.abs(exec - mid) / mid * 100.0;
    }

    @Override
    public String toString() {
        return String.format("%s bid=%.2f(%.4f) ask=%.2f(%.4f) spread=%.4f%% imb=%.2f",
                symbol, bestBid(), bestBidQty(), bestAsk(), bestAskQty(), spreadPercent(), imbalance(5));
    }
}
