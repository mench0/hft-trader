package com.hft.discovery;

/**
 * Прогон правил MeanReversionStrategy по минутным свечам: вход, когда цена ниже скользящего
 * среднего на entryZ сигм; выход, когда z вернулся к -exitZ, сработал стоп или вышло время.
 * Комиссия — taker с обеих сторон. Стакан (дисбаланс, спред) по свечам не проверить — это
 * делает сама стратегия вживую, поэтому бэктест скорее оптимистичен.
 */
public final class MeanReversionBacktest {

    /** Параметры бэктеста: пороги z, стоп-лосс %, окно и максимальное удержание в свечах. */
    public record Params(double entryZ, double exitZ, double stopLossPct, int window, int maxHoldBars) {}

    /** Итог: сделки, выигрышные, чистый результат %, средняя сделка %, максимальная просадка %, свечей. */
    public record Result(int trades, int wins, double netPct, double avgTradePct, double maxDrawdownPct, int bars) {
        /** Доля выигрышных сделок, %. */
        public double winRate() { return trades == 0 ? 0 : wins * 100.0 / trades; }
    }

    /** Утилитный класс — экземпляры не создаются. */
    private MeanReversionBacktest() {}

    /** Прогнать правила возврата к среднему по ценам закрытия с комиссией тейкера на обеих ногах. */
    public static Result run(double[] closes, Params p, double takerFeePct) {
        int n = closes.length, w = p.window();
        int trades = 0, wins = 0;
        double net = 0, peak = 0, maxDd = 0;
        boolean inPos = false;
        double entry = 0;
        int entryBar = 0;
        double sum = 0, sumSq = 0;
        for (int i = 0; i < n; i++) {
            double c = closes[i];
            if (i >= w) {
                double mean = sum / w;
                double var = Math.max(0, sumSq / w - mean * mean);
                double sd = Math.sqrt(var);
                double z = sd > 0 ? (c - mean) / sd : 0;
                if (!inPos) {
                    if (z <= -p.entryZ()) { inPos = true; entry = c; entryBar = i; }
                } else {
                    double pnl = (c - entry) / entry * 100.0;
                    if (z >= -p.exitZ() || pnl <= -p.stopLossPct() || i - entryBar >= p.maxHoldBars()) {
                        double r = pnl - 2 * takerFeePct;
                        trades++;
                        if (r > 0) wins++;
                        net += r;
                        peak = Math.max(peak, net);
                        maxDd = Math.max(maxDd, peak - net);
                        inPos = false;
                    }
                }
                sum -= closes[i - w];
                sumSq -= closes[i - w] * closes[i - w];
            }
            sum += c;
            sumSq += c * c;
        }
        return new Result(trades, wins, net, trades == 0 ? 0 : net / trades, maxDd, n);
    }
}
