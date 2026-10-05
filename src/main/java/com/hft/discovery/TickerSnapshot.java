package com.hft.discovery;

/**
 * Сводка по одному тикеру биржи за 24 часа. NaN — биржа это поле не отдаёт
 * (например, у dYdX в общей сводке нет bid/ask).
 *
 * @param symbol      наш формат: BTCUSDT, BTCUSDC (перп Hyperliquid), BTCUSD (перп dYdX)
 * @param venueSymbol как символ называется на бирже (BTC-USDT, btc_usdt, BTC…)
 * @param perp        бессрочный фьючерс (Hyperliquid, dYdX), иначе спот
 */
public record TickerSnapshot(
        String exchange,
        String symbol,
        String venueSymbol,
        String base,
        String quote,
        boolean perp,
        double last,
        double bid,
        double ask,
        double high24h,
        double low24h,
        double open24h,
        double quoteVolume24h,
        long trades24h
) {
    /** Спред в процентах от середины; NaN — нет bid/ask. */
    public double spreadPct() {
        if (!(bid > 0) || !(ask > 0) || ask < bid) return Double.NaN;
        return (ask - bid) / ((ask + bid) / 2) * 100.0;
    }

    /** Дневной диапазон (high-low) в % от последней цены. */
    public double rangePct() {
        if (!(high24h > 0) || !(low24h > 0) || !(last > 0)) return Double.NaN;
        return (high24h - low24h) / last * 100.0;
    }

    /** Изменение за 24 ч, %; NaN — нет цены открытия. */
    public double changePct() {
        if (!(open24h > 0) || !(last > 0)) return Double.NaN;
        return (last - open24h) / open24h * 100.0;
    }
}
