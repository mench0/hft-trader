package com.hft.config;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Все торговые параметры одной биржи: риск, стратегия и размеры структур данных.
 *
 * В application.yml их нет — задаются только через админку
 * ({@code POST /exchange/params?exchange=bybit&maxPositionQuote=50&entryZ=2.5}),
 * для каждой биржи свои, и сохраняются в SQLite вместе с остальным состоянием.
 *
 * Неизменяемый: при изменении из админки создаётся новый экземпляр и атомарно
 * подменяется в {@link TradingSettings}. Поток стратегии читает одну volatile-ссылку
 * и видит согласованный набор значений, без блокировок.
 */
public record TradingParams(
        // ---- риск ----
        boolean tradingEnabled,
        double maxPositionQuote,
        double maxDailyLossQuote,
        double maxSlippagePercent,
        double feeReservePercent,
        double takerFeePercent,
        int maxOrdersPerMinute,
        long maxDataAgeMs,
        // ---- стратегия mean-reversion ----
        double entryZ,
        double exitZ,
        double stopLossPercent,
        double minImbalance,
        int imbalanceLevels,
        double orderQuote,
        long maxBookAgeMs,
        double maxSpreadPercent,
        long positionTimeoutMs,
        // ---- структуры данных (применяются при следующем /control/start) ----
        int bookDepth,
        int priceWindow
) {

    public static final TradingParams DEFAULTS = new TradingParams(
            false, 100.0, 50.0, 0.3, 0.2, 0.1, 30, 5_000,
            2.0, 0.3, 0.5, 0.15, 5, 20.0, 2_000, 0.1, 3_600_000,
            20, 1000);

    /** Параметры, изменение которых вступает в силу только после перезапуска биржи. */
    public static boolean requiresRestart(String key) {
        return key.equals("bookDepth") || key.equals("priceWindow");
    }

    /**
     * Новый набор: текущие значения, поверх которых применены переданные.
     * Неизвестный ключ или значение вне допустимого диапазона — IllegalArgumentException,
     * и тогда не применяется ничего.
     */
    public TradingParams with(Map<String, String> updates) {
        Map<String, String> m = toStringMap();
        for (var e : updates.entrySet()) {
            if (!m.containsKey(e.getKey())) continue;          // чужие ключи запроса (exchange и т.п.)
            m.put(e.getKey(), e.getValue().trim());
        }
        TradingParams p = new TradingParams(
                bool(m, "tradingEnabled"),
                dbl(m, "maxPositionQuote", 0, 1e9),
                dbl(m, "maxDailyLossQuote", 0, 1e9),
                dbl(m, "maxSlippagePercent", 0, 100),
                dbl(m, "feeReservePercent", 0, 50),
                dbl(m, "takerFeePercent", 0, 5),
                (int) lng(m, "maxOrdersPerMinute", 1, 100_000),
                lng(m, "maxDataAgeMs", 50, 600_000),
                dbl(m, "entryZ", 0.1, 20),
                dbl(m, "exitZ", -20, 20),
                dbl(m, "stopLossPercent", 0.01, 100),
                dbl(m, "minImbalance", -1, 1),
                (int) lng(m, "imbalanceLevels", 1, 1000),
                dbl(m, "orderQuote", 0, 1e9),
                lng(m, "maxBookAgeMs", 10, 600_000),
                dbl(m, "maxSpreadPercent", 0, 100),
                lng(m, "positionTimeoutMs", 1_000, 7L * 24 * 3_600_000),
                (int) lng(m, "bookDepth", 1, 1000),
                (int) lng(m, "priceWindow", 4, 1_000_000));
        if (p.imbalanceLevels > p.bookDepth)
            throw new IllegalArgumentException("imbalanceLevels (" + p.imbalanceLevels + ") больше bookDepth (" + p.bookDepth + ")");
        return p;
    }

    /** Все параметры строками — для сохранения в SQLite и для ответа админки. */
    public Map<String, String> toStringMap() {
        Map<String, String> m = new LinkedHashMap<>();
        m.put("tradingEnabled", String.valueOf(tradingEnabled));
        m.put("maxPositionQuote", String.valueOf(maxPositionQuote));
        m.put("maxDailyLossQuote", String.valueOf(maxDailyLossQuote));
        m.put("maxSlippagePercent", String.valueOf(maxSlippagePercent));
        m.put("feeReservePercent", String.valueOf(feeReservePercent));
        m.put("takerFeePercent", String.valueOf(takerFeePercent));
        m.put("maxOrdersPerMinute", String.valueOf(maxOrdersPerMinute));
        m.put("maxDataAgeMs", String.valueOf(maxDataAgeMs));
        m.put("entryZ", String.valueOf(entryZ));
        m.put("exitZ", String.valueOf(exitZ));
        m.put("stopLossPercent", String.valueOf(stopLossPercent));
        m.put("minImbalance", String.valueOf(minImbalance));
        m.put("imbalanceLevels", String.valueOf(imbalanceLevels));
        m.put("orderQuote", String.valueOf(orderQuote));
        m.put("maxBookAgeMs", String.valueOf(maxBookAgeMs));
        m.put("maxSpreadPercent", String.valueOf(maxSpreadPercent));
        m.put("positionTimeoutMs", String.valueOf(positionTimeoutMs));
        m.put("bookDepth", String.valueOf(bookDepth));
        m.put("priceWindow", String.valueOf(priceWindow));
        return m;
    }

    private static boolean bool(Map<String, String> m, String k) {
        String v = m.get(k);
        if (!v.equalsIgnoreCase("true") && !v.equalsIgnoreCase("false"))
            throw new IllegalArgumentException(k + ": ожидалось true/false, получено " + v);
        return Boolean.parseBoolean(v);
    }

    private static double dbl(Map<String, String> m, String k, double min, double max) {
        double v;
        try { v = Double.parseDouble(m.get(k)); }
        catch (NumberFormatException e) { throw new IllegalArgumentException(k + ": не число: " + m.get(k)); }
        if (!(v >= min && v <= max)) throw new IllegalArgumentException(k + ": " + v + " вне диапазона [" + min + ", " + max + "]");
        return v;
    }

    private static long lng(Map<String, String> m, String k, long min, long max) {
        long v;
        try { v = Long.parseLong(m.get(k)); }
        catch (NumberFormatException e) { throw new IllegalArgumentException(k + ": не целое число: " + m.get(k)); }
        if (v < min || v > max) throw new IllegalArgumentException(k + ": " + v + " вне диапазона [" + min + ", " + max + "]");
        return v;
    }
}
