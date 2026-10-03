package com.hft.store;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Балансы аккаунта в памяти.
 *
 * Зачем отдельное хранилище: когда приходит запрос "купить на весь баланс",
 * нельзя каждый раз ходить в REST API биржи — это +50..200 мс и расход
 * лимита запросов. Балансы кэшируются здесь и обновляются:
 *   - при старте (полная загрузка через REST)
 *   - после каждого исполненного ордера (локальная корректировка)
 *   - периодически из userDataStream или по таймеру (сверка с биржей)
 */
public final class BalanceStore {

    /** Свободные средства по активу: "USDT" -> 1000.0, "BTC" -> 0.05 */
    private final Map<String, Double> free = new ConcurrentHashMap<>();

    /** Заблокированные в открытых ордерах. */
    private final Map<String, Double> locked = new ConcurrentHashMap<>();

    private volatile long lastSyncMs;

    public void set(String asset, double freeAmount, double lockedAmount) {
        free.put(asset.toUpperCase(), freeAmount);
        locked.put(asset.toUpperCase(), lockedAmount);
    }

    public double free(String asset) {
        return free.getOrDefault(asset.toUpperCase(), 0.0);
    }

    public double locked(String asset) {
        return locked.getOrDefault(asset.toUpperCase(), 0.0);
    }

    public double total(String asset) {
        return free(asset) + locked(asset);
    }

    /**
     * Локальная корректировка после исполнения ордера.
     * Нужна, чтобы не ждать ответа REST при серии быстрых сделок.
     */
    public void adjust(String asset, double delta) {
        free.merge(asset.toUpperCase(), delta, Double::sum);
    }

    public void markSynced() {
        lastSyncMs = System.currentTimeMillis();
    }

    public long ageMs() {
        return System.currentTimeMillis() - lastSyncMs;
    }

    /** Балансы старше maxAgeMs считаются протухшими — нужна пересинхронизация. */
    public boolean isStale(long maxAgeMs) {
        return ageMs() > maxAgeMs;
    }

    public Map<String, Double> snapshot() {
        return Map.copyOf(free);
    }

    /**
     * Разбор символа на базовый и котируемый актив.
     * BTCUSDT -> ["BTC", "USDT"], ETHBTC -> ["ETH", "BTC"]
     *
     * Простая эвристика по известным котируемым валютам. В продакшене
     * лучше брать из /api/v3/exchangeInfo, где это указано явно.
     */
    private static final String[] QUOTE_ASSETS = {
            "USDT", "USDC", "FDUSD", "TUSD", "BUSD", "BTC", "ETH", "BNB", "EUR", "TRY", "USD"
    };

    public static String[] splitSymbol(String symbol) {
        String s = symbol.toUpperCase();
        for (String quote : QUOTE_ASSETS) {
            if (s.endsWith(quote) && s.length() > quote.length()) {
                return new String[]{s.substring(0, s.length() - quote.length()), quote};
            }
        }
        throw new IllegalArgumentException("Не удалось разобрать символ: " + symbol);
    }

    public static String baseAsset(String symbol) {
        return splitSymbol(symbol)[0];
    }

    public static String quoteAsset(String symbol) {
        return splitSymbol(symbol)[1];
    }
}
