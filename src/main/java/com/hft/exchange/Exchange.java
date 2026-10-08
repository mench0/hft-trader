package com.hft.exchange;

import java.util.Locale;
import java.util.Optional;

/**
 * Все биржи, о которых знает бот. Единственное место, где записаны их строковые id
 * (они же — ключи в админке, SQLite и префиксы переменных окружения: BINANCE_API_KEY…).
 * Код сравнивает и выбирает биржи по этому enum, а не по строкам.
 */
public enum Exchange {
    BINANCE("binance"),
    BYBIT("bybit"),
    OKX("okx"),
    MEXC("mexc"),
    GATE("gate"),
    HYPERLIQUID("hyperliquid"),
    KUCOIN("kucoin"),
    ASTER("aster"),
    UNISWAPV2("uniswapv2"),
    PANCAKESWAP("pancakeswap"),
    RAYDIUM("raydium"),
    ORCA("orca");

    /** Строковый id: "binance", "bybit"… */
    private final String id;

    Exchange(String id) { this.id = id; }

    /** Строковый id биржи. */
    public String id() { return id; }

    /** Префикс переменных окружения: BINANCE (BINANCE_API_KEY, BINANCE_LIVE…). */
    public String envPrefix() { return id.toUpperCase(Locale.ROOT); }

    /** Это та же биржа, что и строковый id (без учёта регистра). */
    public boolean is(String other) { return other != null && id.equalsIgnoreCase(other); }

    /** Биржа по строковому id; неизвестная — пусто. */
    public static Optional<Exchange> find(String id) {
        if (id == null) return Optional.empty();
        for (Exchange e : values()) if (e.id.equalsIgnoreCase(id.trim())) return Optional.of(e);
        return Optional.empty();
    }

    /** Биржа по строковому id; неизвестная — IllegalArgumentException. */
    public static Exchange of(String id) {
        return find(id).orElseThrow(() -> new IllegalArgumentException("Неизвестная биржа: " + id));
    }

    @Override public String toString() { return id; }
}
