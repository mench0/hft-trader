package com.hft.exchange;

import java.util.Locale;
import java.util.Optional;

/**
 * Все биржи, о которых знает бот. Единственное место, где записаны их строковые id
 * (они же — ключи в админке, SQLite и префиксы переменных окружения: BINANCE_API_KEY…).
 * Код сравнивает и выбирает биржи по этому enum, а не по строкам.
 */
public enum Exchange {
    //          id             спот   перп
    BINANCE    ("binance",     true,  true),
    BYBIT      ("bybit",       true,  true),
    OKX        ("okx",         true,  true),
    MEXC       ("mexc",        true,  true),
    GATE       ("gate",        true,  true),
    HYPERLIQUID("hyperliquid", false, true),
    KUCOIN     ("kucoin",      true,  true),
    ASTER      ("aster",       true,  true),
    UNISWAPV2  ("uniswapv2",   true,  false);   // AMM-пулы обмена: фьючерсов не бывает

    /** Строковый id: "binance", "bybit"… */
    private final String id;
    /** Какие рынки бот умеет на этой бирже: спот (market=spot) и бессрочные фьючерсы (market=perp). */
    private final boolean spot, perp;

    Exchange(String id, boolean spot, boolean perp) { this.id = id; this.spot = spot; this.perp = perp; }

    /** Строковый id биржи. */
    public String id() { return id; }

    /** Бот торгует спотом этой биржи (market=spot). */
    public boolean hasSpot() { return spot; }

    /** Бот торгует бессрочными фьючерсами этой биржи (market=perp). */
    public boolean hasPerp() { return perp; }

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
