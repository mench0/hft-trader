package com.hft.exchange;

import java.util.EnumSet;
import java.util.Locale;
import java.util.Optional;
import java.util.Set;

/**
 * Все биржи, о которых знает бот. Единственное место, где записаны их строковые id
 * (они же — ключи в админке, SQLite и префиксы переменных окружения: BINANCE_API_KEY…).
 * Код сравнивает и выбирает биржи по этому enum, а не по строкам.
 */
public enum Exchange {
    BINANCE    ("binance",     Market.SPOT, Market.PERP),
    BYBIT      ("bybit",       Market.SPOT, Market.PERP),
    OKX        ("okx",         Market.SPOT, Market.PERP),
    MEXC       ("mexc",        Market.SPOT, Market.PERP),
    GATE       ("gate",        Market.SPOT, Market.PERP),
    HYPERLIQUID("hyperliquid", Market.PERP),
    KUCOIN     ("kucoin",      Market.SPOT, Market.PERP),
    ASTER      ("aster",       Market.SPOT, Market.PERP),
    UNISWAPV2  ("uniswapv2",   Market.SPOT);                // AMM-пулы обмена: фьючерсов не бывает

    /** Строковый id: "binance", "bybit"… */
    private final String id;
    /** Какие рынки бот умеет на этой бирже. */
    private final Set<Market> markets;

    Exchange(String id, Market first, Market... rest) { this.id = id; this.markets = EnumSet.of(first, rest); }

    /** Строковый id биржи. */
    public String id() { return id; }

    /** Рынки биржи (копия). */
    public Set<Market> markets() { return EnumSet.copyOf(markets); }

    /** Бот умеет этот рынок на этой бирже. */
    public boolean supports(Market m) { return markets.contains(m); }

    /** Бот торгует спотом этой биржи (market=spot). */
    public boolean hasSpot() { return supports(Market.SPOT); }

    /** Бот торгует бессрочными фьючерсами этой биржи (market=perp). */
    public boolean hasPerp() { return supports(Market.PERP); }

    /** Рынок по умолчанию для новой биржи: перп, если есть, иначе спот. */
    public Market defaultMarket() { return hasPerp() ? Market.PERP : Market.SPOT; }

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
