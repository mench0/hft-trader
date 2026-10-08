package com.hft.exchange;

import java.util.Locale;
import java.util.Optional;

/**
 * Рынок биржи: спот или бессрочные фьючерсы. Параметр биржи {@code market}, его строковые значения
 * ("spot", "perp") хранятся в SQLite и приходят из админки.
 */
public enum Market {
    /** Спот: покупка и продажа монет, только лонг. */
    SPOT("spot"),
    /** Бессрочные фьючерсы (USDT-маржинальные перпы): лонг и шорт, плечо, funding. */
    PERP("perp");

    /** Строковый id в параметрах и API. */
    private final String id;

    Market(String id) { this.id = id; }

    /** Строковый id: "spot" или "perp" (так же и в JSON). */
    @com.fasterxml.jackson.annotation.JsonValue
    public String id() { return id; }

    /** Рынок по строке (без учёта регистра); неизвестный — пусто. */
    public static Optional<Market> find(String id) {
        if (id == null) return Optional.empty();
        String v = id.trim().toLowerCase(Locale.ROOT);
        for (Market m : values()) if (m.id.equals(v)) return Optional.of(m);
        return Optional.empty();
    }

    /** Рынок по строке; неизвестный — IllegalArgumentException. */
    public static Market of(String id) {
        return find(id).orElseThrow(() -> new IllegalArgumentException("Неизвестный рынок: " + id + " (ожидается spot или perp)"));
    }

    @Override public String toString() { return id; }
}
