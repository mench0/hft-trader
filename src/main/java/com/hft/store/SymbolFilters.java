package com.hft.store;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Ограничения биржи по каждому инструменту.
 *
 * Это та вещь, на которой спотыкаются почти все: биржа отклонит ордер,
 * если объём не кратен lotSize или цена не кратна tickSize.
 * Ошибка выглядит как "Filter failure: LOT_SIZE" и не очевидна.
 *
 * Данные берутся один раз при старте из /api/v3/exchangeInfo
 * и дальше живут в памяти.
 */
public final class SymbolFilters {

    /** Правила символа: min/max/шаг объёма, min/max/шаг цены, минимальная сумма ордера. */
    public record Filter(
            double minQty,        // минимальный объём
            double maxQty,        // максимальный объём
            double stepSize,      // шаг объёма: 0.00001 значит 0.123456 -> 0.12345
            double minPrice,
            double maxPrice,
            double tickSize,      // шаг цены
            double minNotional    // минимальная сумма сделки (qty * price)
    ) {}

    /** Правила по символу. */
    private final Map<String, Filter> filters = new ConcurrentHashMap<>();

    /** Задать правила символа. */
    public void put(String symbol, Filter filter) {
        filters.put(symbol.toUpperCase(), filter);
    }

    /** Правила символа или null. */
    public Filter get(String symbol) {
        return filters.get(symbol.toUpperCase());
    }

    /** Правила символа загружены. */
    public boolean has(String symbol) {
        return filters.containsKey(symbol.toUpperCase());
    }

    /**
     * Округлить объём вниз до допустимого шага.
     * Вниз, а не к ближайшему — чтобы не выйти за доступный баланс.
     */
    public double roundQuantity(String symbol, double qty) {
        Filter f = get(symbol);
        if (f == null || f.stepSize() <= 0) return qty;
        return roundDown(qty, f.stepSize());
    }

    /** Округлить цену до допустимого шага. */
    public double roundPrice(String symbol, double price) {
        Filter f = get(symbol);
        if (f == null || f.tickSize() <= 0) return price;
        return roundDown(price, f.tickSize());
    }

    /** Округлить вниз до шага (с поправкой на погрешность double). */
    private static double roundDown(double value, double step) {
        BigDecimal bdValue = BigDecimal.valueOf(value);
        BigDecimal bdStep = BigDecimal.valueOf(step);
        BigDecimal steps = bdValue.divide(bdStep, 0, RoundingMode.DOWN);
        return steps.multiply(bdStep).doubleValue();
    }

    /**
     * Проверка ордера перед отправкой. Возвращает null если всё хорошо,
     * иначе текст ошибки. Дешевле поймать здесь, чем получить отказ биржи.
     */
    public String validate(String symbol, double qty, double price) {
        Filter f = get(symbol);
        if (f == null) return null; // фильтры не загружены — пропускаем

        if (qty < f.minQty()) {
            return String.format("Объём %.8f меньше минимального %.8f", qty, f.minQty());
        }
        if (qty > f.maxQty()) {
            return String.format("Объём %.8f больше максимального %.8f", qty, f.maxQty());
        }
        if (price > 0) {
            if (price < f.minPrice()) {
                return String.format("Цена %.8f меньше минимальной %.8f", price, f.minPrice());
            }
            if (f.maxPrice() > 0 && price > f.maxPrice()) {
                return String.format("Цена %.8f больше максимальной %.8f", price, f.maxPrice());
            }
            double notional = qty * price;
            if (notional < f.minNotional()) {
                return String.format("Сумма сделки %.2f меньше минимальной %.2f",
                        notional, f.minNotional());
            }
        }
        return null;
    }

    /** Сколько знаков после запятой допустимо для объёма — для форматирования. */
    public int quantityScale(String symbol) {
        Filter f = get(symbol);
        if (f == null) return 8;
        return scaleOf(f.stepSize());
    }

    /** Сколько знаков после запятой у шага цены. */
    public int priceScale(String symbol) {
        Filter f = get(symbol);
        if (f == null) return 8;
        return scaleOf(f.tickSize());
    }

    /** Число знаков после запятой у шага (0.001 -> 3). */
    private static int scaleOf(double step) {
        if (step <= 0) return 8;
        BigDecimal bd = BigDecimal.valueOf(step).stripTrailingZeros();
        return Math.max(0, bd.scale());
    }
}
