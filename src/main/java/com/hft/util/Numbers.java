package com.hft.util;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Форматирование чисел для отправки на биржу.
 *
 * Важно: нельзя слать Double.toString() — для мелких чисел он выдаёт
 * научную нотацию (1.0E-5), которую биржа не принимает.
 */
public final class Numbers {

    /** Утилитный класс — экземпляры не создаются. */
    private Numbers() {}

    /** Число в обычной десятичной записи с нужным числом знаков, без хвостовых нулей. */
    public static String plain(double value, int scale) {
        return BigDecimal.valueOf(value)
                .setScale(scale, RoundingMode.DOWN)
                .stripTrailingZeros()
                .toPlainString();
    }

    /** То же, но без указания точности — по фактическому значению. */
    public static String plain(double value) {
        return BigDecimal.valueOf(value).stripTrailingZeros().toPlainString();
    }
}
