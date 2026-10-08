package com.hft.exchange.uniswapv2;

import java.math.BigInteger;
import java.util.List;

/**
 * Кодирование вызовов контрактов (ABI) для узкого набора функций: аргументы бывают
 * uint256 (BigInteger), address (String 0x…) и address[] (List&lt;String&gt;).
 * Селектор (первые 4 байта keccak имени функции) передаётся готовым hex — проверен в тестах
 * независимым keccak.
 */
final class Abi {

    /** Утилитный класс — экземпляры не создаются. */
    private Abi() {}

    /** Данные вызова контракта: селектор + аргументы по ABI (uint, address, bool, массив адресов). */
    static String call(String selectorHex, Object... args) {
        int headSize = 32 * args.length;
        StringBuilder head = new StringBuilder(), tail = new StringBuilder();
        for (Object a : args) {
            if (a instanceof BigInteger n) head.append(word(n));
            else if (a instanceof String addr) head.append(addressWord(addr));
            else if (a instanceof List<?> list) {
                head.append(word(BigInteger.valueOf(headSize + tail.length() / 2)));
                tail.append(word(BigInteger.valueOf(list.size())));
                for (Object x : list) tail.append(addressWord((String) x));
            } else throw new IllegalArgumentException("Abi: неподдерживаемый тип " + a.getClass());
        }
        return "0x" + selectorHex + head + tail;
    }

    /** Число в 32-байтовое слово (hex). */
    private static String word(BigInteger n) {
        if (n.signum() < 0 || n.bitLength() > 256) throw new IllegalArgumentException("uint256 вне диапазона: " + n);
        String h = n.toString(16);
        return "0".repeat(64 - h.length()) + h;
    }

    /** Адрес в 32-байтовое слово (выравнивание влево нулями). */
    private static String addressWord(String addr) {
        String h = addr.startsWith("0x") ? addr.substring(2) : addr;
        if (h.length() != 40) throw new IllegalArgumentException("Некорректный адрес: " + addr);
        return "0".repeat(24) + h.toLowerCase();
    }
}
