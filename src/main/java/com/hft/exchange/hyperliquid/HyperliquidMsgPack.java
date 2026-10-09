package com.hft.exchange.hyperliquid;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Минимальный MessagePack-кодировщик (map, array, str, bool, int, nil) — ровно то, что нужно
 * для подписи действий Hyperliquid. Порядок ключей map сохраняется как в переданном LinkedHashMap,
 * это критично: хеш считается от байтов. Целые кодируются самым коротким форматом.
 */
public final class HyperliquidMsgPack {

    /** Утилитный класс — экземпляры не создаются. */
    private HyperliquidMsgPack() {}

    /** Упаковать значение (Map, List, String, число, boolean, null) в MessagePack — формат действий Hyperliquid. */
    public static byte[] pack(Object value) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        write(out, value);
        return out.toByteArray();
    }

    /** Записать одно значение рекурсивно. */
    private static void write(ByteArrayOutputStream o, Object v) {
        if (v == null) { o.write(0xc0); return; }
        if (v instanceof Boolean b) { o.write(b ? 0xc3 : 0xc2); return; }
        if (v instanceof Integer || v instanceof Long || v instanceof Short || v instanceof Byte) { writeInt(o, ((Number) v).longValue()); return; }
        if (v instanceof String s) { writeStr(o, s); return; }
        if (v instanceof Map<?, ?> m) {
            writeHeader(o, m.size(), 0x80, 0x0f, 0xde, 0xdf, -1);
            for (var e : m.entrySet()) { write(o, e.getKey()); write(o, e.getValue()); }
            return;
        }
        if (v instanceof List<?> l) {
            writeHeader(o, l.size(), 0x90, 0x0f, 0xdc, 0xdd, -1);
            for (Object x : l) write(o, x);
            return;
        }
        throw new IllegalArgumentException("HyperliquidMsgPack: неподдерживаемый тип " + v.getClass());
    }

    /** fix-формат (до 15 элементов), иначе 16- или 32-битная длина. */
    private static void writeHeader(ByteArrayOutputStream o, int n, int fix, int fixMax, int t16, int t32, int unused) {
        if (n <= fixMax) { o.write(fix | n); }
        else if (n <= 0xffff) { o.write(t16); o.write(n >> 8); o.write(n); }
        else { o.write(t32); for (int i = 3; i >= 0; i--) o.write(n >> (8 * i)); }
    }

    /** Строка UTF-8 с заголовком длины (fixstr/str8/str16/str32). */
    private static void writeStr(ByteArrayOutputStream o, String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        int n = b.length;
        if (n <= 31) o.write(0xa0 | n);
        else if (n <= 0xff) { o.write(0xd9); o.write(n); }
        else if (n <= 0xffff) { o.write(0xda); o.write(n >> 8); o.write(n); }
        else { o.write(0xdb); for (int i = 3; i >= 0; i--) o.write(n >> (8 * i)); }
        o.writeBytes(b);
    }

    /** Целое в самой короткой форме MessagePack. */
    private static void writeInt(ByteArrayOutputStream o, long x) {
        if (x >= 0) {
            if (x <= 0x7f) o.write((int) x);
            else if (x <= 0xff) { o.write(0xcc); o.write((int) x); }
            else if (x <= 0xffff) { o.write(0xcd); o.write((int) (x >> 8)); o.write((int) x); }
            else if (x <= 0xffffffffL) { o.write(0xce); for (int i = 3; i >= 0; i--) o.write((int) (x >> (8 * i))); }
            else { o.write(0xcf); for (int i = 7; i >= 0; i--) o.write((int) (x >> (8 * i))); }
        } else {
            if (x >= -32) o.write((int) x);
            else if (x >= Byte.MIN_VALUE) { o.write(0xd0); o.write((int) x); }
            else if (x >= Short.MIN_VALUE) { o.write(0xd1); o.write((int) (x >> 8)); o.write((int) x); }
            else if (x >= Integer.MIN_VALUE) { o.write(0xd2); for (int i = 3; i >= 0; i--) o.write((int) (x >> (8 * i))); }
            else { o.write(0xd3); for (int i = 7; i >= 0; i--) o.write((int) (x >> (8 * i))); }
        }
    }
}
