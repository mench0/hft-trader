package com.hft.crypto;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

/**
 * Минимальный MessagePack-кодер — ровно то, что нужно для хеша действий Hyperliquid:
 * Map (порядок вставки сохраняется), List, String, Boolean, Long/Integer (неотрицательные), null.
 * Целые кодируются самой короткой формой, как делает msgpack-python.
 */
public final class Msgpack {
    private Msgpack() {}

    public static byte[] pack(Object o) {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        write(out, o);
        return out.toByteArray();
    }

    private static void write(ByteArrayOutputStream out, Object o) {
        if (o == null) { out.write(0xc0); return; }
        if (o instanceof Boolean b) { out.write(b ? 0xc3 : 0xc2); return; }
        if (o instanceof Integer i) { writeUint(out, i); return; }
        if (o instanceof Long l) { writeUint(out, l); return; }
        if (o instanceof String s) { writeStr(out, s); return; }
        if (o instanceof List<?> list) {
            int n = list.size();
            if (n < 16) out.write(0x90 | n);
            else if (n < 65536) { out.write(0xdc); out.write(n >> 8); out.write(n); }
            else { out.write(0xdd); be(out, n, 4); }
            for (Object e : list) write(out, e);
            return;
        }
        if (o instanceof Map<?, ?> map) {
            int n = map.size();
            if (n < 16) out.write(0x80 | n);
            else if (n < 65536) { out.write(0xde); out.write(n >> 8); out.write(n); }
            else { out.write(0xdf); be(out, n, 4); }
            for (var e : map.entrySet()) { write(out, e.getKey()); write(out, e.getValue()); }
            return;
        }
        throw new IllegalArgumentException("msgpack: тип " + o.getClass());
    }

    private static void writeUint(ByteArrayOutputStream out, long v) {
        if (v < 0) throw new IllegalArgumentException("msgpack: отрицательные числа не поддерживаются");
        if (v < 128) out.write((int) v);
        else if (v < 256) { out.write(0xcc); out.write((int) v); }
        else if (v < 65536) { out.write(0xcd); be(out, v, 2); }
        else if (v < (1L << 32)) { out.write(0xce); be(out, v, 4); }
        else { out.write(0xcf); be(out, v, 8); }
    }

    private static void writeStr(ByteArrayOutputStream out, String s) {
        byte[] b = s.getBytes(StandardCharsets.UTF_8);
        int n = b.length;
        if (n < 32) out.write(0xa0 | n);
        else if (n < 256) { out.write(0xd9); out.write(n); }
        else if (n < 65536) { out.write(0xda); be(out, n, 2); }
        else { out.write(0xdb); be(out, n, 4); }
        out.writeBytes(b);
    }

    private static void be(ByteArrayOutputStream out, long v, int bytes) {
        for (int i = bytes - 1; i >= 0; i--) out.write((int) (v >>> (8 * i)) & 0xff);
    }
}
