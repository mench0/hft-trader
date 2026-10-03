package com.hft.crypto;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;

/** Минимальный кодировщик protobuf (проводной формат) для транзакций Cosmos/dYdX. Пустые/нулевые поля пропускаются, как в proto3. */
public final class Proto {
    private final ByteArrayOutputStream out = new ByteArrayOutputStream();

    public static Proto create() { return new Proto(); }

    private void tag(int field, int wire) { varint(((long) field << 3) | wire); }

    private void varint(long v) {
        while ((v & ~0x7FL) != 0) { out.write((int) ((v & 0x7F) | 0x80)); v >>>= 7; }
        out.write((int) v);
    }

    public Proto uint(int field, long v) { if (v != 0) { tag(field, 0); varint(v); } return this; }
    public Proto bool(int field, boolean v) { return uint(field, v ? 1 : 0); }
    public Proto fixed32(int field, long v) {
        if (v != 0) { tag(field, 5); for (int i = 0; i < 4; i++) out.write((int) (v >>> (8 * i)) & 0xff); }
        return this;
    }
    public Proto bytes(int field, byte[] b) {
        if (b.length > 0) { tag(field, 2); varint(b.length); out.writeBytes(b); }
        return this;
    }
    public Proto string(int field, String s) { return bytes(field, s.getBytes(StandardCharsets.UTF_8)); }
    /** Вложенное сообщение; пустое всё равно пишется (нужно для oneof/обязательных). */
    public Proto message(int field, Proto m) { tag(field, 2); byte[] b = m.build(); varint(b.length); out.writeBytes(b); return this; }
    public Proto message(int field, byte[] b) { tag(field, 2); varint(b.length); out.writeBytes(b); return this; }
    public byte[] build() { return out.toByteArray(); }
}
