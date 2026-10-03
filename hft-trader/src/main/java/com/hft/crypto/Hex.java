package com.hft.crypto;

import java.math.BigInteger;
import java.util.HexFormat;

public final class Hex {
    private Hex() {}
    public static String enc(byte[] b) { return HexFormat.of().formatHex(b); }
    public static String enc0x(byte[] b) { return "0x" + HexFormat.of().formatHex(b); }
    public static byte[] dec(String s) {
        if (s.startsWith("0x") || s.startsWith("0X")) s = s.substring(2);
        if (s.length() % 2 == 1) s = "0" + s;
        return HexFormat.of().parseHex(s);
    }
    /** Беззнаковое BigInteger -> ровно n байт (старшие нули добавляются, переполнение — ошибка). */
    public static byte[] fixed(BigInteger v, int n) {
        byte[] raw = v.toByteArray();
        int off = raw.length > 1 && raw[0] == 0 ? 1 : 0;
        int len = raw.length - off;
        if (len > n) throw new IllegalArgumentException("число длиннее " + n + " байт");
        byte[] out = new byte[n];
        System.arraycopy(raw, off, out, n - len, len);
        return out;
    }
    public static byte[] concat(byte[]... parts) {
        int n = 0; for (byte[] p : parts) n += p.length;
        byte[] out = new byte[n]; int o = 0;
        for (byte[] p : parts) { System.arraycopy(p, 0, out, o, p.length); o += p.length; }
        return out;
    }
}
