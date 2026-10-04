package com.hft.crypto;

import java.util.HexFormat;

/** Шестнадцатеричная запись и склейка массивов байт для подписи EIP-712. */
public final class Hex {
    /** Утилитный класс — экземпляры не создаются. */
    private Hex() {}
    /** Байты в hex нижним регистром, без 0x. */
    public static String enc(byte[] b) { return HexFormat.of().formatHex(b); }
    /** Склеить массивы байт подряд. */
    public static byte[] concat(byte[]... parts) {
        int n = 0; for (byte[] p : parts) n += p.length;
        byte[] out = new byte[n]; int o = 0;
        for (byte[] p : parts) { System.arraycopy(p, 0, out, o, p.length); o += p.length; }
        return out;
    }
}
