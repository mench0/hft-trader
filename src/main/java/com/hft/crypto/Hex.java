package com.hft.crypto;

import java.util.HexFormat;

public final class Hex {
    private Hex() {}
    public static String enc(byte[] b) { return HexFormat.of().formatHex(b); }
    public static byte[] concat(byte[]... parts) {
        int n = 0; for (byte[] p : parts) n += p.length;
        byte[] out = new byte[n]; int o = 0;
        for (byte[] p : parts) { System.arraycopy(p, 0, out, o, p.length); o += p.length; }
        return out;
    }
}
