package com.hft.crypto;

import java.util.ArrayList;
import java.util.List;

/** Bech32 (BIP-173) — адреса Cosmos, в том числе dydx1... */
public final class Bech32 {
    private Bech32() {}
    private static final String CHARSET = "qpzry9x8gf2tvdw0s3jn54khce6mua7l";
    private static final int[] GEN = {0x3b6a57b2, 0x26508e6d, 0x1ea119fa, 0x3d4233dd, 0x2a1462b3};

    private static int polymod(List<Integer> values) {
        int chk = 1;
        for (int v : values) {
            int top = chk >>> 25;
            chk = (chk & 0x1ffffff) << 5 ^ v;
            for (int i = 0; i < 5; i++) if (((top >>> i) & 1) != 0) chk ^= GEN[i];
        }
        return chk;
    }

    private static List<Integer> hrpExpand(String hrp) {
        List<Integer> r = new ArrayList<>();
        for (char c : hrp.toCharArray()) r.add(c >>> 5);
        r.add(0);
        for (char c : hrp.toCharArray()) r.add(c & 31);
        return r;
    }

    private static List<Integer> convertBits(byte[] data, int from, int to, boolean pad) {
        int acc = 0, bits = 0, maxv = (1 << to) - 1;
        List<Integer> out = new ArrayList<>();
        for (byte b : data) {
            acc = (acc << from) | (b & 0xff);
            bits += from;
            while (bits >= to) { bits -= to; out.add((acc >>> bits) & maxv); }
        }
        if (pad && bits > 0) out.add((acc << (to - bits)) & maxv);
        return out;
    }

    public static String encode(String hrp, byte[] data) {
        List<Integer> d5 = convertBits(data, 8, 5, true);
        List<Integer> vals = hrpExpand(hrp);
        vals.addAll(d5);
        for (int i = 0; i < 6; i++) vals.add(0);
        int mod = polymod(vals) ^ 1;
        StringBuilder sb = new StringBuilder(hrp).append('1');
        for (int v : d5) sb.append(CHARSET.charAt(v));
        for (int i = 0; i < 6; i++) sb.append(CHARSET.charAt((mod >>> (5 * (5 - i))) & 31));
        return sb.toString();
    }
}
