package com.hft.crypto;

/** Keccak-256 (как в Ethereum: паддинг 0x01, а не SHA3-0x06). */
public final class Keccak {
    /** Утилитный класс — экземпляры не создаются. */
    private Keccak() {}

    /** Раундовые константы шага ι. */
    private static final long[] RC = {
        0x0000000000000001L, 0x0000000000008082L, 0x800000000000808aL, 0x8000000080008000L,
        0x000000000000808bL, 0x0000000080000001L, 0x8000000080008081L, 0x8000000000008009L,
        0x000000000000008aL, 0x0000000000000088L, 0x0000000080008009L, 0x000000008000000aL,
        0x000000008000808bL, 0x800000000000008bL, 0x8000000000008089L, 0x8000000000008003L,
        0x8000000000008002L, 0x8000000000000080L, 0x000000000000800aL, 0x800000008000000aL,
        0x8000000080008081L, 0x8000000000008080L, 0x0000000080000001L, 0x8000000080008008L};
    /** Сдвиги вращения для шага ρ. */
    private static final int[] ROT = {1, 3, 6, 10, 15, 21, 28, 36, 45, 55, 2, 14, 27, 41, 56, 8, 25, 43, 62, 18, 39, 61, 20, 44};
    /** Перестановка позиций для шага π. */
    private static final int[] PI = {10, 7, 11, 17, 18, 3, 5, 16, 8, 21, 24, 4, 15, 23, 19, 13, 12, 2, 20, 14, 22, 9, 6, 1};

    /** Перестановка Keccak-f[1600]: 24 раунда θ, ρ, π, χ, ι. */
    private static void permute(long[] a) {
        long[] c = new long[5];
        for (int round = 0; round < 24; round++) {
            for (int x = 0; x < 5; x++) c[x] = a[x] ^ a[x + 5] ^ a[x + 10] ^ a[x + 15] ^ a[x + 20];
            for (int x = 0; x < 5; x++) {
                long d = c[(x + 4) % 5] ^ Long.rotateLeft(c[(x + 1) % 5], 1);
                for (int y = 0; y < 25; y += 5) a[y + x] ^= d;
            }
            long t = a[1];
            for (int i = 0; i < 24; i++) {
                int j = PI[i];
                long tmp = a[j];
                a[j] = Long.rotateLeft(t, ROT[i]);
                t = tmp;
            }
            for (int y = 0; y < 25; y += 5) {
                for (int x = 0; x < 5; x++) c[x] = a[y + x];
                for (int x = 0; x < 5; x++) a[y + x] = c[x] ^ (~c[(x + 1) % 5] & c[(x + 2) % 5]);
            }
            a[0] ^= RC[round];
        }
    }

    /** Keccak-256 (как в Ethereum: паддинг 0x01, не NIST SHA3-256). */
    public static byte[] hash256(byte[] in) {
        final int rate = 136;
        long[] st = new long[25];
        int off = 0;
        while (in.length - off >= rate) {
            absorb(st, in, off, rate);
            permute(st);
            off += rate;
        }
        byte[] last = new byte[rate];
        System.arraycopy(in, off, last, 0, in.length - off);
        last[in.length - off] ^= 0x01;
        last[rate - 1] ^= (byte) 0x80;
        absorb(st, last, 0, rate);
        permute(st);
        byte[] out = new byte[32];
        for (int i = 0; i < 32; i++) out[i] = (byte) (st[i / 8] >>> (8 * (i % 8)));
        return out;
    }

    /** Впитать блок байт в состояние (XOR по 64-битным словам, little-endian). */
    private static void absorb(long[] st, byte[] b, int off, int len) {
        for (int i = 0; i < len; i++) st[i / 8] ^= (long) (b[off + i] & 0xff) << (8 * (i % 8));
    }

    /** Keccak-256 строки UTF-8. */
    public static byte[] hash256(String s) { return hash256(s.getBytes(java.nio.charset.StandardCharsets.UTF_8)); }
}
