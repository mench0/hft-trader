package com.hft.crypto;

import com.hft.util.Hmac;

import java.io.ByteArrayOutputStream;
import java.math.BigInteger;
import java.security.MessageDigest;

/**
 * secp256k1 на BigInteger: открытый ключ и детерминированная подпись ECDSA (RFC 6979, HMAC-SHA256)
 * с низким s и recovery id — как требуют Ethereum, Hyperliquid и Cosmos.
 * Подпись занимает единицы миллисекунд. Секретный ключ в памяти хранится как BigInteger:
 * для торгового бота это приемлемо, для кошелька с большими суммами — нет.
 */
public final class Secp256k1 {
    private Secp256k1() {}

    static final BigInteger P = new BigInteger("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEFFFFFC2F", 16);
    public static final BigInteger N = new BigInteger("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFEBAAEDCE6AF48A03BBFD25E8CD0364141", 16);
    static final BigInteger GX = new BigInteger("79BE667EF9DCBBAC55A06295CE870B07029BFCDB2DCE28D959F2815B16F81798", 16);
    static final BigInteger GY = new BigInteger("483ADA7726A3C4655DA4FBFC0E1108A8FD17B448A68554199C47D08FFB10D4B8", 16);
    private static final BigInteger HALF_N = N.shiftRight(1);

    /** Точка в якобиановых координатах (X, Y, Z); Z=0 — бесконечность. */
    private record Pt(BigInteger x, BigInteger y, BigInteger z) {
        boolean inf() { return z.signum() == 0; }
    }

    private static final Pt INF = new Pt(BigInteger.ONE, BigInteger.ONE, BigInteger.ZERO);
    private static final Pt G = new Pt(GX, GY, BigInteger.ONE);

    private static Pt dbl(Pt a) {
        if (a.inf() || a.y.signum() == 0) return INF;
        BigInteger ysq = a.y.multiply(a.y).mod(P);
        BigInteger s = a.x.multiply(ysq).shiftLeft(2).mod(P);
        BigInteger m = a.x.multiply(a.x).multiply(BigInteger.valueOf(3)).mod(P);       // a = 0
        BigInteger x3 = m.multiply(m).subtract(s.shiftLeft(1)).mod(P);
        BigInteger y3 = m.multiply(s.subtract(x3)).subtract(ysq.multiply(ysq).shiftLeft(3)).mod(P);
        BigInteger z3 = a.y.multiply(a.z).shiftLeft(1).mod(P);
        return new Pt(x3, y3, z3);
    }

    private static Pt add(Pt a, Pt b) {
        if (a.inf()) return b;
        if (b.inf()) return a;
        BigInteger z1sq = a.z.multiply(a.z).mod(P), z2sq = b.z.multiply(b.z).mod(P);
        BigInteger u1 = a.x.multiply(z2sq).mod(P), u2 = b.x.multiply(z1sq).mod(P);
        BigInteger s1 = a.y.multiply(z2sq).multiply(b.z).mod(P), s2 = b.y.multiply(z1sq).multiply(a.z).mod(P);
        if (u1.equals(u2)) return s1.equals(s2) ? dbl(a) : INF;
        BigInteger h = u2.subtract(u1).mod(P), r = s2.subtract(s1).mod(P);
        BigInteger h2 = h.multiply(h).mod(P), h3 = h2.multiply(h).mod(P), v = u1.multiply(h2).mod(P);
        BigInteger x3 = r.multiply(r).subtract(h3).subtract(v.shiftLeft(1)).mod(P);
        BigInteger y3 = r.multiply(v.subtract(x3)).subtract(s1.multiply(h3)).mod(P);
        BigInteger z3 = a.z.multiply(b.z).multiply(h).mod(P);
        return new Pt(x3, y3, z3);
    }

    private static Pt mul(BigInteger k, Pt p) {
        Pt r = INF;
        for (int i = k.bitLength() - 1; i >= 0; i--) {
            r = dbl(r);
            if (k.testBit(i)) r = add(r, p);
        }
        return r;
    }

    private static BigInteger[] affine(Pt p) {
        BigInteger zi = p.z.modInverse(P), zi2 = zi.multiply(zi).mod(P);
        return new BigInteger[]{p.x.multiply(zi2).mod(P), p.y.multiply(zi2).multiply(zi).mod(P)};
    }

    /** Несжатый открытый ключ без префикса 0x04: 64 байта X||Y. */
    public static byte[] publicKeyXY(BigInteger priv) {
        checkKey(priv);
        BigInteger[] a = affine(mul(priv, G));
        return Hex.concat(Hex.fixed(a[0], 32), Hex.fixed(a[1], 32));
    }

    /** Сжатый открытый ключ (33 байта) — для Cosmos. */
    public static byte[] publicKeyCompressed(BigInteger priv) {
        checkKey(priv);
        BigInteger[] a = affine(mul(priv, G));
        return Hex.concat(new byte[]{(byte) (a[1].testBit(0) ? 3 : 2)}, Hex.fixed(a[0], 32));
    }

    /** Адрес Ethereum: последние 20 байт keccak256(X||Y). */
    public static byte[] ethAddress(BigInteger priv) {
        byte[] h = Keccak.hash256(publicKeyXY(priv));
        byte[] out = new byte[20];
        System.arraycopy(h, 12, out, 0, 20);
        return out;
    }

    private static void checkKey(BigInteger priv) {
        if (priv.signum() <= 0 || priv.compareTo(N) >= 0) throw new IllegalArgumentException("неверный секретный ключ");
    }

    public static BigInteger parseKey(String hex) { return new BigInteger(1, Hex.dec(hex.trim())); }

    /** Подпись: r, s (низкий) и recId 0/1 (чётность Y точки R). */
    public record Sig(BigInteger r, BigInteger s, int recId) {
        /** Ethereum-форма v = 27 + recId. */
        public int v() { return 27 + recId; }
        public byte[] rs64() { return Hex.concat(Hex.fixed(r, 32), Hex.fixed(s, 32)); }
    }

    /** Подпись 32-байтного хеша. */
    public static Sig sign(byte[] hash32, BigInteger priv) {
        checkKey(priv);
        if (hash32.length != 32) throw new IllegalArgumentException("нужен 32-байтный хеш");
        BigInteger z = new BigInteger(1, hash32);
        BigInteger k = nonce(hash32, priv);
        while (true) {
            Pt R = mul(k, G);
            BigInteger[] ra = affine(R);
            BigInteger r = ra[0].mod(N);
            if (r.signum() != 0) {
                BigInteger s = k.modInverse(N).multiply(z.add(r.multiply(priv))).mod(N);
                if (s.signum() != 0) {
                    int rec = (ra[1].testBit(0) ? 1 : 0) | (ra[0].compareTo(N) >= 0 ? 2 : 0);
                    if (s.compareTo(HALF_N) > 0) { s = N.subtract(s); rec ^= 1; }
                    return new Sig(r, s, rec);
                }
            }
            k = k.add(BigInteger.ONE).mod(N);   // практически недостижимо
        }
    }

    /** Проверка подписи (для тестов и самопроверки). */
    public static boolean verify(byte[] hash32, Sig sig, BigInteger priv) {
        BigInteger z = new BigInteger(1, hash32);
        BigInteger w = sig.s.modInverse(N);
        Pt q = mul(priv, G);
        Pt p = add(mul(z.multiply(w).mod(N), G), mul(sig.r.multiply(w).mod(N), q));
        return !p.inf() && affine(p)[0].mod(N).equals(sig.r);
    }

    /** RFC 6979 с HMAC-SHA256. */
    private static BigInteger nonce(byte[] h1, BigInteger x) {
        byte[] xb = Hex.fixed(x, 32);
        byte[] hb = Hex.fixed(new BigInteger(1, h1).mod(N), 32);
        byte[] v = new byte[32], k = new byte[32];
        java.util.Arrays.fill(v, (byte) 1);
        k = hmac(k, Hex.concat(v, new byte[]{0}, xb, hb));
        v = hmac(k, v);
        k = hmac(k, Hex.concat(v, new byte[]{1}, xb, hb));
        v = hmac(k, v);
        while (true) {
            v = hmac(k, v);
            BigInteger cand = new BigInteger(1, v);
            if (cand.signum() > 0 && cand.compareTo(N) < 0) return cand;
            k = hmac(k, Hex.concat(v, new byte[]{0}));
            v = hmac(k, v);
        }
    }

    private static byte[] hmac(byte[] key, byte[] data) {
        try {
            javax.crypto.Mac mac = javax.crypto.Mac.getInstance("HmacSHA256");
            mac.init(new javax.crypto.spec.SecretKeySpec(key, "HmacSHA256"));
            return mac.doFinal(data);
        } catch (Exception e) { throw new IllegalStateException(e); }
    }

    public static byte[] sha256(byte[] in) {
        try { return MessageDigest.getInstance("SHA-256").digest(in); }
        catch (Exception e) { throw new IllegalStateException(e); }
    }
}
