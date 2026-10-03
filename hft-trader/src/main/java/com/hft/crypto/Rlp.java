package com.hft.crypto;

import java.io.ByteArrayOutputStream;
import java.math.BigInteger;
import java.util.List;

/** RLP-кодирование для транзакций EVM. Элементы: byte[], BigInteger (>=0), Long, List. */
public final class Rlp {
    private Rlp() {}

    public static byte[] encode(Object o) {
        if (o instanceof byte[] b) return encodeBytes(b);
        if (o instanceof BigInteger v) return encodeBytes(v.signum() == 0 ? new byte[0] : stripZero(v.toByteArray()));
        if (o instanceof Long l) return encode(BigInteger.valueOf(l));
        if (o instanceof List<?> list) {
            ByteArrayOutputStream body = new ByteArrayOutputStream();
            for (Object e : list) body.writeBytes(encode(e));
            byte[] b = body.toByteArray();
            return Hex.concat(prefix(b.length, 0xc0), b);
        }
        throw new IllegalArgumentException("RLP: тип " + o.getClass());
    }

    private static byte[] encodeBytes(byte[] b) {
        if (b.length == 1 && (b[0] & 0xff) < 0x80) return b;
        return Hex.concat(prefix(b.length, 0x80), b);
    }

    private static byte[] prefix(int len, int base) {
        if (len < 56) return new byte[]{(byte) (base + len)};
        byte[] l = stripZero(BigInteger.valueOf(len).toByteArray());
        return Hex.concat(new byte[]{(byte) (base + 55 + l.length)}, l);
    }

    private static byte[] stripZero(byte[] b) {
        int i = 0;
        while (i < b.length - 1 && b[i] == 0) i++;
        byte[] out = new byte[b.length - i];
        System.arraycopy(b, i, out, 0, out.length);
        return out;
    }
}
