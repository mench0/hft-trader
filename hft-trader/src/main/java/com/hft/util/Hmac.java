package com.hft.util;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.Base64;
import java.util.HexFormat;

/** HMAC и хеши для подписи запросов. Mac не потокобезопасен, поэтому каждый вызов создаёт свой. */
public final class Hmac {

    private Hmac() {}

    public static byte[] raw(String algorithm, String secret, String data) {
        try {
            Mac mac = Mac.getInstance(algorithm);
            mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), algorithm));
            return mac.doFinal(data.getBytes(StandardCharsets.UTF_8));
        } catch (Exception e) {
            throw new IllegalStateException("HMAC " + algorithm, e);
        }
    }

    public static String sha256Hex(String secret, String data) {
        return HexFormat.of().formatHex(raw("HmacSHA256", secret, data));
    }

    public static String sha256Base64(String secret, String data) {
        return Base64.getEncoder().encodeToString(raw("HmacSHA256", secret, data));
    }

    public static String sha512Hex(String secret, String data) {
        return HexFormat.of().formatHex(raw("HmacSHA512", secret, data));
    }

    /** SHA-512 от тела запроса (для подписи Gate). */
    public static String sha512HexOf(String data) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-512").digest(data.getBytes(StandardCharsets.UTF_8)));
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }
}
