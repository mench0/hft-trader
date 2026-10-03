package com.hft.util;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import java.nio.charset.StandardCharsets;
import java.util.HexFormat;

/**
 * HMAC-SHA256 подпись запросов.
 *
 * Mac создаётся один раз и переиспользуется — инициализация ключа
 * стоит около микросекунды, и на серии ордеров это заметно.
 * Класс не потокобезопасен, на каждый поток нужен свой экземпляр.
 */
public final class Signer {

    private final Mac mac;

    public Signer(String secret) {
        try {
            this.mac = Mac.getInstance("HmacSHA256");
            this.mac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), "HmacSHA256"));
        } catch (Exception e) {
            throw new IllegalStateException("Не удалось создать HMAC", e);
        }
    }

    public synchronized String sign(String payload) {
        byte[] hash = mac.doFinal(payload.getBytes(StandardCharsets.UTF_8));
        return HexFormat.of().formatHex(hash);
    }
}
