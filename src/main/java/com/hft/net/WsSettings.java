package com.hft.net;

import com.hft.exchange.Exchange;

/**
 * Параметры WebSocket-соединения, которые у бирж разные (адрес, подписка и пинг — в диалектах и протоколах бирж).
 * Значения — по документации бирж, с запасом; не сверялись с живыми серверами.
 *
 * @param maxMessageBytes  предел одного сообщения: больше — соединение рвётся (защита памяти)
 * @param connectTimeoutMs таймаут TCP-подключения
 * @param handshakeTimeoutMs таймаут рукопожатия WebSocket
 * @param maxLifetimeMs    биржа сама рвёт соединение через это время (Binance, MEXC, KuCoin — 24 ч): бот переподключается
 *                         заранее, в спокойный момент; 0 — без ограничения
 */
public record WsSettings(int maxMessageBytes, int connectTimeoutMs, long handshakeTimeoutMs, long maxLifetimeMs) {

    /** 1 МБ. */
    private static final int MB = 1 << 20;
    /** Плановое переподключение для бирж с жизнью соединения 24 ч: за 30 минут до разрыва. */
    private static final long DAY_MINUS = 23L * 3_600_000 + 30 * 60_000;

    /** По умолчанию: 8 МБ, 5 с, 10 с, без ограничения жизни. */
    public static final WsSettings DEFAULT = new WsSettings(8 * MB, 5_000, 10_000, 0);

    /** Параметры биржи по её id; неизвестная биржа (тесты, своя нода) — {@link #DEFAULT}. */
    public static WsSettings forExchange(String id) {
        return switch (Exchange.find(id).orElse(null)) {
            case BINANCE, ASTER -> new WsSettings(2 * MB, 5_000, 10_000, DAY_MINUS);   // соединение живёт 24 ч
            case MEXC -> new WsSettings(2 * MB, 5_000, 10_000, DAY_MINUS);            // 24 ч, protobuf — сообщения маленькие
            case KUCOIN -> new WsSettings(2 * MB, 5_000, 10_000, DAY_MINUS);          // токен bullet и соединение — до 24 ч
            case BYBIT, GATE -> new WsSettings(2 * MB, 5_000, 10_000, 0);
            case OKX -> new WsSettings(4 * MB, 5_000, 10_000, 0);                     // снимок books — до 400 уровней
            case HYPERLIQUID -> new WsSettings(8 * MB, 5_000, 10_000, 0);             // ответы info через WS post бывают большими
            case UNISWAPV2 -> new WsSettings(16 * MB, 10_000, 15_000, 0);             // JSON-RPC ноды: логи и ответы бывают большими
            case null, default -> DEFAULT;
        };
    }
}
