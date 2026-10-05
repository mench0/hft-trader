package com.hft.exchange.generic;

import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Всё, что отличает WebSocket одной биржи от другой: адрес, формат подписки, пинг,
 * формат сообщений. Общий жизненный цикл (подключение, переподключение, локальный стакан)
 * в {@link WsBookFeed}.
 */
public interface WsDialect {

    /** Адрес по умолчанию, если в конфиге не задан ws-url. */
    String defaultUrl(boolean testnet);

    /** Адрес с учётом REST/RPC-адреса из конфига (для нод: https -> wss того же хоста). */
    default String defaultUrl(boolean testnet, String restUrl) { return defaultUrl(testnet); }

    /**
     * Адрес для очередного подключения. По умолчанию — url как есть; KuCoin перед каждым
     * подключением получает по REST токен и адрес сервера.
     */
    default String connectUrl(String url, String restUrl) throws Exception { return url; }

    /** Как символ называется на бирже: BTCUSDT -> BTC-USDT, btc_usdt, BTC… */
    String venueSymbol(String symbol);

    /** Сообщения подписки на список символов. */
    List<String> subscribe(List<String> venueSymbols, int depth);

    /** Сообщения отписки от символов. */
    List<String> unsubscribe(List<String> venueSymbols, int depth);

    /** Прикладной пинг. null — не нужен (биржа шлёт ping-кадры, а JDK отвечает на них сам). */
    default String pingMessage() { return null; }

    /** Интервал прикладного пинга, мс. */
    default long pingIntervalMs() { return 15_000; }

    /** Бинарные сообщения (например, gzip у BingX) -> текст. */
    default String decodeBinary(byte[] data) throws Exception { return new String(data, StandardCharsets.UTF_8); }

    /** Бинарные кадры разбираются самим диалектом ({@link #parseBinary}), а не через decodeBinary + parse (protobuf у MEXC). */
    default boolean parsesBinary() { return false; }

    /**
     * Разбор бинарного кадра прямо в out (как {@link #parse}); вызывается, только если {@link #parsesBinary()}.
     * Возвращает ответ, который нужно отправить немедленно, или null.
     */
    default String parseBinary(byte[] data, int len, BookBatch out) throws Exception {
        throw new UnsupportedOperationException("диалект не разбирает бинарные кадры");
    }

    /**
     * Потоковый разбор сообщения из буфера символов (без промежуточного дерева JSON).
     * Если это стакан — заполняет out (venue, snapshot, уровни; объём 0 = убрать уровень).
     * Возвращает ответ, который нужно отправить немедленно (pong), или null.
     * Неожиданный формат или ошибка биржи — исключение: молча пропускать нельзя.
     */
    String parse(char[] buf, int len, BookBatch out) throws Exception;
}
