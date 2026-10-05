package com.hft.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.hft.model.OrderEnums.Side;
import com.hft.model.OrderResult;
import com.hft.rest.WsRpcChannel.Msg;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Приватный поток событий аккаунта через listenKey (BingX, Aster — как у Binance): ключ берётся по REST
 * перед каждым подключением, продлевается раз в 25 минут (биржи держат его 60 минут), события идут по WS.
 * Ордера у этих бирж — только REST, поток нужен, чтобы исполнения и баланс приходили без опроса.
 */
public final class UserStream {

    /** Логгер. */
    private static final Logger log = LoggerFactory.getLogger(UserStream.class);

    /** Что зависит от биржи. */
    public interface Api {
        /** Новый listenKey (REST). */
        String newListenKey() throws Exception;
        /** Продлить listenKey (REST). */
        void keepAlive(String listenKey) throws Exception;
        /** Адрес WS для ключа. */
        String url(String listenKey);
        /** Подписки после подключения (у Aster не нужны — поток определяется ключом). */
        default List<String> subscriptions() { return List.of(); }
        /** Разбор: событие, служебное или ответ на пинг. */
        Msg parse(String text) throws Exception;
        /** Прикладной пинг; null — не нужен. */
        default String ping() { return null; }
        /** Бинарный кадр в текст (gzip у BingX). */
        default String decodeBinary(byte[] d) throws Exception { return new String(d, java.nio.charset.StandardCharsets.UTF_8); }
    }

    /** Биржа (для логов). */
    private final String name;
    /** Сокет потока. */
    private final WsRpcChannel channel;
    /** Продление ключа. */
    private final ScheduledExecutorService keepAlive;
    /** Текущий ключ. */
    private volatile String listenKey;

    /**
     * @param name биржа
     * @param api биржевая часть
     * @param sink получатель событий
     */
    public UserStream(String name, Api api, WsRpcChannel.EventSink sink) {
        this.name = name;
        this.channel = new WsRpcChannel(name, new WsRpcChannel.Protocol() {
            @Override public String url() {
                try { listenKey = api.newListenKey(); }
                catch (Exception e) { throw new IllegalStateException(name + " listenKey: " + e.getMessage(), e); }
                return api.url(listenKey);
            }
            @Override public List<String> login() { return List.of(); }
            @Override public List<String> subscriptions() { return api.subscriptions(); }
            @Override public Msg parse(String text) throws Exception { return api.parse(text); }
            @Override public String ping() { return api.ping(); }
            @Override public String decodeBinary(byte[] d) throws Exception { return api.decodeBinary(d); }
        }).onEvent(sink);
        this.keepAlive = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "listenkey-" + name);
            t.setDaemon(true);
            return t;
        });
        keepAlive.scheduleAtFixedRate(() -> {
            String k = listenKey;
            if (k == null) return;
            try { api.keepAlive(k); }
            catch (Exception e) { log.warn("[{}] продление listenKey не удалось: {} — при обрыве ключ возьмётся заново", name, e.getMessage()); }
        }, 25, 25, TimeUnit.MINUTES);
    }

    /** Сокет потока (для готовности и метрик). */
    public WsRpcChannel channel() { return channel; }

    /** Запустить. */
    public UserStream start() { channel.start(); return this; }

    /** Остановить сокет и продление ключа. */
    public void stop() {
        channel.stop();
        keepAlive.shutdownNow();
    }

    /** executionReport в формате Binance (s, c, S, X, i, q, z, Z) в OrderResult; symbol — наш формат. */
    public static OrderResult executionReport(JsonNode e, long id, String symbol) {
        double exec = e.path("z").asDouble(0), quote = e.path("Z").asDouble(0);
        return new OrderResult(id, e.path("c").asText(""), symbol,
                "SELL".equalsIgnoreCase(e.path("S").asText()) ? Side.SELL : Side.BUY, e.path("X").asText(),
                e.path("q").asDouble(0), exec, exec > 0 ? quote / exec : 0, 0);
    }
}
