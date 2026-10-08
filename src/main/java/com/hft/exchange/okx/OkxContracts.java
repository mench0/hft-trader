package com.hft.exchange.okx;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hft.store.BalanceStore;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Размер контракта (ctVal) перпов OKX: стакан и ордера SWAP считаются в контрактах
 * (BTC-USDT-SWAP — 0.01 BTC за контракт), а бот везде работает в базовой валюте.
 * Справочник читается один раз публичным запросом /api/v5/public/instruments?instType=SWAP.
 */
public final class OkxContracts {

    /** Утилитный класс — экземпляры не создаются. */
    private OkxContracts() {}

    /** instId -> ctVal. */
    private static final Map<String, Double> CT_VAL = new ConcurrentHashMap<>();
    /** Разбор JSON. */
    private static final ObjectMapper JSON = new ObjectMapper();

    /** Имя перпа на OKX: BTCUSDT -> BTC-USDT-SWAP. */
    public static String instId(String symbol) {
        return BalanceStore.baseAsset(symbol) + "-" + BalanceStore.quoteAsset(symbol) + "-SWAP";
    }

    /** Задать размер контракта (из ответа биржи или в тестах). */
    public static void put(String instId, double ctVal) { if (ctVal > 0) CT_VAL.put(instId, ctVal); }

    /** Размер контракта; неизвестен — загрузить справочник с restUrl (если ещё нет — 1 и исключение). */
    public static double ctVal(String restUrl, String instId) {
        Double v = CT_VAL.get(instId);
        if (v != null) return v;
        load(restUrl);
        v = CT_VAL.get(instId);
        if (v == null) throw new IllegalStateException("OKX: нет перпа " + instId);
        return v;
    }

    /** Известный размер контракта или 1 (для горячего пути, после load). */
    public static double ctValOrOne(String instId) { return CT_VAL.getOrDefault(instId, 1.0); }

    /** Загрузить справочник SWAP-инструментов. */
    public static synchronized void load(String restUrl) {
        try {
            HttpResponse<String> r = HttpClient.newHttpClient().send(HttpRequest.newBuilder(
                    URI.create(restUrl + "/api/v5/public/instruments?instType=SWAP")).timeout(Duration.ofSeconds(10)).GET().build(),
                    HttpResponse.BodyHandlers.ofString());
            JsonNode n = JSON.readTree(r.body());
            for (JsonNode x : n.path("data")) put(x.path("instId").asText(), x.path("ctVal").asDouble(0));
        } catch (Exception e) {
            throw new IllegalStateException("OKX: справочник контрактов не загружен: " + e.getMessage(), e);
        }
    }
}
