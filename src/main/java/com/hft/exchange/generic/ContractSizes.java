package com.hft.exchange.generic;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hft.exchange.Exchange;
import com.hft.store.BalanceStore;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Размер контракта перпов, где объём считается в контрактах, а не в монетах: Gate (quanto_multiplier),
 * KuCoin Futures (multiplier), MEXC Contract (contractSize). Бот везде работает в монетах базовой валюты,
 * поэтому стакан и ордера пересчитываются: монеты = контракты × размер.
 *
 * Справочник биржи читается один раз публичным запросом (при подписке фида или загрузке правил).
 */
public final class ContractSizes {

    /** Утилитный класс — экземпляры не создаются. */
    private ContractSizes() {}

    /** "биржа:инструмент" -> размер контракта. */
    private static final Map<String, Double> SIZE = new ConcurrentHashMap<>();
    /** Разбор JSON. */
    private static final ObjectMapper JSON = new ObjectMapper();

    /** Имя перпа на бирже: Gate/MEXC — BTC_USDT, KuCoin — XBTUSDTM (биткоин у KuCoin Futures называется XBT). */
    public static String instrument(Exchange ex, String symbol) {
        String base = BalanceStore.baseAsset(symbol), quote = BalanceStore.quoteAsset(symbol);
        return switch (ex) {
            case KUCOIN -> (base.equals("BTC") ? "XBT" : base) + quote + "M";
            case GATE, MEXC -> base + "_" + quote;
            default -> throw new IllegalArgumentException("у " + ex + " объём перпа не в контрактах");
        };
    }

    /** Задать размер (из ответа биржи или в тестах). */
    public static void put(Exchange ex, String instrument, double size) {
        if (size > 0) SIZE.put(ex.id() + ":" + instrument, size);
    }

    /** Известный размер или 1 (горячий путь фида, справочник уже загружен при подписке). */
    public static double orOne(Exchange ex, String instrument) {
        return SIZE.getOrDefault(ex.id() + ":" + instrument, 1.0);
    }

    /** Размер; неизвестен — загрузить справочник биржи с restUrl, нет и там — исключение. */
    public static double get(Exchange ex, String restUrl, String instrument) {
        Double v = SIZE.get(ex.id() + ":" + instrument);
        if (v != null) return v;
        load(ex, restUrl);
        v = SIZE.get(ex.id() + ":" + instrument);
        if (v == null) throw new IllegalStateException(ex + ": нет перпа " + instrument);
        return v;
    }

    /** Загрузить справочник контрактов биржи. */
    public static synchronized void load(Exchange ex, String restUrl) {
        try {
            switch (ex) {
                case GATE -> {
                    for (JsonNode c : get(restUrl + "/api/v4/futures/usdt/contracts"))
                        put(ex, c.path("name").asText(), c.path("quanto_multiplier").asDouble(0));
                }
                case KUCOIN -> {
                    for (JsonNode c : get(restUrl + "/api/v1/contracts/active").path("data"))
                        put(ex, c.path("symbol").asText(), c.path("multiplier").asDouble(0));
                }
                case MEXC -> {
                    for (JsonNode c : get(restUrl + "/api/v1/contract/detail").path("data"))
                        put(ex, c.path("symbol").asText(), c.path("contractSize").asDouble(0));
                }
                default -> throw new IllegalArgumentException("у " + ex + " объём перпа не в контрактах");
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new IllegalStateException(ex + ": справочник контрактов не загружен: " + e.getMessage(), e);
        }
    }

    /** GET и разбор JSON. */
    private static JsonNode get(String url) throws Exception {
        HttpResponse<String> r = HttpClient.newHttpClient().send(
                HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(10)).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        if (r.statusCode() >= 400) throw new IllegalStateException("HTTP " + r.statusCode());
        return JSON.readTree(r.body());
    }
}
