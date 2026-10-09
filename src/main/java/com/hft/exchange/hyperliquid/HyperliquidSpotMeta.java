package com.hft.exchange.hyperliquid;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Справочник спотовых пар Hyperliquid ({@code POST /info {"type":"spotMeta"}}).
 * <p>
 * На споте у Hyperliquid всё не так, как на перпах:
 * <ul>
 *   <li>имя пары на бирже — {@code PURR/USDC} у первой (каноничной) пары и {@code @<индекс>} у остальных;
 *       по нему подписка на стакан, allMids, ордера и события ({@code coin});</li>
 *   <li>номер актива в ордере — {@code 10000 + индекс пары};</li>
 *   <li>шаг объёма — szDecimals базового токена, у цены до 8 знаков (на перпах до 6).</li>
 * </ul>
 * Символ бота — имена токенов подряд: HYPE/USDC → HYPEUSDC, обёрнутый биткоин UBTC/USDC → UBTCUSDC.
 * Справочник читается один раз на адрес REST (клиент, фид стакана и REST-запас пользуются одним).
 */
public final class HyperliquidSpotMeta {

    /** Утилитный класс — экземпляры не создаются. */
    private HyperliquidSpotMeta() {}

    /** Адрес REST по умолчанию. */
    public static final String DEFAULT_REST = "https://api.hyperliquid.xyz";

    /**
     * Спотовая пара.
     * @param coin имя на бирже (PURR/USDC или @107)
     * @param asset номер актива для ордеров (10000 + индекс)
     * @param szDecimals знаков объёма (базовый токен)
     * @param base базовый токен
     * @param quote котируемый токен
     */
    public record Pair(String coin, int asset, int szDecimals, String base, String quote) {
        /** Символ бота: base + quote. */
        public String symbol() { return base + quote; }
    }

    /** Адрес REST -> (символ -> пара). */
    private static final Map<String, Map<String, Pair>> BY_SYMBOL = new ConcurrentHashMap<>();
    /** Адрес REST -> (имя на бирже -> пара). */
    private static final Map<String, Map<String, Pair>> BY_COIN = new ConcurrentHashMap<>();
    /** Разбор JSON. */
    private static final ObjectMapper JSON = new ObjectMapper();
    /** HTTP для загрузки справочника. */
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    /** Адрес REST или адрес по умолчанию. */
    private static String rest(String restUrl) { return restUrl == null || restUrl.isBlank() ? DEFAULT_REST : restUrl; }

    /** Запомнить справочник из ответа spotMeta (клиент читает его сам, тесты — подставляют). */
    public static void put(String restUrl, JsonNode spotMeta) {
        Map<Integer, JsonNode> tokens = new java.util.HashMap<>();
        for (JsonNode t : spotMeta.path("tokens")) tokens.put(t.path("index").asInt(), t);
        Map<String, Pair> bySymbol = new ConcurrentHashMap<>(), byCoin = new ConcurrentHashMap<>();
        for (JsonNode u : spotMeta.path("universe")) {
            JsonNode tk = u.path("tokens");
            JsonNode b = tokens.get(tk.path(0).asInt(-1)), q = tokens.get(tk.path(1).asInt(-1));
            if (b == null || q == null) continue;
            Pair p = new Pair(u.path("name").asText(), 10_000 + u.path("index").asInt(),
                    b.path("szDecimals").asInt(2), b.path("name").asText().toUpperCase(), q.path("name").asText().toUpperCase());
            bySymbol.putIfAbsent(p.symbol(), p);                // первая (каноничная) пара с этими токенами
            byCoin.put(p.coin(), p);
        }
        BY_SYMBOL.put(rest(restUrl), bySymbol);
        BY_COIN.put(rest(restUrl), byCoin);
    }

    /** Пара символа; справочник не загружен — загрузить с restUrl. */
    public static Optional<Pair> find(String restUrl, String symbol) {
        Map<String, Pair> m = BY_SYMBOL.get(rest(restUrl));
        if (m == null) { load(restUrl); m = BY_SYMBOL.getOrDefault(rest(restUrl), Map.of()); }
        return Optional.ofNullable(m.get(symbol.toUpperCase()));
    }

    /** Пара символа; нет такой — исключение с понятным текстом. */
    public static Pair require(String restUrl, String symbol) {
        return find(restUrl, symbol).orElseThrow(() -> new IllegalArgumentException(
                "Hyperliquid spot: нет пары " + symbol + " (символ — имена токенов подряд, например HYPEUSDC, UBTCUSDC)"));
    }

    /** Пара по имени на бирже (PURR/USDC, @107); только из уже загруженного справочника. */
    public static Optional<Pair> byCoin(String restUrl, String coin) {
        return Optional.ofNullable(BY_COIN.getOrDefault(rest(restUrl), Map.of()).get(coin));
    }

    /** Загрузить справочник с биржи. */
    public static synchronized void load(String restUrl) {
        if (BY_SYMBOL.containsKey(rest(restUrl))) return;
        try {
            HttpRequest rq = HttpRequest.newBuilder(URI.create(rest(restUrl) + "/info")).timeout(Duration.ofSeconds(5))
                    .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString("{\"type\":\"spotMeta\"}")).build();
            HttpResponse<String> r = HTTP.send(rq, HttpResponse.BodyHandlers.ofString());
            if (r.statusCode() != 200) throw new IllegalStateException("HTTP " + r.statusCode());
            put(restUrl, JSON.readTree(r.body()));
        } catch (Exception e) {
            throw new IllegalStateException("Hyperliquid spotMeta: " + e.getMessage(), e);
        }
    }
}
