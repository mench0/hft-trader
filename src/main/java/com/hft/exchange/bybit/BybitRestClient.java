package com.hft.exchange.bybit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hft.config.Credentials;
import com.hft.config.ExchangeConfig;
import com.hft.model.OrderEnums.Side;
import com.hft.model.OrderEnums.TimeInForce;
import com.hft.model.OrderEnums.Type;
import com.hft.model.OrderResult;
import com.hft.rest.ExchangeOrderApi;
import com.hft.rest.RateLimited;
import com.hft.rest.RateBudget;
import com.hft.store.SymbolFilters;
import com.hft.util.Numbers;
import com.hft.util.Signer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;

/**
 * REST-клиент Bybit v5 (unified account, spot).
 *
 * Отличия от Binance, из-за которых нельзя переиспользовать один клиент:
 *   - подпись передаётся в заголовках (X-BAPI-*), а не в query/body
 *   - строка для подписи: timestamp + apiKey + recvWindow + (query или body)
 *   - тело запроса — JSON, а не application/x-www-form-urlencoded
 *   - ответ всегда обёрнут в {retCode, retMsg, result: {...}}
 *   - объём в ордере называется qty, но для MARKET BUY на споте это
 *     сумма в котируемой валюте, а для MARKET SELL — объём в базовой
 *     (в отличие от Binance, где это выбирается параметром quoteOrderQty)
 */
public final class BybitRestClient implements ExchangeOrderApi {

    private static final Logger log = LoggerFactory.getLogger(BybitRestClient.class);

    private final String baseUrl;
    private final Credentials credentials;
    private final int recvWindow;
    private final HttpClient http;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Signer signer;
    private final SymbolFilters filters;
    private final AtomicLong clientOrderSeq = new AtomicLong(System.currentTimeMillis());

    public BybitRestClient(ExchangeConfig config, Credentials credentials, SymbolFilters filters) {
        this.baseUrl = config.restUrl();
        this.credentials = credentials;
        this.recvWindow = config.recvWindowMs();
        this.filters = filters;
        this.signer = credentials.isPresent() ? new Signer(credentials.apiSecret()) : null;
        this.http = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    // ======================= ПУБЛИЧНЫЕ =======================

    public double price(String symbol) throws Exception {
        JsonNode json = getPublic("/v5/market/tickers?category=spot&symbol=" + symbol.toUpperCase());
        return json.get("result").get("list").get(0).get("lastPrice").asDouble();
    }

    /** Загрузка торговых правил (шаг цены/объёма, минимальная сумма). */
    public void loadFilters(Iterable<String> symbols) throws Exception {
        JsonNode root = getPublic("/v5/market/instruments-info?category=spot");
        JsonNode list = root.path("result").path("list");
        int loaded = 0;

        for (JsonNode s : list) {
            String symbol = s.get("symbol").asText();
            boolean needed = false;
            for (String want : symbols) {
                if (want.equalsIgnoreCase(symbol)) { needed = true; break; }
            }
            if (!needed) continue;

            JsonNode lotSizeFilter = s.get("lotSizeFilter");
            JsonNode priceFilter = s.get("priceFilter");

            double minQty = lotSizeFilter.path("minOrderQty").asDouble(0);
            double maxQty = lotSizeFilter.path("maxOrderQty").asDouble(Double.MAX_VALUE);
            double stepSize = lotSizeFilter.path("basePrecision").asDouble(0);
            double tickSize = priceFilter.path("tickSize").asDouble(0);
            double minNotional = lotSizeFilter.path("minOrderAmt").asDouble(0);

            filters.put(symbol, new SymbolFilters.Filter(
                    minQty, maxQty, stepSize, 0, 0, tickSize, minNotional));
            loaded++;
        }
        log.info("[bybit] Загружены торговые правила для {} символов", loaded);
    }

    // ======================= БАЛАНСЫ =======================

    public void loadBalances(com.hft.store.BalanceStore store) throws Exception {
        credentials.require();
        JsonNode json = getSigned("/v5/account/wallet-balance", "accountType=UNIFIED");
        JsonNode list = json.path("result").path("list");
        int count = 0;
        for (JsonNode account : list) {
            for (JsonNode coin : account.path("coin")) {
                double free = coin.path("walletBalance").asDouble(0);
                double locked = coin.path("locked").asDouble(0);
                if (free > 0 || locked > 0) {
                    store.set(coin.get("coin").asText(), free, locked);
                    count++;
                }
            }
        }
        store.markSynced();
        log.info("[bybit] Загружены балансы по {} активам", count);
    }

    // ======================= ОРДЕРА =======================

    public OrderResult buyLimit(String symbol, double qty, double price, TimeInForce tif) throws Exception {
        return placeOrder(symbol, Side.BUY, Type.LIMIT, qty, price, tif, false);
    }

    public OrderResult sellLimit(String symbol, double qty, double price, TimeInForce tif) throws Exception {
        return placeOrder(symbol, Side.SELL, Type.LIMIT, qty, price, tif, false);
    }

    public OrderResult buyMarket(String symbol, double qty) throws Exception {
        return placeOrder(symbol, Side.BUY, Type.MARKET, qty, 0, null, false);
    }

    /**
     * У Bybit рыночная покупка на споте всегда задаётся суммой в котируемой
     * валюте (это поведение биржи, не наш выбор) — используем этот флаг,
     * чтобы OrderService мог единообразно вызывать оба варианта.
     */
    public OrderResult buyMarketForQuote(String symbol, double quoteAmount) throws Exception {
        return placeOrder(symbol, Side.BUY, Type.MARKET, quoteAmount, 0, null, true);
    }

    public OrderResult sellMarket(String symbol, double qty) throws Exception {
        return placeOrder(symbol, Side.SELL, Type.MARKET, qty, 0, null, false);
    }

    private OrderResult placeOrder(String symbol, Side side, Type type, double qty, double price,
                                   TimeInForce tif, boolean qtyIsQuote) throws Exception {
        credentials.require();
        String sym = symbol.toUpperCase();

        double roundedQty = qtyIsQuote ? qty : filters.roundQuantity(sym, qty);
        if (!qtyIsQuote) {
            String err = filters.validate(sym, roundedQty, type == Type.LIMIT ? price : 0);
            if (err != null) throw new IllegalArgumentException("Ордер не прошёл проверку: " + err);
        }

        String clientOrderId = "hft" + clientOrderSeq.incrementAndGet();

        StringBuilder body = new StringBuilder(200);
        body.append('{')
            .append("\"category\":\"spot\",")
            .append("\"symbol\":\"").append(sym).append("\",")
            .append("\"side\":\"").append(side == Side.BUY ? "Buy" : "Sell").append("\",")
            .append("\"orderType\":\"").append(type == Type.LIMIT ? "Limit" : "Market").append("\",")
            .append("\"qty\":\"").append(Numbers.plain(roundedQty, filters.quantityScale(sym))).append("\",")
            .append("\"orderLinkId\":\"").append(clientOrderId).append("\"");

        if (qtyIsQuote) {
            body.append(",\"marketUnit\":\"quoteCoin\"");
        }
        if (type == Type.LIMIT) {
            double roundedPrice = filters.roundPrice(sym, price);
            body.append(",\"price\":\"").append(Numbers.plain(roundedPrice, filters.priceScale(sym))).append('"');
            body.append(",\"timeInForce\":\"").append(bybitTif(tif)).append('"');
        }
        body.append('}');

        long start = System.nanoTime();
        JsonNode json = postSigned("/v5/order/create", body.toString());
        long latency = System.nanoTime() - start;

        JsonNode result = json.path("result");
        long orderId = result.path("orderId").asLong(0);

        // Bybit не возвращает детали исполнения в ответе на создание —
        // нужен отдельный запрос статуса, если требуется executedQty сразу
        return new OrderResult(orderId, clientOrderId, sym, side, "NEW",
                qtyIsQuote ? 0 : roundedQty, 0, 0, latency);
    }

    private static String bybitTif(TimeInForce tif) {
        if (tif == null) return "GTC";
        return switch (tif) {
            case GTC -> "GTC";
            case IOC -> "IOC";
            case FOK -> "FOK";
        };
    }

    public void cancelOrder(String symbol, long orderId) throws Exception {
        credentials.require();
        String body = String.format(
                "{\"category\":\"spot\",\"symbol\":\"%s\",\"orderId\":\"%d\"}",
                symbol.toUpperCase(), orderId);
        postSigned("/v5/order/cancel", body);
        log.info("[bybit] Ордер {} по {} отменён", orderId, symbol);
    }

    public int cancelAll(String symbol) throws Exception {
        credentials.require();
        String body = String.format("{\"category\":\"spot\",\"symbol\":\"%s\"}", symbol.toUpperCase());
        JsonNode json = postSigned("/v5/order/cancel-all", body);
        JsonNode list = json.path("result").path("list");
        int count = list.isArray() ? list.size() : 0;
        log.info("[bybit] Отменено {} ордеров по {}", count, symbol);
        return count;
    }

    public OrderResult orderStatus(String symbol, long orderId) throws Exception {
        credentials.require();
        String query = "category=spot&symbol=" + symbol.toUpperCase() + "&orderId=" + orderId;
        JsonNode json = getSigned("/v5/order/realtime", query);
        JsonNode order = json.path("result").path("list").get(0);
        if (order == null) throw new IllegalStateException("Ордер не найден");

        Side side = "Buy".equals(order.get("side").asText()) ? Side.BUY : Side.SELL;
        String status = mapBybitStatus(order.get("orderStatus").asText());
        double origQty = order.path("qty").asDouble(0);
        double execQty = order.path("cumExecQty").asDouble(0);
        double avgPrice = order.path("avgPrice").asDouble(0);

        return new OrderResult(orderId, order.path("orderLinkId").asText(""),
                symbol.toUpperCase(), side, status, origQty, execQty, avgPrice, 0);
    }

    private static String mapBybitStatus(String bybitStatus) {
        return switch (bybitStatus) {
            case "New" -> "NEW";
            case "PartiallyFilled" -> "PARTIALLY_FILLED";
            case "Filled" -> "FILLED";
            case "Cancelled" -> "CANCELED";
            case "Rejected" -> "REJECTED";
            default -> bybitStatus.toUpperCase();
        };
    }

    // ======================= HTTP + ПОДПИСЬ =======================
    //
    // Bybit v5 подписывает конкатенацию timestamp+apiKey+recvWindow+payload,
    // где payload — это query string для GET и тело JSON для POST.
    // Подпись передаётся в заголовке X-BAPI-SIGN, а не в самом запросе —
    // этим и отличается от Binance, где подпись — часть query/body.

    private JsonNode getPublic(String path) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();
        return send(req);
    }

    private JsonNode getSigned(String path, String query) throws Exception {
        long ts = System.currentTimeMillis();
        String payload = ts + credentials.apiKey() + recvWindow + query;
        String sign = signer.sign(payload);

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path + "?" + query))
                .header("X-BAPI-API-KEY", credentials.apiKey())
                .header("X-BAPI-TIMESTAMP", String.valueOf(ts))
                .header("X-BAPI-RECV-WINDOW", String.valueOf(recvWindow))
                .header("X-BAPI-SIGN", sign)
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();
        return send(req);
    }

    private JsonNode postSigned(String path, String jsonBody) throws Exception {
        long ts = System.currentTimeMillis();
        String payload = ts + credentials.apiKey() + recvWindow + jsonBody;
        String sign = signer.sign(payload);

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .header("X-BAPI-API-KEY", credentials.apiKey())
                .header("X-BAPI-TIMESTAMP", String.valueOf(ts))
                .header("X-BAPI-RECV-WINDOW", String.valueOf(recvWindow))
                .header("X-BAPI-SIGN", sign)
                .header("Content-Type", "application/json")
                .timeout(Duration.ofSeconds(10))
                .POST(HttpRequest.BodyPublishers.ofString(jsonBody))
                .build();
        return send(req);
    }

    /** Общий бюджет запросов Bybit: X-Bapi-Limit-Status / Reset-Timestamp подтягивают счёт к счёту биржи. */
    private final RateBudget budget = RateBudget.of("bybit");

    private JsonNode send(HttpRequest req) throws Exception {
        String path = req.uri().getPath();
        boolean order = path.startsWith("/v5/order/create") || path.startsWith("/v5/order/cancel");
        RateBudget.Kind kind = order ? RateBudget.Kind.ORDER
                : req.headers().firstValue("X-BAPI-API-KEY").isPresent() ? RateBudget.Kind.PRIVATE : RateBudget.Kind.PUBLIC;
        budget.acquire(kind, 1, order ? 1000 : 3000);
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        budget.onResponse(resp.statusCode(), resp.headers());
        JsonNode json = mapper.readTree(resp.body());
        int retCode = json.path("retCode").asInt(-1);
        if (retCode == 10006 || retCode == 10018) budget.onLimitError("retCode " + retCode);
        if (resp.statusCode() >= 400 || retCode != 0) {
            throw new ExchangeException(resp.statusCode(), retCode, json.path("retMsg").asText(resp.body()));
        }
        return json;
    }

    /** retCode 10006 — rate limit у Bybit. */
    public static final class ExchangeException extends RuntimeException implements RateLimited {
        private final int httpStatus;
        private final int retCode;

        public ExchangeException(int httpStatus, int retCode, String message) {
            super("HTTP " + httpStatus + " retCode " + retCode + ": " + message);
            this.httpStatus = httpStatus;
            this.retCode = retCode;
        }

        @Override
        public boolean isRateLimit() {
            return retCode == 10006 || httpStatus == 429;
        }
    }
}
