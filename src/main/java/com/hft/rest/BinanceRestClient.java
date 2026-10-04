package com.hft.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hft.config.Credentials;
import com.hft.config.ExchangeConfig;
import com.hft.model.OrderEnums.Side;
import com.hft.model.OrderEnums.TimeInForce;
import com.hft.model.OrderEnums.Type;
import com.hft.model.OrderResult;
import com.hft.store.BalanceStore;
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
 * REST-клиент Binance на встроенном java.net.http.HttpClient.
 *
 * Почему не OkHttp / Apache HttpClient: встроенный клиент в Java 21
 * поддерживает HTTP/2, пул соединений и keep-alive из коробки.
 * Для REST-части (ордера, балансы) его производительности достаточно,
 * а зависимостей меньше.
 *
 * Класс потокобезопасен — HttpClient и Signer можно вызывать из разных потоков.
 */
public final class BinanceRestClient implements ExchangeOrderApi {

    /** Логгер. */
    private static final Logger log = LoggerFactory.getLogger(BinanceRestClient.class);

    /** REST-адрес. */
    private final String baseUrl;
    /** API-ключи. */
    private final Credentials credentials;
    /** Окно годности подписи, мс. */
    private final int recvWindow;
    /** HTTP/2-клиент. */
    private final HttpClient http;
    /** Разбор JSON. */
    private final ObjectMapper mapper = new ObjectMapper();
    /** Подпись HMAC-SHA256. */
    private final Signer signer;
    /** Правила символов. */
    private final SymbolFilters filters;
    /** Счётчик для newClientOrderId. */
    private final AtomicLong clientOrderSeq = new AtomicLong(System.currentTimeMillis());

    /** Разница между временем биржи и локальным. Без неё подпись может отвергаться. */
    private volatile long timeOffsetMs = 0;

    /**
     * @param config подключение и параметры
     * @param credentials ключи
     * @param filters правила символов
     */
    public BinanceRestClient(ExchangeConfig config, Credentials credentials, SymbolFilters filters) {
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

    // ======================= ПУБЛИЧНЫЕ ЭНДПОИНТЫ =======================

    /**
     * Синхронизация часов с биржей. Вызывать при старте и раз в несколько минут:
     * если локальные часы уйдут больше чем на recvWindow, биржа начнёт
     * отклонять все подписанные запросы.
     */
    public void syncTime() throws Exception {
        long before = System.currentTimeMillis();
        JsonNode json = getPublic("/api/v3/time");
        long after = System.currentTimeMillis();
        long serverTime = json.get("serverTime").asLong();
        long localMid = (before + after) / 2;
        timeOffsetMs = serverTime - localMid;
        log.info("Часы синхронизированы, смещение {} мс (RTT {} мс)", timeOffsetMs, after - before);
    }

    /** Текущая цена одного символа. */
    public double price(String symbol) throws Exception {
        JsonNode json = getPublic("/api/v3/ticker/price?symbol=" + symbol.toUpperCase());
        return json.get("price").asDouble();
    }

    /**
     * Загрузка торговых правил по символам: шаг цены, шаг объёма, минимальная сумма.
     * Вызывается один раз при старте, результат живёт в памяти.
     */
    public void loadFilters(Iterable<String> symbols) throws Exception {
        JsonNode root = getPublic("/api/v3/exchangeInfo");
        JsonNode list = root.get("symbols");
        if (list == null || !list.isArray()) return;

        int loaded = 0;
        for (JsonNode s : list) {
            String symbol = s.get("symbol").asText();
            boolean needed = false;
            for (String want : symbols) {
                if (want.equalsIgnoreCase(symbol)) { needed = true; break; }
            }
            if (!needed) continue;

            double minQty = 0, maxQty = Double.MAX_VALUE, stepSize = 0;
            double minPrice = 0, maxPrice = 0, tickSize = 0, minNotional = 0;

            for (JsonNode f : s.get("filters")) {
                String type = f.get("filterType").asText();
                switch (type) {
                    case "LOT_SIZE" -> {
                        minQty = f.get("minQty").asDouble();
                        maxQty = f.get("maxQty").asDouble();
                        stepSize = f.get("stepSize").asDouble();
                    }
                    case "PRICE_FILTER" -> {
                        minPrice = f.get("minPrice").asDouble();
                        maxPrice = f.get("maxPrice").asDouble();
                        tickSize = f.get("tickSize").asDouble();
                    }
                    case "NOTIONAL", "MIN_NOTIONAL" -> {
                        JsonNode mn = f.has("minNotional") ? f.get("minNotional") : null;
                        if (mn != null) minNotional = mn.asDouble();
                    }
                    default -> { }
                }
            }
            filters.put(symbol, new SymbolFilters.Filter(
                    minQty, maxQty, stepSize, minPrice, maxPrice, tickSize, minNotional));
            loaded++;
        }
        log.info("Загружены торговые правила для {} символов", loaded);
    }

    // ======================= БАЛАНСЫ =======================

    /** Полная загрузка балансов в память. */
    public void loadBalances(BalanceStore store) throws Exception {
        credentials.require();
        JsonNode json = getSigned("/api/v3/account", "");
        JsonNode balances = json.get("balances");
        int count = 0;
        for (JsonNode b : balances) {
            double free = b.get("free").asDouble();
            double locked = b.get("locked").asDouble();
            if (free > 0 || locked > 0) {
                store.set(b.get("asset").asText(), free, locked);
                count++;
            }
        }
        store.markSynced();
        log.info("Загружены балансы по {} активам", count);
    }

    // ======================= ОРДЕРА =======================

    /**
     * Лимитный ордер на покупку.
     *
     * @param qty объём в базовой валюте (для BTCUSDT — количество BTC)
     * @param tif GTC (висеть до отмены), IOC (частичное исполнение, остаток отменить),
     *            FOK (только целиком)
     */
    public OrderResult buyLimit(String symbol, double qty, double price, TimeInForce tif) throws Exception {
        return placeOrder(symbol, Side.BUY, Type.LIMIT, qty, price, tif, 0);
    }

    /** Лимитный ордер на продажу. */
    public OrderResult sellLimit(String symbol, double qty, double price, TimeInForce tif) throws Exception {
        return placeOrder(symbol, Side.SELL, Type.LIMIT, qty, price, tif, 0);
    }

    /**
     * Рыночная покупка на заданный объём базовой валюты.
     * Например, купить ровно 0.01 BTC, сколько бы это ни стоило.
     */
    public OrderResult buyMarket(String symbol, double qty) throws Exception {
        return placeOrder(symbol, Side.BUY, Type.MARKET, qty, 0, null, 0);
    }

    /**
     * Рыночная покупка на заданную сумму в котируемой валюте.
     * Например, потратить ровно 100 USDT, сколько бы BTC ни получилось.
     * Это отдельный параметр биржи quoteOrderQty — удобно для входа "на всю сумму".
     */
    public OrderResult buyMarketForQuote(String symbol, double quoteAmount) throws Exception {
        return placeOrder(symbol, Side.BUY, Type.MARKET, 0, 0, null, quoteAmount);
    }

    /** Рыночная продажа заданного объёма базовой валюты. */
    public OrderResult sellMarket(String symbol, double qty) throws Exception {
        return placeOrder(symbol, Side.SELL, Type.MARKET, qty, 0, null, 0);
    }

    /**
     * Базовый метод размещения. Остальные — обёртки над ним.
     *
     * @param quoteOrderQty если > 0, объём задаётся суммой в котируемой валюте
     *                      вместо количества базовой (только для MARKET)
     */
    public OrderResult placeOrder(String symbol, Side side, Type type,
                                  double qty, double price, TimeInForce tif,
                                  double quoteOrderQty) throws Exception {
        credentials.require();
        String sym = symbol.toUpperCase();

        StringBuilder params = new StringBuilder(160);
        params.append("symbol=").append(sym)
              .append("&side=").append(side)
              .append("&type=").append(type);

        if (quoteOrderQty > 0) {
            if (type != Type.MARKET) {
                throw new IllegalArgumentException("quoteOrderQty работает только с MARKET");
            }
            params.append("&quoteOrderQty=").append(Numbers.plain(quoteOrderQty, 8));
        } else {
            double roundedQty = filters.roundQuantity(sym, qty);
            String err = filters.validate(sym, roundedQty, type == Type.LIMIT ? price : 0);
            if (err != null) {
                throw new IllegalArgumentException("Ордер не прошёл проверку: " + err);
            }
            params.append("&quantity=")
                  .append(Numbers.plain(roundedQty, filters.quantityScale(sym)));
        }

        if (type == Type.LIMIT) {
            double roundedPrice = filters.roundPrice(sym, price);
            params.append("&price=")
                  .append(Numbers.plain(roundedPrice, filters.priceScale(sym)))
                  .append("&timeInForce=")
                  .append(tif == null ? TimeInForce.GTC : tif);
        }

        String clientOrderId = "hft" + clientOrderSeq.incrementAndGet();
        params.append("&newClientOrderId=").append(clientOrderId);
        params.append("&newOrderRespType=FULL"); // просим полный ответ с деталями исполнения

        long start = System.nanoTime();
        JsonNode json = postSigned("/api/v3/order", params.toString());
        long latency = System.nanoTime() - start;

        return parseOrderResult(json, sym, side, latency);
    }

    /** Отмена ордера по ID биржи. */
    public void cancelOrder(String symbol, long orderId) throws Exception {
        credentials.require();
        String params = "symbol=" + symbol.toUpperCase() + "&orderId=" + orderId;
        deleteSigned("/api/v3/order", params);
        log.info("Ордер {} по {} отменён", orderId, symbol);
    }

    /** Отмена всех открытых ордеров по символу — быстрый выход из рынка. */
    public int cancelAll(String symbol) throws Exception {
        credentials.require();
        String params = "symbol=" + symbol.toUpperCase();
        JsonNode json = deleteSigned("/api/v3/openOrders", params);
        int count = json.isArray() ? json.size() : 0;
        log.info("Отменено {} ордеров по {}", count, symbol);
        return count;
    }

    /** Статус ордера: NEW, PARTIALLY_FILLED, FILLED, CANCELED, REJECTED, EXPIRED. */
    public OrderResult orderStatus(String symbol, long orderId) throws Exception {
        credentials.require();
        String params = "symbol=" + symbol.toUpperCase() + "&orderId=" + orderId;
        JsonNode json = getSigned("/api/v3/order", params);
        Side side = Side.valueOf(json.get("side").asText());
        return parseOrderResult(json, symbol.toUpperCase(), side, 0);
    }

    /** Список открытых ордеров по символу. */
    public JsonNode openOrders(String symbol) throws Exception {
        credentials.require();
        return getSigned("/api/v3/openOrders", "symbol=" + symbol.toUpperCase());
    }

    /** Ответ на ордер в OrderResult (средняя цена — по fills или cummulativeQuoteQty). */
    private OrderResult parseOrderResult(JsonNode json, String symbol, Side side, long latency) {
        long orderId = json.path("orderId").asLong();
        String clientId = json.path("clientOrderId").asText("");
        String status = json.path("status").asText("UNKNOWN");
        double origQty = json.path("origQty").asDouble(0);
        double execQty = json.path("executedQty").asDouble(0);
        double cummQuote = json.path("cummulativeQuoteQty").asDouble(0);
        double avgPrice = execQty > 0 ? cummQuote / execQty : 0;

        // Для MARKET с quoteOrderQty поле origQty может быть нулевым
        if (origQty == 0 && execQty > 0) origQty = execQty;

        return new OrderResult(orderId, clientId, symbol, side, status,
                origQty, execQty, avgPrice, latency);
    }

    // ======================= HTTP =======================

    /** Время с поправкой на расхождение часов с биржей. */
    private long timestamp() {
        return System.currentTimeMillis() + timeOffsetMs;
    }

    /** Публичный GET. */
    private JsonNode getPublic(String path) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();
        return send(req);
    }

    /** Добавить timestamp, recvWindow и подпись. */
    private String withSignature(String params) {
        String full = params.isEmpty()
                ? "timestamp=" + timestamp() + "&recvWindow=" + recvWindow
                : params + "&timestamp=" + timestamp() + "&recvWindow=" + recvWindow;
        return full + "&signature=" + signer.sign(full);
    }

    /** Подписанный GET. */
    private JsonNode getSigned(String path, String params) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path + "?" + withSignature(params)))
                .header("X-MBX-APIKEY", credentials.apiKey())
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();
        return send(req);
    }

    /** Подписанный POST (параметры в теле). */
    private JsonNode postSigned(String path, String params) throws Exception {
        String body = withSignature(params);
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .header("X-MBX-APIKEY", credentials.apiKey())
                .header("Content-Type", "application/x-www-form-urlencoded")
                .timeout(Duration.ofSeconds(10))
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
        return send(req);
    }

    /** Подписанный DELETE. */
    private JsonNode deleteSigned(String path, String params) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path + "?" + withSignature(params)))
                .header("X-MBX-APIKEY", credentials.apiKey())
                .timeout(Duration.ofSeconds(10))
                .DELETE()
                .build();
        return send(req);
    }

    /** Общий бюджет запросов Binance: вес по документации, заголовки X-MBX-* подтягивают счёт к счёту биржи. */
    private final RateBudget budget = RateBudget.of("binance");

    /** Отправить с учётом общего бюджета лимитов; ошибка HTTP — ExchangeException. */
    private JsonNode send(HttpRequest req) throws Exception {
        String path = req.uri().getPath();
        boolean order = path.equals("/api/v3/order") && !"GET".equals(req.method())
                || path.equals("/api/v3/openOrders") && "DELETE".equals(req.method());
        RateBudget.Kind kind = order ? RateBudget.Kind.ORDER
                : req.headers().firstValue("X-MBX-APIKEY").isPresent() ? RateBudget.Kind.PRIVATE : RateBudget.Kind.PUBLIC;
        budget.acquire(kind, RateLimits.weight("binance", req.method(), path, req.uri().getRawQuery()), order ? 1000 : 3000);
        HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
        budget.onResponse(resp.statusCode(), resp.headers());
        if (resp.statusCode() >= 400) {
            throw new ExchangeException(resp.statusCode(), resp.body());
        }
        return mapper.readTree(resp.body());
    }

    /** Ошибка от биржи с кодом и телом ответа — по ним понятно, что именно не так. */
    public static final class ExchangeException extends RuntimeException implements RateLimited {
        /** HTTP-код. */
        private final int httpStatus;

        /**
         * @param httpStatus HTTP-код
         * @param body тело ответа
         */
        public ExchangeException(int httpStatus, String body) {
            super("HTTP " + httpStatus + ": " + body);
            this.httpStatus = httpStatus;
        }

        /** HTTP-код. */
        public int httpStatus() { return httpStatus; }

        /** 429 и 418 — превышен лимит запросов, нужно притормозить. */
        @Override
        public boolean isRateLimit() {
            return httpStatus == 429 || httpStatus == 418;
        }
    }
}
