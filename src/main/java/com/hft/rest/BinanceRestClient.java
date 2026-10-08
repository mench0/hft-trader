package com.hft.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hft.config.Credentials;
import com.hft.config.ExchangeConfig;
import com.hft.exchange.Exchange;
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
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * REST-клиент Binance на встроенном java.net.http.HttpClient.
 * <p>
 * Почему не OkHttp / Apache HttpClient: встроенный клиент в Java 21
 * поддерживает HTTP/2, пул соединений и keep-alive из коробки.
 * Для REST-части (ордера, балансы) его производительности достаточно,
 * а зависимостей меньше.
 * <p>
 * Ордера, отмены и статусы в LIVE идут по WebSocket API ({@link BinanceWsApi}), если он готов
 * (параметр биржи wsTrade=true); REST — запасной канал. Если WS-запрос ушёл, а ответа нет,
 * ордер не повторяется вслепую: его судьба выясняется по REST через origClientOrderId.
 * <p>
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

    /** Подключение и параметры биржи. */
    private final ExchangeConfig config;
    /** WebSocket API (ордера и события аккаунта); null — только REST. */
    private volatile BinanceWsApi ws;
    /** Сокет WebSocket API. */
    private volatile WsRpcChannel wsChannel;
    /** Свой адрес WebSocket API (тесты, прокси). */
    private volatile String wsApiUrl;
    /** WS не помог (не готов или исход неизвестен) — запрос ушёл по REST. */
    private final AtomicLong wsFallbacks = new AtomicLong();

    /**
     * @param config подключение и параметры
     * @param credentials ключи
     * @param filters правила символов
     */
    public BinanceRestClient(ExchangeConfig config, Credentials credentials, SymbolFilters filters) {
        this.config = config;
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

        Map<String, String> p = new LinkedHashMap<>();
        p.put("symbol", sym);
        p.put("side", side.name());
        p.put("type", type.name());

        if (quoteOrderQty > 0) {
            if (type != Type.MARKET) {
                throw new IllegalArgumentException("quoteOrderQty работает только с MARKET");
            }
            p.put("quoteOrderQty", Numbers.plain(quoteOrderQty, 8));
        } else {
            double roundedQty = filters.roundQuantity(sym, qty);
            String err = filters.validate(sym, roundedQty, type == Type.LIMIT ? price : 0);
            if (err != null) {
                throw new IllegalArgumentException("Ордер не прошёл проверку: " + err);
            }
            p.put("quantity", Numbers.plain(roundedQty, filters.quantityScale(sym)));
        }

        if (type == Type.LIMIT) {
            double roundedPrice = filters.roundPrice(sym, price);
            p.put("price", Numbers.plain(roundedPrice, filters.priceScale(sym)));
            p.put("timeInForce", (tif == null ? TimeInForce.GTC : tif).name());
        }

        String clientOrderId = "hft" + clientOrderSeq.incrementAndGet();
        p.put("newClientOrderId", clientOrderId);
        p.put("newOrderRespType", "FULL"); // просим полный ответ с деталями исполнения

        long start = System.nanoTime();
        JsonNode json;
        try {
            json = wsCall("order.place", p, true);              // сначала WebSocket
        } catch (WsRpcChannel.WsUnknownOutcomeException e) {
            wsFallbacks.incrementAndGet();
            log.warn("[binance] исход ордера {} неизвестен ({}), проверяю по REST", clientOrderId, e.getMessage());
            json = getSigned("/api/v3/order", "symbol=" + sym + "&origClientOrderId=" + clientOrderId);
        }
        if (json == null) json = postSigned("/api/v3/order", query(p));   // REST — только если WS не готов
        long latency = System.nanoTime() - start;

        return parseOrderResult(json, sym, side, latency);
    }

    /** "k=v&k=v" в порядке добавления. */
    private static String query(Map<String, String> p) {
        StringBuilder sb = new StringBuilder(160);
        for (var e : p.entrySet()) {
            if (sb.length() > 0) sb.append('&');
            sb.append(e.getKey()).append('=').append(e.getValue());
        }
        return sb.toString();
    }

    /** Отмена ордера по ID биржи. */
    public void cancelOrder(String symbol, long orderId) throws Exception {
        credentials.require();
        String params = "symbol=" + symbol.toUpperCase() + "&orderId=" + orderId;
        JsonNode r = null;
        try { r = wsCall("order.cancel", Map.of("symbol", symbol.toUpperCase(), "orderId", Long.toString(orderId)), true); }
        catch (WsRpcChannel.WsUnknownOutcomeException e) { wsFallbacks.incrementAndGet(); }   // отмена идемпотентна — повторяем через REST
        if (r == null) deleteSigned("/api/v3/order", params);
        log.info("Ордер {} по {} отменён", orderId, symbol);
    }

    /** Отмена всех открытых ордеров по символу — быстрый выход из рынка. */
    public int cancelAll(String symbol) throws Exception {
        credentials.require();
        String params = "symbol=" + symbol.toUpperCase();
        JsonNode json = null;
        try { json = wsCall("openOrders.cancelAll", Map.of("symbol", symbol.toUpperCase()), true); }
        catch (WsRpcChannel.WsUnknownOutcomeException e) { wsFallbacks.incrementAndGet(); }
        catch (ExchangeException e) { if (e.getMessage().contains("-2011")) return 0; throw e; }   // открытых ордеров нет
        if (json == null) json = deleteSigned("/api/v3/openOrders", params);
        int count = json.isArray() ? json.size() : 0;
        log.info("Отменено {} ордеров по {}", count, symbol);
        return count;
    }

    /** Статус ордера: NEW, PARTIALLY_FILLED, FILLED, CANCELED, REJECTED, EXPIRED. */
    public OrderResult orderStatus(String symbol, long orderId) throws Exception {
        credentials.require();
        BinanceWsApi api = ws;
        if (api != null && wsReady()) {                       // итог из события — без запроса
            OrderResult st = api.streamed.get(orderId);
            if (st != null && !"NEW".equals(st.status()) && !"PARTIALLY_FILLED".equals(st.status())) return st;
        }
        String params = "symbol=" + symbol.toUpperCase() + "&orderId=" + orderId;
        JsonNode json = null;
        try { json = wsCall("order.status", Map.of("symbol", symbol.toUpperCase(), "orderId", Long.toString(orderId)), false); }
        catch (WsRpcChannel.WsUnknownOutcomeException e) { wsFallbacks.incrementAndGet(); }   // чтение — можно повторить по REST
        if (json == null) json = getSigned("/api/v3/order", params);
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

    // ======================= WebSocket API =======================

    /** Свой адрес WebSocket API (тесты, прокси). */
    public void setWsApiUrl(String url) { this.wsApiUrl = url; }

    /**
     * Поднять WebSocket API: ордера и события аккаунта (исполнения, балансы).
     * Без ключей или при wsTrade=false — ничего, всё по REST.
     */
    public void startStreams(BalanceStore store) {
        if (!credentials.isPresent() || !config.params().wsTrade() || wsChannel != null) return;
        String url = wsApiUrl != null ? wsApiUrl : config.testnet() ? BinanceWsApi.TESTNET : BinanceWsApi.MAINNET;
        BinanceWsApi api = new BinanceWsApi(url, credentials.apiKey(), signer, this::timestamp);
        api.balances = store;
        WsRpcChannel ch = new WsRpcChannel(Exchange.BINANCE.id(), api).onEvent(api::onEvent);
        ws = api;
        wsChannel = ch;
        ch.start();
        log.info("[binance] WebSocket API: {}", url);
    }

    /** Остановить WebSocket API. */
    public void stopStreams() {
        WsRpcChannel ch = wsChannel;
        if (ch != null) ch.stop();
        wsChannel = null;
        ws = null;
    }

    /** Подождать готовности сокета (но не дольше ms). */
    public void awaitStreams(long ms) throws InterruptedException {
        long until = System.currentTimeMillis() + ms;
        while (wsChannel != null && !wsReady() && !wsChannel.isDisabled() && System.currentTimeMillis() < until) Thread.sleep(50);
    }

    /** WebSocket API готов принимать запросы. */
    public boolean wsReady() { WsRpcChannel c = wsChannel; return c != null && c.isReady(); }

    /** Балансы приходят событиями и актуальны — REST-сверку можно реже. */
    public boolean balancesStreamed() { BinanceWsApi a = ws; return a != null && wsReady() && a.accountSeen; }

    /** Метрики WebSocket API для админки. */
    public Map<String, Object> stats() {
        Map<String, Object> m = new LinkedHashMap<>();
        WsRpcChannel c = wsChannel;
        m.put("ws", c == null ? "выключен (нет ключей или wsTrade=false)" : c.stats());
        m.put("wsFallbacks", wsFallbacks.get());
        m.put("blockedForMs", budget.blockedForMs());
        return m;
    }

    /**
     * Запрос по WebSocket API. null — сокет не готов, вызывающий идёт в REST.
     * @throws WsRpcChannel.WsUnknownOutcomeException запрос мог уйти, ответа нет
     */
    private JsonNode wsCall(String method, Map<String, String> params, boolean order) throws Exception {
        BinanceWsApi api = ws;
        WsRpcChannel ch = wsChannel;
        if (api == null || ch == null || !ch.isReady()) return null;
        budget.acquire(order ? RateBudget.Kind.ORDER : RateBudget.Kind.PRIVATE, order ? 1 : 4, order ? 1000 : 3000);
        String id = api.nextId();
        try {
            return api.result(ch.call(id, api.request(id, method, params), 5000));
        } catch (WsRpcChannel.WsNotReadyException e) {
            wsFallbacks.incrementAndGet();
            return null;
        }
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
    private final RateBudget budget = RateBudget.of(Exchange.BINANCE.id());

    /** Отправить с учётом общего бюджета лимитов; ошибка HTTP — ExchangeException. */
    private JsonNode send(HttpRequest req) throws Exception {
        String path = req.uri().getPath();
        boolean order = path.equals("/api/v3/order") && !"GET".equals(req.method())
                || path.equals("/api/v3/openOrders") && "DELETE".equals(req.method());
        RateBudget.Kind kind = order ? RateBudget.Kind.ORDER
                : req.headers().firstValue("X-MBX-APIKEY").isPresent() ? RateBudget.Kind.PRIVATE : RateBudget.Kind.PUBLIC;
        budget.acquire(kind, RateLimits.weight(Exchange.BINANCE.id(), req.method(), path, req.uri().getRawQuery()), order ? 1000 : 3000);
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
