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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Общий скелет REST-клиента биржи с подписью запросов. Держит то, что у всех
 * одинаково: округление по фильтрам, проверку, лимитер, метрики запросов,
 * дочитывание исполнения для рыночных/IOC/FOK, соответствие числовых id
 * и строковых id бирж. Подпись, формат тела и разбор ответа — в наследниках.
 *
 * ВНИМАНИЕ: наследники написаны по документации бирж без доступа к живым API
 * и не проверены. Подключайте только с минимальной суммой и сначала в тестовой сети.
 */
public abstract class SignedCexClient implements ExchangeOrderApi {

    private static final Logger log = LoggerFactory.getLogger(SignedCexClient.class);

    /** Параметры одного ордера, уже округлённые и проверенные. */
    protected record Order(String symbol, Side side, Type type, TimeInForce tif,
                           double qty, double price, boolean qtyIsQuote, String clientId) {}

    protected final String exchangeId;
    protected final String baseUrl;
    protected final Credentials credentials;
    protected final SymbolFilters filters;
    protected final ExchangeConfig config;
    protected final ObjectMapper mapper = new ObjectMapper();
    protected final PacedLimiter callLimiter;
    protected final PacedLimiter orderLimiter;

    /** Последнее известное состояние ордеров из приватного WS-потока (orderId -> результат). */
    protected final Map<Long, OrderResult> streamed = new ConcurrentHashMap<>();
    /** Приватный WS-канал (ордера/события); null — только REST. */
    protected volatile WsRpcChannel wsChannel;
    /** Исход WS-запроса неизвестен: счётчик для метрик. */
    protected final AtomicLong wsFallbacks = new AtomicLong();

    private final HttpClient http = HttpClient.newBuilder()
            .version(HttpClient.Version.HTTP_2)
            .connectTimeout(Duration.ofSeconds(5)).build();
    private final AtomicLong clientSeq = new AtomicLong(System.currentTimeMillis());
    private final AtomicLong requests = new AtomicLong(), errors = new AtomicLong(), rateLimited = new AtomicLong();
    private final AtomicLong latencyNanos = new AtomicLong(), maxLatencyNanos = new AtomicLong();

    // числовой id интерфейса <-> строковый id биржи
    private static final long ID_BASE = 9_000_000_000_000_000_000L;
    private final Map<String, Long> toLong = new ConcurrentHashMap<>();
    private final Map<Long, String> toVenue = new ConcurrentHashMap<>();
    private final AtomicLong idSeq = new AtomicLong();

    protected SignedCexClient(String exchangeId, ExchangeConfig config, Credentials credentials, SymbolFilters filters,
                              double callsPerSec, double ordersPerSec) {
        this.exchangeId = exchangeId;
        this.config = config;
        this.baseUrl = config.restUrl();
        this.credentials = credentials;
        this.filters = filters;
        this.callLimiter = new PacedLimiter(callsPerSec, 2000);
        this.orderLimiter = new PacedLimiter(ordersPerSec, 1000);
    }

    // ------------------------------------------------------------ для наследников

    protected abstract OrderResult placeRaw(Order o) throws Exception;

    public abstract OrderResult orderStatus(String symbol, long orderId) throws Exception;

    @Override public abstract void cancelOrder(String symbol, long orderId) throws Exception;

    @Override public abstract int cancelAll(String symbol) throws Exception;

    public abstract void loadFilters(Iterable<String> symbols) throws Exception;

    public abstract void loadBalances(BalanceStore store) throws Exception;

    /** Бросает ApiException, если по HTTP-коду или телу видно, что биржа вернула ошибку. */
    protected abstract void checkError(int httpStatus, JsonNode body) throws ApiException;

    // ------------------------------------------------------------ WebSocket-потоки

    /** Поднять приватные WS-каналы (ордера, исполнения, балансы). По умолчанию — нет, всё через REST. */
    public void startStreams(BalanceStore store) throws Exception {}

    public void stopStreams() {
        WsRpcChannel c = wsChannel;
        if (c != null) c.stop();
    }

    /** Баланс приходит по WS и актуален — планировщик может не дёргать REST. */
    public boolean balancesStreamed() { return false; }

    /** Готов ли WS для отправки ордеров прямо сейчас. */
    public boolean wsReady() { WsRpcChannel c = wsChannel; return c != null && c.isReady(); }

    /** Подождать, пока WS-канал станет готов (но не дольше ms): дальше стартовые запросы пойдут по сокету, а не по REST. */
    public void awaitStreams(long ms) throws InterruptedException {
        long until = System.currentTimeMillis() + ms;
        while (wsChannel != null && !wsReady() && !wsChannel.isDisabled() && System.currentTimeMillis() < until) Thread.sleep(50);
    }

    /** Отключить WS-торговлю переменной окружения <ID>_WS_TRADE=false. */
    protected boolean wsTradeAllowed() {
        return !"false".equalsIgnoreCase(System.getenv(exchangeId.toUpperCase() + "_WS_TRADE"));
    }

    // ------------------------------------------------------------ ExchangeOrderApi

    @Override public OrderResult buyLimit(String s, double q, double p, TimeInForce t) throws Exception { return place(s, Side.BUY, Type.LIMIT, t, q, p, false); }
    @Override public OrderResult sellLimit(String s, double q, double p, TimeInForce t) throws Exception { return place(s, Side.SELL, Type.LIMIT, t, q, p, false); }
    @Override public OrderResult buyMarket(String s, double q) throws Exception { return place(s, Side.BUY, Type.MARKET, null, q, 0, false); }
    @Override public OrderResult sellMarket(String s, double q) throws Exception { return place(s, Side.SELL, Type.MARKET, null, q, 0, false); }
    @Override public OrderResult buyMarketForQuote(String s, double quote) throws Exception { return place(s, Side.BUY, Type.MARKET, null, quote, 0, true); }

    private OrderResult place(String symbol, Side side, Type type, TimeInForce tif,
                              double qty, double price, boolean qtyIsQuote) throws Exception {
        credentials.require();
        String sym = symbol.toUpperCase();
        double q = qtyIsQuote ? qty : filters.roundQuantity(sym, qty);
        double p = type == Type.LIMIT ? filters.roundPrice(sym, price) : 0;
        if (!qtyIsQuote) {
            String err = filters.validate(sym, q, p);
            if (err != null) throw new IllegalArgumentException("Ордер не прошёл проверку: " + err);
        }
        TimeInForce effTif = type == Type.LIMIT ? (tif == null ? TimeInForce.GTC : tif) : null;
        String clientId = "hft" + clientSeq.incrementAndGet();

        long t0 = System.nanoTime();
        OrderResult r = placeRaw(new Order(sym, side, type, effTif, q, p, qtyIsQuote, clientId));

        boolean immediate = type == Type.MARKET || effTif == TimeInForce.IOC || effTif == TimeInForce.FOK;
        if (immediate && r.orderId() != 0) r = awaitTerminal(r, effTif);
        return new OrderResult(r.orderId(), r.clientOrderId(), r.symbol(), r.side(), r.status(),
                r.requestedQty(), r.executedQty(), r.avgPrice(), System.nanoTime() - t0);
    }

    /** Рыночный/IOC/FOK ордер уже завершён на бирже, но ответ на создание может этого не содержать — читаем статус. */
    private OrderResult awaitTerminal(OrderResult r, TimeInForce tif) throws Exception {
        OrderResult cur = r;
        if (wsReady()) {                                   // сначала ждём событие из WS: REST тратит лимит
            long until = System.currentTimeMillis() + 400;
            while (System.currentTimeMillis() < until) {
                OrderResult s = streamed.get(r.orderId());
                if (s != null && !"NEW".equals(s.status())) { cur = s; break; }
                Thread.sleep(15);
            }
        }
        for (int i = 0; i < 4; i++) {
            if (!"NEW".equals(cur.status())) break;
            Thread.sleep(40L * (i + 1));
            try { cur = orderStatus(r.symbol(), r.orderId()); }
            catch (ApiException e) { if (e.isRateLimit()) break; throw e; }
        }
        String st = cur.status();
        if ("CANCELED".equals(st) || "NEW".equals(st) && tif != null) {
            st = cur.executedQty() > 0 ? "PARTIALLY_FILLED" : "EXPIRED";
        }
        return new OrderResult(cur.orderId(), cur.clientOrderId(), cur.symbol(), cur.side(), st,
                cur.requestedQty() > 0 ? cur.requestedQty() : r.requestedQty(), cur.executedQty(), cur.avgPrice(), 0);
    }

    // ------------------------------------------------------------ id

    protected long registerId(String venueId) {
        try { return Long.parseLong(venueId); } catch (NumberFormatException ignore) { /* строковый id */ }
        return toLong.computeIfAbsent(venueId, k -> {
            long n = ID_BASE + idSeq.incrementAndGet();
            toVenue.put(n, k);
            return n;
        });
    }

    protected String venueId(long id) { return toVenue.getOrDefault(id, Long.toString(id)); }

    // ------------------------------------------------------------ HTTP

    protected static String enc(String s) { return URLEncoder.encode(s, StandardCharsets.UTF_8); }

    /** query из параметров в порядке добавления: a=1&b=2 */
    protected static String query(Map<String, String> params) {
        StringBuilder sb = new StringBuilder();
        for (var e : params.entrySet()) {
            if (sb.length() > 0) sb.append('&');
            sb.append(e.getKey()).append('=').append(enc(e.getValue()));
        }
        return sb.toString();
    }

    protected static Map<String, String> params() { return new LinkedHashMap<>(); }

    protected static HttpRequest.Builder req(String url) {
        return HttpRequest.newBuilder(URI.create(url)).timeout(Duration.ofSeconds(10));
    }

    /** Выполнить запрос с лимитером, метриками и разбором ошибок. order=true — считается в лимит ордеров. */
    protected JsonNode exec(HttpRequest request, boolean order) throws Exception {
        // ордера и фоновые запросы (статусы, балансы, правила) — разные лимитеры:
        // синхронизация баланса не должна задерживать ордер
        if (order) orderLimiter.acquire(); else callLimiter.acquire();
        long t0 = System.nanoTime();
        requests.incrementAndGet();
        HttpResponse<String> resp;
        try {
            resp = http.send(request, HttpResponse.BodyHandlers.ofString());
        } catch (java.io.IOException e) {
            errors.incrementAndGet();
            throw e;
        }
        long dt = System.nanoTime() - t0;
        latencyNanos.addAndGet(dt);
        maxLatencyNanos.accumulateAndGet(dt, Math::max);

        int code = resp.statusCode();
        if (code == 429 || code == 418 || code == 403) {
            rateLimited.incrementAndGet();
            long pause = code == 429 ? 10_000 : 60_000;
            callLimiter.blockFor(pause);
            orderLimiter.blockFor(pause);
            log.warn("[{}] HTTP {} — запросы приостановлены на {} c", exchangeId, code, pause / 1000);
        }
        String text = resp.body() == null ? "" : resp.body();
        JsonNode json = text.isBlank() ? mapper.createObjectNode() : parseLenient(text);
        try {
            checkError(code, json);
        } catch (ApiException e) {
            errors.incrementAndGet();
            if (e.isRateLimit() && code != 429) callLimiter.blockFor(5_000);
            throw e;
        }
        return json;
    }

    private JsonNode parseLenient(String text) throws Exception {
        try { return mapper.readTree(text); }
        catch (Exception e) { return mapper.createObjectNode().put("raw", text); }
    }

    public Map<String, Object> stats() {
        long n = requests.get();
        var m = new LinkedHashMap<String, Object>();
        m.put("requests", n);
        m.put("errors", errors.get());
        m.put("rateLimited", rateLimited.get());
        m.put("avgLatencyMs", n == 0 ? 0 : latencyNanos.get() / 1e6 / n);
        m.put("maxLatencyMs", maxLatencyNanos.get() / 1e6);
        m.put("blockedForMs", callLimiter.blockedForMs());
        WsRpcChannel c = wsChannel;
        if (c != null) { m.put("ws", c.stats()); m.put("wsFallbacks", wsFallbacks.get()); }
        return m;
    }

    protected static double d(JsonNode n, String field) {
        String s = n.path(field).asText("");
        if (s.isEmpty()) return 0;
        try { return Double.parseDouble(s); } catch (NumberFormatException e) { return 0; }
    }

    protected static String plain(double v, int scale) { return com.hft.util.Numbers.plain(v, scale); }
}
