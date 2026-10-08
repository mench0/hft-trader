package com.hft.exchange.bybit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hft.config.Credentials;
import com.hft.config.ExchangeConfig;
import com.hft.exchange.Exchange;
import com.hft.model.OrderEnums.Side;
import com.hft.model.OrderEnums.TimeInForce;
import com.hft.model.OrderEnums.Type;
import com.hft.model.OrderResult;
import com.hft.rest.ExchangeOrderApi;
import com.hft.rest.RateLimited;
import com.hft.rest.RateBudget;
import com.hft.rest.WsRpcChannel;
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
 *
 * В LIVE ордера и отмены идут по торговому WebSocket (/v5/trade), исполнения и баланс — по
 * приватному (/v5/private), см. {@link BybitWs}; REST — запасной канал. Если WS-запрос ушёл,
 * а ответа нет, ордер не повторяется вслепую: его судьба выясняется по orderLinkId через REST.
 */
public final class BybitRestClient implements ExchangeOrderApi {

    /** Логгер. */
    private static final Logger log = LoggerFactory.getLogger(BybitRestClient.class);

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
    /** Счётчик для orderLinkId. */
    private final AtomicLong clientOrderSeq = new AtomicLong(System.currentTimeMillis());
    /** Подключение и параметры биржи. */
    private final ExchangeConfig config;
    /** WS-протоколы и состояние из потоков; null — только REST. */
    private volatile BybitWs ws;
    /** Торговый и приватный сокеты. */
    private volatile WsRpcChannel tradeChannel, privateChannel;
    /** Свои адреса сокетов (тесты, прокси). */
    private volatile String tradeWsUrl, privateWsUrl;
    /** WS не помог — запрос ушёл по REST. */
    private final AtomicLong wsFallbacks = new AtomicLong();
    /** Категория Bybit v5: spot или linear (бессрочные USDT-контракты, market=perp). */
    private final String category;
    /** Куда пишутся позиции из приватного потока. */
    private volatile com.hft.store.PositionStore positionStore;

    /**
     * @param config подключение и параметры биржи
     * @param credentials ключи из окружения
     * @param filters правила символов
     */
    public BybitRestClient(ExchangeConfig config, Credentials credentials, SymbolFilters filters) {
        this.config = config;
        this.baseUrl = config.restUrl();
        this.credentials = credentials;
        this.recvWindow = config.recvWindowMs();
        this.filters = filters;
        this.category = config.params().isPerp() ? "linear" : "spot";
        this.signer = credentials.isPresent() ? new Signer(credentials.apiSecret()) : null;
        this.http = HttpClient.newBuilder()
                .version(HttpClient.Version.HTTP_2)
                .connectTimeout(Duration.ofSeconds(5))
                .build();
    }

    /** Перпы (category=linear). */
    @Override
    public boolean isPerp() { return category.equals("linear"); }

    // ======================= ПУБЛИЧНЫЕ =======================

    /** Последняя цена символа (для оценки рыночного ордера). */
    public double price(String symbol) throws Exception {
        JsonNode json = getPublic("/v5/market/tickers?category=" + category + "&symbol=" + symbol.toUpperCase());
        return json.get("result").get("list").get(0).get("lastPrice").asDouble();
    }

    /** Загрузка торговых правил (шаг цены/объёма, минимальная сумма). */
    public void loadFilters(Iterable<String> symbols) throws Exception {
        JsonNode root = getPublic("/v5/market/instruments-info?category=" + category + "&limit=1000");
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
            // спот: шаг — basePrecision, минимум — minOrderAmt; перпы: qtyStep и minNotionalValue
            double stepSize = isPerp() ? lotSizeFilter.path("qtyStep").asDouble(0) : lotSizeFilter.path("basePrecision").asDouble(0);
            double tickSize = priceFilter.path("tickSize").asDouble(0);
            double minNotional = isPerp() ? lotSizeFilter.path("minNotionalValue").asDouble(0) : lotSizeFilter.path("minOrderAmt").asDouble(0);

            filters.put(symbol, new SymbolFilters.Filter(
                    minQty, maxQty, stepSize, 0, 0, tickSize, minNotional));
            loaded++;
        }
        log.info("[bybit] Загружены торговые правила для {} символов", loaded);
    }

    // ======================= БАЛАНСЫ =======================

    /** Загрузить балансы. */
    public void loadBalances(com.hft.store.BalanceStore store) throws Exception {
        credentials.require();
        JsonNode json = getSigned("/v5/account/wallet-balance", "accountType=UNIFIED");
        JsonNode list = json.path("result").path("list");
        int count = 0;
        for (JsonNode account : list) {
            for (JsonNode coin : account.path("coin")) {
                double[] fl = BybitWs.freeLocked(coin, category);   // walletBalance включает заблокированное
                double free = fl[0], locked = fl[1];
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

    /** Лимитная покупка. */
    public OrderResult buyLimit(String symbol, double qty, double price, TimeInForce tif) throws Exception {
        return placeOrder(symbol, Side.BUY, Type.LIMIT, qty, price, tif, false);
    }

    /** Лимитная продажа. */
    public OrderResult sellLimit(String symbol, double qty, double price, TimeInForce tif) throws Exception {
        return placeOrder(symbol, Side.SELL, Type.LIMIT, qty, price, tif, false);
    }

    /** Рыночная покупка qty базовой валюты. */
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

    /** Рыночная продажа qty базовой валюты. */
    public OrderResult sellMarket(String symbol, double qty) throws Exception {
        return placeOrder(symbol, Side.SELL, Type.MARKET, qty, 0, null, false);
    }

    /** Закрытие позиции: на перпах — reduceOnly. */
    @Override
    public OrderResult reduceMarket(String symbol, Side side, double qty) throws Exception {
        return placeOrder(symbol, side, Type.MARKET, qty, 0, null, false, isPerp());
    }

    private OrderResult placeOrder(String symbol, Side side, Type type, double qty, double price,
                                   TimeInForce tif, boolean qtyIsQuote) throws Exception {
        return placeOrder(symbol, side, type, qty, price, tif, qtyIsQuote, false);
    }

    private OrderResult placeOrder(String symbol, Side side, Type type, double qty, double price,
                                   TimeInForce tif, boolean qtyIsQuote, boolean reduceOnly) throws Exception {
        if (qtyIsQuote && isPerp()) throw new IllegalArgumentException("Bybit linear: ордер на сумму не поддерживается — задайте объём");
        credentials.require();
        String sym = symbol.toUpperCase();

        double roundedQty = qtyIsQuote ? qty : filters.roundQuantity(sym, qty);
        if (!qtyIsQuote && !reduceOnly) {
            String err = filters.validate(sym, roundedQty, type == Type.LIMIT ? price : 0);
            if (err != null) throw new IllegalArgumentException("Ордер не прошёл проверку: " + err);
        }

        String clientOrderId = "hft" + clientOrderSeq.incrementAndGet();

        ObjectNode body = mapper.createObjectNode()
                .put("category", category)
                .put("symbol", sym)
                .put("side", side == Side.BUY ? "Buy" : "Sell")
                .put("orderType", type == Type.LIMIT ? "Limit" : "Market")
                .put("qty", Numbers.plain(roundedQty, filters.quantityScale(sym)))
                .put("orderLinkId", clientOrderId);
        // рыночная покупка на споте по умолчанию считается в котируемой валюте — единицу указываем явно
        if (type == Type.MARKET && !isPerp()) body.put("marketUnit", qtyIsQuote ? "quoteCoin" : "baseCoin");
        if (reduceOnly) body.put("reduceOnly", true);
        if (type == Type.LIMIT) {
            double roundedPrice = filters.roundPrice(sym, price);
            body.put("price", Numbers.plain(roundedPrice, filters.priceScale(sym)));
            body.put("timeInForce", bybitTif(tif));
        }

        long start = System.nanoTime();
        JsonNode result;
        try {
            result = wsCall("order.create", body);                  // сначала WebSocket
        } catch (WsRpcChannel.WsUnknownOutcomeException e) {
            wsFallbacks.incrementAndGet();
            log.warn("[bybit] исход ордера {} неизвестен ({}), проверяю по REST", clientOrderId, e.getMessage());
            OrderResult st = statusByLinkId(sym, clientOrderId);
            return new OrderResult(st.orderId(), clientOrderId, sym, side, st.status(),
                    qtyIsQuote ? 0 : roundedQty, st.executedQty(), st.avgPrice(), System.nanoTime() - start);
        }
        if (result == null) result = postSigned("/v5/order/create", body.toString()).path("result");   // REST — если WS не готов
        long latency = System.nanoTime() - start;
        long orderId = result.path("orderId").asLong(0);

        // ответ на создание не содержит исполнения: для рыночных/IOC/FOK ждём событие из приватного WS
        boolean immediate = type == Type.MARKET || tif == TimeInForce.IOC || tif == TimeInForce.FOK;
        BybitWs w = ws;
        if (immediate && orderId != 0 && w != null && privateReady()) {
            long until = System.currentTimeMillis() + config.params().marketFillWaitMs();
            while (System.currentTimeMillis() < until) {
                OrderResult st = w.streamed.get(orderId);
                if (st != null && !"NEW".equals(st.status()) && !"PARTIALLY_FILLED".equals(st.status())) {   // ждём итог, а не первое частичное
                    return new OrderResult(orderId, clientOrderId, sym, side, st.status(),
                            qtyIsQuote ? st.requestedQty() : roundedQty, st.executedQty(), st.avgPrice(), latency);
                }
                Thread.sleep(10);
            }
        }
        return new OrderResult(orderId, clientOrderId, sym, side, "NEW",
                qtyIsQuote ? 0 : roundedQty, 0, 0, latency);
    }

    /** Время жизни в формате Bybit. */
    private static String bybitTif(TimeInForce tif) {
        if (tif == null) return "GTC";
        return switch (tif) {
            case GTC -> "GTC";
            case IOC -> "IOC";
            case FOK -> "FOK";
        };
    }

    /** Отменить ордер. */
    public void cancelOrder(String symbol, long orderId) throws Exception {
        credentials.require();
        ObjectNode body = mapper.createObjectNode().put("category", category)
                .put("symbol", symbol.toUpperCase()).put("orderId", Long.toString(orderId));
        JsonNode r = null;
        try { r = wsCall("order.cancel", body); }
        catch (WsRpcChannel.WsUnknownOutcomeException e) { wsFallbacks.incrementAndGet(); }   // отмена идемпотентна — повторяем через REST
        if (r == null) postSigned("/v5/order/cancel", body.toString());
        log.info("[bybit] Ордер {} по {} отменён", orderId, symbol);
    }

    /** Отменить все открытые ордера символа; возвращает их число. */
    public int cancelAll(String symbol) throws Exception {
        credentials.require();
        String body = String.format("{\"category\":\"%s\",\"symbol\":\"%s\"}", category, symbol.toUpperCase());
        JsonNode json = postSigned("/v5/order/cancel-all", body);
        JsonNode list = json.path("result").path("list");
        int count = list.isArray() ? list.size() : 0;
        log.info("[bybit] Отменено {} ордеров по {}", count, symbol);
        return count;
    }

    /** Статус ордера: из WS-потока, если есть, иначе запрос к бирже. */
    public OrderResult orderStatus(String symbol, long orderId) throws Exception {
        credentials.require();
        BybitWs w = ws;
        if (w != null && privateReady()) {                     // итог из приватного потока — без запроса
            OrderResult st = w.streamed.get(orderId);
            if (st != null && !"NEW".equals(st.status()) && !"PARTIALLY_FILLED".equals(st.status())) return st;
        }
        return fromRealtime(symbol, orderId, "category=" + category + "&symbol=" + symbol.toUpperCase() + "&orderId=" + orderId);
    }

    /** Статус ордера по нашему orderLinkId (после обрыва WS). */
    private OrderResult statusByLinkId(String symbol, String linkId) throws Exception {
        return fromRealtime(symbol, 0, "category=" + category + "&symbol=" + symbol + "&orderLinkId=" + linkId);
    }

    /** Ордер из /v5/order/realtime. */
    private OrderResult fromRealtime(String symbol, long orderId, String query) throws Exception {
        JsonNode json = getSigned("/v5/order/realtime", query);
        JsonNode order = json.path("result").path("list").get(0);
        if (order == null) throw new IllegalStateException("Ордер не найден");

        Side side = "Buy".equals(order.get("side").asText()) ? Side.BUY : Side.SELL;
        String status = mapBybitStatus(order.get("orderStatus").asText());
        double origQty = order.path("qty").asDouble(0);
        double execQty = order.path("cumExecQty").asDouble(0);
        double avgPrice = order.path("avgPrice").asDouble(0);

        if (orderId == 0) orderId = order.path("orderId").asLong(0);
        return new OrderResult(orderId, order.path("orderLinkId").asText(""),
                symbol.toUpperCase(), side, status, origQty, execQty, avgPrice, 0);
    }

    /** Статус Bybit в наш (NEW, PARTIALLY_FILLED, FILLED, CANCELED, REJECTED). */
    private static String mapBybitStatus(String bybitStatus) { return BybitWs.status(bybitStatus); }

    // ======================= ПЕРПЫ =======================

    /** Плечо символа (обе стороны); «не изменилось» (110043) — не ошибка. */
    @Override
    public void setLeverage(String symbol, int leverage) throws Exception {
        if (!isPerp()) return;
        credentials.require();
        String body = mapper.createObjectNode().put("category", "linear").put("symbol", symbol.toUpperCase())
                .put("buyLeverage", Integer.toString(leverage)).put("sellLeverage", Integer.toString(leverage)).toString();
        try { postSigned("/v5/position/set-leverage", body); }
        catch (ExchangeException e) { if (e.getMessage() == null || !e.getMessage().contains("110043")) throw e; }
        log.info("[bybit] плечо {}x для {}", leverage, symbol);
    }

    /** Открытые позиции USDT-перпов; позиции приходят и по WS (поток position). */
    @Override
    public void loadPositions(com.hft.store.PositionStore store) throws Exception {
        if (!isPerp()) return;
        credentials.require();
        positionStore = store;
        BybitWs w = ws;
        if (w != null) w.positions = store;
        JsonNode list = getSigned("/v5/position/list", "category=linear&settleCoin=USDT").path("result").path("list");
        java.util.Set<String> seen = new java.util.HashSet<>();
        for (JsonNode p : list) {
            double size = p.path("size").asDouble(0);
            String sym = p.path("symbol").asText();
            store.set(sym, "Sell".equals(p.path("side").asText()) ? -size : size, p.path("avgPrice").asDouble(0));
            if (size != 0) seen.add(sym);
        }
        for (String s : store.snapshot().keySet()) if (!seen.contains(s)) store.set(s, 0, 0);   // закрыта на бирже
    }

    // ======================= WebSocket =======================

    /** Свои адреса сокетов (тесты, прокси). */
    public void setWsUrls(String trade, String priv) { this.tradeWsUrl = trade; this.privateWsUrl = priv; }

    /** Поднять торговый и приватный сокеты. Без ключей или при wsTrade=false — всё по REST. */
    public void startStreams(BalanceStore store) {
        if (!credentials.isPresent() || !config.params().wsTrade() || tradeChannel != null) return;
        String host = config.testnet() ? "wss://stream-testnet.bybit.com" : "wss://stream.bybit.com";
        BybitWs w = new BybitWs(tradeWsUrl != null ? tradeWsUrl : host + "/v5/trade",
                privateWsUrl != null ? privateWsUrl : host + "/v5/private", credentials.apiKey(), signer, recvWindow, category);
        w.balances = store;
        w.positions = positionStore;
        ws = w;
        privateChannel = new WsRpcChannel(Exchange.BYBIT.id(), w.priv).onEvent(w::onEvent);
        tradeChannel = new WsRpcChannel(Exchange.BYBIT.id(), w.trade);
        privateChannel.start();
        tradeChannel.start();
        log.info("[bybit] WebSocket: торговый {} и приватный {}", w.trade.url(), w.priv.url());
    }

    /** Остановить сокеты. */
    public void stopStreams() {
        WsRpcChannel t = tradeChannel, p = privateChannel;
        if (t != null) t.stop();
        if (p != null) p.stop();
        tradeChannel = privateChannel = null;
        ws = null;
    }

    /** Подождать готовности сокетов (но не дольше ms). */
    public void awaitStreams(long ms) throws InterruptedException {
        long until = System.currentTimeMillis() + ms;
        while (tradeChannel != null && !(wsReady() && privateReady()) && !tradeChannel.isDisabled() && System.currentTimeMillis() < until) Thread.sleep(50);
    }

    /** Торговый сокет готов. */
    public boolean wsReady() { WsRpcChannel c = tradeChannel; return c != null && c.isReady(); }

    /** Приватный сокет готов. */
    private boolean privateReady() { WsRpcChannel c = privateChannel; return c != null && c.isReady(); }

    /** Баланс приходит по WS и актуален. */
    public boolean balancesStreamed() { BybitWs w = ws; return w != null && privateReady() && w.walletSeen; }

    /** Метрики сокетов для админки. */
    public Map<String, Object> stats() {
        Map<String, Object> m = new LinkedHashMap<>();
        WsRpcChannel t = tradeChannel, p = privateChannel;
        m.put("wsTrade", t == null ? "выключен (нет ключей или wsTrade=false)" : t.stats());
        m.put("wsPrivate", p == null ? "выключен" : p.stats());
        m.put("wsFallbacks", wsFallbacks.get());
        m.put("blockedForMs", budget.blockedForMs());
        return m;
    }

    /**
     * Запрос по торговому сокету. null — сокет не готов, вызывающий идёт в REST.
     * @throws WsRpcChannel.WsUnknownOutcomeException запрос мог уйти, ответа нет
     */
    private JsonNode wsCall(String op, ObjectNode body) throws Exception {
        BybitWs w = ws;
        WsRpcChannel ch = tradeChannel;
        if (w == null || ch == null || !ch.isReady()) return null;
        budget.acquire(RateBudget.Kind.ORDER, 1, 1000);
        String id = w.nextId();
        try {
            return w.result(ch.call(id, w.request(id, op, body), 5000));
        } catch (WsRpcChannel.WsNotReadyException e) {
            wsFallbacks.incrementAndGet();
            return null;
        }
    }

    // ======================= HTTP + ПОДПИСЬ =======================
    //
    // Bybit v5 подписывает конкатенацию timestamp+apiKey+recvWindow+payload,
    // где payload — это query string для GET и тело JSON для POST.
    // Подпись передаётся в заголовке X-BAPI-SIGN, а не в самом запросе —
    // этим и отличается от Binance, где подпись — часть query/body.

    /** Публичный GET. */
    private JsonNode getPublic(String path) throws Exception {
        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(baseUrl + path))
                .timeout(Duration.ofSeconds(10))
                .GET()
                .build();
        return send(req);
    }

    /** Подписанный GET. */
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

    /** Подписанный POST с JSON. */
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
    private final RateBudget budget = RateBudget.of(Exchange.BYBIT.id());

    /** Отправить с учётом общего бюджета лимитов; retCode ≠ 0 — ExchangeException. */
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
        /** HTTP-код. */
        private final int httpStatus;
        /** Код ошибки Bybit. */
        private final int retCode;

        /**
         * @param httpStatus HTTP-код
         * @param retCode код ошибки Bybit
         * @param message текст ошибки
         */
        public ExchangeException(int httpStatus, int retCode, String message) {
            super("HTTP " + httpStatus + " retCode " + retCode + ": " + message);
            this.httpStatus = httpStatus;
            this.retCode = retCode;
        }

        /** retCode 10006 или HTTP 429 — превышен лимит. */
        @Override
        public boolean isRateLimit() {
            return retCode == 10006 || httpStatus == 429;
        }
    }
}
