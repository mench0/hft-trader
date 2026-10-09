package com.hft.exchange.binance;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hft.config.Credentials;
import com.hft.config.ExchangeConfig;
import com.hft.exchange.Exchange;
import com.hft.model.OrderEnums.Side;
import com.hft.model.OrderEnums.Type;
import com.hft.model.OrderResult;
import com.hft.rest.ApiException;
import com.hft.rest.SignedClient;
import com.hft.rest.UserStream;
import com.hft.rest.WsRpcChannel;
import com.hft.rest.WsRpcChannel.Msg;
import com.hft.store.BalanceStore;
import com.hft.store.PositionStore;
import com.hft.store.SymbolFilters;
import com.hft.util.Signer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Binance USDⓈ-M Futures (бессрочные контракты с расчётом в USDT), market=perp.
 *
 * <ul>
 *   <li>REST {@code /fapi/v1|v2}: правила, баланс, позиции, плечо; ордера — запасной канал.</li>
 *   <li>Ордера, отмены, статусы — WebSocket API {@code wss://ws-fapi.binance.com/ws-fapi/v1}
 *       ({@code order.place}, {@code order.cancel}, {@code order.status}); подпись — как у спота
 *       (параметры по алфавиту, HMAC-SHA256).</li>
 *   <li>Исполнения, баланс и позиции — поток по listenKey ({@code POST /fapi/v1/listenKey},
 *       {@code wss://fstream.binance.com/ws/<key>}): {@code ORDER_TRADE_UPDATE}, {@code ACCOUNT_UPDATE}.</li>
 * </ul>
 *
 * Режим позиций — односторонний (One-way, по умолчанию у Binance); режим хеджирования не поддержан.
 * ВНИМАНИЕ: формат взят из документации Binance без доступа к живому API — сначала testnet.
 */
public final class BinanceFuturesClient extends SignedClient {

    /** Логгер. */
    private static final Logger log = LoggerFactory.getLogger(BinanceFuturesClient.class);

    /** Подпись HMAC-SHA256. */
    private final Signer signer;
    /** Поправка часов: время биржи минус локальное. */
    private volatile long timeOffsetMs;
    /** Номера WS-запросов. */
    private final AtomicLong wsSeq = new AtomicLong();
    /** Поток событий аккаунта (listenKey). */
    private volatile UserStream userStream;
    /** Свои адреса сокетов (тесты, прокси). */
    private volatile String wsApiUrl, userStreamBase;
    /** Куда пишутся балансы и позиции из потока. */
    private volatile BalanceStore streamBalances;
    /** Позиции из потока. */
    private volatile PositionStore positions;
    /** Баланс хотя бы раз пришёл по WS. */
    private volatile boolean accountSeen;

    /**
     * @param config подключение (restUrl — fapi)
     * @param credentials ключи BINANCE_API_KEY/_SECRET
     * @param filters правила символов
     */
    public BinanceFuturesClient(ExchangeConfig config, Credentials credentials, SymbolFilters filters) {
        super(Exchange.BINANCE.id(), config, credentials, filters);
        this.signer = credentials.isPresent() ? new Signer(credentials.apiSecret()) : null;
    }

    @Override public boolean isPerp() { return true; }

    // ------------------------------------------------------------ HTTP

    /** Время с поправкой на часы биржи. */
    private long now() { return System.currentTimeMillis() + timeOffsetMs; }

    /** Публичный GET. */
    private JsonNode publicGet(String pathAndQuery) throws Exception {
        return exec(req(baseUrl + pathAndQuery).GET().build(), false);
    }

    /** Подписанный запрос: параметры в query, подпись — последним параметром. */
    private JsonNode signed(String method, String path, Map<String, String> p, boolean order) throws Exception {
        credentials.require();
        p.put("timestamp", Long.toString(now()));
        p.put("recvWindow", Integer.toString(config.recvWindowMs()));
        String q = query(p);
        String url = baseUrl + path + "?" + q + "&signature=" + signer.sign(q);
        var b = req(url).header("X-MBX-APIKEY", credentials.apiKey());
        b.method(method, java.net.http.HttpRequest.BodyPublishers.noBody());
        return exec(b.build(), order);
    }

    /** Ошибка Binance: HTTP ≥ 400 и {code<0, msg}; 429/418 и -1003 — лимит. */
    @Override
    protected void checkError(int http, JsonNode body) {
        int code = body.path("code").asInt(0);
        boolean limit = http == 429 || http == 418 || code == -1003 || code == -1015;
        if (http >= 400 || code < 0) {
            throw new ApiException(http, Integer.toString(code), body.path("msg").asText(body.toString()), limit);
        }
    }

    // ------------------------------------------------------------ правила, баланс, позиции, плечо

    /** Сверить часы и загрузить правила (шаг объёма и цены, минимальная сумма). */
    @Override
    public void loadFilters(Iterable<String> symbols) throws Exception {
        long t0 = System.currentTimeMillis();
        long server = publicGet("/fapi/v1/time").path("serverTime").asLong(t0);
        timeOffsetMs = server - (t0 + System.currentTimeMillis()) / 2;
        Set<String> want = new HashSet<>();
        for (String s : symbols) want.add(s.toUpperCase());
        int loaded = 0;
        for (JsonNode s : publicGet("/fapi/v1/exchangeInfo").path("symbols")) {
            String sym = s.path("symbol").asText();
            if (!want.contains(sym) || !"PERPETUAL".equals(s.path("contractType").asText("PERPETUAL"))) continue;
            double minQty = 0, maxQty = Double.MAX_VALUE, step = 0, tick = 0, minNotional = 0;
            for (JsonNode f : s.path("filters")) {
                switch (f.path("filterType").asText()) {
                    case "LOT_SIZE" -> { minQty = d(f, "minQty"); maxQty = d(f, "maxQty"); step = d(f, "stepSize"); }
                    case "PRICE_FILTER" -> tick = d(f, "tickSize");
                    case "MIN_NOTIONAL" -> minNotional = d(f, "notional");
                    default -> { }
                }
            }
            filters.put(sym, new SymbolFilters.Filter(minQty, maxQty > 0 ? maxQty : Double.MAX_VALUE, step, 0, 0, tick, minNotional));
            loaded++;
        }
        log.info("[binance] фьючерсы: правила загружены для {} символов, смещение часов {} мс", loaded, timeOffsetMs);
    }

    /** Баланс фьючерсного счёта: свободно — availableBalance, занято — остальное. */
    @Override
    public void loadBalances(BalanceStore store) throws Exception {
        for (JsonNode b : signed("GET", "/fapi/v2/balance", params(), false)) {
            double total = d(b, "balance"), free = d(b, "availableBalance");
            if (total != 0 || free != 0) store.set(b.path("asset").asText(), free, Math.max(0, total - free));
        }
        store.markSynced();
    }

    /** Плечо символа. */
    @Override
    public void setLeverage(String symbol, int leverage) throws Exception {
        var p = params();
        p.put("symbol", symbol.toUpperCase());
        p.put("leverage", Integer.toString(leverage));
        signed("POST", "/fapi/v1/leverage", p, false);
        log.info("[binance] плечо {}x для {}", leverage, symbol);
    }

    /** Открытые позиции (односторонний режим). */
    @Override
    public void loadPositions(PositionStore store) throws Exception {
        positions = store;
        Set<String> seen = new HashSet<>();
        for (JsonNode p : signed("GET", "/fapi/v2/positionRisk", params(), false)) {
            double amt = d(p, "positionAmt");
            if (amt == 0) continue;
            String sym = p.path("symbol").asText();
            store.set(sym, amt, d(p, "entryPrice"));
            seen.add(sym);
        }
        for (String s : store.snapshot().keySet()) if (!seen.contains(s)) store.set(s, 0, 0);   // закрыта на бирже
    }

    // ------------------------------------------------------------ ордера

    /** Параметры ордера Binance. */
    private Map<String, String> orderParams(Order o) {
        Map<String, String> p = new TreeMap<>();
        p.put("symbol", o.symbol());
        p.put("side", o.side().name());
        p.put("type", o.type() == Type.MARKET ? "MARKET" : "LIMIT");
        p.put("quantity", plain(o.qty(), filters.quantityScale(o.symbol())));
        if (o.type() == Type.LIMIT) {
            p.put("price", plain(o.price(), filters.priceScale(o.symbol())));
            p.put("timeInForce", o.tif().name());
        }
        if (o.reduceOnly()) p.put("reduceOnly", "true");
        p.put("newClientOrderId", o.clientId());
        p.put("newOrderRespType", "RESULT");
        return p;
    }

    /** Отправить ордер: WebSocket API, если готов, иначе REST. */
    @Override
    protected OrderResult placeRaw(Order o) throws Exception {
        if (o.qtyIsQuote()) throw new IllegalArgumentException("Binance futures: ордер на сумму не поддерживается — задайте объём");
        Map<String, String> p = orderParams(o);
        JsonNode r;
        try {
            r = wsCall("order.place", p, true);
        } catch (WsRpcChannel.WsUnknownOutcomeException e) {
            wsFallbacks.incrementAndGet();
            log.warn("[binance] исход ордера {} неизвестен ({}), проверяю по REST", o.clientId(), e.getMessage());
            var q = params();
            q.put("symbol", o.symbol());
            q.put("origClientOrderId", o.clientId());
            r = signed("GET", "/fapi/v1/order", q, false);
        }
        if (r == null) r = signed("POST", "/fapi/v1/order", new java.util.LinkedHashMap<>(p), true);
        return fromOrder(r, o.symbol());
    }

    /** Ответ об ордере (REST и WS одинаковы) в OrderResult. */
    private OrderResult fromOrder(JsonNode r, String symbol) {
        long id = r.path("orderId").asLong();
        double exec = d(r, "executedQty");
        double avg = d(r, "avgPrice");
        if (avg == 0 && exec > 0) avg = d(r, "cumQuote") / exec;
        return new OrderResult(id, r.path("clientOrderId").asText(""), symbol,
                "SELL".equals(r.path("side").asText()) ? Side.SELL : Side.BUY, r.path("status").asText("NEW"),
                d(r, "origQty"), exec, avg, 0);
    }

    /** Статус ордера: итог из потока, иначе запрос. */
    @Override
    public OrderResult orderStatus(String symbol, long orderId) throws Exception {
        OrderResult st = streamed.get(orderId);
        if (st != null && terminal(st.status())) return st;
        Map<String, String> p = new TreeMap<>(Map.of("symbol", symbol.toUpperCase(), "orderId", Long.toString(orderId)));
        JsonNode r = null;
        try { r = wsCall("order.status", p, false); }
        catch (WsRpcChannel.WsUnknownOutcomeException e) { wsFallbacks.incrementAndGet(); }
        if (r == null) r = signed("GET", "/fapi/v1/order", new java.util.LinkedHashMap<>(p), false);
        return fromOrder(r, symbol.toUpperCase());
    }

    /** Отменить ордер. */
    @Override
    public void cancelOrder(String symbol, long orderId) throws Exception {
        Map<String, String> p = new TreeMap<>(Map.of("symbol", symbol.toUpperCase(), "orderId", Long.toString(orderId)));
        JsonNode r = null;
        try { r = wsCall("order.cancel", p, true); }
        catch (WsRpcChannel.WsUnknownOutcomeException e) { wsFallbacks.incrementAndGet(); }   // отмена идемпотентна
        if (r == null) signed("DELETE", "/fapi/v1/order", new java.util.LinkedHashMap<>(p), true);
    }

    /** Отменить все ордера символа (сколько было — по списку открытых). */
    @Override
    public int cancelAll(String symbol) throws Exception {
        var q = params();
        q.put("symbol", symbol.toUpperCase());
        int n = signed("GET", "/fapi/v1/openOrders", q, false).size();
        if (n == 0) return 0;
        var p = params();
        p.put("symbol", symbol.toUpperCase());
        signed("DELETE", "/fapi/v1/allOpenOrders", p, true);
        log.info("[binance] отменено {} ордеров по {}", n, symbol);
        return n;
    }

    // ------------------------------------------------------------ WebSocket

    /** Свои адреса WebSocket API и потока (тесты, прокси). */
    public void setWsUrls(String wsApi, String userStreamBase) { this.wsApiUrl = wsApi; this.userStreamBase = userStreamBase; }

    /** Сокет ордеров (ws-fapi) и поток событий по listenKey. Без ключей или при wsTrade=false — всё по REST. */
    @Override
    public void startStreams(BalanceStore store) throws Exception {
        if (!credentials.isPresent() || !wsTradeAllowed() || wsChannel != null) return;
        streamBalances = store;
        String api = wsApiUrl != null ? wsApiUrl
                : config.testnet() ? "wss://testnet.binancefuture.com/ws-fapi/v1" : "wss://ws-fapi.binance.com/ws-fapi/v1";
        String base = userStreamBase != null ? userStreamBase
                : config.testnet() ? "wss://fstream.binancefuture.com/ws/" : "wss://fstream.binance.com/ws/";
        WsRpcChannel ch = new WsRpcChannel(Exchange.BINANCE.id(), new Trade(api));
        wsChannel = ch;
        ch.start();
        UserStream us = new UserStream(Exchange.BINANCE.id(), new UserStream.Api() {
            @Override public String newListenKey() throws Exception { return signedKey("POST", null); }
            @Override public void keepAlive(String key) throws Exception { signedKey("PUT", key); }
            @Override public String url(String key) { return base + key; }
            @Override public Msg parse(String text) { return text.contains("\"e\"") ? Msg.event(text) : Msg.ignore(); }
        }, this::onUserEvent);
        userStream = us;
        us.start();
        log.info("[binance] фьючерсы: WebSocket API {}, поток аккаунта {}", api, base);
    }

    /** listenKey: POST — новый, PUT — продлить (ключ API в заголовке, без подписи). */
    private String signedKey(String method, String key) throws Exception {
        var b = req(baseUrl + "/fapi/v1/listenKey").header("X-MBX-APIKEY", credentials.apiKey());
        b.method(method, java.net.http.HttpRequest.BodyPublishers.noBody());
        return exec(b.build(), false).path("listenKey").asText(key);
    }

    /** Остановить сокеты. */
    @Override
    public void stopStreams() {
        super.stopStreams();
        wsChannel = null;
        UserStream us = userStream;
        if (us != null) us.stop();
        userStream = null;
    }

    /** Баланс приходит по WS. */
    @Override
    public boolean balancesStreamed() {
        UserStream us = userStream;
        return us != null && us.channel().isReady() && accountSeen;
    }

    /**
     * Запрос по WebSocket API. null — сокет не готов, вызывающий идёт в REST.
     * @throws WsRpcChannel.WsUnknownOutcomeException запрос мог уйти, ответа нет
     */
    private JsonNode wsCall(String method, Map<String, String> params, boolean order) throws Exception {
        WsRpcChannel ch = wsChannel;
        if (ch == null || !ch.isReady()) return null;
        (order ? orderLimiter : callLimiter).acquire();
        String id = "f" + wsSeq.incrementAndGet();
        try {
            JsonNode n = mapper.readTree(ch.call(id, wsRequest(id, method, params), 5000));
            int status = n.path("status").asInt(0);
            if (status != 200) {
                JsonNode err = n.path("error");
                int code = err.path("code").asInt(0);
                throw new ApiException(status == 0 ? 400 : status, Integer.toString(code), err.path("msg").asText(n.toString()),
                        status == 429 || status == 418 || code == -1003);
            }
            return n.path("result");
        } catch (WsRpcChannel.WsNotReadyException e) {
            wsFallbacks.incrementAndGet();
            return null;
        }
    }

    /** Подписанный запрос WebSocket API: параметры по алфавиту + apiKey + timestamp, подпись HMAC. */
    String wsRequest(String id, String method, Map<String, String> params) {
        TreeMap<String, String> p = new TreeMap<>(params);
        p.put("apiKey", credentials.apiKey());
        p.put("timestamp", Long.toString(now()));
        StringBuilder q = new StringBuilder();
        for (var e : p.entrySet()) {
            if (q.length() > 0) q.append('&');
            q.append(e.getKey()).append('=').append(e.getValue());
        }
        ObjectNode m = mapper.createObjectNode().put("id", id).put("method", method);
        ObjectNode ps = m.putObject("params");
        p.forEach(ps::put);
        ps.put("signature", signer.sign(q.toString()));
        return m.toString();
    }

    /** Протокол ws-fapi: логина нет (каждый запрос подписан), ответы по id. */
    private final class Trade implements WsRpcChannel.Protocol {
        /** Адрес. */
        private final String url;
        Trade(String url) { this.url = url; }
        @Override public String url() { return url; }
        @Override public List<String> login() { return List.of(); }
        @Override public List<String> subscriptions() { return List.of(); }
        @Override public Msg parse(String text) throws Exception {
            JsonNode n = mapper.readTree(text);
            if (n.has("id")) return Msg.reply(n.path("id").asText(), text);
            return Msg.ignore();
        }
    }

    /**
     * ORDER_TRADE_UPDATE — состояние ордера (o: s, c, S, X, i, q, z, ap);
     * ACCOUNT_UPDATE — балансы (a.B: a, wb, cw) и позиции (a.P: s, pa, ep).
     */
    void onUserEvent(String text) throws Exception {
        JsonNode e = mapper.readTree(text);
        switch (e.path("e").asText()) {
            case "ORDER_TRADE_UPDATE" -> {
                JsonNode o = e.path("o");
                long id = o.path("i").asLong();
                streamed.put(id, new OrderResult(id, o.path("c").asText(""), o.path("s").asText(),
                        "SELL".equals(o.path("S").asText()) ? Side.SELL : Side.BUY, o.path("X").asText(),
                        d(o, "q"), d(o, "z"), d(o, "ap"), 0));
            }
            case "ACCOUNT_UPDATE" -> {
                JsonNode a = e.path("a");
                BalanceStore store = streamBalances;
                if (store != null) {
                    // cw — баланс без нереализованного результата изолированных позиций; маржа кросс-позиций
                    // в нём не вычтена, поэтому точное «свободно» даст сверка /fapi/v2/balance
                    for (JsonNode b : a.path("B")) {
                        double wallet = d(b, "wb"), cross = d(b, "cw");
                        store.set(b.path("a").asText(), cross, Math.max(0, wallet - cross));
                    }
                    store.markSynced();
                    accountSeen = true;
                }
                PositionStore ps = positions;
                if (ps != null) for (JsonNode p : a.path("P")) {
                    if (!"BOTH".equals(p.path("ps").asText("BOTH"))) continue;   // только односторонний режим
                    ps.set(p.path("s").asText(), d(p, "pa"), d(p, "ep"));
                }
            }
            default -> { }
        }
    }
}
