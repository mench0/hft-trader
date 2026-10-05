package com.hft.exchange.kucoin;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hft.config.Credentials;
import com.hft.config.ExchangeConfig;
import com.hft.model.OrderEnums.Side;
import com.hft.model.OrderEnums.Type;
import com.hft.model.OrderResult;
import com.hft.rest.ApiException;
import com.hft.rest.SignedCexClient;
import com.hft.rest.WsRpcChannel;
import com.hft.store.BalanceStore;
import com.hft.store.SymbolFilters;
import com.hft.util.Hmac;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpRequest;
import java.util.HashSet;
import java.util.Set;

/**
 * KuCoin спот, REST API v1/v2. Не проверялся на живой бирже.
 *
 *  - подпись: KC-API-SIGN = base64(HMAC-SHA256(secret, timestamp + METHOD + путь?query + тело)),
 *    KC-API-PASSPHRASE — тоже HMAC (версия ключа 2), нужен KUCOIN_PASSPHRASE;
 *  - ответ {"code":"200000","data":...}; 429000 — превышен лимит;
 *  - символы с дефисом: BTCUSDT -> BTC-USDT;
 *  - orderId строковый — наружу числовой псевдоним.
 *
 * В LIVE ордера и отмены идут по торговому WebSocket (Pro WS API), исполнения и балансы — по
 * приватному потоку (bullet-private), см. {@link KucoinWs}; REST — запасной канал.
 */
public final class KucoinRestClient extends SignedCexClient {

    /** Логгер. */
    private static final Logger log = LoggerFactory.getLogger(KucoinRestClient.class);

    /** Фраза API-ключа. */
    private final String passphrase;

    /**
     * @param config подключение и параметры биржи
     * @param credentials ключи из окружения
     * @param filters правила символов
     */
    public KucoinRestClient(ExchangeConfig config, Credentials credentials, SymbolFilters filters) {
        super("kucoin", config, credentials, filters);
        String p = com.hft.config.Env.get("KUCOIN_PASSPHRASE");
        if (credentials.isPresent() && (p == null || p.isBlank())) {
            throw new IllegalStateException("KuCoin требует KUCOIN_PASSPHRASE (фраза, заданная при создании API-ключа)");
        }
        this.passphrase = p == null ? "" : p.trim();
    }

    /** BTCUSDT -> BTC-USDT */
    static String venue(String symbol) {
        String[] bq = BalanceStore.splitSymbol(symbol.toUpperCase());
        return bq[0] + "-" + bq[1];
    }

    // ------------------------------------------------------------ HTTP

    /** Публичный GET. */
    private JsonNode publicGet(String pathAndQuery) throws Exception {
        return exec(req(baseUrl + pathAndQuery).GET().build(), false);
    }

    /** Подписанный запрос. */
    JsonNode signed(String method, String pathAndQuery, String body, boolean order) throws Exception {
        credentials.require();
        String ts = String.valueOf(System.currentTimeMillis());
        String sign = Hmac.sha256Base64(credentials.apiSecret(), ts + method + pathAndQuery + body);
        HttpRequest r = req(baseUrl + pathAndQuery)
                .header("KC-API-KEY", credentials.apiKey())
                .header("KC-API-SIGN", sign)
                .header("KC-API-TIMESTAMP", ts)
                .header("KC-API-PASSPHRASE", Hmac.sha256Base64(credentials.apiSecret(), passphrase))
                .header("KC-API-KEY-VERSION", "2")
                .header("Content-Type", "application/json")
                .method(method, body.isEmpty() ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body))
                .build();
        return exec(r, order);
    }

    /** Ошибка в HTTP-коде или теле ответа — ApiException (лимит — с признаком rateLimit). */
    @Override
    protected void checkError(int http, JsonNode body) {
        String code = body.path("code").asText("");
        if (http == 429 || code.equals("429000")) throw new ApiException(http, "429000", body.path("msg").asText("слишком часто"), true);
        if (http >= 400 || !code.isEmpty() && !code.equals("200000")) {
            throw new ApiException(http, code.isEmpty() ? "?" : code, body.path("msg").asText(body.toString()), false);
        }
    }

    // ------------------------------------------------------------ ордера

    /** Отправить ордер (по WebSocket, если можно, иначе REST); исход ждёт вызывающий. */
    @Override
    protected OrderResult placeRaw(Order o) throws Exception {
        ObjectNode b = mapper.createObjectNode()
                .put("clientOid", o.clientId())
                .put("side", o.side() == Side.BUY ? "buy" : "sell")
                .put("symbol", venue(o.symbol()));
        if (o.type() == Type.MARKET) {
            b.put("type", "market");
            if (o.qtyIsQuote()) b.put("funds", plain(o.qty(), 8));
            else b.put("size", plain(o.qty(), filters.quantityScale(o.symbol())));
        } else {
            b.put("type", "limit")
             .put("timeInForce", o.tif().name())
             .put("size", plain(o.qty(), filters.quantityScale(o.symbol())))
             .put("price", plain(o.price(), filters.priceScale(o.symbol())));
        }
        JsonNode data;
        try {
            data = wsOp("spot.order", b);                          // сначала WebSocket
        } catch (WsRpcChannel.WsUnknownOutcomeException e) {
            wsFallbacks.incrementAndGet();
            log.warn("[kucoin] исход ордера {} неизвестен ({}), проверяю по REST", o.clientId(), e.getMessage());
            JsonNode d = signed("GET", "/api/v1/order/client-order/" + o.clientId(), "", false).path("data");
            String vid = d.path("id").asText("");
            if (vid.isEmpty()) throw new ApiException(200, "WS_UNKNOWN", "ордер " + o.clientId() + " не найден после обрыва WS", false);
            OrderResult st = fromOrder(d, registerId(vid), o.symbol());
            return new OrderResult(st.orderId(), o.clientId(), o.symbol(), o.side(), st.status(), o.qtyIsQuote() ? 0 : o.qty(), st.executedQty(), st.avgPrice(), 0);
        }
        if (data == null) data = signed("POST", "/api/v1/orders", b.toString(), true).path("data");   // REST — если WS не готов
        String id = data.path("orderId").asText("");
        if (id.isEmpty()) throw new ApiException(200, "NO_ID", "нет orderId в ответе: " + data, false);
        return new OrderResult(registerId(id), o.clientId(), o.symbol(), o.side(), "NEW", o.qtyIsQuote() ? 0 : o.qty(), 0, 0, 0);
    }

    /** Статус ордера: из WS-потока, если есть, иначе запрос к бирже. */
    @Override
    public OrderResult orderStatus(String symbol, long orderId) throws Exception {
        OrderResult st = streamed.get(orderId);
        if (st != null && privateReady() && !"NEW".equals(st.status()) && !"PARTIALLY_FILLED".equals(st.status())) return st;
        return fromOrder(signed("GET", "/api/v1/orders/" + venueId(orderId), "", false).path("data"), orderId, symbol.toUpperCase());
    }

    /** isActive — ордер в стакане; cancelExist — был отменён (возможно, после частичного исполнения). */
    static OrderResult fromOrder(JsonNode o, long id, String symbol) {
        double exec = d(o, "dealSize"), funds = d(o, "dealFunds");
        boolean active = o.path("isActive").asBoolean(false), cancelled = o.path("cancelExist").asBoolean(false);
        String status = active ? (exec > 0 ? "PARTIALLY_FILLED" : "NEW") : cancelled ? "CANCELED" : "FILLED";
        Side side = "sell".equals(o.path("side").asText()) ? Side.SELL : Side.BUY;
        return new OrderResult(id, o.path("clientOid").asText(""), symbol, side, status, d(o, "size"), exec, exec > 0 ? funds / exec : 0, 0);
    }

    /** Отменить ордер. */
    @Override
    public void cancelOrder(String symbol, long orderId) throws Exception {
        JsonNode r = null;
        try { r = wsOp("spot.cancel", mapper.createObjectNode().put("symbol", venue(symbol)).put("orderId", venueId(orderId))); }
        catch (WsRpcChannel.WsUnknownOutcomeException e) { wsFallbacks.incrementAndGet(); }   // отмена идемпотентна — повторяем через REST
        if (r == null) signed("DELETE", "/api/v1/orders/" + venueId(orderId), "", true);
        log.info("[kucoin] ордер {} по {} отменён", orderId, symbol);
    }

    /** Отменить все открытые ордера символа; возвращает их число. */
    @Override
    public int cancelAll(String symbol) throws Exception {
        JsonNode r = signed("DELETE", "/api/v1/orders?symbol=" + venue(symbol), "", true);
        int n = r.path("data").path("cancelledOrderIds").size();
        log.info("[kucoin] отменено {} ордеров по {}", n, symbol);
        return n;
    }

    // ------------------------------------------------------------ WebSocket

    /** Протоколы и состояние сокетов; null — только REST. */
    private volatile KucoinWs ws;
    /** Приватный поток (исполнения, балансы); торговый сокет — wsChannel базового класса. */
    private volatile WsRpcChannel privateChannel;
    /** Свои адреса (тесты, прокси): торговый сокет и приватный поток. */
    private volatile String tradeWsUrl, privateWsUrl;

    /** Свои адреса сокетов (тесты, прокси). */
    public void setWsUrls(String trade, String priv) { this.tradeWsUrl = trade; this.privateWsUrl = priv; }

    /** Поднять торговый сокет и приватный поток. Без ключей или при wsTrade=false — всё по REST. */
    @Override
    public void startStreams(BalanceStore store) {
        if (!credentials.isPresent() || !wsTradeAllowed() || wsChannel != null) return;
        KucoinWs w = new KucoinWs(tradeWsUrl != null ? tradeWsUrl : "wss://wsapi.kucoin.com/v1/private", this::bulletPrivate,
                credentials.apiKey(), credentials.apiSecret(), passphrase, streamed, this::registerId);
        w.balances = store;
        ws = w;
        privateChannel = new WsRpcChannel("kucoin", w.priv).onEvent(w::onEvent);
        wsChannel = new WsRpcChannel("kucoin", w.trade);
        privateChannel.start();
        wsChannel.start();
    }

    /** Остановить оба сокета. */
    @Override
    public void stopStreams() {
        super.stopStreams();
        WsRpcChannel p = privateChannel;
        if (p != null) p.stop();
        wsChannel = privateChannel = null;
        ws = null;
    }

    /** Баланс приходит по WS и актуален. */
    @Override
    public boolean balancesStreamed() { KucoinWs w = ws; return w != null && privateReady() && w.balanceSeen; }

    /** Метрики запросов и обоих сокетов. */
    @Override
    public java.util.Map<String, Object> stats() {
        var m = super.stats();
        WsRpcChannel p = privateChannel;
        if (p != null) m.put("wsPrivate", p.stats());
        return m;
    }

    /** Приватный поток готов. */
    private boolean privateReady() { WsRpcChannel c = privateChannel; return c != null && c.isReady(); }

    /** Адрес приватного потока с токеном: POST /api/v1/bullet-private. */
    private String bulletPrivate() throws Exception {
        if (privateWsUrl != null) return privateWsUrl;
        JsonNode d = signed("POST", "/api/v1/bullet-private", "", false).path("data");
        JsonNode server = d.path("instanceServers").path(0);
        return server.path("endpoint").asText() + "?token=" + d.path("token").asText() + "&connectId=hft" + System.nanoTime();
    }

    /**
     * Запрос по торговому сокету. null — сокет не готов, вызывающий идёт в REST.
     * @throws WsRpcChannel.WsUnknownOutcomeException запрос мог уйти, ответа нет
     */
    private JsonNode wsOp(String op, ObjectNode args) throws Exception {
        KucoinWs w = ws;
        WsRpcChannel ch = wsChannel;
        if (w == null || ch == null || !ch.isReady()) return null;
        orderLimiter.acquire();
        String id = w.nextId();
        try {
            return w.result(ch.call(id, w.request(id, op, args), 5000));
        } catch (WsRpcChannel.WsNotReadyException e) {
            wsFallbacks.incrementAndGet();
            return null;
        }
    }

    // ------------------------------------------------------------ правила и баланс

    /** Загрузить правила торговли символов (шаги объёма и цены, минимальная сумма). */
    @Override
    public void loadFilters(Iterable<String> symbols) throws Exception {
        Set<String> want = new HashSet<>();
        for (String s : symbols) want.add(venue(s));
        int loaded = 0;
        for (JsonNode s : publicGet("/api/v2/symbols").path("data")) {
            String v = s.path("symbol").asText();
            if (!want.contains(v)) continue;
            double minFunds = d(s, "minFunds");
            filters.put(v.replace("-", ""), new SymbolFilters.Filter(
                    d(s, "baseMinSize"), d(s, "baseMaxSize") > 0 ? d(s, "baseMaxSize") : Double.MAX_VALUE, d(s, "baseIncrement"),
                    0, 0, d(s, "priceIncrement"), minFunds > 0 ? minFunds : Math.max(d(s, "quoteMinSize"), 1.0)));
            loaded++;
        }
        if (loaded == 0) throw new IllegalStateException("KuCoin: правила торговли не найдены");
        log.info("[kucoin] правила загружены для {} символов", loaded);
    }

    /** Загрузить балансы. */
    @Override
    public void loadBalances(BalanceStore store) throws Exception {
        for (JsonNode a : signed("GET", "/api/v1/accounts?type=trade", "", false).path("data")) {
            double free = d(a, "available"), locked = d(a, "holds");
            if (free > 0 || locked > 0) store.set(a.path("currency").asText(), free, locked);
        }
        store.markSynced();
    }
}
