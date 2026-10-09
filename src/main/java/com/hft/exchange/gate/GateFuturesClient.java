package com.hft.exchange.gate;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hft.config.Credentials;
import com.hft.config.ExchangeConfig;
import com.hft.exchange.Exchange;
import com.hft.exchange.generic.ContractSizes;
import com.hft.model.OrderEnums.Side;
import com.hft.model.OrderEnums.Type;
import com.hft.model.OrderResult;
import com.hft.rest.ApiException;
import com.hft.rest.SignedClient;
import com.hft.store.BalanceStore;
import com.hft.store.PositionStore;
import com.hft.store.SymbolFilters;
import com.hft.util.Hmac;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hft.rest.WsRpcChannel;
import com.hft.rest.WsRpcChannel.Msg;

import java.net.URI;
import java.net.http.HttpRequest;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Gate USDT-фьючерсы (API v4, /api/v4/futures/usdt), market=perp. Не проверялся на живой бирже.
 *
 * <ul>
 *   <li>Контракт BTC_USDT; объём — целое число контрактов со знаком (+ покупка, − продажа),
 *       контракт = quanto_multiplier монет. Бот считает в монетах и пересчитывает сам.</li>
 *   <li>Рыночный ордер — price "0" и tif "ioc"; reduce_only — закрытие.</li>
 *   <li>Подпись та же, что у спота: HMAC-SHA512(method\npath\nquery\nsha512(body)\nts) в заголовках KEY/SIGN/Timestamp.</li>
 *   <li>Ордера, отмены и статусы — WebSocket API на сокете фьючерсов ({@code futures.order_place},
 *       {@code futures.order_cancel}, {@code futures.order_cancel_cp}, {@code futures.order_status}) после
 *       {@code futures.login}; REST — запасной канал.</li>
 *   <li>Исполнения, позиции и баланс — приватные каналы того же сокета: {@code futures.orders},
 *       {@code futures.positions}, {@code futures.balances} (нужен id пользователя — берётся из /accounts на старте).</li>
 *   <li>WS-запрос ушёл, ответа нет — ордер ищется по REST по своему id ({@code t-…}), вслепую не повторяется.</li>
 * </ul>
 */
public final class GateFuturesClient extends SignedClient {

    /** Логгер. */
    private static final Logger log = LoggerFactory.getLogger(GateFuturesClient.class);
    /** Префикс путей. */
    private static final String API = "/api/v4";
    /** Расчётная валюта. */
    private static final String SETTLE = "/futures/usdt";

    /**
     * @param config подключение (restUrl — api.gateio.ws или fx-api-testnet)
     * @param credentials GATE_API_KEY/_SECRET
     * @param filters правила символов
     */
    public GateFuturesClient(ExchangeConfig config, Credentials credentials, SymbolFilters filters) {
        super(Exchange.GATE.id(), config, credentials, filters);
    }

    @Override public boolean isPerp() { return true; }

    /** Имя контракта: BTCUSDT -> BTC_USDT. */
    private static String contract(String symbol) { return ContractSizes.instrument(Exchange.GATE, symbol); }

    /** Размер контракта в монетах. */
    private double mult(String symbol) { return ContractSizes.get(Exchange.GATE, baseUrl, contract(symbol)); }

    // ------------------------------------------------------------ HTTP

    /** Публичный GET. */
    private JsonNode publicGet(String path) throws Exception {
        return exec(req(baseUrl + API + path).header("Accept", "application/json").GET().build(), false);
    }

    /** Подписанный запрос. */
    private JsonNode signed(String method, String path, String query, String body, boolean order) throws Exception {
        credentials.require();
        String ts = String.valueOf(System.currentTimeMillis() / 1000);
        String payload = method + "\n" + API + path + "\n" + query + "\n" + Hmac.sha512HexOf(body) + "\n" + ts;
        String url = baseUrl + API + path + (query.isEmpty() ? "" : "?" + query);
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url)).timeout(java.time.Duration.ofSeconds(10))
                .header("KEY", credentials.apiKey())
                .header("SIGN", Hmac.sha512Hex(credentials.apiSecret(), payload))
                .header("Timestamp", ts)
                .header("Accept", "application/json")
                .header("Content-Type", "application/json");
        b.method(method, body.isEmpty() ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        return exec(b.build(), order);
    }

    /** Ошибка Gate: {label, message}. */
    @Override
    protected void checkError(int http, JsonNode body) {
        String label = body.path("label").asText("");
        if (http == 429 || "TOO_MANY_REQUESTS".equals(label)) throw new ApiException(http, label, body.path("message").asText("слишком часто"), true);
        if (http >= 400 || !label.isEmpty()) throw new ApiException(http, label.isEmpty() ? "?" : label, body.path("message").asText(body.toString()), false);
    }

    // ------------------------------------------------------------ правила, баланс, позиции, плечо

    /** Правила: шаг объёма = 1 контракт (quanto_multiplier монет), шаг цены = order_price_round. */
    @Override
    public void loadFilters(Iterable<String> symbols) throws Exception {
        Set<String> want = new HashSet<>();
        for (String s : symbols) want.add(contract(s.toUpperCase()));
        int loaded = 0;
        for (JsonNode c : publicGet(SETTLE + "/contracts")) {
            String name = c.path("name").asText();
            double m = d(c, "quanto_multiplier");
            ContractSizes.put(Exchange.GATE, name, m);
            if (!want.contains(name)) continue;
            double max = d(c, "order_size_max");
            filters.put(name.replace("_", ""), new SymbolFilters.Filter(d(c, "order_size_min") * m, max > 0 ? max * m : Double.MAX_VALUE,
                    m, 0, 0, d(c, "order_price_round"), 1.0));
            loaded++;
        }
        if (loaded == 0) throw new IllegalStateException("Gate futures: контракты не найдены для " + symbols);
        log.info("[gate] фьючерсы: правила загружены для {} контрактов", loaded);
    }

    /** Баланс фьючерсного счёта: available — свободно, остальное — маржа. */
    @Override
    public void loadBalances(BalanceStore store) throws Exception {
        JsonNode a = signed("GET", SETTLE + "/accounts", "", "", false);
        double total = d(a, "total"), free = d(a, "available");
        store.set(a.path("currency").asText("USDT"), free, Math.max(0, total - free));
        store.markSynced();
    }

    /** Плечо контракта (0 у Gate — кросс-маржа, поэтому задаём явно). */
    @Override
    public void setLeverage(String symbol, int leverage) throws Exception {
        signed("POST", SETTLE + "/positions/" + contract(symbol) + "/leverage", "leverage=" + leverage, "", false);
        log.info("[gate] плечо {}x для {}", leverage, symbol);
    }

    /** Открытые позиции: size — контракты со знаком. */
    @Override
    public void loadPositions(PositionStore store) throws Exception {
        positionStore = store;
        Set<String> seen = new HashSet<>();
        for (JsonNode p : signed("GET", SETTLE + "/positions", "", "", false)) {
            double size = d(p, "size");
            if (size == 0) continue;
            String c = p.path("contract").asText(), sym = c.replace("_", "");
            store.set(sym, size * ContractSizes.get(Exchange.GATE, baseUrl, c), d(p, "entry_price"));
            seen.add(sym);
        }
        for (String s : store.snapshot().keySet()) if (!seen.contains(s)) store.set(s, 0, 0);
    }

    // ------------------------------------------------------------ защитный стоп

    /**
     * Стоп на бирже: ценовой триггер {@code /price_orders} по цене маркировки (price_type 1),
     * исполнение — рыночное закрытие всей позиции (size 0, close, reduce_only). Лонг — при цене ≤ стопа (rule 2), шорт — ≥ (rule 1).
     */
    @Override
    public String placeStopLoss(String symbol, Side side, double qty, double stopPrice) throws Exception {
        ObjectNode b = mapper.createObjectNode();
        b.putObject("initial").put("contract", contract(symbol)).put("size", 0).put("price", "0")
                .put("tif", "ioc").put("close", true).put("reduce_only", true);
        b.putObject("trigger").put("strategy_type", 0).put("price_type", 1)
                .put("price", plain(stopPrice, filters.priceScale(symbol))).put("rule", side == Side.SELL ? 2 : 1);
        String id = signed("POST", SETTLE + "/price_orders", "", b.toString(), true).path("id").asText("");
        if (id.isEmpty()) throw new ApiException(200, "NO_ID", "нет id у стопа", false);
        return id;
    }

    /** Снять стоп. */
    @Override
    public void cancelStopLoss(String symbol, String stopId) throws Exception {
        signed("DELETE", SETTLE + "/price_orders/" + stopId, "", "", true);
    }

    // ------------------------------------------------------------ ордера

    /** Монеты -> целое число контрактов со знаком стороны. */
    private long contracts(Order o) {
        long n = (long) Math.floor(o.qty() / mult(o.symbol()) + 1e-9);
        if (n <= 0) throw new IllegalArgumentException("Gate futures: объём меньше одного контракта");
        return o.side() == Side.BUY ? n : -n;
    }

    /** Отправить ордер: WebSocket API, если сокет готов, иначе REST. */
    @Override
    protected OrderResult placeRaw(Order o) throws Exception {
        if (o.qtyIsQuote()) throw new IllegalArgumentException("Gate futures: ордер на сумму не поддерживается — задайте объём");
        ObjectNode b = mapper.createObjectNode()
                .put("contract", contract(o.symbol()))
                .put("size", contracts(o))
                .put("text", "t-" + o.clientId());
        if (o.type() == Type.MARKET) {
            b.put("price", "0").put("tif", "ioc");
        } else {
            b.put("price", plain(o.price(), filters.priceScale(o.symbol())))
             .put("tif", switch (o.tif()) { case GTC -> "gtc"; case IOC -> "ioc"; case FOK -> "fok"; });
        }
        if (o.reduceOnly()) b.put("reduce_only", true);
        JsonNode r;
        try {
            r = wsCall("futures.order_place", b);                       // сначала WebSocket
        } catch (WsRpcChannel.WsUnknownOutcomeException e) {
            wsFallbacks.incrementAndGet();
            log.warn("[gate] фьючерсы: исход ордера {} неизвестен ({}), проверяю по REST", o.clientId(), e.getMessage());
            try {
                return parse(signed("GET", SETTLE + "/orders/t-" + o.clientId(), "", "", false), o.symbol());
            } catch (ApiException nf) {
                throw new ApiException(200, "WS_UNKNOWN", "ордер " + o.clientId() + " не найден после обрыва WS: " + nf.getMessage(), false);
            }
        }
        if (r == null) r = signed("POST", SETTLE + "/orders", "", b.toString(), true);   // REST — только если WS не готов
        return parse(r, o.symbol());
    }

    /** Ответ об ордере: size/left — контракты; finished + left 0 — исполнен. */
    private OrderResult parse(JsonNode r, String symbol) {
        long id = r.path("id").asLong();
        double m = mult(symbol);
        double size = Math.abs(d(r, "size")), left = Math.abs(d(r, "left"));
        double exec = (size - left) * m;
        String status;
        if ("finished".equals(r.path("status").asText())) status = left == 0 ? "FILLED" : "CANCELED";
        else status = exec > 0 ? "PARTIALLY_FILLED" : "NEW";
        Side side = d(r, "size") < 0 ? Side.SELL : Side.BUY;
        String text = r.path("text").asText("");
        return new OrderResult(id, text.startsWith("t-") ? text.substring(2) : text, symbol.toUpperCase(), side, status,
                size * m, exec, d(r, "fill_price"), 0);
    }

    /** Статус: итог из потока futures.orders, иначе WS API, иначе REST. */
    @Override
    public OrderResult orderStatus(String symbol, long orderId) throws Exception {
        OrderResult st = streamed.get(orderId);
        if (st != null && wsReady() && terminal(st.status())) return st;
        JsonNode r = null;
        try { r = wsCall("futures.order_status", mapper.createObjectNode().put("order_id", Long.toString(orderId))); }
        catch (WsRpcChannel.WsUnknownOutcomeException e) { wsFallbacks.incrementAndGet(); }   // чтение — можно по REST
        if (r == null) r = signed("GET", SETTLE + "/orders/" + orderId, "", "", false);
        return parse(r, symbol);
    }

    /** Отмена: WS API, иначе REST (отмена идемпотентна). */
    @Override
    public void cancelOrder(String symbol, long orderId) throws Exception {
        JsonNode r = null;
        try { r = wsCall("futures.order_cancel", mapper.createObjectNode().put("order_id", Long.toString(orderId))); }
        catch (WsRpcChannel.WsUnknownOutcomeException e) { wsFallbacks.incrementAndGet(); }
        if (r == null) signed("DELETE", SETTLE + "/orders/" + orderId, "", "", true);
    }

    /** Отмена всех ордеров контракта: WS API futures.order_cancel_cp, иначе REST. */
    @Override
    public int cancelAll(String symbol) throws Exception {
        JsonNode r = null;
        try { r = wsCall("futures.order_cancel_cp", mapper.createObjectNode().put("contract", contract(symbol))); }
        catch (WsRpcChannel.WsUnknownOutcomeException e) { wsFallbacks.incrementAndGet(); }
        if (r == null) r = signed("DELETE", SETTLE + "/orders", "contract=" + contract(symbol), "", true);
        int n = r.isArray() ? r.size() : 0;
        log.info("[gate] фьючерсы: отменено {} ордеров по {}", n, symbol);
        return n;
    }

    // ------------------------------------------------------------ WebSocket: ордера, исполнения, позиции, баланс

    /** Свой адрес сокета (тесты, прокси). */
    private volatile String privateWsUrl;
    /** Куда пишутся баланс и позиции из потока. */
    private volatile BalanceStore balanceStore;
    private volatile PositionStore positionStore;
    /** id пользователя Gate — нужен в подписках futures.* (берётся из /accounts). */
    private volatile String userId = "";
    /** Баланс хотя бы раз пришёл по WS. */
    private volatile boolean accountSeen;
    /** Номера WS-запросов. */
    private final AtomicLong wsSeq = new AtomicLong();

    /** Свой адрес сокета (тесты, прокси). */
    public void setPrivateWsUrl(String url) { this.privateWsUrl = url; }

    /** Поднять сокет: логин, WS API ордеров и приватные каналы. Без ключей или при wsTrade=false — всё по REST. */
    @Override
    public void startStreams(BalanceStore store) throws Exception {
        if (!credentials.isPresent() || !wsTradeAllowed() || wsChannel != null) return;
        balanceStore = store;
        userId = signed("GET", SETTLE + "/accounts", "", "", false).path("user").asText("");
        WsRpcChannel ch = new WsRpcChannel(Exchange.GATE.id(), new Private()).onEvent(this::onEvent);
        wsChannel = ch;
        ch.start();
    }

    @Override public boolean balancesStreamed() { return wsReady() && accountSeen; }

    /** Запрос WS API. null — сокет не готов, идти в REST; бизнес-ошибка — ApiException. */
    private JsonNode wsCall(String channel, ObjectNode param) throws Exception {
        WsRpcChannel ch = wsChannel;
        if (ch == null || !ch.isReady()) return null;
        orderLimiter.acquire();
        String id = "gf" + wsSeq.incrementAndGet();
        ObjectNode m = mapper.createObjectNode().put("time", System.currentTimeMillis() / 1000).put("channel", channel).put("event", "api");
        ObjectNode pl = m.putObject("payload");
        pl.put("req_id", id);
        pl.set("req_param", param);
        try {
            JsonNode r = mapper.readTree(ch.call(id, m.toString(), 5000));
            String status = r.path("header").path("status").asText("200");
            if (!"200".equals(status)) {
                JsonNode e = r.path("data").path("errs");
                String label = e.path("label").asText("?");
                int code = 400;
                try { code = Integer.parseInt(status); } catch (NumberFormatException ignore) { /* 400 */ }
                throw new ApiException(code, label, e.path("message").asText(r.toString()), code == 429 || "TOO_MANY_REQUESTS".equals(label));
            }
            return r.path("data").path("result");
        } catch (WsRpcChannel.WsNotReadyException e) {
            wsFallbacks.incrementAndGet();
            return null;
        }
    }

    /** Событие приватного канала: ордер, позиция или баланс. */
    void onEvent(String text) throws Exception {
        JsonNode n = mapper.readTree(text);
        switch (n.path("channel").asText()) {
            case "futures.orders" -> {
                for (JsonNode o : n.path("result")) {
                    String sym = o.path("contract").asText().replace("_", "");
                    OrderResult r = parse(o, sym);
                    streamed.put(r.orderId(), r);
                }
            }
            case "futures.positions" -> {
                PositionStore ps = positionStore;
                if (ps == null) return;
                for (JsonNode p : n.path("result")) {
                    String c = p.path("contract").asText();
                    ps.set(c.replace("_", ""), d(p, "size") * ContractSizes.get(Exchange.GATE, baseUrl, c), d(p, "entry_price"));
                }
            }
            case "futures.balances" -> {
                BalanceStore store = balanceStore;
                if (store == null) return;
                for (JsonNode b : n.path("result")) {
                    String ccy = b.path("currency").asText("USDT").toUpperCase();
                    double locked = store.locked(ccy);                  // маржу поток не присылает — берём из последней сверки
                    store.set(ccy, Math.max(0, d(b, "balance") - locked), locked);
                }
                store.markSynced();
                accountSeen = true;
            }
            default -> { }
        }
    }

    /** Протокол сокета фьючерсов: логин futures.login, WS API, подписки с подписью. */
    private final class Private implements WsRpcChannel.Protocol {
        @Override public String url() {
            if (privateWsUrl != null) return privateWsUrl;
            return config.testnet() ? "wss://fx-ws-testnet.gateio.ws/v4/ws/usdt" : "wss://fx-ws.gateio.ws/v4/ws/usdt";
        }

        @Override public List<String> login() {
            long ts = System.currentTimeMillis() / 1000;
            String sign = Hmac.sha512Hex(credentials.apiSecret(), "api\nfutures.login\n\n" + ts);
            ObjectNode m = mapper.createObjectNode().put("time", ts).put("channel", "futures.login").put("event", "api");
            m.putObject("payload").put("api_key", credentials.apiKey()).put("signature", sign)
                    .put("timestamp", String.valueOf(ts)).put("req_id", "login");
            return List.of(m.toString());
        }

        /** Подписка приватного канала с подписью. */
        private String subscribe(String channel, List<String> payload) {
            long ts = System.currentTimeMillis() / 1000;
            ObjectNode m = mapper.createObjectNode().put("time", ts).put("channel", channel).put("event", "subscribe");
            var arr = m.putArray("payload");
            payload.forEach(arr::add);
            m.putObject("auth").put("method", "api_key").put("KEY", credentials.apiKey())
                    .put("SIGN", Hmac.sha512Hex(credentials.apiSecret(), "channel=" + channel + "&event=subscribe&time=" + ts));
            return m.toString();
        }

        @Override public List<String> subscriptions() {
            return List.of(subscribe("futures.orders", List.of(userId, "!all")),
                    subscribe("futures.positions", List.of(userId, "!all")),
                    subscribe("futures.balances", List.of(userId)));
        }

        @Override public String ping() {
            return mapper.createObjectNode().put("time", System.currentTimeMillis() / 1000).put("channel", "futures.ping").toString();
        }

        @Override public long pingIntervalMs() { return 15_000; }

        @Override public Msg parse(String text) throws Exception {
            JsonNode n = mapper.readTree(text);
            String ch = n.path("channel").asText("");
            if (ch.equals("futures.pong")) return Msg.ignore();
            if (n.has("header")) {                                     // ответ WS API
                if (n.path("ack").asBoolean(false)) return Msg.ignore();
                if (n.path("header").path("channel").asText().equals("futures.login")) {
                    if ("200".equals(n.path("header").path("status").asText())) return Msg.loginOk();
                    throw new IllegalStateException("Gate futures login: " + n.path("data").path("errs"));
                }
                return Msg.reply(n.path("request_id").asText(), text);
            }
            if (n.hasNonNull("error")) throw new IllegalStateException("Gate futures WS error " + n.path("error").path("code").asText() + ": " + n.path("error").path("message").asText());
            String event = n.path("event").asText("");
            if (event.equals("subscribe") || event.equals("unsubscribe")) return Msg.ignore();
            if (event.equals("update") && ch.startsWith("futures.")) return Msg.event(text);
            throw new IllegalStateException("Gate futures WS: неожиданное сообщение " + (text.length() > 120 ? text.substring(0, 120) : text));
        }
    }
}
