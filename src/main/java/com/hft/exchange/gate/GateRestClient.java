package com.hft.exchange.gate;

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
import com.hft.rest.WsRpcChannel;
import com.hft.rest.WsRpcChannel.Msg;
import com.hft.store.BalanceStore;
import com.hft.store.SymbolFilters;
import com.hft.util.Hmac;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpRequest;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Gate APIv4, спот. Не проверялся на живой бирже.
 *
 * Особенности:
 *  - подпись HMAC-SHA512 (hex) от «METHOD \n /api/v4/путь \n query \n SHA512(тело) \n секунды»;
 *  - заголовки KEY, SIGN, Timestamp (в секундах, не в мс);
 *  - рыночный ордер обязан быть IOC; amount при покупке — в котируемой валюте, при продаже — в базовой;
 *  - своё поле text должно начинаться с «t-»;
 *  - ошибки приходят как {label, message}.
 */
public final class GateRestClient extends SignedClient {

    /** Логгер. */
    private static final Logger log = LoggerFactory.getLogger(GateRestClient.class);
    /** Префикс пути REST API v4. */
    private static final String API = "/api/v4";

    /**
     * @param config подключение и параметры биржи
     * @param credentials ключи из окружения
     * @param filters правила символов
     */
    public GateRestClient(ExchangeConfig config, Credentials credentials, SymbolFilters filters) {
        super(Exchange.GATE.id(), config, credentials, filters);
    }

    /** Имя символа на бирже. */
    static String pair(String symbol) {
        return BalanceStore.baseAsset(symbol) + "_" + BalanceStore.quoteAsset(symbol);
    }

    // ------------------------------------------------------------ HTTP

    /** Публичный GET. */
    private JsonNode publicGet(String path, String query) throws Exception {
        String url = baseUrl + API + path + (query.isEmpty() ? "" : "?" + query);
        return exec(req(url).header("Accept", "application/json").GET().build(), false);
    }

    /** Подписанный запрос. */
    private JsonNode signed(String method, String path, String query, String body, boolean order) throws Exception {
        credentials.require();
        String ts = String.valueOf(System.currentTimeMillis() / 1000);
        String payload = method + "\n" + API + path + "\n" + query + "\n" + Hmac.sha512HexOf(body) + "\n" + ts;
        String sign = Hmac.sha512Hex(credentials.apiSecret(), payload);
        String url = baseUrl + API + path + (query.isEmpty() ? "" : "?" + query);
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url)).timeout(java.time.Duration.ofSeconds(10))
                .header("KEY", credentials.apiKey())
                .header("SIGN", sign)
                .header("Timestamp", ts)
                .header("Accept", "application/json")
                .header("Content-Type", "application/json");
        b.method(method, body.isEmpty() ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        return exec(b.build(), order);
    }

    /** Ошибка в HTTP-коде или теле ответа — ApiException (лимит — с признаком rateLimit). */
    @Override
    protected void checkError(int http, JsonNode body) {
        String label = body.path("label").asText("");
        if (http == 429 || "TOO_MANY_REQUESTS".equals(label)) {
            throw new ApiException(http, label, body.path("message").asText("слишком часто"), true);
        }
        if (http >= 400 || !label.isEmpty()) {
            throw new ApiException(http, label.isEmpty() ? "?" : label, body.path("message").asText(body.toString()), false);
        }
    }

    // ------------------------------------------------------------ ордера

    /** Отправить ордер (по WebSocket, если можно, иначе REST); исход ждёт вызывающий. */
    @Override
    protected OrderResult placeRaw(Order o) throws Exception {
        ObjectNode b = mapper.createObjectNode();
        b.put("text", "t-" + o.clientId());
        b.put("currency_pair", pair(o.symbol()));
        b.put("account", "spot");
        b.put("side", o.side() == Side.BUY ? "buy" : "sell");
        if (o.type() == Type.MARKET) {
            b.put("type", "market");
            b.put("time_in_force", "ioc");
            b.put("amount", plain(o.qty(), o.qtyIsQuote() ? 8 : filters.quantityScale(o.symbol())));
        } else {
            b.put("type", "limit");
            b.put("time_in_force", o.tif().name().toLowerCase());
            b.put("amount", plain(o.qty(), filters.quantityScale(o.symbol())));
            b.put("price", plain(o.price(), filters.priceScale(o.symbol())));
        }
        JsonNode r;
        try {
            r = wsCall("spot.order_place", b);                    // сначала WebSocket
        } catch (WsRpcChannel.WsUnknownOutcomeException e) {
            return resolveUnknownPlacement(o, e);
        }
        if (r == null) r = signed("POST", "/spot/orders", "", b.toString(), true);   // REST — только если WS не готов
        return parse(r, o.symbol(), o.side(), o.qtyIsQuote() ? 0 : o.qty());
    }

    /** Статус ордера: из WS-потока, если есть, иначе запрос к бирже. */
    @Override
    public OrderResult orderStatus(String symbol, long orderId) throws Exception {
        OrderResult st = streamed.get(orderId);
        if (st != null && wsReady() && !"NEW".equals(st.status()) && !"PARTIALLY_FILLED".equals(st.status())) return st;
        JsonNode r = null;
        try {
            r = wsCall("spot.order_status", mapper.createObjectNode().put("order_id", venueId(orderId)).put("currency_pair", pair(symbol)));
        } catch (WsRpcChannel.WsUnknownOutcomeException e) { wsFallbacks.incrementAndGet(); }
        if (r == null) r = signed("GET", "/spot/orders/" + venueId(orderId), "currency_pair=" + pair(symbol), "", false);
        return parse(r, symbol, "buy".equals(r.path("side").asText()) ? Side.BUY : Side.SELL, 0);
    }

    /** Разбор объекта ордера Gate. Исполненный объём в базовой валюте считаем по доступным полям. */
    private OrderResult parse(JsonNode r, String symbol, Side side, double requested) {
        String id = r.path("id").asText("");
        if (id.isEmpty()) throw new ApiException(200, "NO_ID", "нет id в ответе: " + r, false);
        double amount = d(r, "amount"), left = d(r, "left");
        double avg = d(r, "avg_deal_price");
        double filledTotal = d(r, "filled_total");        // в котируемой валюте
        double exec = d(r, "filled_amount");                 // в базовой, если поле есть
        if (exec <= 0) {
            boolean marketBuy = "market".equals(r.path("type").asText()) && side == Side.BUY;
            if (marketBuy) exec = avg > 0 ? filledTotal / avg : 0;   // amount в quote — left считать нельзя
            else exec = Math.max(0, amount - left);
        }
        if (avg <= 0 && exec > 0 && filledTotal > 0) avg = filledTotal / exec;

        String st = r.path("status").asText();
        String finish = r.path("finish_as").asText("");
        String status;
        if ("open".equals(st)) status = exec > 0 ? "PARTIALLY_FILLED" : "NEW";
        else if ("closed".equals(st)) status = "filled".equals(finish) ? "FILLED" : (exec > 0 ? "PARTIALLY_FILLED" : "EXPIRED");
        else if ("cancelled".equals(st)) status = exec > 0 ? "PARTIALLY_FILLED" : "CANCELED";
        else status = st.toUpperCase();

        double req = requested > 0 ? requested : amount;
        return new OrderResult(registerId(id), r.path("text").asText("").replaceFirst("^t-", ""),
                symbol.toUpperCase(), side, status, req, exec, avg, 0);
    }

    /** Отменить ордер. */
    @Override
    public void cancelOrder(String symbol, long orderId) throws Exception {
        JsonNode ws = null;
        try {
            ws = wsCall("spot.order_cancel", mapper.createObjectNode().put("order_id", venueId(orderId)).put("currency_pair", pair(symbol)));
        } catch (WsRpcChannel.WsUnknownOutcomeException e) { wsFallbacks.incrementAndGet(); }   // отмена идемпотентна
        if (ws == null) signed("DELETE", "/spot/orders/" + venueId(orderId), "currency_pair=" + pair(symbol), "", true);
        log.info("[gate] ордер {} по {} отменён", orderId, symbol);
    }

    /** Отменить все открытые ордера символа; возвращает их число. */
    @Override
    public int cancelAll(String symbol) throws Exception {
        JsonNode r = null;
        try {
            r = wsCall("spot.order_cancel_cp", mapper.createObjectNode().put("currency_pair", pair(symbol)));
        } catch (WsRpcChannel.WsUnknownOutcomeException e) { wsFallbacks.incrementAndGet(); }
        if (r == null) r = signed("DELETE", "/spot/orders", "currency_pair=" + pair(symbol) + "&account=spot", "", true);
        int n = r.isArray() ? r.size() : 0;
        log.info("[gate] отменено {} ордеров по {}", n, symbol);
        return n;
    }

    // ------------------------------------------------------------ WebSocket API: ордера, исполнения, балансы

    /** Адрес приватного WS вместо стандартного (тесты). */
    private volatile String privateWsUrl;
    /** Куда пишутся балансы из WS. */
    private volatile BalanceStore balanceStore;
    /** Баланс хотя бы раз пришёл по WS. */
    private volatile boolean accountSeen;
    /** Номера WS-запросов. */
    private final AtomicLong wsSeq = new AtomicLong();

    /** Задать адрес приватного WebSocket (тесты). */
    public void setPrivateWsUrl(String url) { this.privateWsUrl = url; }

    /** Поднять приватный WS-канал: ордера, исполнения, балансы. */
    @Override
    public void startStreams(BalanceStore store) {
        if (!credentials.isPresent() || !wsTradeAllowed() || wsChannel != null) return;
        balanceStore = store;
        WsRpcChannel ch = new WsRpcChannel(Exchange.GATE.id(), new Private()).onEvent(this::onEvent);
        wsChannel = ch;
        ch.start();
    }

    @Override public boolean balancesStreamed() { return wsReady() && accountSeen; }

    /** null — WS не готов, идти в REST. WsUnknownOutcome — запрос мог уйти. Бизнес-ошибка — ApiException. */
    private JsonNode wsCall(String channel, ObjectNode param) throws Exception {
        WsRpcChannel ch = wsChannel;
        if (ch == null || !ch.isReady()) return null;
        orderLimiter.acquire();
        String id = "g" + wsSeq.incrementAndGet();
        ObjectNode m = mapper.createObjectNode().put("time", System.currentTimeMillis() / 1000)
                .put("channel", channel).put("event", "api");
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
                try { code = Integer.parseInt(status); } catch (NumberFormatException ignore) { /* оставим 400 */ }
                throw new ApiException(code, label, e.path("message").asText(r.toString()), code == 429 || "TOO_MANY_REQUESTS".equals(label));
            }
            return r.path("data").path("result");
        } catch (WsRpcChannel.WsNotReadyException e) {
            wsFallbacks.incrementAndGet();
            return null;
        }
    }

    /** Обрыв WS после отправки ордера: найти ордер по clientId через REST. */
    private OrderResult resolveUnknownPlacement(Order o, WsRpcChannel.WsUnknownOutcomeException cause) throws Exception {
        wsFallbacks.incrementAndGet();
        log.warn("[gate] исход ордера {} неизвестен ({}), проверяю по REST", o.clientId(), cause.getMessage());
        try {
            JsonNode r = signed("GET", "/spot/orders/t-" + o.clientId(), "currency_pair=" + pair(o.symbol()), "", false);
            return parse(r, o.symbol(), o.side(), o.qtyIsQuote() ? 0 : o.qty());
        } catch (ApiException e) {
            throw new ApiException(200, "WS_UNKNOWN", "ордер " + o.clientId() + " не найден после обрыва WS: " + e.getMessage(), false);
        }
    }

    /** Событие приватного канала: обновить состояние ордера или баланс. */
    private void onEvent(String text) throws Exception {
        JsonNode n = mapper.readTree(text);
        String ch = n.path("channel").asText();
        if (ch.equals("spot.orders")) {
            for (JsonNode o : n.path("result")) {
                ObjectNode copy = o.deepCopy();
                String fin = o.path("finish_as").asText("open");
                copy.put("status", switch (fin) { case "open" -> "open"; case "cancelled" -> "cancelled"; default -> "closed"; });
                String sym = o.path("currency_pair").asText().replace("_", "");
                Side side = "buy".equals(o.path("side").asText()) ? Side.BUY : Side.SELL;
                OrderResult r = parse(copy, sym, side, 0);
                streamed.put(r.orderId(), r);
            }
        } else if (ch.equals("spot.balances")) {
            BalanceStore store = balanceStore;
            for (JsonNode b : n.path("result")) {
                double total = d(b, "total");
                double freeze = b.has("freeze") ? d(b, "freeze") : Math.max(0, total - d(b, "available"));
                double available = b.has("available") ? d(b, "available") : Math.max(0, total - freeze);
                if (store != null) store.set(b.path("currency").asText(), available, freeze);
            }
            if (store != null) store.markSynced();
            accountSeen = true;
        }
    }

    /** Протокол приватного WS-канала биржи. */
    private final class Private implements WsRpcChannel.Protocol {
        @Override public String url() {
            if (privateWsUrl != null) return privateWsUrl;
            return config.testnet() ? "wss://ws-testnet.gate.com/v4/ws/spot" : "wss://api.gateio.ws/ws/v4/";
        }

        @Override public List<String> login() {
            long ts = System.currentTimeMillis() / 1000;
            String sign = Hmac.sha512Hex(credentials.apiSecret(), "api\nspot.login\n\n" + ts);
            ObjectNode m = mapper.createObjectNode().put("time", ts).put("channel", "spot.login").put("event", "api");
            m.putObject("payload").put("api_key", credentials.apiKey()).put("signature", sign)
                    .put("timestamp", String.valueOf(ts)).put("req_id", "login");
            return List.of(m.toString());
        }

        /** Сообщение подписки приватного канала с подписью. */
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
            List<String> pairs = config.symbols().stream().map(GateRestClient::pair).toList();
            return List.of(subscribe("spot.orders", pairs), subscribe("spot.balances", List.of()));
        }

        @Override public String ping() {
            return mapper.createObjectNode().put("time", System.currentTimeMillis() / 1000).put("channel", "spot.ping").toString();
        }

        @Override public long pingIntervalMs() { return 15_000; }

        @Override public Msg parse(String text) throws Exception {
            JsonNode n = mapper.readTree(text);
            String ch = n.path("channel").asText("");
            if (ch.equals("spot.pong")) return Msg.ignore();
            if (n.has("header")) {                                   // ответ WS API
                if (n.path("ack").asBoolean(false)) return Msg.ignore();
                if (n.path("header").path("channel").asText().equals("spot.login")) {
                    if ("200".equals(n.path("header").path("status").asText())) return Msg.loginOk();
                    throw new IllegalStateException("Gate login: " + n.path("data").path("errs"));
                }
                return Msg.reply(n.path("request_id").asText(), text);
            }
            if (n.hasNonNull("error")) throw new IllegalStateException("Gate WS error " + n.path("error").path("code").asText() + ": " + n.path("error").path("message").asText());
            String event = n.path("event").asText("");
            if (event.equals("subscribe") || event.equals("unsubscribe")) return Msg.ignore();
            if (event.equals("update") && (ch.equals("spot.orders") || ch.equals("spot.balances"))) return Msg.event(text);
            throw new IllegalStateException("Gate WS: неожиданное сообщение " + (text.length() > 120 ? text.substring(0, 120) : text));
        }
    }

    // ------------------------------------------------------------ правила и баланс

    /** Загрузить правила торговли символов (шаги объёма и цены, минимальная сумма). */
    @Override
    public void loadFilters(Iterable<String> symbols) throws Exception {
        JsonNode list = publicGet("/spot/currency_pairs", "");
        int loaded = 0;
        for (JsonNode s : list) {
            String sym = s.path("id").asText().replace("_", "");
            boolean wanted = false;
            for (String w : symbols) if (w.equalsIgnoreCase(sym)) { wanted = true; break; }
            if (!wanted) continue;
            double minBase = d(s, "min_base_amount"), minQuote = d(s, "min_quote_amount");
            filters.put(sym, new SymbolFilters.Filter(minBase, Double.MAX_VALUE,
                    Math.pow(10, -s.path("amount_precision").asInt(6)), 0, 0,
                    Math.pow(10, -s.path("precision").asInt(8)), minQuote > 0 ? minQuote : 1.0));
            loaded++;
        }
        if (loaded == 0) throw new IllegalStateException("Gate: правила торговли не найдены для " + symbols);
        log.info("[gate] правила загружены для {} символов", loaded);
    }

    /** Загрузить балансы. */
    @Override
    public void loadBalances(BalanceStore store) throws Exception {
        JsonNode r = signed("GET", "/spot/accounts", "", "", false);
        for (JsonNode b : r) {
            double free = d(b, "available"), locked = d(b, "locked");
            if (free > 0 || locked > 0) store.set(b.path("currency").asText(), free, locked);
        }
        store.markSynced();
    }
}
