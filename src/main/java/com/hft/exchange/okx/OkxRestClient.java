package com.hft.exchange.okx;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
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

import java.net.http.HttpRequest;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

/**
 * OKX v5, спот (tdMode=cash). Не проверялся на живой бирже.
 *
 * Особенности:
 *  - подпись Base64(HMAC-SHA256(timestamp + METHOD + путь_с_query + тело)) в заголовках OK-ACCESS-*;
 *  - нужен ещё passphrase (OKX_PASSPHRASE), который задаётся при создании ключа;
 *  - ответ всегда {code, msg, data:[...]}, успех — code "0" и sCode "0" у каждого элемента;
 *  - рыночная покупка по умолчанию задаётся суммой в котируемой валюте, поэтому tgtCcy указываем явно;
 *  - IOC/FOK — это отдельные ordType, а не timeInForce;
 *  - testnet=true включает демо-торговлю заголовком x-simulated-trading.
 */
public final class OkxRestClient extends SignedClient {

    /** Логгер. */
    private static final Logger log = LoggerFactory.getLogger(OkxRestClient.class);
    /** Время для подписи OKX (ISO, UTC, миллисекунды). */
    private static final DateTimeFormatter TS = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    /** Фраза API-ключа. */
    private final String passphrase;
    /** Перпы (market=perp): инструменты -SWAP, объём в контрактах, tdMode=cross. */
    private final boolean swap;
    /** Шаг объёма в контрактах по instId (для формата sz). */
    private final Map<String, Double> lotSz = new ConcurrentHashMap<>();
    /** Куда пишутся позиции из WS. */
    private volatile com.hft.store.PositionStore positionStore;

    /**
     * @param config подключение и параметры биржи
     * @param credentials ключи из окружения
     * @param filters правила символов
     */
    public OkxRestClient(ExchangeConfig config, Credentials credentials, SymbolFilters filters) {
        super(Exchange.OKX.id(), config, credentials, filters);
        String p = com.hft.config.Env.get("OKX_PASSPHRASE");
        if (credentials.isPresent() && (p == null || p.isBlank())) {
            throw new IllegalStateException("OKX требует OKX_PASSPHRASE (фраза, заданная при создании API-ключа)");
        }
        this.passphrase = p == null ? "" : p.trim();
        this.swap = config.params().isPerp();
    }

    @Override public boolean isPerp() { return swap; }

    /** Имя символа на бирже: BTC-USDT (спот) или BTC-USDT-SWAP (перп). */
    private String instId(String symbol) {
        return swap ? OkxContracts.instId(symbol) : BalanceStore.baseAsset(symbol) + "-" + BalanceStore.quoteAsset(symbol);
    }

    /** Наш символ по instId: BTC-USDT-SWAP -> BTCUSDT. */
    static String symbolOf(String instId) { return instId.replace("-SWAP", "").replace("-", ""); }

    /** Тип инструментов для запросов: SPOT или SWAP. */
    private String instType() { return swap ? "SWAP" : "SPOT"; }

    /** Множитель «контракты -> базовая валюта» (на споте 1). */
    private double ct(String instId) { return instId.endsWith("-SWAP") ? OkxContracts.ctValOrOne(instId) : 1; }

    /** Объём в базовой валюте -> строка sz в контрактах (перп) или как есть (спот). */
    private String sz(String symbol, double qty) {
        if (!swap) return plain(qty, filters.quantityScale(symbol));
        String inst = instId(symbol);
        double lot = lotSz.getOrDefault(inst, 1.0);
        double contracts = Math.floor(qty / ct(inst) / lot + 1e-9) * lot;
        return java.math.BigDecimal.valueOf(contracts).setScale(Math.max(0, java.math.BigDecimal.valueOf(lot).stripTrailingZeros().scale()),
                java.math.RoundingMode.DOWN).stripTrailingZeros().toPlainString();
    }

    // ------------------------------------------------------------ HTTP

    /** Публичный GET. */
    private JsonNode publicGet(String pathAndQuery) throws Exception {
        return exec(req(baseUrl + pathAndQuery).GET().build(), false);
    }

    /** Подписанный запрос. */
    private JsonNode signed(String method, String pathAndQuery, String body, boolean order) throws Exception {
        credentials.require();
        String ts = ZonedDateTime.now(ZoneOffset.UTC).format(TS);
        String sign = Hmac.sha256Base64(credentials.apiSecret(), ts + method + pathAndQuery + body);
        HttpRequest.Builder b = req(baseUrl + pathAndQuery)
                .header("OK-ACCESS-KEY", credentials.apiKey())
                .header("OK-ACCESS-SIGN", sign)
                .header("OK-ACCESS-TIMESTAMP", ts)
                .header("OK-ACCESS-PASSPHRASE", passphrase)
                .header("Content-Type", "application/json");
        if (config.testnet()) b.header("x-simulated-trading", "1");
        b.method(method, method.equals("GET") ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        return exec(b.build(), order);
    }

    /** Ошибка в HTTP-коде или теле ответа — ApiException (лимит — с признаком rateLimit). */
    @Override
    protected void checkError(int http, JsonNode body) {
        String code = body.path("code").asText("");
        if (http == 429 || "50011".equals(code) || "50061".equals(code)) {
            throw new ApiException(http, code, body.path("msg").asText("слишком часто"), true);
        }
        if (http >= 400 || (!code.isEmpty() && !"0".equals(code))) {
            throw new ApiException(http, code, firstError(body), false);
        }
        JsonNode first = body.path("data").path(0);
        String sCode = first.path("sCode").asText("0");
        if (!"0".equals(sCode)) throw new ApiException(http, sCode, first.path("sMsg").asText(), "50011".equals(sCode));
    }

    /** Текст первой ошибки из data[].sMsg или msg. */
    private static String firstError(JsonNode body) {
        String s = body.path("data").path(0).path("sMsg").asText("");
        return s.isEmpty() ? body.path("msg").asText(body.toString()) : s;
    }

    // ------------------------------------------------------------ ордера

    /** Отправить ордер (по WebSocket, если можно, иначе REST); исход ждёт вызывающий. */
    @Override
    protected OrderResult placeRaw(Order o) throws Exception {
        ObjectNode b = mapper.createObjectNode();
        if (swap && o.qtyIsQuote()) throw new IllegalArgumentException("OKX SWAP: ордер на сумму не поддерживается — задайте объём");
        b.put("instId", instId(o.symbol()));
        b.put("tdMode", swap ? "cross" : "cash");
        b.put("side", o.side() == Side.BUY ? "buy" : "sell");
        b.put("clOrdId", o.clientId());
        if (o.reduceOnly()) b.put("reduceOnly", true);
        if (o.type() == Type.MARKET) {
            b.put("ordType", "market");
            if (!swap) b.put("tgtCcy", o.qtyIsQuote() ? "quote_ccy" : "base_ccy");
            b.put("sz", o.qtyIsQuote() ? plain(o.qty(), 8) : sz(o.symbol(), o.qty()));
        } else {
            b.put("ordType", switch (o.tif()) { case GTC -> "limit"; case IOC -> "ioc"; case FOK -> "fok"; });
            b.put("px", plain(o.price(), filters.priceScale(o.symbol())));
            b.put("sz", sz(o.symbol(), o.qty()));
        }
        JsonNode r;
        try {
            r = wsOp("order", List.of(b));                       // сначала WebSocket
        } catch (WsRpcChannel.WsUnknownOutcomeException e) {
            return resolveUnknownPlacement(o, e);
        }
        if (r == null) r = signed("POST", "/api/v5/trade/order", b.toString(), true);   // REST — только если WS не готов
        String ordId = r.path("data").path(0).path("ordId").asText("");
        if (ordId.isEmpty()) throw new ApiException(200, "NO_ID", "нет ordId в ответе: " + r, false);
        return new OrderResult(registerId(ordId), o.clientId(), o.symbol(), o.side(), "NEW",
                o.qtyIsQuote() ? 0 : o.qty(), 0, 0, 0);
    }

    /** Статус ордера: из WS-потока, если есть, иначе запрос к бирже. */
    @Override
    public OrderResult orderStatus(String symbol, long orderId) throws Exception {
        OrderResult st = streamed.get(orderId);
        if (st != null && wsReady() && !"NEW".equals(st.status()) && !"PARTIALLY_FILLED".equals(st.status())) return st;
        JsonNode r = signed("GET", "/api/v5/trade/order?instId=" + instId(symbol) + "&ordId=" + venueId(orderId), "", false);
        return fromOrder(r.path("data").path(0), orderId, symbol);
    }

    /** Ответ биржи об ордере в OrderResult. */
    private OrderResult fromOrder(JsonNode o, long orderId, String symbol) {
        if (o.isMissingNode() || o.isNull()) throw new IllegalStateException("Ордер не найден");
        double exec = d(o, "accFillSz");
        String status = switch (o.path("state").asText()) {
            case "live" -> "NEW";
            case "partially_filled" -> "PARTIALLY_FILLED";
            case "filled" -> "FILLED";
            case "canceled", "mmp_canceled" -> "CANCELED";
            default -> o.path("state").asText().toUpperCase();
        };
        Side side = "buy".equals(o.path("side").asText()) ? Side.BUY : Side.SELL;
        double k = ct(o.path("instId").asText(""));
        return new OrderResult(orderId, o.path("clOrdId").asText(""), symbol.toUpperCase(), side, status,
                d(o, "sz") * k, exec * k, d(o, "avgPx"), 0);
    }

    /** Отменить ордер. */
    @Override
    public void cancelOrder(String symbol, long orderId) throws Exception {
        ObjectNode b = mapper.createObjectNode().put("instId", instId(symbol)).put("ordId", venueId(orderId));
        JsonNode ws = null;
        try { ws = wsOp("cancel-order", List.of(b)); }
        catch (WsRpcChannel.WsUnknownOutcomeException e) { wsFallbacks.incrementAndGet(); }   // отмена идемпотентна — повторяем через REST
        if (ws == null) signed("POST", "/api/v5/trade/cancel-order", b.toString(), true);
        openOrders.remove(venueId(orderId));
        log.info("[okx] ордер {} по {} отменён", orderId, symbol);
    }

    /** Отменить все открытые ордера символа; возвращает их число. */
    @Override
    public int cancelAll(String symbol) throws Exception {
        if (openKnown && wsReady()) {
            String inst = instId(symbol);
            List<ObjectNode> batch = new java.util.ArrayList<>();
            for (var e : openOrders.entrySet())
                if (inst.equals(e.getValue())) batch.add(mapper.createObjectNode().put("instId", inst).put("ordId", e.getKey()));
            try {
                int done = 0;
                for (int i = 0; i < batch.size(); i += 20) {
                    List<ObjectNode> part = batch.subList(i, Math.min(batch.size(), i + 20));
                    if (wsOp("batch-cancel-orders", part) == null) throw new IllegalStateException("WS не готов");
                    done += part.size();
                }
                for (ObjectNode n : batch) openOrders.remove(n.path("ordId").asText());
                log.info("[okx] по WS отменено {} ордеров по {}", done, symbol);
                return done;
            } catch (Exception e) {
                wsFallbacks.incrementAndGet();
                log.warn("[okx] отмена по WS не удалась ({}), делаю через REST", e.getMessage());
            }
        }
        JsonNode pending = signed("GET", "/api/v5/trade/orders-pending?instType=" + instType() + "&instId=" + instId(symbol), "", false);
        ArrayNode batch = mapper.createArrayNode();
        int total = 0;
        for (JsonNode o : pending.path("data")) {
            batch.addObject().put("instId", instId(symbol)).put("ordId", o.path("ordId").asText());
            if (batch.size() == 20) { signed("POST", "/api/v5/trade/cancel-batch-orders", batch.toString(), true); total += 20; batch.removeAll(); }
        }
        if (batch.size() > 0) { total += batch.size(); signed("POST", "/api/v5/trade/cancel-batch-orders", batch.toString(), true); }
        log.info("[okx] отменено {} ордеров по {}", total, symbol);
        return total;
    }

    // ------------------------------------------------------------ WebSocket: ордера, исполнения, балансы

    /** Адрес приватного WS вместо стандартного (тесты). */
    private volatile String privateWsUrl;
    /** Куда пишутся балансы из WS. */
    private volatile BalanceStore balanceStore;
    /** Баланс хотя бы раз пришёл по WS. */
    private volatile boolean openKnown, accountSeen;
    private final Map<String, String> openOrders = new ConcurrentHashMap<>();   // ordId -> instId
    /** Номера WS-запросов. */
    private final AtomicLong wsSeq = new AtomicLong();

    /** Свой адрес приватного WS (тесты, прокси). */
    public void setPrivateWsUrl(String url) { this.privateWsUrl = url; }

    /** Поднять приватный WS-канал: ордера, исполнения, балансы. */
    @Override
    public void startStreams(BalanceStore store) throws Exception {
        if (!credentials.isPresent() || !wsTradeAllowed() || wsChannel != null) return;
        balanceStore = store;
        try {                                                   // единственный REST: список открытых ордеров на старте
            JsonNode pending = signed("GET", "/api/v5/trade/orders-pending?instType=" + instType(), "", false);
            for (JsonNode o : pending.path("data")) openOrders.put(o.path("ordId").asText(), o.path("instId").asText());
            openKnown = true;
        } catch (Exception e) {
            openKnown = false;
            log.warn("[okx] не удалось прочитать открытые ордера: {} — отмена пойдёт через REST", e.getMessage());
        }
        WsRpcChannel ch = new WsRpcChannel(Exchange.OKX.id(), new Private()).onEvent(this::onEvent);
        wsChannel = ch;
        ch.start();
    }

    @Override public boolean balancesStreamed() { return wsReady() && accountSeen; }

    /** null — WS не готов, надо идти в REST. Исключение WsUnknownOutcome — запрос мог уйти. */
    private JsonNode wsOp(String op, List<ObjectNode> args) throws Exception {
        WsRpcChannel ch = wsChannel;
        if (ch == null || !ch.isReady()) return null;
        orderLimiter.acquire();
        String id = "w" + wsSeq.incrementAndGet();
        ObjectNode m = mapper.createObjectNode().put("id", id).put("op", op);
        ArrayNode a = m.putArray("args");
        args.forEach(a::add);
        try {
            JsonNode r = mapper.readTree(ch.call(id, m.toString(), 5000));
            checkError(200, r);
            return r;
        } catch (WsRpcChannel.WsNotReadyException e) {
            wsFallbacks.incrementAndGet();
            return null;
        }
    }

    /** WS оборвался после отправки ордера: смотрим на бирже по clOrdId, что с ним стало. */
    private OrderResult resolveUnknownPlacement(Order o, WsRpcChannel.WsUnknownOutcomeException cause) throws Exception {
        wsFallbacks.incrementAndGet();
        log.warn("[okx] исход ордера {} неизвестен ({}), проверяю по REST", o.clientId(), cause.getMessage());
        try {
            JsonNode r = signed("GET", "/api/v5/trade/order?instId=" + instId(o.symbol()) + "&clOrdId=" + o.clientId(), "", false);
            JsonNode d = r.path("data").path(0);
            long id = registerId(d.path("ordId").asText());
            OrderResult st = fromOrder(d, id, o.symbol());
            return new OrderResult(st.orderId(), o.clientId(), st.symbol(), st.side(), st.status(),
                    o.qtyIsQuote() ? 0 : o.qty(), st.executedQty(), st.avgPrice(), 0);
        } catch (ApiException e) {
            throw new ApiException(200, "WS_UNKNOWN", "ордер " + o.clientId() + " не найден после обрыва WS: " + e.getMessage(), false);
        }
    }

    /** Событие приватного канала: обновить состояние ордера или баланс. */
    private void onEvent(String text) throws Exception {
        JsonNode n = mapper.readTree(text);
        String ch = n.path("arg").path("channel").asText();
        if (ch.equals("orders")) {
            for (JsonNode o : n.path("data")) {
                String ordId = o.path("ordId").asText();
                String inst = o.path("instId").asText();
                String state = o.path("state").asText();
                String sym = symbolOf(inst);
                double k = ct(inst);
                long id = registerId(ordId);
                Side side = "buy".equals(o.path("side").asText()) ? Side.BUY : Side.SELL;
                String status = switch (state) {
                    case "live" -> "NEW";
                    case "partially_filled" -> "PARTIALLY_FILLED";
                    case "filled" -> "FILLED";
                    case "canceled", "mmp_canceled" -> "CANCELED";
                    default -> state.toUpperCase();
                };
                streamed.put(id, new OrderResult(id, o.path("clOrdId").asText(""), sym, side, status,
                        d(o, "sz") * k, d(o, "accFillSz") * k, d(o, "avgPx"), 0));
                if ("live".equals(state) || "partially_filled".equals(state)) openOrders.put(ordId, inst);
                else openOrders.remove(ordId);
            }
        } else if (ch.equals("account")) {
            BalanceStore store = balanceStore;
            for (JsonNode acc : n.path("data"))
                for (JsonNode bal : acc.path("details")) {
                    if (store != null) store.set(bal.path("ccy").asText(), d(bal, "availBal"), d(bal, "frozenBal"));
                }
            if (store != null) store.markSynced();
            accountSeen = true;
        } else if (ch.equals("positions")) {
            com.hft.store.PositionStore ps = positionStore;
            if (ps == null) return;
            for (JsonNode p : n.path("data")) {
                String inst = p.path("instId").asText();
                if (!inst.endsWith("-SWAP") || !"net".equals(p.path("posSide").asText("net"))) continue;
                ps.set(symbolOf(inst), d(p, "pos") * ct(inst), d(p, "avgPx"));
            }
        }
    }

    /** Протокол приватного WS-канала биржи. */
    private final class Private implements WsRpcChannel.Protocol {
        @Override public String url() {
            if (privateWsUrl != null) return privateWsUrl;
            return config.testnet() ? "wss://wspap.okx.com:8443/ws/v5/private?brokerId=9999" : "wss://ws.okx.com:8443/ws/v5/private";
        }

        @Override public List<String> login() {
            String ts = String.valueOf(System.currentTimeMillis() / 1000);
            ObjectNode m = mapper.createObjectNode().put("op", "login");
            m.putArray("args").addObject().put("apiKey", credentials.apiKey()).put("passphrase", passphrase)
                    .put("timestamp", ts).put("sign", Hmac.sha256Base64(credentials.apiSecret(), ts + "GET" + "/users/self/verify"));
            return List.of(m.toString());
        }

        @Override public List<String> subscriptions() {
            ObjectNode m = mapper.createObjectNode().put("op", "subscribe");
            ArrayNode a = m.putArray("args");
            a.addObject().put("channel", "orders").put("instType", instType());
            a.addObject().put("channel", "account");
            if (swap) a.addObject().put("channel", "positions").put("instType", "SWAP");
            return List.of(m.toString());
        }

        @Override public String ping() { return "ping"; }

        @Override public Msg parse(String text) throws Exception {
            if (text.equals("pong")) return Msg.ignore();
            JsonNode n = mapper.readTree(text);
            String event = n.path("event").asText("");
            if (event.equals("login")) {
                if ("0".equals(n.path("code").asText())) return Msg.loginOk();
                throw new IllegalStateException("OKX login: " + n.path("code").asText() + " " + n.path("msg").asText());
            }
            if (event.equals("error")) throw new IllegalStateException("OKX WS error " + n.path("code").asText() + ": " + n.path("msg").asText());
            if (event.equals("subscribe") || event.equals("unsubscribe") || event.equals("channel-conn-count")) return Msg.ignore();
            if (n.has("op") && n.has("id")) return Msg.reply(n.path("id").asText(), text);
            if (n.has("arg") && n.has("data")) return Msg.event(text);
            throw new IllegalStateException("OKX WS: неожиданное сообщение " + (text.length() > 120 ? text.substring(0, 120) : text));
        }
    }

    // ------------------------------------------------------------ перпы: плечо и позиции

    /** Плечо инструмента (кросс-маржа). */
    @Override
    public void setLeverage(String symbol, int leverage) throws Exception {
        if (!swap) return;
        ObjectNode b = mapper.createObjectNode().put("instId", instId(symbol)).put("lever", Integer.toString(leverage)).put("mgnMode", "cross");
        signed("POST", "/api/v5/account/set-leverage", b.toString(), false);
        log.info("[okx] плечо {}x для {}", leverage, symbol);
    }

    /** Открытые позиции SWAP (режим net), объём — в базовой валюте. */
    @Override
    public void loadPositions(com.hft.store.PositionStore store) throws Exception {
        if (!swap) return;
        positionStore = store;
        java.util.Set<String> seen = new java.util.HashSet<>();
        for (JsonNode p : signed("GET", "/api/v5/account/positions?instType=SWAP", "", false).path("data")) {
            String inst = p.path("instId").asText();
            double pos = d(p, "pos") * ct(inst);
            if (pos == 0) continue;
            store.set(symbolOf(inst), pos, d(p, "avgPx"));
            seen.add(symbolOf(inst));
        }
        for (String s : store.snapshot().keySet()) if (!seen.contains(s)) store.set(s, 0, 0);
    }

    // ------------------------------------------------------------ правила и баланс

    /** Загрузить правила торговли символов (шаги объёма и цены, минимальная сумма). */
    @Override
    public void loadFilters(Iterable<String> symbols) throws Exception {
        JsonNode r = publicGet("/api/v5/public/instruments?instType=" + instType());
        int loaded = 0;
        for (JsonNode s : r.path("data")) {
            String inst = s.path("instId").asText();
            String sym = symbolOf(inst);
            boolean wanted = false;
            for (String w : symbols) if (w.equalsIgnoreCase(sym)) { wanted = true; break; }
            if (!wanted || swap && !"USDT".equals(s.path("settleCcy").asText("USDT"))) continue;
            // перпы: размеры в контрактах -> в базовую валюту (ctVal), чтобы бот везде считал в монетах
            double k = 1;
            if (swap) {
                k = d(s, "ctVal");
                OkxContracts.put(inst, k);
                lotSz.put(inst, d(s, "lotSz"));
            }
            double max = d(s, "maxLmtSz");
            filters.put(sym, new SymbolFilters.Filter(d(s, "minSz") * k, max > 0 ? max * k : Double.MAX_VALUE,
                    d(s, "lotSz") * k, 0, 0, d(s, "tickSz"), 1.0));
            loaded++;
        }
        if (loaded == 0) throw new IllegalStateException("OKX: правила торговли не найдены для " + symbols);
        log.info("[okx] правила загружены для {} символов (minNotional по умолчанию 1.0)", loaded);
    }

    /** Загрузить балансы. */
    @Override
    public void loadBalances(BalanceStore store) throws Exception {
        JsonNode r = signed("GET", "/api/v5/account/balance", "", false);
        for (JsonNode bal : r.path("data").path(0).path("details")) {
            double free = d(bal, swap && !bal.path("availEq").asText("").isEmpty() ? "availEq" : "availBal"), locked = d(bal, "frozenBal");
            if (free > 0 || locked > 0) store.set(bal.path("ccy").asText(), free, locked);
        }
        store.markSynced();
    }
}
