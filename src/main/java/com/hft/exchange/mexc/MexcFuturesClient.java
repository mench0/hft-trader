package com.hft.exchange.mexc;

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

import java.net.http.HttpRequest;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * MEXC Contract (USDT-перпы, contract.mexc.com), market=perp. Не проверялся на живой бирже.
 *
 * <ul>
 *   <li>Символ BTC_USDT; объём (vol) — контракты, контракт = contractSize монет.</li>
 *   <li>Подпись: заголовки ApiKey, Request-Time, Signature = hex(HMAC-SHA256(secret, apiKey + время + параметры)),
 *       где параметры — тело JSON для POST и отсортированная строка query для GET.</li>
 *   <li>Сторона ордера у MEXC задаёт и направление позиции: 1 — открыть лонг, 2 — закрыть шорт, 3 — открыть шорт,
 *       4 — закрыть лонг. Закрытие (reduceOnly) идёт сторонами 2/4, всё остальное открывает позицию.</li>
 *   <li>Маржа — кросс (openType=2), плечо уходит в каждом ордере.</li>
 *   <li>Ордера — REST: WebSocket-ордеров у MEXC Contract нет. Результаты — по WebSocket: после входа
 *       ({@code login}) сокет {@code wss://contract.mexc.com/edge} присылает {@code push.personal.order},
 *       {@code push.personal.position}, {@code push.personal.asset}.</li>
 * </ul>
 *
 * ВНИМАНИЕ: MEXC ограничивает размещение фьючерсных ордеров через API (доступ выдаётся отдельно). Без него ордера
 * отклоняются биржей; стакан, funding и бумажная торговля работают без ключей.
 */
public final class MexcFuturesClient extends SignedClient {

    /** Логгер. */
    private static final Logger log = LoggerFactory.getLogger(MexcFuturesClient.class);

    /** Наш символ по символу биржи. */
    private final Map<String, String> symbolOf = new HashMap<>();

    /**
     * @param config подключение (restUrl — contract.mexc.com)
     * @param credentials MEXC_API_KEY/_SECRET
     * @param filters правила символов
     */
    public MexcFuturesClient(ExchangeConfig config, Credentials credentials, SymbolFilters filters) {
        super(Exchange.MEXC.id(), config, credentials, filters);
        for (String s : config.symbols()) symbolOf.put(instrument(s), s.toUpperCase());
    }

    @Override public boolean isPerp() { return true; }

    /** BTCUSDT -> BTC_USDT. */
    private static String instrument(String symbol) { return ContractSizes.instrument(Exchange.MEXC, symbol); }

    /** Монет в контракте. */
    private double mult(String symbol) { return ContractSizes.get(Exchange.MEXC, baseUrl, instrument(symbol)); }

    // ------------------------------------------------------------ HTTP

    private JsonNode publicGet(String path) throws Exception {
        return exec(req(baseUrl + path).GET().build(), false).path("data");
    }

    /** Подписанный GET (query уже отсортирован) или POST с телом JSON. */
    private JsonNode signed(String method, String path, String query, String body, boolean order) throws Exception {
        credentials.require();
        String ts = String.valueOf(System.currentTimeMillis());
        String params = method.equals("GET") ? query : body;
        HttpRequest r = req(baseUrl + path + (query.isEmpty() ? "" : "?" + query))
                .header("ApiKey", credentials.apiKey())
                .header("Request-Time", ts)
                .header("Signature", Hmac.sha256Hex(credentials.apiSecret(), credentials.apiKey() + ts + params))
                .header("Content-Type", "application/json")
                .method(method, body.isEmpty() ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body))
                .build();
        return exec(r, order).path("data");
    }

    /** Ошибка MEXC: success=false или code ≠ 0; 510 — слишком часто. */
    @Override
    protected void checkError(int http, JsonNode body) {
        int code = body.path("code").asInt(0);
        boolean failed = body.has("success") && !body.path("success").asBoolean();
        if (http == 429 || code == 510) throw new ApiException(http, String.valueOf(code), body.path("message").asText("слишком часто"), true);
        if (http >= 400 || failed || code != 0)
            throw new ApiException(http, String.valueOf(code), body.path("message").asText(body.toString()), false);
    }

    // ------------------------------------------------------------ правила, баланс, позиции, плечо

    /** Правила: шаг = volUnit × contractSize, минимум = minVol × contractSize, цена — priceUnit. */
    @Override
    public void loadFilters(Iterable<String> symbols) throws Exception {
        Set<String> want = new HashSet<>();
        for (String s : symbols) want.add(instrument(s));
        int loaded = 0;
        for (JsonNode c : publicGet("/api/v1/contract/detail")) {
            String inst = c.path("symbol").asText();
            double m = d(c, "contractSize");
            ContractSizes.put(Exchange.MEXC, inst, m);
            if (!want.contains(inst)) continue;
            double unit = Math.max(1, d(c, "volUnit")), max = d(c, "maxVol");
            filters.put(symbolOf.getOrDefault(inst, inst.replace("_", "")), new SymbolFilters.Filter(Math.max(1, d(c, "minVol")) * m,
                    max > 0 ? max * m : Double.MAX_VALUE, unit * m, 0, 0, d(c, "priceUnit"), 1.0));
            loaded++;
        }
        if (loaded == 0) throw new IllegalStateException("MEXC futures: контракты не найдены для " + symbols);
        log.info("[mexc] фьючерсы: правила загружены для {} контрактов", loaded);
    }

    /** Баланс USDT: availableBalance — свободно, остальное — маржа и заморозка. */
    @Override
    public void loadBalances(BalanceStore store) throws Exception {
        JsonNode a = signed("GET", "/api/v1/private/account/asset/USDT", "", "", false);
        double equity = d(a, "equity"), free = d(a, "availableBalance");
        store.set("USDT", free, Math.max(0, equity - free));
        store.markSynced();
    }

    /** Плечо для лонга и шорта (кросс-маржа). */
    @Override
    public void setLeverage(String symbol, int leverage) throws Exception {
        for (int positionType = 1; positionType <= 2; positionType++) {
            String body = mapper.createObjectNode().put("symbol", instrument(symbol)).put("leverage", leverage)
                    .put("openType", 2).put("positionType", positionType).toString();
            signed("POST", "/api/v1/private/position/change_leverage", "", body, false);
        }
        log.info("[mexc] плечо {}x для {}", leverage, symbol);
    }

    /** Открытые позиции: holdVol — контракты, positionType 1 — лонг, 2 — шорт (сумма со знаком). */
    @Override
    public void loadPositions(PositionStore store) throws Exception {
        positionStore = store;
        Map<String, double[]> agg = new HashMap<>();                     // символ -> [объём со знаком, сумма для средней]
        for (JsonNode p : signed("GET", "/api/v1/private/position/open_positions", "", "", false)) {
            String inst = p.path("symbol").asText(), sym = symbolOf.get(inst);
            if (sym == null) continue;
            double q = d(p, "holdVol") * ContractSizes.get(Exchange.MEXC, baseUrl, inst) * (p.path("positionType").asInt(1) == 2 ? -1 : 1);
            double[] a = agg.computeIfAbsent(sym, k -> new double[2]);
            a[0] += q;
            a[1] += Math.abs(q) * d(p, "holdAvgPrice");
        }
        Set<String> seen = new HashSet<>();
        agg.forEach((sym, a) -> {
            if (a[0] == 0) return;
            store.set(sym, a[0], a[1] / Math.abs(a[0]));
            seen.add(sym);
        });
        for (String s : store.snapshot().keySet()) if (!seen.contains(s)) store.set(s, 0, 0);
    }

    // ------------------------------------------------------------ ордера

    @Override
    protected OrderResult placeRaw(Order o) throws Exception {
        if (o.qtyIsQuote()) throw new IllegalArgumentException("MEXC futures: ордер на сумму не поддерживается — задайте объём");
        long vol = (long) Math.floor(o.qty() / mult(o.symbol()) + 1e-9);
        if (vol <= 0) throw new IllegalArgumentException("MEXC futures: объём меньше одного контракта");
        int side = o.side() == Side.BUY ? (o.reduceOnly() ? 2 : 1) : (o.reduceOnly() ? 4 : 3);
        int type = o.type() == Type.MARKET ? 5 : switch (o.tif()) { case GTC -> 1; case IOC -> 3; case FOK -> 4; };
        ObjectNode b = mapper.createObjectNode()
                .put("symbol", instrument(o.symbol()))
                .put("vol", vol)
                .put("side", side)
                .put("type", type)
                .put("openType", 2)
                .put("leverage", Math.max(1, config.params().leverage()))
                .put("externalOid", o.clientId());
        if (o.type() == Type.LIMIT) b.put("price", o.price());
        JsonNode d = signed("POST", "/api/v1/private/order/submit", "", b.toString(), true);
        String id = d.isValueNode() ? d.asText() : d.path("orderId").asText("");
        if (id.isEmpty()) throw new ApiException(200, "NO_ID", "нет orderId в ответе: " + d, false);
        return new OrderResult(registerId(id), o.clientId(), o.symbol(), o.side(), "NEW", vol * mult(o.symbol()), 0, 0, 0);
    }

    /** Ордер: vol/dealVol — контракты; state 1–2 — в работе, 3 — исполнен, 4 — отменён, 5 — недействителен. */
    private OrderResult parse(JsonNode o, long id, String symbol) {
        double m = mult(symbol);
        double vol = d(o, "vol") * m, deal = d(o, "dealVol") * m;
        String status = switch (o.path("state").asInt(2)) {
            case 3 -> "FILLED";
            case 4 -> "CANCELED";
            case 5 -> "REJECTED";
            default -> deal > 0 ? "PARTIALLY_FILLED" : "NEW";
        };
        int side = o.path("side").asInt(1);
        return new OrderResult(id, o.path("externalOid").asText(""), symbol.toUpperCase(),
                side == 1 || side == 2 ? Side.BUY : Side.SELL, status, vol, deal, d(o, "dealAvgPrice"), 0);
    }

    /** Статус: итог из потока push.personal.order, иначе REST. */
    @Override
    public OrderResult orderStatus(String symbol, long orderId) throws Exception {
        OrderResult st = streamed.get(orderId);
        if (st != null && wsReady() && terminal(st.status())) return st;
        return parse(signed("GET", "/api/v1/private/order/get/" + venueId(orderId), "", "", false), orderId, symbol);
    }

    @Override
    public void cancelOrder(String symbol, long orderId) throws Exception {
        signed("POST", "/api/v1/private/order/cancel", "", "[" + venueId(orderId) + "]", true);
    }

    @Override
    public int cancelAll(String symbol) throws Exception {
        signed("POST", "/api/v1/private/order/cancel_all", "", mapper.createObjectNode().put("symbol", instrument(symbol)).toString(), true);
        log.info("[mexc] фьючерсы: отменены ордера по {}", symbol);
        return 0;                                                      // число отменённых MEXC не возвращает
    }

    // ------------------------------------------------------------ WebSocket: результаты ордеров, позиции, баланс

    /** Свой адрес сокета (тесты, прокси). */
    private volatile String privateWsUrl;
    /** Куда пишутся баланс и позиции из потока. */
    private volatile BalanceStore balanceStore;
    private volatile PositionStore positionStore;
    /** Баланс хотя бы раз пришёл по WS. */
    private volatile boolean accountSeen;
    /** Позиции по сторонам (у MEXC лонг и шорт раздельные): символ -> [лонг, цена лонга, шорт, цена шорта]. */
    private final Map<String, double[]> legs = new ConcurrentHashMap<>();

    /** Свой адрес сокета (тесты, прокси). */
    public void setPrivateWsUrl(String url) { this.privateWsUrl = url; }

    /** Поднять приватный сокет. Без ключей или при wsTrade=false — всё по REST. */
    @Override
    public void startStreams(BalanceStore store) {
        if (!credentials.isPresent() || !wsTradeAllowed() || wsChannel != null) return;
        balanceStore = store;
        WsRpcChannel ch = new WsRpcChannel(Exchange.MEXC.id(), new Private()).onEvent(this::onEvent);
        wsChannel = ch;
        ch.start();
    }

    @Override public boolean balancesStreamed() { return wsReady() && accountSeen; }

    /** Событие: ордер, позиция или баланс. */
    void onEvent(String text) throws Exception {
        JsonNode n = mapper.readTree(text);
        JsonNode d = n.path("data");
        switch (n.path("channel").asText()) {
            case "push.personal.order" -> {
                String inst = d.path("symbol").asText(), sym = symbolOf.get(inst);
                if (sym == null) return;
                long id = registerId(d.path("orderId").asText());
                streamed.put(id, parse(d, id, sym));
            }
            case "push.personal.position" -> {
                PositionStore ps = positionStore;
                String inst = d.path("symbol").asText(), sym = symbolOf.get(inst);
                if (ps == null || sym == null) return;
                double m = ContractSizes.get(Exchange.MEXC, baseUrl, inst);
                double vol = d.path("state").asInt(1) == 3 ? 0 : d(d, "holdVol") * m;   // state 3 — позиция закрыта
                double[] l = legs.computeIfAbsent(sym, k -> new double[4]);
                synchronized (l) {
                    if (d.path("positionType").asInt(1) == 2) { l[2] = vol; l[3] = d(d, "holdAvgPrice"); }
                    else { l[0] = vol; l[1] = d(d, "holdAvgPrice"); }
                    double net = l[0] - l[2];
                    double avg = l[0] + l[2] > 0 ? (l[0] * l[1] + l[2] * l[3]) / (l[0] + l[2]) : 0;
                    ps.set(sym, net, avg);
                }
            }
            case "push.personal.asset" -> {
                BalanceStore store = balanceStore;
                if (store == null) return;
                store.set(d.path("currency").asText("USDT"), d(d, "availableBalance"), d(d, "frozenBalance") + d(d, "positionMargin"));
                store.markSynced();
                accountSeen = true;
            }
            default -> { }
        }
    }

    /** Протокол сокета: login с подписью HMAC-SHA256(apiKey + время), личные потоки приходят без подписки. */
    private final class Private implements WsRpcChannel.Protocol {
        @Override public String url() { return privateWsUrl != null ? privateWsUrl : "wss://contract.mexc.com/edge"; }
        @Override public List<String> login() {
            String ts = String.valueOf(System.currentTimeMillis());
            ObjectNode m = mapper.createObjectNode().put("method", "login");
            m.putObject("param").put("apiKey", credentials.apiKey()).put("reqTime", ts)
                    .put("signature", Hmac.sha256Hex(credentials.apiSecret(), credentials.apiKey() + ts));
            return List.of(m.toString());
        }
        @Override public List<String> subscriptions() { return List.of(); }
        @Override public String ping() { return "{\"method\":\"ping\"}"; }
        @Override public long pingIntervalMs() { return 15_000; }
        @Override public Msg parse(String text) throws Exception {
            JsonNode n = mapper.readTree(text);
            String ch = n.path("channel").asText("");
            if (ch.equals("rs.login")) {
                if ("success".equals(n.path("data").asText())) return Msg.loginOk();
                throw new IllegalStateException("MEXC futures login: " + n.path("data").asText());
            }
            if (ch.equals("rs.error")) throw new IllegalStateException("MEXC futures WS: " + n.path("data").asText());
            if (ch.startsWith("push.personal.")) return Msg.event(text);
            return Msg.ignore();                                          // pong и прочие служебные
        }
    }
}
