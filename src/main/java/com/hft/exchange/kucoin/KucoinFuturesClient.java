package com.hft.exchange.kucoin;

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
import com.hft.rest.SignedCexClient;
import com.hft.store.BalanceStore;
import com.hft.store.PositionStore;
import com.hft.store.SymbolFilters;
import com.hft.util.Hmac;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hft.rest.WsRpcChannel;
import com.hft.rest.WsRpcChannel.Msg;
import com.hft.util.BoundedMap;

import java.net.http.HttpRequest;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * KuCoin Futures (USDT-маржинальные перпы, api-futures.kucoin.com), market=perp. Не проверялся на живой бирже.
 *
 * <ul>
 *   <li>Символ XBTUSDTM (биткоин у фьючерсов KuCoin — XBT); объём — целое число лотов, лот = multiplier монет.</li>
 *   <li>Подпись — как у спота KuCoin (KC-API-*, ключ версии 2), нужен KUCOIN_PASSPHRASE.</li>
 *   <li>Плечо передаётся в каждом ордере (leverage) и выставляется для кросс-маржи при старте.</li>
 *   <li>Ордера и отмены — Pro WS API {@code wss://wsapi.kucoin.com/v1/private}, команды {@code futures.order} и
 *       {@code futures.cancel} (тот же вход, что у спота: приветствие и подпись); REST — запасной канал.</li>
 *   <li>Исполнения, позиции и баланс — приватный поток фьючерсов (bullet-private на api-futures):
 *       {@code /contractMarket/tradeOrders}, {@code /contract/positionAll}, {@code /contractAccount/wallet}.</li>
 *   <li>WS-запрос ушёл, ответа нет — ордер ищется по REST по clientOid, вслепую не повторяется.</li>
 * </ul>
 */
public final class KucoinFuturesClient extends SignedCexClient {

    /** Логгер. */
    private static final Logger log = LoggerFactory.getLogger(KucoinFuturesClient.class);

    /** Фраза API-ключа. */
    private final String passphrase;
    /** Наш символ по символу биржи (XBTUSDTM -> BTCUSDT). */
    private final Map<String, String> symbolOf = new HashMap<>();

    /**
     * @param config подключение (restUrl — api-futures.kucoin.com)
     * @param credentials KUCOIN_API_KEY/_SECRET
     * @param filters правила символов
     */
    public KucoinFuturesClient(ExchangeConfig config, Credentials credentials, SymbolFilters filters) {
        super(Exchange.KUCOIN.id(), config, credentials, filters);
        String p = com.hft.config.Env.get("KUCOIN_PASSPHRASE");
        if (credentials.isPresent() && (p == null || p.isBlank()))
            throw new IllegalStateException("KuCoin требует KUCOIN_PASSPHRASE (фраза, заданная при создании API-ключа)");
        this.passphrase = p == null ? "" : p.trim();
        for (String s : config.symbols()) symbolOf.put(instrument(s), s.toUpperCase());
    }

    @Override public boolean isPerp() { return true; }

    /** BTCUSDT -> XBTUSDTM. */
    private static String instrument(String symbol) { return ContractSizes.instrument(Exchange.KUCOIN, symbol); }

    /** Монет в одном лоте. */
    private double mult(String symbol) { return ContractSizes.get(Exchange.KUCOIN, baseUrl, instrument(symbol)); }

    // ------------------------------------------------------------ HTTP

    private JsonNode publicGet(String pathAndQuery) throws Exception {
        return exec(req(baseUrl + pathAndQuery).GET().build(), false);
    }

    /** Подписанный запрос (как у спота KuCoin). */
    private JsonNode signed(String method, String pathAndQuery, String body, boolean order) throws Exception {
        credentials.require();
        String ts = String.valueOf(System.currentTimeMillis());
        HttpRequest r = req(baseUrl + pathAndQuery)
                .header("KC-API-KEY", credentials.apiKey())
                .header("KC-API-SIGN", Hmac.sha256Base64(credentials.apiSecret(), ts + method + pathAndQuery + body))
                .header("KC-API-TIMESTAMP", ts)
                .header("KC-API-PASSPHRASE", Hmac.sha256Base64(credentials.apiSecret(), passphrase))
                .header("KC-API-KEY-VERSION", "2")
                .header("Content-Type", "application/json")
                .method(method, body.isEmpty() ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body))
                .build();
        return exec(r, order).path("data");
    }

    /** Ошибка KuCoin: code ≠ 200000. */
    @Override
    protected void checkError(int http, JsonNode body) {
        String code = body.path("code").asText("");
        if (http == 429 || code.equals("429000")) throw new ApiException(http, "429000", body.path("msg").asText("слишком часто"), true);
        if (http >= 400 || !code.isEmpty() && !code.equals("200000"))
            throw new ApiException(http, code.isEmpty() ? "?" : code, body.path("msg").asText(body.toString()), false);
    }

    // ------------------------------------------------------------ правила, баланс, позиции, плечо

    /** Правила: шаг = lotSize × multiplier монет, цена — tickSize. */
    @Override
    public void loadFilters(Iterable<String> symbols) throws Exception {
        Set<String> want = new HashSet<>();
        for (String s : symbols) want.add(instrument(s));
        int loaded = 0;
        for (JsonNode c : publicGet("/api/v1/contracts/active").path("data")) {
            String inst = c.path("symbol").asText();
            double m = d(c, "multiplier");
            ContractSizes.put(Exchange.KUCOIN, inst, m);
            if (!want.contains(inst)) continue;
            double lot = Math.max(1, d(c, "lotSize")), max = d(c, "maxOrderQty");
            filters.put(symbolOf.getOrDefault(inst, inst), new SymbolFilters.Filter(lot * m, max > 0 ? max * m : Double.MAX_VALUE,
                    lot * m, 0, 0, d(c, "tickSize"), 1.0));
            loaded++;
        }
        if (loaded == 0) throw new IllegalStateException("KuCoin futures: контракты не найдены для " + symbols);
        log.info("[kucoin] фьючерсы: правила загружены для {} контрактов", loaded);
    }

    /** Баланс USDT фьючерсного счёта: availableBalance — свободно, остальное — маржа. */
    @Override
    public void loadBalances(BalanceStore store) throws Exception {
        JsonNode a = signed("GET", "/api/v1/account-overview?currency=USDT", "", false);
        double equity = d(a, "accountEquity"), free = d(a, "availableBalance");
        store.set("USDT", free, Math.max(0, equity - free));
        store.markSynced();
    }

    /** Плечо для кросс-маржи; неудача не критична — плечо всё равно уходит в каждом ордере. */
    @Override
    public void setLeverage(String symbol, int leverage) throws Exception {
        String body = mapper.createObjectNode().put("symbol", instrument(symbol)).put("leverage", Integer.toString(leverage)).toString();
        signed("POST", "/api/v2/changeCrossUserLeverage", body, false);
        log.info("[kucoin] плечо {}x для {}", leverage, symbol);
    }

    /** Открытые позиции: currentQty — лоты со знаком. */
    @Override
    public void loadPositions(PositionStore store) throws Exception {
        positionStore = store;
        Set<String> seen = new HashSet<>();
        for (JsonNode p : signed("GET", "/api/v1/positions", "", false)) {
            double lots = d(p, "currentQty");
            String inst = p.path("symbol").asText(), sym = symbolOf.get(inst);
            if (lots == 0 || sym == null) continue;
            store.set(sym, lots * ContractSizes.get(Exchange.KUCOIN, baseUrl, inst), d(p, "avgEntryPrice"));
            seen.add(sym);
        }
        for (String s : store.snapshot().keySet()) if (!seen.contains(s)) store.set(s, 0, 0);
    }

    // ------------------------------------------------------------ ордера

    @Override
    protected OrderResult placeRaw(Order o) throws Exception {
        if (o.qtyIsQuote()) throw new IllegalArgumentException("KuCoin futures: ордер на сумму не поддерживается — задайте объём");
        long lots = (long) Math.floor(o.qty() / mult(o.symbol()) + 1e-9);
        if (lots <= 0) throw new IllegalArgumentException("KuCoin futures: объём меньше одного лота");
        ObjectNode b = mapper.createObjectNode()
                .put("clientOid", o.clientId())
                .put("side", o.side() == Side.BUY ? "buy" : "sell")
                .put("symbol", instrument(o.symbol()))
                .put("size", lots)
                .put("leverage", Math.max(1, config.params().leverage()))
                .put("marginMode", "CROSS");
        if (o.type() == Type.MARKET) b.put("type", "market");
        else b.put("type", "limit").put("price", plain(o.price(), filters.priceScale(o.symbol())))
              .put("timeInForce", o.tif() == com.hft.model.OrderEnums.TimeInForce.GTC ? "GTC" : "IOC");   // FOK у KuCoin нет
        if (o.reduceOnly()) b.put("reduceOnly", true);
        JsonNode d;
        try {
            d = wsOp("futures.order", b);                                 // сначала WebSocket
        } catch (WsRpcChannel.WsUnknownOutcomeException e) {
            wsFallbacks.incrementAndGet();
            log.warn("[kucoin] фьючерсы: исход ордера {} неизвестен ({}), проверяю по REST", o.clientId(), e.getMessage());
            JsonNode found = signed("GET", "/api/v1/orders/byClientOid?clientOid=" + o.clientId(), "", false);
            String vid = found.path("id").asText("");
            if (vid.isEmpty()) throw new ApiException(200, "WS_UNKNOWN", "ордер " + o.clientId() + " не найден после обрыва WS", false);
            return parse(found, registerId(vid), o.symbol());
        }
        if (d == null) d = signed("POST", "/api/v1/orders", b.toString(), true);   // REST — если WS не готов
        String id = d.path("orderId").asText("");
        if (id.isEmpty()) throw new ApiException(200, "NO_ID", "нет orderId в ответе: " + d, false);
        return new OrderResult(registerId(id), o.clientId(), o.symbol(), o.side(), "NEW", lots * mult(o.symbol()), 0, 0, 0);
    }

    /** Ордер: size/filledSize — лоты, filledValue — сумма в USDT. */
    private OrderResult parse(JsonNode o, long id, String symbol) {
        double m = mult(symbol);
        double filled = d(o, "filledSize") * m, size = d(o, "size") * m;
        double value = d(o, "filledValue");
        String status = o.path("isActive").asBoolean(true)
                ? (filled > 0 ? "PARTIALLY_FILLED" : "NEW")
                : (filled >= size - 1e-12 && filled > 0 ? "FILLED" : "CANCELED");
        return new OrderResult(id, o.path("clientOid").asText(""), symbol.toUpperCase(),
                "sell".equals(o.path("side").asText()) ? Side.SELL : Side.BUY, status, size, filled, filled > 0 ? value / filled : 0, 0);
    }

    /** Статус: итог из приватного потока, иначе REST. */
    @Override
    public OrderResult orderStatus(String symbol, long orderId) throws Exception {
        OrderResult st = streamed.get(orderId);
        if (st != null && privateReady() && terminal(st.status())) return st;
        return parse(signed("GET", "/api/v1/orders/" + venueId(orderId), "", false), orderId, symbol);
    }

    /** Отмена: WS API futures.cancel, иначе REST (отмена идемпотентна). */
    @Override
    public void cancelOrder(String symbol, long orderId) throws Exception {
        JsonNode r = null;
        try { r = wsOp("futures.cancel", mapper.createObjectNode().put("orderId", venueId(orderId)).put("symbol", instrument(symbol))); }
        catch (WsRpcChannel.WsUnknownOutcomeException e) { wsFallbacks.incrementAndGet(); }
        if (r == null) signed("DELETE", "/api/v1/orders/" + venueId(orderId), "", true);
    }

    @Override
    public int cancelAll(String symbol) throws Exception {
        JsonNode d = signed("DELETE", "/api/v1/orders?symbol=" + instrument(symbol), "", true);
        int n = d.path("cancelledOrderIds").size();
        log.info("[kucoin] фьючерсы: отменено {} ордеров по {}", n, symbol);
        return n;
    }

    // ------------------------------------------------------------ WebSocket

    /** Торговый протокол Pro WS API (общий со спотом). */
    private volatile KucoinWs ws;
    /** Приватный поток фьючерсов; торговый сокет — wsChannel базового класса. */
    private volatile WsRpcChannel privateChannel;
    /** Свои адреса (тесты, прокси). */
    private volatile String tradeWsUrl, privateWsUrl;
    /** Куда пишутся баланс и позиции из потока. */
    private volatile BalanceStore balanceStore;
    private volatile PositionStore positionStore;
    /** Баланс хотя бы раз пришёл по WS. */
    private volatile boolean balanceSeen;
    /** Исполненная сумма по ордеру из событий match — для средней цены. */
    private final Map<String, double[]> fills = BoundedMap.create(MAX_TRACKED_ORDERS);
    /** Номера подписок и пингов. */
    private final AtomicLong seq = new AtomicLong();

    /** Свои адреса сокетов (тесты, прокси). */
    public void setWsUrls(String trade, String priv) { this.tradeWsUrl = trade; this.privateWsUrl = priv; }

    /** Торговый сокет и приватный поток. Без ключей или при wsTrade=false — всё по REST. */
    @Override
    public void startStreams(BalanceStore store) {
        if (!credentials.isPresent() || !wsTradeAllowed() || wsChannel != null) return;
        balanceStore = store;
        KucoinWs w = new KucoinWs(tradeWsUrl != null ? tradeWsUrl : "wss://wsapi.kucoin.com/v1/private", this::bulletPrivate,
                credentials.apiKey(), credentials.apiSecret(), passphrase, streamed, this::registerId);
        ws = w;
        privateChannel = new WsRpcChannel(Exchange.KUCOIN.id(), new Private()).onEvent(this::onEvent);
        wsChannel = new WsRpcChannel(Exchange.KUCOIN.id(), w.trade);
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

    @Override public boolean balancesStreamed() { return privateReady() && balanceSeen; }

    /** Метрики запросов и обоих сокетов. */
    @Override
    public Map<String, Object> stats() {
        var m = super.stats();
        WsRpcChannel p = privateChannel;
        if (p != null) m.put("wsPrivate", p.stats());
        return m;
    }

    /** Приватный поток готов. */
    private boolean privateReady() { WsRpcChannel c = privateChannel; return c != null && c.isReady(); }

    /** Адрес приватного потока фьючерсов: POST /api/v1/bullet-private на api-futures. */
    private String bulletPrivate() throws Exception {
        if (privateWsUrl != null) return privateWsUrl;
        JsonNode d = signed("POST", "/api/v1/bullet-private", "", false);
        JsonNode server = d.path("instanceServers").path(0);
        return server.path("endpoint").asText() + "?token=" + d.path("token").asText() + "&connectId=hft" + System.nanoTime();
    }

    /** Запрос по торговому сокету. null — сокет не готов, идти в REST. */
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

    /**
     * Событие потока: /contractMarket/tradeOrders (ордер: type open/match/filled/canceled, size и filledSize — лоты),
     * /contract/positionAll (currentQty — лоты со знаком), /contractAccount/wallet (availableBalance, holdBalance).
     */
    void onEvent(String text) throws Exception {
        JsonNode n = mapper.readTree(text);
        String topic = n.path("topic").asText();
        JsonNode d = n.path("data");
        if (topic.startsWith("/contractMarket/tradeOrders")) {
            String inst = d.path("symbol").asText(), sym = symbolOf.get(inst);
            if (sym == null) return;
            double m = ContractSizes.get(Exchange.KUCOIN, baseUrl, inst);
            String venueId = d.path("orderId").asText();
            long id = registerId(venueId);
            double[] f = fills.computeIfAbsent(venueId, k -> new double[2]);   // [монеты, сумма]
            if ("match".equals(d.path("type").asText())) {
                double q = d.path("matchSize").asDouble(0) * m, px = d.path("matchPrice").asDouble(0);
                synchronized (f) { f[0] += q; f[1] += q * px; }
            }
            double exec = d.path("filledSize").asDouble(0) * m, size = d.path("size").asDouble(0) * m, avg;
            synchronized (f) { avg = f[0] > 0 ? f[1] / f[0] : 0; }
            String type = d.path("type").asText(), status = d.path("status").asText();
            String st = switch (type) {
                case "filled" -> "FILLED";
                case "canceled" -> "CANCELED";
                default -> "done".equals(status) ? (exec >= size - 1e-12 && exec > 0 ? "FILLED" : "CANCELED")
                        : exec > 0 ? "PARTIALLY_FILLED" : "NEW";
            };
            streamed.put(id, new OrderResult(id, d.path("clientOid").asText(""), sym,
                    "sell".equals(d.path("side").asText()) ? Side.SELL : Side.BUY, st, size, exec, avg, 0));
        } else if (topic.startsWith("/contract/position")) {
            PositionStore ps = positionStore;
            String inst = d.path("symbol").asText(), sym = symbolOf.get(inst);
            if (ps == null || sym == null || !d.has("currentQty")) return;
            ps.set(sym, d.path("currentQty").asDouble(0) * ContractSizes.get(Exchange.KUCOIN, baseUrl, inst), d.path("avgEntryPrice").asDouble(0));
        } else if (topic.startsWith("/contractAccount/wallet")) {
            BalanceStore store = balanceStore;
            if (store == null || !d.has("availableBalance")) return;
            String ccy = d.path("currency").asText("USDT");
            store.set(ccy, d.path("availableBalance").asDouble(0), d.path("holdBalance").asDouble(store.locked(ccy)));
            store.markSynced();
            balanceSeen = true;
        }
    }

    /** Приватный поток фьючерсов: welcome — готов; подписки на ордера, позиции, кошелёк. */
    private final class Private implements WsRpcChannel.Protocol {
        @Override public String url() {
            try { return bulletPrivate(); }
            catch (Exception e) { throw new IllegalStateException("KuCoin futures bullet-private: " + e.getMessage(), e); }
        }
        @Override public List<String> login() { return List.of(); }
        @Override public boolean serverInitiatedLogin() { return true; }
        @Override public List<String> subscriptions() {
            return List.of(sub("/contractMarket/tradeOrders"), sub("/contract/positionAll"), sub("/contractAccount/wallet"));
        }
        private String sub(String topic) {
            return mapper.createObjectNode().put("id", "fs" + seq.incrementAndGet()).put("type", "subscribe")
                    .put("topic", topic).put("privateChannel", true).put("response", true).toString();
        }
        @Override public String ping() { return "{\"id\":\"fp" + seq.incrementAndGet() + "\",\"type\":\"ping\"}"; }
        @Override public long pingIntervalMs() { return 15_000; }
        @Override public Msg parse(String text) throws Exception {
            JsonNode n = mapper.readTree(text);
            return switch (n.path("type").asText()) {
                case "welcome" -> Msg.loginOk();
                case "message" -> Msg.event(text);
                case "error" -> throw new IllegalStateException("KuCoin futures private: " + n.path("code").asText() + " " + n.path("data").asText());
                default -> Msg.ignore();                                    // ack, pong
            };
        }
    }
}
