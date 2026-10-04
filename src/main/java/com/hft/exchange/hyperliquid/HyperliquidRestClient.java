package com.hft.exchange.hyperliquid;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hft.config.Credentials;
import com.hft.config.ExchangeConfig;
import com.hft.crypto.EvmCrypto;
import com.hft.crypto.Web3jCrypto;
import com.hft.model.OrderEnums.Side;
import com.hft.model.OrderEnums.Type;
import com.hft.model.OrderResult;
import com.hft.rest.ApiException;
import com.hft.rest.SignedCexClient;
import com.hft.rest.WsRpcChannel;
import com.hft.rest.WsRpcChannel.Msg;
import com.hft.store.BalanceStore;
import com.hft.store.SymbolFilters;
import com.hft.util.MsgPack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hft.util.BoundedMap;
import java.io.ByteArrayOutputStream;
import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Hyperliquid, бессрочные контракты. Не проверялся на живой бирже и не компилировался
 * вместе с web3j (Maven был недоступен), поэтому подпись нужно сверить с официальным SDK
 * на тестовой сети, прежде чем пускать деньги.
 *
 * Учётные данные (переменные окружения):
 *   HYPERLIQUID_API_KEY    — адрес ОСНОВНОГО аккаунта (0x…), на котором лежат средства
 *   HYPERLIQUID_API_SECRET — приватный ключ API-кошелька (agent), созданного в интерфейсе Hyperliquid;
 *                            у agent нет права вывода средств — не используйте ключ основного кошелька.
 *
 * Как устроена подпись действия ("L1 action"):
 *   1. действие кодируется в MessagePack (порядок ключей важен);
 *   2. к байтам добавляются nonce (8 байт, big-endian) и 0x00 (нет vault) -> keccak256 = connectionId;
 *   3. "фантомный агент" {source: "a" (mainnet) | "b" (testnet), connectionId} подписывается по EIP-712
 *      с доменом {name: "Exchange", version: "1", chainId: 1337, verifyingContract: 0x0};
 *   4. в запрос кладутся action, nonce и подпись {r, s, v}.
 *
 * Символ в системе — BTCUSDC: монета BTC, котируемая валюта USDC (маржа). Цены и размеры форматируются
 * по правилам биржи: размер — szDecimals монеты, цена — до 5 значащих цифр и не более 6 - szDecimals знаков.
 * Рыночный ордер — это IOC-лимитка с запасом цены 5% от середины.
 */
public final class HyperliquidRestClient extends SignedCexClient {

    private static final Logger log = LoggerFactory.getLogger(HyperliquidRestClient.class);
    private static final double MARKET_SLIPPAGE = 0.05;

    private final EvmCrypto crypto;
    private final String account;
    private final String source;
    private final AtomicLong nonce = new AtomicLong(System.currentTimeMillis());
    private final Map<String, Integer> assetIndex = new ConcurrentHashMap<>();
    private final Map<String, Integer> szDecimals = new ConcurrentHashMap<>();

    public HyperliquidRestClient(ExchangeConfig config, Credentials credentials, SymbolFilters filters) {
        this(config, credentials, filters, credentials.isPresent() ? new Web3jCrypto(credentials.apiSecret()) : null);
    }

    public HyperliquidRestClient(ExchangeConfig config, Credentials credentials, SymbolFilters filters, EvmCrypto crypto) {
        super("hyperliquid", config, credentials, filters);
        this.crypto = crypto;
        this.account = credentials.isPresent() ? credentials.apiKey().toLowerCase() : "";
        this.source = config.testnet() ? "b" : "a";
    }

    private static String coin(String symbol) { return BalanceStore.baseAsset(symbol); }

    // ------------------------------------------------------------ подпись

    /** Хеш EIP-712 для фантомного агента. Вынесено отдельно для тестов. */
    byte[] agentDigest(byte[] connectionId) {
        byte[] domainType = crypto.keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)".getBytes(StandardCharsets.UTF_8));
        byte[] agentType = crypto.keccak256("Agent(string source,bytes32 connectionId)".getBytes(StandardCharsets.UTF_8));
        byte[] domainSep = crypto.keccak256(concat(domainType,
                crypto.keccak256("Exchange".getBytes(StandardCharsets.UTF_8)),
                crypto.keccak256("1".getBytes(StandardCharsets.UTF_8)),
                uint256(1337), new byte[32]));
        byte[] structHash = crypto.keccak256(concat(agentType,
                crypto.keccak256(source.getBytes(StandardCharsets.UTF_8)), connectionId));
        return crypto.keccak256(concat(new byte[]{0x19, 0x01}, domainSep, structHash));
    }

    byte[] connectionId(Map<String, Object> action, long nonceValue) {
        byte[] packed = MsgPack.pack(action);
        ByteBuffer buf = ByteBuffer.allocate(packed.length + 8 + 1);
        buf.put(packed).putLong(nonceValue).put((byte) 0);
        return crypto.keccak256(buf.array());
    }

    /** Тело запроса /exchange для действия. */
    Map<String, Object> signedBody(Map<String, Object> action, long nonceValue) {
        EvmCrypto.Signature sig = crypto.sign(agentDigest(connectionId(action, nonceValue)));
        Map<String, Object> signature = new LinkedHashMap<>();
        signature.put("r", "0x" + HexFormat.of().formatHex(sig.r()));
        signature.put("s", "0x" + HexFormat.of().formatHex(sig.s()));
        signature.put("v", sig.v());
        Map<String, Object> body = new LinkedHashMap<>();
        body.put("action", action);
        body.put("nonce", nonceValue);
        body.put("signature", signature);
        body.put("vaultAddress", null);
        return body;
    }

    private static byte[] uint256(long v) {
        byte[] out = new byte[32];
        for (int i = 0; i < 8; i++) out[31 - i] = (byte) (v >> (8 * i));
        return out;
    }

    private static byte[] concat(byte[]... parts) {
        ByteArrayOutputStream o = new ByteArrayOutputStream();
        for (byte[] p : parts) o.writeBytes(p);
        return o.toByteArray();
    }

    // ------------------------------------------------------------ HTTP

    private JsonNode info(Map<String, Object> body, boolean order) throws Exception {
        try {
            JsonNode ws = wsPost("info", body, order);
            if (ws != null) {
                JsonNode data = normalizeInfo(String.valueOf(body.get("type")), ws);
                if (data != null) return data;
                wsFallbacks.incrementAndGet();                       // неожиданная форма ответа — читаем по REST
            }
        } catch (WsRpcChannel.WsUnknownOutcomeException e) {
            wsFallbacks.incrementAndGet();                           // чтение идемпотентно
        }
        return exec(req(baseUrl + "/info").header("Content-Type", "application/json")
                .POST(java.net.http.HttpRequest.BodyPublishers.ofString(mapper.writeValueAsString(body))).build(), order);
    }

    private JsonNode infoOf(String type, Object... kv) throws Exception {
        Map<String, Object> b = new LinkedHashMap<>();
        b.put("type", type);
        for (int i = 0; i < kv.length; i += 2) b.put((String) kv[i], kv[i + 1]);
        return info(b, false);
    }

    private JsonNode exchange(Map<String, Object> action) throws Exception {
        if (crypto == null) credentials.require();
        long n = nonce.incrementAndGet();
        Map<String, Object> signed = signedBody(action, n);
        String body = mapper.writeValueAsString(signed);
        try {
            JsonNode ws = wsPost("action", signed, true);            // сначала WebSocket
            if (ws != null) return ws;
        } catch (WsRpcChannel.WsUnknownOutcomeException e) {
            // Исход неизвестен. Тот же подписанный запрос с тем же nonce биржа второй раз не исполнит,
            // поэтому повторяем его по REST: либо выполнится, либо отвергнется как дубликат.
            wsFallbacks.incrementAndGet();
            log.warn("[hyperliquid] исход WS-запроса неизвестен ({}), повторяю тот же nonce по REST", e.getMessage());
            try {
                return exec(req(baseUrl + "/exchange").header("Content-Type", "application/json")
                        .POST(java.net.http.HttpRequest.BodyPublishers.ofString(body)).build(), true);
            } catch (ApiException dup) {
                if (String.valueOf(dup.getMessage()).toLowerCase().contains("nonce"))
                    throw new ApiException(200, "WS_UNKNOWN", "запрос мог быть исполнен до обрыва WS — сверьте позиции и ордера: " + dup.getMessage(), false);
                throw dup;
            }
        }
        return exec(req(baseUrl + "/exchange").header("Content-Type", "application/json")
                .POST(java.net.http.HttpRequest.BodyPublishers.ofString(body)).build(), true);
    }

    @Override
    protected void checkError(int http, JsonNode body) {
        if (http == 429) throw new ApiException(http, "429", "слишком часто", true);
        if (http >= 400) throw new ApiException(http, "?", body.toString(), false);
        if ("err".equals(body.path("status").asText())) {
            String msg = body.path("response").asText(body.toString());
            throw new ApiException(http, "ERR", msg, msg.toLowerCase().contains("rate limit") || msg.toLowerCase().contains("too many"));
        }
        for (JsonNode st : body.path("response").path("data").path("statuses")) {
            if (st.has("error")) throw new ApiException(http, "ORDER", st.path("error").asText(), false);
        }
    }

    // ------------------------------------------------------------ форматирование

    /** Цена по правилам Hyperliquid: ≤5 значащих цифр и ≤ (6 - szDecimals) знаков после запятой; целые не режем. */
    public static String formatPrice(double px, int szDec) {
        BigDecimal exact = BigDecimal.valueOf(px);
        BigDecimal bd = exact.round(new MathContext(5, RoundingMode.HALF_UP));
        if (exact.compareTo(BigDecimal.valueOf(100_000)) >= 0) bd = exact.setScale(0, RoundingMode.HALF_UP);
        int maxScale = Math.max(0, 6 - szDec);
        if (bd.scale() > maxScale) bd = bd.setScale(maxScale, RoundingMode.HALF_UP);
        return bd.stripTrailingZeros().toPlainString();
    }

    private int asset(String symbol) {
        Integer i = assetIndex.get(coin(symbol));
        if (i == null) throw new IllegalStateException("Hyperliquid: неизвестная монета " + coin(symbol) + " (вызовите loadFilters)");
        return i;
    }

    private int szDec(String symbol) { return szDecimals.getOrDefault(coin(symbol), 4); }

    private double mid(String symbol) throws Exception {
        double m = d(infoOf("allMids"), coin(symbol));
        if (m <= 0) throw new IllegalStateException("Hyperliquid: нет средней цены для " + coin(symbol));
        return m;
    }

    // ------------------------------------------------------------ ордера

    @Override
    protected OrderResult placeRaw(Order o) throws Exception {
        int sd = szDec(o.symbol());
        boolean buy = o.side() == Side.BUY;
        double size = o.qty(), px = o.price();
        String tif = "Gtc";
        if (o.type() == Type.MARKET || o.qtyIsQuote()) {
            double m = mid(o.symbol());
            if (o.qtyIsQuote()) size = o.qty() / m;
            if (o.type() == Type.MARKET) { px = m * (buy ? 1 + MARKET_SLIPPAGE : 1 - MARKET_SLIPPAGE); tif = "Ioc"; }
        }
        if (o.type() == Type.LIMIT) {
            tif = switch (o.tif()) { case GTC -> "Gtc"; case IOC, FOK -> "Ioc"; };   // FOK на бирже нет: IOC
        }
        String szStr = com.hft.util.Numbers.plain(size, sd);
        if (new BigDecimal(szStr).signum() <= 0) throw new IllegalArgumentException("Hyperliquid: размер округлился до нуля");

        Map<String, Object> order = new LinkedHashMap<>();
        order.put("a", asset(o.symbol()));
        order.put("b", buy);
        order.put("p", formatPrice(px, sd));
        order.put("s", szStr);
        order.put("r", false);
        order.put("t", Map.of("limit", Map.of("tif", tif)));
        List<Object> orders = new ArrayList<>();
        orders.add(order);
        Map<String, Object> action = new LinkedHashMap<>();
        action.put("type", "order");
        action.put("orders", orders);
        action.put("grouping", "na");

        JsonNode r = exchange(action);
        JsonNode st = r.path("response").path("data").path("statuses").path(0);
        double req = new BigDecimal(szStr).doubleValue();
        if (st.has("filled")) {
            JsonNode f = st.get("filled");
            double exec = d(f, "totalSz");
            String status = exec >= req * 0.9999 ? "FILLED" : "PARTIALLY_FILLED";
            return new OrderResult(registerId(f.path("oid").asText()), o.clientId(), o.symbol(), o.side(), status,
                    req, exec, d(f, "avgPx"), 0);
        }
        if (st.has("resting")) {
            return new OrderResult(registerId(st.get("resting").path("oid").asText()), o.clientId(), o.symbol(), o.side(), "NEW",
                    req, 0, 0, 0);
        }
        throw new ApiException(200, "NO_STATUS", "неожиданный ответ на ордер: " + r, false);
    }

    @Override
    public OrderResult orderStatus(String symbol, long orderId) throws Exception {
        OrderResult cached = streamed.get(orderId);
        if (cached != null && wsReady() && !"NEW".equals(cached.status()) && !"PARTIALLY_FILLED".equals(cached.status())) return cached;
        JsonNode r = infoOf("orderStatus", "user", account, "oid", Long.parseLong(venueId(orderId)));
        JsonNode wrap = r.path("order");
        if (wrap.isMissingNode()) throw new IllegalStateException("Ордер не найден");
        JsonNode o = wrap.path("order");
        double orig = d(o, "origSz"), left = d(o, "sz");
        String st = wrap.path("status").asText();
        String status;
        double exec = Math.max(0, orig - left);
        if ("open".equals(st)) status = exec > 0 ? "PARTIALLY_FILLED" : "NEW";
        else if ("filled".equals(st)) { status = "FILLED"; exec = orig; }
        else if ("rejected".equals(st)) status = "REJECTED";
        else status = exec > 0 ? "PARTIALLY_FILLED" : "CANCELED";    // canceled, marginCanceled, …
        Side side = "B".equals(o.path("side").asText()) ? Side.BUY : Side.SELL;
        return new OrderResult(orderId, "", symbol.toUpperCase(), side, status, orig, exec, d(o, "limitPx"), 0);
    }

    @Override
    public void cancelOrder(String symbol, long orderId) throws Exception {
        cancel(List.of(cancelItem(asset(symbol), Long.parseLong(venueId(orderId)))));
        log.info("[hyperliquid] ордер {} по {} отменён", orderId, symbol);
    }

    @Override
    public int cancelAll(String symbol) throws Exception {
        JsonNode open = infoOf("openOrders", "user", account);
        List<Object> items = new ArrayList<>();
        for (JsonNode o : open) {
            if (coin(symbol).equals(o.path("coin").asText())) items.add(cancelItem(asset(symbol), o.path("oid").asLong()));
        }
        if (!items.isEmpty()) cancel(items);
        log.info("[hyperliquid] отменено {} ордеров по {}", items.size(), symbol);
        return items.size();
    }

    private static Map<String, Object> cancelItem(int asset, long oid) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("a", asset);
        m.put("o", oid);
        return m;
    }

    private void cancel(List<?> items) throws Exception {
        Map<String, Object> action = new LinkedHashMap<>();
        action.put("type", "cancel");
        action.put("cancels", items);
        exchange(action);
    }

    // ------------------------------------------------------------ WebSocket: post, orderUpdates, userFills

    private volatile String wsUrlOverride;
    private final AtomicLong wsSeq = new AtomicLong();
    private record Upd(String symbol, Side side, double orig, double left, String status, double limitPx) {}
    private final Map<Long, Upd> updates = BoundedMap.create(MAX_TRACKED_ORDERS);
    private final Map<Long, double[]> fills = BoundedMap.create(MAX_TRACKED_ORDERS);   // oid -> {объём, объём*цена}

    public void setWsUrl(String url) { this.wsUrlOverride = url; }

    @Override
    public void startStreams(BalanceStore store) {
        if (crypto == null || !wsTradeAllowed() || wsChannel != null) return;
        WsRpcChannel ch = new WsRpcChannel("hyperliquid", new Stream()).onEvent(this::onEvent);
        wsChannel = ch;
        ch.start();
    }

    /** Ответ WS-post: payload действия/запроса, null — WS не готов. Дубликат nonce и прочее решает вызывающий. */
    private JsonNode wsPost(String type, Object payload, boolean order) throws Exception {
        WsRpcChannel ch = wsChannel;
        if (ch == null || !ch.isReady()) return null;
        if (order) orderLimiter.acquire(); else callLimiter.acquire();
        long id = wsSeq.incrementAndGet();
        ObjectNode m = mapper.createObjectNode().put("method", "post").put("id", id);
        ObjectNode rq = m.putObject("request");
        rq.put("type", type);
        rq.set("payload", mapper.valueToTree(payload));
        try {
            JsonNode r = mapper.readTree(ch.call(String.valueOf(id), m.toString(), 5000));
            JsonNode resp = r.path("data").path("response");
            if ("error".equals(resp.path("type").asText())) {
                String msg = resp.path("payload").asText(resp.toString());
                throw new ApiException(200, "WS_POST", msg, msg.toLowerCase().contains("rate limit") || msg.toLowerCase().contains("too many"));
            }
            JsonNode out = resp.path("payload");
            if (type.equals("action")) checkError(200, out);
            return out;
        } catch (WsRpcChannel.WsNotReadyException e) {
            wsFallbacks.incrementAndGet();
            return null;
        }
    }

    /** WS-ответ на info оборачивает данные в {type,data}, а allMids — ещё и в {mids}. Приводим к форме REST; null — форма не та. */
    private static JsonNode normalizeInfo(String type, JsonNode n) {
        JsonNode v = n;
        if (v.isObject() && v.has("type") && v.has("data")) v = v.get("data");
        if (type.equals("allMids") && v.has("mids")) v = v.get("mids");
        return switch (type) {
            case "meta" -> v.has("universe") ? v : null;
            case "allMids" -> v.isObject() ? v : null;
            case "clearinghouseState" -> v.has("marginSummary") ? v : null;
            case "openOrders" -> v.isArray() ? v : null;
            case "orderStatus" -> v.has("status") ? v : null;
            default -> v;
        };
    }

    private String symbolFor(String coin) {
        for (String s : config.symbols()) if (coin(s).equals(coin)) return s.toUpperCase();
        return coin;
    }

    private void publish(long oid) {
        Upd u = updates.get(oid);
        if (u == null) return;
        double[] f = fills.get(oid);
        double exec = f != null ? f[0] : Math.max(0, u.orig() - u.left());
        double avg = f != null && f[0] > 0 ? f[1] / f[0] : (exec > 0 ? u.limitPx() : 0);
        String status;
        switch (u.status()) {
            case "open" -> status = exec > 0 ? "PARTIALLY_FILLED" : "NEW";
            case "filled" -> { status = "FILLED"; if (exec <= 0) exec = u.orig(); }
            case "rejected" -> status = "REJECTED";
            default -> status = exec > 0 ? "PARTIALLY_FILLED" : "CANCELED";
        }
        long id = registerId(Long.toString(oid));
        streamed.put(id, new OrderResult(id, "", u.symbol(), u.side(), status, u.orig(), exec, avg, 0));
    }

    private void onEvent(String text) throws Exception {
        JsonNode n = mapper.readTree(text);
        String ch = n.path("channel").asText();
        if (ch.equals("orderUpdates")) {
            for (JsonNode e : n.path("data")) {
                JsonNode o = e.path("order");
                long oid = o.path("oid").asLong();
                Side side = "B".equals(o.path("side").asText()) ? Side.BUY : Side.SELL;
                updates.put(oid, new Upd(symbolFor(o.path("coin").asText()), side, d(o, "origSz"), d(o, "sz"),
                        e.path("status").asText(), d(o, "limitPx")));
                publish(oid);
            }
        } else if (ch.equals("userFills")) {
            JsonNode data = n.path("data");
            if (data.path("isSnapshot").asBoolean(false)) return;       // история нам не нужна
            for (JsonNode f : data.path("fills")) {
                long oid = f.path("oid").asLong();
                double sz = d(f, "sz"), px = d(f, "px");
                fills.merge(oid, new double[]{sz, sz * px}, (a, b) -> new double[]{a[0] + b[0], a[1] + b[1]});
                publish(oid);
            }
        }
    }

    private final class Stream implements WsRpcChannel.Protocol {
        @Override public String url() {
            if (wsUrlOverride != null) return wsUrlOverride;
            return config.wsUrl() != null && !config.wsUrl().isBlank() ? config.wsUrl()
                    : (config.testnet() ? "wss://api.hyperliquid-testnet.xyz/ws" : "wss://api.hyperliquid.xyz/ws");
        }

        @Override public List<String> login() { return List.of(); }          // подпись — в каждом запросе

        @Override public List<String> subscriptions() {
            List<String> out = new ArrayList<>();
            for (String t : List.of("orderUpdates", "userFills")) {
                ObjectNode m = mapper.createObjectNode().put("method", "subscribe");
                m.putObject("subscription").put("type", t).put("user", account);
                out.add(m.toString());
            }
            return out;
        }

        @Override public String ping() { return "{\"method\":\"ping\"}"; }

        @Override public Msg parse(String text) throws Exception {
            JsonNode n = mapper.readTree(text);
            switch (n.path("channel").asText()) {
                case "post" -> { return Msg.reply(n.path("data").path("id").asText(), text); }
                case "orderUpdates", "userFills" -> { return Msg.event(text); }
                case "pong", "subscriptionResponse" -> { return Msg.ignore(); }
                case "error" -> throw new IllegalStateException("Hyperliquid WS error: " + n.path("data").asText(text));
                default -> { return Msg.ignore(); }
            }
        }
    }

    // ------------------------------------------------------------ правила и баланс

    @Override
    public void loadFilters(Iterable<String> symbols) throws Exception {
        JsonNode universe = infoOf("meta").path("universe");
        for (int i = 0; i < universe.size(); i++) {
            assetIndex.put(universe.get(i).path("name").asText(), i);
            szDecimals.put(universe.get(i).path("name").asText(), universe.get(i).path("szDecimals").asInt(4));
        }
        int loaded = 0;
        for (String s : symbols) {
            Integer sd = szDecimals.get(coin(s));
            if (sd == null) continue;
            // минимальный ордер на бирже — 10 USDC; tick не задаём (цену форматирует клиент)
            filters.put(s.toUpperCase(), new SymbolFilters.Filter(Math.pow(10, -sd), Double.MAX_VALUE,
                    Math.pow(10, -sd), 0, 0, 1e-9, 10.0));
            loaded++;
        }
        if (loaded == 0) throw new IllegalStateException("Hyperliquid: монеты не найдены для " + symbols);
        log.info("[hyperliquid] правила загружены для {} символов", loaded);
    }

    @Override
    public void loadBalances(BalanceStore store) throws Exception {
        JsonNode r = infoOf("clearinghouseState", "user", account);
        double value = d(r.path("marginSummary"), "accountValue");
        double withdrawable = d(r, "withdrawable");
        store.set("USDC", withdrawable, Math.max(0, value - withdrawable));
        store.markSynced();
    }
}
