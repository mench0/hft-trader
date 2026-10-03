package com.hft.exchange.generic;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hft.store.BalanceStore;

import java.io.ByteArrayInputStream;
import java.math.BigInteger;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.GZIPInputStream;

import static com.hft.exchange.generic.FastJson.*;

/**
 * WebSocket-диалекты бирж. Разбор потоковый (Jackson streaming): без дерева узлов и без строк на числа.
 * Форматы сообщений записаны по документации ПО ПАМЯТИ и не сверялись с живыми серверами —
 * проверяются тестами на локальном фейковом сервере. Любое неожиданное сообщение даёт исключение.
 */
public final class WsDialects {

    private WsDialects() {}

    private static final ObjectMapper JSON = new ObjectMapper();

    /** Есть ли WS-диалект для биржи (у MEXC нет: спотовый WS отдаёт protobuf). */
    public static Optional<WsDialect> forExchange(String id) {
        return switch (id) {
            case "okx" -> Optional.of(new Okx());
            case "gate" -> Optional.of(new Gate());
            case "bingx" -> Optional.of(new Bingx());
            case "lbank" -> Optional.of(new Lbank());
            case "hyperliquid" -> Optional.of(new Hyperliquid());
            case "dydx" -> Optional.of(new Dydx());
            case "uniswapv2" -> Optional.of(new Uniswap(System.getenv("UNISWAPV2_POOLS")));
            default -> Optional.empty();
        };
    }

    /** Диалект Uniswap с явным списком пулов (тесты). */
    public static WsDialect uniswap(String poolsSpec) { return new Uniswap(poolsSpec); }

    // ───────────────────────── helpers ─────────────────────────

    private static String base(String s) { return BalanceStore.baseAsset(s); }
    private static String quote(String s) { return BalanceStore.quoteAsset(s); }
    private static String msg(ObjectNode o) { return o.toString(); }

    static String abbreviate(char[] c, int len) { return len > 200 ? new String(c, 0, 200) + "…" : new String(c, 0, len); }
    static String abbreviate(String s) { return s.length() > 200 ? s.substring(0, 200) + "…" : s; }

    private static JsonParser open(char[] buf, int len) throws Exception {
        JsonParser p = F.createParser(buf, 0, len);
        if (p.nextToken() != JsonToken.START_OBJECT) { p.close(); throw new IllegalStateException("ожидался JSON-объект: " + abbreviate(buf, len)); }
        return p;
    }

    // ───────────────────────── OKX ─────────────────────────

    static final class Okx implements WsDialect {
        public String defaultUrl(boolean testnet) {
            return testnet ? "wss://wspap.okx.com:8443/ws/v5/public" : "wss://ws.okx.com:8443/ws/v5/public";
        }
        public String venueSymbol(String s) { return base(s) + "-" + quote(s); }
        public List<String> subscribe(List<String> v, int d) { return op("subscribe", v); }
        public List<String> unsubscribe(List<String> v, int d) { return op("unsubscribe", v); }
        private List<String> op(String op, List<String> v) {
            List<String> out = new ArrayList<>();
            for (int i = 0; i < v.size(); i += 20) {
                ObjectNode o = JSON.createObjectNode().put("op", op);
                ArrayNode args = o.putArray("args");
                for (String s : v.subList(i, Math.min(v.size(), i + 20)))
                    args.addObject().put("channel", "books").put("instId", s);
                out.add(msg(o));
            }
            return out;
        }
        @Override public String pingMessage() { return "ping"; }
        @Override public long pingIntervalMs() { return 20_000; }

        public String parse(char[] c, int len, BookBatch out) throws Exception {
            out.reset();
            if (FastJson.equals(c, len, "pong")) return null;
            boolean books = false, hasData = false, snap = false;
            String event = null, code = null, errMsg = null;
            try (JsonParser p = open(c, len)) {
                while (p.nextToken() == JsonToken.FIELD_NAME) {
                    String f = p.currentName();
                    p.nextToken();
                    switch (f) {
                        case "event" -> event = p.getText();
                        case "code" -> code = p.getText();
                        case "msg" -> errMsg = p.getText();
                        case "action" -> snap = textIs(p, "snapshot");
                        case "arg" -> {
                            while (p.nextToken() == JsonToken.FIELD_NAME) {
                                String g = p.currentName();
                                p.nextToken();
                                if (g.equals("channel")) books = textIs(p, "books");
                                else if (g.equals("instId")) out.venue = out.resolve(p.getTextCharacters(), p.getTextOffset(), p.getTextLength());
                                else p.skipChildren();
                            }
                        }
                        case "data" -> {
                            hasData = true;
                            if (p.currentToken() != JsonToken.START_ARRAY) throw new IllegalStateException("OKX: data не массив");
                            while (p.nextToken() == JsonToken.START_OBJECT) {
                                while (p.nextToken() == JsonToken.FIELD_NAME) {
                                    String g = p.currentName();
                                    p.nextToken();
                                    switch (g) {
                                        case "bids" -> levels(p, out, true);
                                        case "asks" -> levels(p, out, false);
                                        case "ts" -> out.tsMs = longOf(p, System.currentTimeMillis());
                                        default -> p.skipChildren();
                                    }
                                }
                            }
                        }
                        default -> p.skipChildren();
                    }
                }
            }
            if (event != null) {
                out.reset();
                if (event.equals("error")) throw new IllegalStateException("OKX error " + code + ": " + errMsg);
                return null;
            }
            if (!books || !hasData || out.venue == null) throw new IllegalStateException("OKX: неожиданное сообщение " + abbreviate(c, len));
            out.snapshot = snap;
            if (out.tsMs == 0) out.tsMs = System.currentTimeMillis();
            return null;
        }
    }

    // ───────────────────────── Gate ─────────────────────────

    static final class Gate implements WsDialect {
        public String defaultUrl(boolean testnet) {
            return testnet ? "wss://ws-testnet.gate.io/v4/ws/spot" : "wss://api.gateio.ws/ws/v4/";
        }
        public String venueSymbol(String s) { return base(s) + "_" + quote(s); }
        public List<String> subscribe(List<String> v, int d) { return ev("subscribe", v); }
        public List<String> unsubscribe(List<String> v, int d) { return ev("unsubscribe", v); }
        private List<String> ev(String event, List<String> v) {
            List<String> out = new ArrayList<>();
            for (String s : v) {
                ObjectNode o = JSON.createObjectNode().put("time", System.currentTimeMillis() / 1000)
                        .put("channel", "spot.order_book").put("event", event);
                o.putArray("payload").add(s).add("20").add("100ms");
                out.add(msg(o));
            }
            return out;
        }
        @Override public String pingMessage() {
            return msg(JSON.createObjectNode().put("time", System.currentTimeMillis() / 1000).put("channel", "spot.ping"));
        }

        public String parse(char[] c, int len, BookBatch out) throws Exception {
            out.reset();
            boolean orderBook = false, pong = false, update = false, sub = false;
            String errCode = null, errMsg = null;
            try (JsonParser p = open(c, len)) {
                while (p.nextToken() == JsonToken.FIELD_NAME) {
                    String f = p.currentName();
                    p.nextToken();
                    switch (f) {
                        case "channel" -> { orderBook = textIs(p, "spot.order_book"); pong = textIs(p, "spot.pong"); }
                        case "event" -> { update = textIs(p, "update"); sub = textIs(p, "subscribe") || textIs(p, "unsubscribe"); }
                        case "error" -> {
                            if (p.currentToken() == JsonToken.START_OBJECT) {
                                while (p.nextToken() == JsonToken.FIELD_NAME) {
                                    String g = p.currentName(); p.nextToken();
                                    if (g.equals("code")) errCode = p.getText();
                                    else if (g.equals("message")) errMsg = p.getText();
                                    else p.skipChildren();
                                }
                            }
                        }
                        case "result" -> {
                            if (p.currentToken() != JsonToken.START_OBJECT) { p.skipChildren(); break; }
                            while (p.nextToken() == JsonToken.FIELD_NAME) {
                                String g = p.currentName(); p.nextToken();
                                switch (g) {
                                    case "s" -> out.venue = out.resolve(p.getTextCharacters(), p.getTextOffset(), p.getTextLength());
                                    case "t" -> out.tsMs = longOf(p, 0);
                                    case "bids" -> levels(p, out, true);
                                    case "asks" -> levels(p, out, false);
                                    default -> p.skipChildren();
                                }
                            }
                        }
                        default -> p.skipChildren();
                    }
                }
            }
            if (pong) { out.reset(); return null; }
            if (errCode != null || errMsg != null) { out.reset(); throw new IllegalStateException("Gate error " + errCode + ": " + errMsg); }
            if (!orderBook) { out.reset(); throw new IllegalStateException("Gate: неожиданное сообщение " + abbreviate(c, len)); }
            if (!update) { out.reset(); return null; }                     // ответы на подписку
            if (out.venue == null) throw new IllegalStateException("Gate: нет пары в обновлении");
            out.snapshot = true;
            if (out.tsMs == 0) out.tsMs = System.currentTimeMillis();
            return null;
        }
    }

    // ───────────────────────── BingX ─────────────────────────

    static final class Bingx implements WsDialect {
        public String defaultUrl(boolean testnet) { return "wss://open-api-ws.bingx.com/market"; }
        public String venueSymbol(String s) { return base(s) + "-" + quote(s); }
        public List<String> subscribe(List<String> v, int d) { return op("sub", v); }
        public List<String> unsubscribe(List<String> v, int d) { return op("unsub", v); }
        private List<String> op(String type, List<String> v) {
            List<String> out = new ArrayList<>();
            for (String s : v)
                out.add(msg(JSON.createObjectNode().put("id", java.util.UUID.randomUUID().toString())
                        .put("reqType", type).put("dataType", s + "@depth20")));
            return out;
        }
        @Override public String decodeBinary(byte[] data) throws Exception {
            try (GZIPInputStream gz = new GZIPInputStream(new ByteArrayInputStream(data))) {
                return new String(gz.readAllBytes(), StandardCharsets.UTF_8);
            }
        }

        public String parse(char[] c, int len, BookBatch out) throws Exception {
            out.reset();
            if (FastJson.equals(c, len, "Ping") || FastJson.equals(c, len, "ping")) return "Pong";
            if (FastJson.equals(c, len, "Pong") || FastJson.equals(c, len, "pong")) return null;
            int code = 0;
            String errMsg = null;
            boolean depth = false, data = false;
            try (JsonParser p = open(c, len)) {
                while (p.nextToken() == JsonToken.FIELD_NAME) {
                    String f = p.currentName();
                    p.nextToken();
                    switch (f) {
                        case "code" -> code = (int) longOf(p, 0);
                        case "msg" -> errMsg = p.getText();
                        case "dataType" -> {
                            char[] t = p.getTextCharacters(); int off = p.getTextOffset(), n = p.getTextLength();
                            int at = -1;
                            for (int i = 0; i < n; i++) if (t[off + i] == '@') { at = i; break; }
                            if (at > 0) {
                                out.venue = out.resolve(t, off, at);
                                depth = n - at >= 6 && t[off + at + 1] == 'd' && t[off + at + 2] == 'e' && t[off + at + 3] == 'p';
                            } else if (n > 0) throw new IllegalStateException("BingX: неожиданный dataType " + p.getText());
                        }
                        case "data" -> {
                            if (p.currentToken() != JsonToken.START_OBJECT) { p.skipChildren(); break; }
                            data = true;
                            while (p.nextToken() == JsonToken.FIELD_NAME) {
                                String g = p.currentName(); p.nextToken();
                                switch (g) {
                                    case "bids" -> levels(p, out, true);
                                    case "asks" -> levels(p, out, false);
                                    default -> p.skipChildren();
                                }
                            }
                        }
                        case "ts" -> out.tsMs = longOf(p, 0);
                        default -> p.skipChildren();
                    }
                }
            }
            if (code != 0) { out.reset(); throw new IllegalStateException("BingX error " + code + ": " + errMsg); }
            if (!data || out.venue == null) { out.reset(); return null; }   // ответ на подписку
            if (!depth) { out.reset(); throw new IllegalStateException("BingX: не стакан"); }
            out.snapshot = true;
            if (out.tsMs == 0) out.tsMs = System.currentTimeMillis();
            return null;
        }
    }

    // ───────────────────────── LBank ─────────────────────────

    static final class Lbank implements WsDialect {
        public String defaultUrl(boolean testnet) { return "wss://www.lbkex.net/ws/V2/"; }
        public String venueSymbol(String s) { return (base(s) + "_" + quote(s)).toLowerCase(Locale.ROOT); }
        public List<String> subscribe(List<String> v, int d) { return op("subscribe", v); }
        public List<String> unsubscribe(List<String> v, int d) { return op("unsubscribe", v); }
        private List<String> op(String action, List<String> v) {
            List<String> out = new ArrayList<>();
            for (String s : v)
                out.add(msg(JSON.createObjectNode().put("action", action).put("subscribe", "depth")
                        .put("depth", "100").put("pair", s)));
            return out;
        }

        public String parse(char[] c, int len, BookBatch out) throws Exception {
            out.reset();
            boolean ping = false, isDepth = false, hasDepth = false;
            String pingId = null, status = null;
            try (JsonParser p = open(c, len)) {
                while (p.nextToken() == JsonToken.FIELD_NAME) {
                    String f = p.currentName();
                    p.nextToken();
                    switch (f) {
                        case "action" -> ping = textIs(p, "ping");
                        case "ping" -> pingId = p.getText();
                        case "type" -> isDepth = textIs(p, "depth");
                        case "status" -> status = p.getText();
                        case "pair" -> out.venue = out.resolve(p.getTextCharacters(), p.getTextOffset(), p.getTextLength());
                        case "depth" -> {
                            if (p.currentToken() != JsonToken.START_OBJECT) { p.skipChildren(); break; }
                            hasDepth = true;
                            while (p.nextToken() == JsonToken.FIELD_NAME) {
                                String g = p.currentName(); p.nextToken();
                                switch (g) {
                                    case "bids" -> levels(p, out, true);
                                    case "asks" -> levels(p, out, false);
                                    default -> p.skipChildren();
                                }
                            }
                        }
                        default -> p.skipChildren();
                    }
                }
            }
            if (ping) { out.reset(); return msg(JSON.createObjectNode().put("action", "pong").put("pong", pingId)); }
            if (status != null && !status.equalsIgnoreCase("success") && !hasDepth) { out.reset(); throw new IllegalStateException("LBank: " + abbreviate(c, len)); }
            if (!isDepth || !hasDepth) { out.reset(); return null; }
            if (out.venue == null) throw new IllegalStateException("LBank: нет pair в depth");
            out.snapshot = true;
            out.tsMs = System.currentTimeMillis();
            return null;
        }
    }

    // ───────────────────────── Hyperliquid ─────────────────────────

    static final class Hyperliquid implements WsDialect {
        public String defaultUrl(boolean testnet) {
            return testnet ? "wss://api.hyperliquid-testnet.xyz/ws" : "wss://api.hyperliquid.xyz/ws";
        }
        public String venueSymbol(String s) { return base(s); }
        public List<String> subscribe(List<String> v, int d) { return op("subscribe", v); }
        public List<String> unsubscribe(List<String> v, int d) { return op("unsubscribe", v); }
        private List<String> op(String method, List<String> v) {
            List<String> out = new ArrayList<>();
            for (String s : v) {
                ObjectNode o = JSON.createObjectNode().put("method", method);
                o.putObject("subscription").put("type", "l2Book").put("coin", s);
                out.add(msg(o));
            }
            return out;
        }
        @Override public String pingMessage() { return "{\"method\":\"ping\"}"; }
        @Override public long pingIntervalMs() { return 20_000; }

        public String parse(char[] c, int len, BookBatch out) throws Exception {
            out.reset();
            int kind = -1;                  // 0 — служебное, 1 — ошибка, 2 — стакан
            String errText = null;
            boolean levels = false;
            try (JsonParser p = open(c, len)) {
                while (p.nextToken() == JsonToken.FIELD_NAME) {
                    String f = p.currentName();
                    p.nextToken();
                    if (f.equals("channel")) {
                        if (textIs(p, "pong") || textIs(p, "subscriptionResponse")) kind = 0;
                        else if (textIs(p, "error")) kind = 1;
                        else if (textIs(p, "l2Book")) kind = 2;
                        else kind = 3;
                    } else if (f.equals("data")) {
                        if (p.currentToken() != JsonToken.START_OBJECT) { errText = text(p); continue; }
                        while (p.nextToken() == JsonToken.FIELD_NAME) {
                            String g = p.currentName(); p.nextToken();
                            switch (g) {
                                case "coin" -> out.venue = out.resolve(p.getTextCharacters(), p.getTextOffset(), p.getTextLength());
                                case "time" -> out.tsMs = longOf(p, 0);
                                case "levels" -> {
                                    if (p.currentToken() != JsonToken.START_ARRAY) throw new IllegalStateException("Hyperliquid: нет levels");
                                    p.nextToken(); levels(p, out, true);
                                    p.nextToken(); levels(p, out, false);
                                    if (p.nextToken() != JsonToken.END_ARRAY) throw new IllegalStateException("Hyperliquid: levels не из двух сторон");
                                    levels = true;
                                }
                                default -> p.skipChildren();
                            }
                        }
                    } else p.skipChildren();
                }
            }
            switch (kind) {
                case 0 -> { out.reset(); return null; }
                case 1 -> { out.reset(); throw new IllegalStateException("Hyperliquid error: " + abbreviate(errText == null ? "" : errText)); }
                case 2 -> {
                    if (!levels || out.venue == null) throw new IllegalStateException("Hyperliquid: нет levels");
                    out.snapshot = true;
                    if (out.tsMs == 0) out.tsMs = System.currentTimeMillis();
                    return null;
                }
                default -> { out.reset(); throw new IllegalStateException("Hyperliquid: неожиданное сообщение " + abbreviate(c, len)); }
            }
        }
    }

    // ───────────────────────── dYdX v4 (indexer) ─────────────────────────

    static final class Dydx implements WsDialect {
        public String defaultUrl(boolean testnet) {
            return testnet ? "wss://indexer.v4testnet.dydx.exchange/v4/ws" : "wss://indexer.dydx.trade/v4/ws";
        }
        public String venueSymbol(String s) { return base(s) + "-" + quote(s); }
        public List<String> subscribe(List<String> v, int d) {
            List<String> out = new ArrayList<>();
            for (String s : v)
                out.add(msg(JSON.createObjectNode().put("type", "subscribe").put("channel", "v4_orderbook")
                        .put("id", s).put("batched", true)));
            return out;
        }
        public List<String> unsubscribe(List<String> v, int d) {
            List<String> out = new ArrayList<>();
            for (String s : v)
                out.add(msg(JSON.createObjectNode().put("type", "unsubscribe").put("channel", "v4_orderbook").put("id", s)));
            return out;
        }

        private static void contents(JsonParser p, BookBatch out) throws Exception {
            if (p.currentToken() != JsonToken.START_OBJECT) throw new IllegalStateException("dYdX: contents не объект");
            while (p.nextToken() == JsonToken.FIELD_NAME) {
                String g = p.currentName(); p.nextToken();
                switch (g) {
                    case "bids" -> levels(p, out, true);
                    case "asks" -> levels(p, out, false);
                    default -> p.skipChildren();
                }
            }
        }

        public String parse(char[] c, int len, BookBatch out) throws Exception {
            out.reset();
            String type = null, message = null;
            boolean hasContents = false;
            try (JsonParser p = open(c, len)) {
                while (p.nextToken() == JsonToken.FIELD_NAME) {
                    String f = p.currentName();
                    p.nextToken();
                    switch (f) {
                        case "type" -> type = p.getText();          // короткие интернированные строки Jackson не держит — это одна строка на сообщение
                        case "message" -> message = p.getText();
                        case "id" -> out.venue = out.resolve(p.getTextCharacters(), p.getTextOffset(), p.getTextLength());
                        case "contents" -> {
                            hasContents = true;
                            if (p.currentToken() == JsonToken.START_ARRAY) {
                                while (p.nextToken() == JsonToken.START_OBJECT) {
                                    while (p.nextToken() == JsonToken.FIELD_NAME) {
                                        String g = p.currentName(); p.nextToken();
                                        switch (g) {
                                            case "bids" -> levels(p, out, true);
                                            case "asks" -> levels(p, out, false);
                                            default -> p.skipChildren();
                                        }
                                    }
                                }
                            } else contents(p, out);
                        }
                        default -> p.skipChildren();
                    }
                }
            }
            if (type == null) throw new IllegalStateException("dYdX: нет type");
            switch (type) {
                case "connected", "unsubscribed" -> { out.reset(); return null; }
                case "error" -> { out.reset(); throw new IllegalStateException("dYdX error: " + message); }
                case "subscribed", "channel_data", "channel_batch_data" -> {
                    if (!hasContents || out.venue == null) throw new IllegalStateException("dYdX: нет contents/id");
                    out.snapshot = type.equals("subscribed");
                    out.tsMs = System.currentTimeMillis();
                    return null;
                }
                default -> { out.reset(); throw new IllegalStateException("dYdX: неожиданный тип " + type); }
            }
        }
    }

    // ───────────────────────── Uniswap V2 через JSON-RPC по WebSocket ─────────────────────────

    /**
     * Нода Ethereum: подписка eth_subscribe("logs") на событие Sync(uint112,uint112) нужных пар.
     * Каждый своп меняет резервы и сразу приходит в сокете — опрос eth_call не нужен.
     * Первый снимок — одним eth_call на пару по тому же сокету (Sync приходит только при свопе).
     * Раз в 15 с — eth_blockNumber как сердцебиение, чтобы тихий пул не считался оборвавшимся.
     * Сообщения редкие (раз в блок), поэтому здесь обычный разбор деревом.
     */
    static final class Uniswap implements WsDialect {
        // keccak256("Sync(uint112,uint112)") — сверяется тестом независимым keccak
        static final String SYNC_TOPIC = "0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1";
        private final Dialects.UniswapV2 amm;
        private final Map<String, Dialects.UniswapV2.Pool> byAddr = new ConcurrentHashMap<>();

        Uniswap(String poolsSpec) { amm = new Dialects.UniswapV2(poolsSpec); }

        public String defaultUrl(boolean testnet) { return "wss://ethereum-rpc.publicnode.com"; }
        @Override public String defaultUrl(boolean testnet, String restUrl) {
            return restUrl == null || restUrl.isBlank() ? defaultUrl(testnet) : restUrl.replaceFirst("^http", "ws");
        }

        public String venueSymbol(String s) {
            Dialects.UniswapV2.Pool p = amm.pools.get(s.toUpperCase());
            if (p == null) throw new IllegalArgumentException("Нет пула для " + s + " в UNISWAPV2_POOLS");
            String addr = p.pair().toLowerCase();
            byAddr.put(addr, p);
            return addr;
        }

        public List<String> subscribe(List<String> v, int d) {
            List<String> out = new ArrayList<>();
            ObjectNode sub = JSON.createObjectNode().put("jsonrpc", "2.0").put("id", "sub:logs").put("method", "eth_subscribe");
            ArrayNode params = sub.putArray("params").add("logs");
            ObjectNode filter = params.addObject();
            ArrayNode addrs = filter.putArray("address");
            v.forEach(addrs::add);
            filter.putArray("topics").add(SYNC_TOPIC);
            out.add(msg(sub));
            for (String addr : v) {
                ObjectNode call = JSON.createObjectNode().put("jsonrpc", "2.0").put("id", "call:" + addr).put("method", "eth_call");
                ArrayNode cp = call.putArray("params");
                cp.addObject().put("to", addr).put("data", "0x0902f1ac");
                cp.add("latest");
                out.add(msg(call));
            }
            return out;
        }

        public List<String> unsubscribe(List<String> v, int d) { return List.of(); }

        @Override public String pingMessage() { return "{\"jsonrpc\":\"2.0\",\"id\":\"hb\",\"method\":\"eth_blockNumber\",\"params\":[]}"; }

        public String parse(char[] c, int len, BookBatch out) throws Exception {
            out.reset();
            JsonNode n = JSON.readTree(new String(c, 0, len));
            if ("eth_subscription".equals(n.path("method").asText())) {
                JsonNode r = n.path("params").path("result");
                if (r.path("removed").asBoolean(false)) return null;          // реорг: следующий Sync исправит
                fill(out, r.path("address").asText().toLowerCase(), r.path("data").asText());
                return null;
            }
            if (n.has("id")) {
                if (n.hasNonNull("error")) throw new IllegalStateException("RPC error " + n.path("error").path("code").asText() + ": " + n.path("error").path("message").asText());
                String id = n.path("id").asText();
                if (id.equals("hb") || id.equals("sub:logs")) return null;
                if (id.startsWith("call:")) { fill(out, id.substring(5), n.path("result").asText()); return null; }
            }
            throw new IllegalStateException("Uniswap WS: неожиданное сообщение " + abbreviate(c, len));
        }

        private void fill(BookBatch out, String addr, String hex) {
            Dialects.UniswapV2.Pool p = byAddr.get(addr);
            if (p == null) throw new IllegalStateException("Uniswap WS: неизвестная пара " + addr);
            if (!hex.startsWith("0x") || hex.length() < 2 + 128) throw new IllegalStateException("Uniswap WS: короткие данные " + abbreviate(hex));
            String h = hex.substring(2);
            BookDialect.ParsedBook b = amm.fromReserves(p, new BigInteger(h.substring(0, 64), 16), new BigInteger(h.substring(64, 128), 16));
            out.venue = out.resolve(addr);
            out.snapshot = true;
            out.tsMs = System.currentTimeMillis();
            for (int i = 0; i < b.bp().length; i++) out.bid(b.bp()[i], b.bq()[i]);
            for (int i = 0; i < b.ap().length; i++) out.ask(b.ap()[i], b.aq()[i]);
        }
    }
}
