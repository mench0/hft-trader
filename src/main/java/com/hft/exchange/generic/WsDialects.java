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

    /** Утилитный класс — экземпляры не создаются. */
    private WsDialects() {}

    /** Сборка и разбор JSON (только для редких сообщений; стакан — потоково). */
    private static final ObjectMapper JSON = new ObjectMapper();

    /** WS-диалект биржи с её параметрами (для Uniswap — пулы из uniPools). */
    public static Optional<WsDialect> forExchange(String id, com.hft.config.ExchangeConfig cfg) {
        return id.equals("uniswapv2") ? Optional.of(new Uniswap(cfg.params().uniPools())) : forExchange(id);
    }

    /** WS-диалект биржи без её параметров (для Uniswap пулы пусты); пусто — у биржи нет WS-стакана. */
    public static Optional<WsDialect> forExchange(String id) {
        return switch (id) {
            case "okx" -> Optional.of(new Okx());
            case "gate" -> Optional.of(new Gate());
            case "bingx" -> Optional.of(new Bingx());
            case "hyperliquid" -> Optional.of(new Hyperliquid());
            case "dydx" -> Optional.of(new Dydx());
            case "uniswapv2" -> Optional.of(new Uniswap(""));
            case "kucoin" -> Optional.of(new Kucoin());
            case "aster" -> Optional.of(new Aster());
            case "mexc" -> Optional.of(new MexcWsDialect());
            default -> Optional.empty();
        };
    }

    /** Диалект Uniswap с явным списком пулов (тесты). */
    public static WsDialect uniswap(String poolsSpec) { return new Uniswap(poolsSpec); }

    // ───────────────────────── helpers ─────────────────────────

    /** Базовая валюта символа. */
    private static String base(String s) { return BalanceStore.baseAsset(s); }
    /** Котируемая валюта символа. */
    private static String quote(String s) { return BalanceStore.quoteAsset(s); }
    /** JSON сообщения строкой. */
    private static String msg(ObjectNode o) { return o.toString(); }

    /** Начало сообщения для текста ошибки. */
    static String abbreviate(char[] c, int len) { return len > 200 ? new String(c, 0, 200) + "…" : new String(c, 0, len); }
    /** Начало строки для текста ошибки. */
    static String abbreviate(String s) { return s.length() > 200 ? s.substring(0, 200) + "…" : s; }

    /** Потоковый парсер; сообщение должно быть JSON-объектом. */
    private static JsonParser open(char[] buf, int len) throws Exception {
        JsonParser p = F.createParser(buf, 0, len);
        if (p.nextToken() != JsonToken.START_OBJECT) { p.close(); throw new IllegalStateException("ожидался JSON-объект: " + abbreviate(buf, len)); }
        return p;
    }

    // ───────────────────────── OKX ─────────────────────────

    /** OKX: канал books (снимок + изменения), пинг «ping». */
    static final class Okx implements WsDialect {
        /** Адрес по умолчанию (основная или тестовая сеть). */
        public String defaultUrl(boolean testnet) {
            return testnet ? "wss://wspap.okx.com:8443/ws/v5/public" : "wss://ws.okx.com:8443/ws/v5/public";
        }
        /** Имя символа на бирже. */
        public String venueSymbol(String s) { return base(s) + "-" + quote(s); }
        /** Сообщения подписки на стаканы символов. */
        public List<String> subscribe(List<String> v, int d) { return op("subscribe", v); }
        /** Сообщения подписки на стаканы символов. */
        public List<String> unsubscribe(List<String> v, int d) { return op("unsubscribe", v); }
        /** Сообщения подписки/отписки пачками. */
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

        /** Разобрать сообщение биржи в out; служебные — пропустить, ошибки — исключение. */
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

    /** Gate: spot.order_book (снимки), пинг spot.ping. */
    static final class Gate implements WsDialect {
        /** Адрес по умолчанию (основная или тестовая сеть). */
        public String defaultUrl(boolean testnet) {
            return testnet ? "wss://ws-testnet.gate.com/v4/ws/spot" : "wss://api.gateio.ws/ws/v4/";
        }
        /** Имя символа на бирже. */
        public String venueSymbol(String s) { return base(s) + "_" + quote(s); }
        /** Сообщения подписки на стаканы символов. */
        public List<String> subscribe(List<String> v, int d) { return ev("subscribe", v); }
        /** Сообщения подписки на стаканы символов. */
        public List<String> unsubscribe(List<String> v, int d) { return ev("unsubscribe", v); }
        /** Сообщения подписки/отписки пачками. */
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

        /** Разобрать сообщение биржи в out; служебные — пропустить, ошибки — исключение. */
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

    /** BingX: depth20 (снимки в gzip), пинг Ping/Pong. */
    static final class Bingx implements WsDialect {
        public String defaultUrl(boolean testnet) { return "wss://open-api-ws.bingx.com/market"; }
        /** Имя символа на бирже. */
        public String venueSymbol(String s) { return base(s) + "-" + quote(s); }
        /** Сообщения подписки на стаканы символов. */
        public List<String> subscribe(List<String> v, int d) { return op("sub", v); }
        /** Сообщения подписки на стаканы символов. */
        public List<String> unsubscribe(List<String> v, int d) { return op("unsub", v); }
        /** Сообщения подписки/отписки пачками. */
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

        /** Разобрать сообщение биржи в out; служебные — пропустить, ошибки — исключение. */
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

    // ───────────────────────── Hyperliquid ─────────────────────────

    /** Hyperliquid: l2Book (снимки), пинг {"method":"ping"}. */
    static final class Hyperliquid implements WsDialect {
        /** Адрес по умолчанию (основная или тестовая сеть). */
        public String defaultUrl(boolean testnet) {
            return testnet ? "wss://api.hyperliquid-testnet.xyz/ws" : "wss://api.hyperliquid.xyz/ws";
        }
        /** Имя символа на бирже. */
        public String venueSymbol(String s) { return base(s); }
        /** Сообщения подписки на стаканы символов. */
        public List<String> subscribe(List<String> v, int d) { return op("subscribe", v); }
        /** Сообщения подписки на стаканы символов. */
        public List<String> unsubscribe(List<String> v, int d) { return op("unsubscribe", v); }
        /** Сообщения подписки/отписки пачками. */
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

        /** Разобрать сообщение биржи в out; служебные — пропустить, ошибки — исключение. */
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

    /** dYdX v4: v4_orderbook (снимок + изменения). */
    static final class Dydx implements WsDialect {
        /** Адрес по умолчанию (основная или тестовая сеть). */
        public String defaultUrl(boolean testnet) {
            return testnet ? "wss://indexer.v4testnet.dydx.exchange/v4/ws" : "wss://indexer.dydx.trade/v4/ws";
        }
        /** Имя символа на бирже. */
        public String venueSymbol(String s) { return base(s) + "-" + quote(s); }
        /** Сообщения подписки на стаканы символов. */
        public List<String> subscribe(List<String> v, int d) {
            List<String> out = new ArrayList<>();
            for (String s : v)
                out.add(msg(JSON.createObjectNode().put("type", "subscribe").put("channel", "v4_orderbook")
                        .put("id", s).put("batched", true)));
            return out;
        }
        /** Сообщения подписки на стаканы символов. */
        public List<String> unsubscribe(List<String> v, int d) {
            List<String> out = new ArrayList<>();
            for (String s : v)
                out.add(msg(JSON.createObjectNode().put("type", "unsubscribe").put("channel", "v4_orderbook").put("id", s)));
            return out;
        }

        /** Уровни из contents сообщения dYdX. */
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

        /** Разобрать сообщение биржи в out; служебные — пропустить, ошибки — исключение. */
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
        /** REST-диалект пулов: формула синтетического стакана. */
        private final Dialects.UniswapV2 amm;
        /** Пул по адресу пары (события Sync приходят с адресом). */
        private final Map<String, Dialects.UniswapV2.Pool> byAddr = new ConcurrentHashMap<>();

        Uniswap(String poolsSpec) { amm = new Dialects.UniswapV2(poolsSpec); }

        public String defaultUrl(boolean testnet) { return "wss://ethereum-rpc.publicnode.com"; }
        @Override public String defaultUrl(boolean testnet, String restUrl) {
            return restUrl == null || restUrl.isBlank() ? defaultUrl(testnet) : restUrl.replaceFirst("^http", "ws");
        }

        /** Имя символа на бирже. */
        public String venueSymbol(String s) {
            Dialects.UniswapV2.Pool p = amm.pools.get(s.toUpperCase());
            if (p == null) throw new IllegalArgumentException("Нет пула для " + s + " в UNISWAPV2_POOLS");
            String addr = p.pair().toLowerCase();
            byAddr.put(addr, p);
            return addr;
        }

        /** Сообщения подписки на стаканы символов. */
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

        /** Сообщения подписки на стаканы символов. */
        public List<String> unsubscribe(List<String> v, int d) { return List.of(); }

        @Override public String pingMessage() { return "{\"jsonrpc\":\"2.0\",\"id\":\"hb\",\"method\":\"eth_blockNumber\",\"params\":[]}"; }

        /** Разобрать сообщение биржи в out; служебные — пропустить, ошибки — исключение. */
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

        /** Синтетический стакан Uniswap по резервам пула из Sync или getReserves. */
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

    // ───────────────────────── KuCoin ─────────────────────────

    /**
     * Перед подключением POST /api/v1/bullet-public -> токен, адрес сервера и интервал пинга.
     * Подписка: /spotMarket/level2Depth50:BTC-USDT,ETH-USDT (снимок 50 уровней каждые 100 мс, до 100 символов в теме).
     * Сообщения: welcome, ack, pong, error, message{topic, subject:"level2", data{asks,bids,timestamp}}.
     */
    static final class Kucoin implements WsDialect {
        private static final java.net.http.HttpClient HTTP = java.net.http.HttpClient.newBuilder()
                .connectTimeout(java.time.Duration.ofSeconds(5)).build();
        /** Интервал пинга из bullet-public, мс. */
        private volatile long pingMs = 18_000;
        /** Номера сообщений подписки и пинга. */
        private final java.util.concurrent.atomic.AtomicLong ids = new java.util.concurrent.atomic.AtomicLong();

        /** Адрес по умолчанию (основная или тестовая сеть). */
        public String defaultUrl(boolean testnet) { return "kucoin:bullet-public"; }

        @Override public String connectUrl(String url, String restUrl) throws Exception {
            if (!url.startsWith("kucoin:")) return url;                       // адрес задан в конфиге явно
            com.hft.rest.RateBudget budget = com.hft.rest.RateBudget.of("kucoin");
            budget.acquire(com.hft.rest.RateBudget.Kind.PUBLIC, 10, 60_000);
            var req = java.net.http.HttpRequest.newBuilder(java.net.URI.create(restUrl.replaceAll("/+$", "") + "/api/v1/bullet-public"))
                    .timeout(java.time.Duration.ofSeconds(10)).POST(java.net.http.HttpRequest.BodyPublishers.noBody()).build();
            var resp = HTTP.send(req, java.net.http.HttpResponse.BodyHandlers.ofString());
            budget.onResponse(resp.statusCode(), resp.headers());
            JsonNode r = JSON.readTree(resp.body());
            if (!"200000".equals(r.path("code").asText())) throw new IllegalStateException("KuCoin bullet-public: " + abbreviate(resp.body()));
            JsonNode d = r.path("data");
            JsonNode server = d.path("instanceServers").path(0);
            long interval = server.path("pingInterval").asLong(18_000);
            pingMs = Math.max(5_000, interval - 2_000);
            return server.path("endpoint").asText() + "?token=" + d.path("token").asText() + "&connectId=hft" + System.nanoTime();
        }

        /** Имя символа на бирже. */
        public String venueSymbol(String s) { return base(s) + "-" + quote(s); }
        /** Сообщения подписки на стаканы символов. */
        public List<String> subscribe(List<String> v, int d) { return op("subscribe", v); }
        /** Сообщения подписки на стаканы символов. */
        public List<String> unsubscribe(List<String> v, int d) { return op("unsubscribe", v); }
        /** Сообщения подписки/отписки пачками. */
        private List<String> op(String type, List<String> v) {
            List<String> out = new ArrayList<>();
            for (int i = 0; i < v.size(); i += 100) {
                ObjectNode o = JSON.createObjectNode().put("id", String.valueOf(ids.incrementAndGet())).put("type", type)
                        .put("topic", "/spotMarket/level2Depth50:" + String.join(",", v.subList(i, Math.min(v.size(), i + 100))))
                        .put("privateChannel", false).put("response", true);
                out.add(msg(o));
            }
            return out;
        }
        @Override public String pingMessage() { return "{\"id\":\"" + ids.incrementAndGet() + "\",\"type\":\"ping\"}"; }
        @Override public long pingIntervalMs() { return pingMs; }

        /** Разобрать сообщение биржи в out; служебные — пропустить, ошибки — исключение. */
        public String parse(char[] c, int len, BookBatch out) throws Exception {
            out.reset();
            String type = null, code = null, data = null;
            boolean level2 = false;
            try (JsonParser p = open(c, len)) {
                while (p.nextToken() == JsonToken.FIELD_NAME) {
                    String f = p.currentName();
                    p.nextToken();
                    switch (f) {
                        case "type" -> type = p.getText();
                        case "code" -> code = p.getText();
                        case "subject" -> level2 = textIs(p, "level2");
                        case "topic" -> {
                            char[] t = p.getTextCharacters(); int off = p.getTextOffset(), n = p.getTextLength(), colon = -1;
                            for (int i = 0; i < n; i++) if (t[off + i] == ':') { colon = i; break; }
                            if (colon >= 0) out.venue = out.resolve(t, off + colon + 1, n - colon - 1);
                        }
                        case "data" -> {
                            if (p.currentToken() == JsonToken.START_OBJECT) {
                                while (p.nextToken() == JsonToken.FIELD_NAME) {
                                    String g = p.currentName();
                                    p.nextToken();
                                    switch (g) {
                                        case "bids" -> levels(p, out, true);
                                        case "asks" -> levels(p, out, false);
                                        case "timestamp" -> out.tsMs = longOf(p, System.currentTimeMillis());
                                        default -> p.skipChildren();
                                    }
                                }
                            } else data = p.getText();
                        }
                        default -> p.skipChildren();
                    }
                }
            }
            if (type == null) throw new IllegalStateException("KuCoin: сообщение без type " + abbreviate(c, len));
            switch (type) {
                case "welcome", "ack", "pong" -> { out.reset(); return null; }
                case "error" -> throw new IllegalStateException("KuCoin error " + code + ": " + data);
                case "message" -> {
                    if (!level2 || out.venue == null) throw new IllegalStateException("KuCoin: неожиданное сообщение " + abbreviate(c, len));
                    out.snapshot = true;                            // level2Depth50 — каждый раз полный снимок
                    if (out.tsMs == 0) out.tsMs = System.currentTimeMillis();
                    return null;
                }
                default -> throw new IllegalStateException("KuCoin: неизвестный type " + type);
            }
        }
    }

    // ───────────────────────── Aster ─────────────────────────

    /**
     * Спот Aster, формат Binance: wss://sstream.asterdex.com/stream, подписка {"method":"SUBSCRIBE",
     * "params":["btcusdt@depth20@100ms"],"id":N}. Сообщение {"stream":"btcusdt@depth20@100ms","data":{
     * lastUpdateId, bids, asks}} (у фьючерсного формата — b/a и E). Каждое сообщение — снимок.
     */
    static final class Aster implements WsDialect {
        /** Номера сообщений подписки. */
        private final java.util.concurrent.atomic.AtomicLong ids = new java.util.concurrent.atomic.AtomicLong();

        /** Адрес по умолчанию (основная или тестовая сеть). */
        public String defaultUrl(boolean testnet) {
            return testnet ? "wss://sstream.asterdex-testnet.com/stream" : "wss://sstream.asterdex.com/stream";
        }
        /** Имя символа на бирже. */
        public String venueSymbol(String s) { return s.toLowerCase(java.util.Locale.ROOT); }
        /** Сообщения подписки на стаканы символов. */
        public List<String> subscribe(List<String> v, int d) { return op("SUBSCRIBE", v, d); }
        /** Сообщения подписки на стаканы символов. */
        public List<String> unsubscribe(List<String> v, int d) { return op("UNSUBSCRIBE", v, d); }
        /** Сообщения подписки/отписки пачками. */
        private List<String> op(String method, List<String> v, int d) {
            String depth = d <= 5 ? "5" : d <= 10 ? "10" : "20";
            List<String> out = new ArrayList<>();
            for (int i = 0; i < v.size(); i += 50) {
                ObjectNode o = JSON.createObjectNode().put("method", method).put("id", ids.incrementAndGet());
                ArrayNode params = o.putArray("params");
                for (String s : v.subList(i, Math.min(v.size(), i + 50))) params.add(s + "@depth" + depth + "@100ms");
                out.add(msg(o));
            }
            return out;
        }

        /** Разобрать сообщение биржи в out; служебные — пропустить, ошибки — исключение. */
        public String parse(char[] c, int len, BookBatch out) throws Exception {
            out.reset();
            boolean reply = false, book = false;
            String errMsg = null;
            try (JsonParser p = open(c, len)) {
                while (p.nextToken() == JsonToken.FIELD_NAME) {
                    String f = p.currentName();
                    p.nextToken();
                    switch (f) {
                        case "result", "id" -> { reply = true; p.skipChildren(); }
                        case "error" -> errMsg = text(p);
                        case "stream" -> {
                            char[] t = p.getTextCharacters(); int off = p.getTextOffset(), n = p.getTextLength(), at = n;
                            for (int i = 0; i < n; i++) if (t[off + i] == '@') { at = i; break; }
                            out.venue = out.resolve(t, off, at);
                        }
                        case "data" -> {
                            if (p.currentToken() != JsonToken.START_OBJECT) throw new IllegalStateException("Aster: data не объект");
                            while (p.nextToken() == JsonToken.FIELD_NAME) {
                                String g = p.currentName();
                                p.nextToken();
                                switch (g) {
                                    case "bids", "b" -> { book = true; levels(p, out, true); }
                                    case "asks", "a" -> { book = true; levels(p, out, false); }
                                    case "E", "T" -> out.tsMs = longOf(p, System.currentTimeMillis());
                                    default -> p.skipChildren();
                                }
                            }
                        }
                        default -> p.skipChildren();
                    }
                }
            }
            if (errMsg != null) throw new IllegalStateException("Aster WS error: " + errMsg);
            if (!book) {
                out.reset();
                if (reply) return null;                              // ответ на SUBSCRIBE
                throw new IllegalStateException("Aster: неожиданное сообщение " + abbreviate(c, len));
            }
            if (out.venue == null) throw new IllegalStateException("Aster: стакан без stream " + abbreviate(c, len));
            out.snapshot = true;
            if (out.tsMs == 0) out.tsMs = System.currentTimeMillis();
            return null;
        }
    }
}
