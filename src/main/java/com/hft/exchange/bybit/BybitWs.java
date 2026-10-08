package com.hft.exchange.bybit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hft.model.OrderEnums.Side;
import com.hft.model.OrderResult;
import com.hft.rest.WsRpcChannel;
import com.hft.rest.WsRpcChannel.Msg;
import com.hft.store.BalanceStore;
import com.hft.store.PositionStore;
import com.hft.util.BoundedMap;
import com.hft.util.Signer;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bybit v5 по WebSocket: торговый сокет (/v5/trade — order.create, order.cancel) и приватный
 * (/v5/private — потоки order и wallet). Оба логинятся одинаково: op "auth" с
 * [apiKey, expires, hex(HMAC-SHA256("GET/realtime" + expires))]. Пинг — {"op":"ping"} раз в 20 с.
 *
 * ВНИМАНИЕ: формат взят из документации Bybit без доступа к живому API — проверяйте в testnet.
 */
final class BybitWs {

    /** Разбор и сборка JSON. */
    private final ObjectMapper mapper = new ObjectMapper();
    /** API-ключ. */
    private final String apiKey;
    /** Подпись HMAC-SHA256. */
    private final Signer signer;
    /** Окно годности запроса, мс. */
    private final int recvWindow;
    /** Номера запросов. */
    private final AtomicLong seq = new AtomicLong();
    /** Последнее состояние ордеров из потока order (orderId -> результат). */
    final Map<Long, OrderResult> streamed = BoundedMap.create(10_000);
    /** Куда пишутся балансы из потока wallet. */
    volatile BalanceStore balances;
    /** Баланс хотя бы раз пришёл по WS. */
    volatile boolean walletSeen;
    /** Куда пишутся позиции из потока position (только linear). */
    volatile PositionStore positions;
    /** Категория Bybit: spot или linear (перпы USDT). */
    final String category;

    /** Торговый и приватный протоколы. */
    final Trade trade;
    final Private priv;

    /**
     * @param tradeUrl адрес /v5/trade
     * @param privateUrl адрес /v5/private
     */
    BybitWs(String tradeUrl, String privateUrl, String apiKey, Signer signer, int recvWindow) {
        this(tradeUrl, privateUrl, apiKey, signer, recvWindow, "spot");
    }

    /** @param category spot или linear */
    BybitWs(String tradeUrl, String privateUrl, String apiKey, Signer signer, int recvWindow, String category) {
        this.category = category;
        this.apiKey = apiKey;
        this.signer = signer;
        this.recvWindow = recvWindow;
        this.trade = new Trade(tradeUrl);
        this.priv = new Private(privateUrl);
    }

    /** Новый id запроса. */
    String nextId() { return "y" + seq.incrementAndGet(); }

    /** Сообщение логина. */
    private String auth() {
        long expires = System.currentTimeMillis() + 10_000;
        ObjectNode m = mapper.createObjectNode().put("req_id", "auth" + seq.incrementAndGet()).put("op", "auth");
        m.putArray("args").add(apiKey).add(expires).add(signer.sign("GET/realtime" + expires));
        return m.toString();
    }

    /** Запрос торгового сокета: {reqId, header, op, args:[body]}. */
    String request(String id, String op, ObjectNode body) {
        ObjectNode m = mapper.createObjectNode().put("reqId", id).put("op", op);
        m.putObject("header").put("X-BAPI-TIMESTAMP", Long.toString(System.currentTimeMillis()))
                .put("X-BAPI-RECV-WINDOW", Integer.toString(recvWindow));
        m.putArray("args").add(body);
        return m.toString();
    }

    /** data из ответа; retCode ≠ 0 — ExchangeException с кодом Bybit. */
    JsonNode result(String reply) throws Exception {
        JsonNode n = mapper.readTree(reply);
        int code = n.path("retCode").asInt(-1);
        if (code != 0) throw new BybitRestClient.ExchangeException(200, code, n.path("retMsg").asText(reply));
        return n.path("data");
    }

    /** Новый объект JSON. */
    ObjectNode obj() { return mapper.createObjectNode(); }

    /** Торговый сокет: ответы по reqId. */
    final class Trade implements WsRpcChannel.Protocol {
        /** Адрес. */
        private final String url;
        Trade(String url) { this.url = url; }
        @Override public String url() { return url; }
        @Override public List<String> login() { return List.of(auth()); }
        @Override public List<String> subscriptions() { return List.of(); }
        @Override public String ping() { return "{\"op\":\"ping\"}"; }
        @Override public Msg parse(String text) throws Exception {
            JsonNode n = mapper.readTree(text);
            String op = n.path("op").asText("");
            if (op.equals("auth")) {
                if (n.path("retCode").asInt(-1) == 0) return Msg.loginOk();
                throw new IllegalStateException("Bybit trade auth: " + n.path("retCode").asText() + " " + n.path("retMsg").asText());
            }
            if (op.equals("pong") || op.equals("ping")) return Msg.ignore();
            if (n.has("reqId")) return Msg.reply(n.path("reqId").asText(), text);
            if (n.path("retCode").asInt(0) != 0) throw new IllegalStateException("Bybit trade: " + n.path("retMsg").asText());
            return Msg.ignore();
        }
    }

    /** Приватный сокет: потоки order и wallet. */
    final class Private implements WsRpcChannel.Protocol {
        /** Адрес. */
        private final String url;
        Private(String url) { this.url = url; }
        @Override public String url() { return url; }
        @Override public List<String> login() { return List.of(auth()); }
        @Override public List<String> subscriptions() {
            ObjectNode m = mapper.createObjectNode().put("req_id", "sub" + seq.incrementAndGet()).put("op", "subscribe");
            var args = m.putArray("args").add("order").add("wallet");
            if (category.equals("linear")) args.add("position");
            return List.of(m.toString());
        }
        @Override public String ping() { return "{\"op\":\"ping\"}"; }
        @Override public Msg parse(String text) throws Exception {
            JsonNode n = mapper.readTree(text);
            if (n.has("topic")) return Msg.event(text);
            String op = n.path("op").asText("");
            boolean ok = n.path("success").asBoolean(false);
            if (op.equals("auth")) {
                if (ok) return Msg.loginOk();
                throw new IllegalStateException("Bybit private auth: " + n.path("ret_msg").asText());
            }
            if (op.equals("subscribe") && !ok) throw new IllegalStateException("Bybit: подписка отклонена: " + n.path("ret_msg").asText());
            return Msg.ignore();                               // pong, подтверждение подписки
        }
    }

    /** Событие приватного сокета: ордер или кошелёк. */
    void onEvent(String text) throws Exception {
        JsonNode n = mapper.readTree(text);
        switch (n.path("topic").asText()) {
            case "order" -> {
                for (JsonNode o : n.path("data")) {
                    if (!category.equals(o.path("category").asText(category))) continue;
                    long id = o.path("orderId").asLong();
                    streamed.put(id, new OrderResult(id, o.path("orderLinkId").asText(""), o.path("symbol").asText(),
                            "Buy".equals(o.path("side").asText()) ? Side.BUY : Side.SELL, status(o.path("orderStatus").asText()),
                            o.path("qty").asDouble(0), o.path("cumExecQty").asDouble(0), o.path("avgPrice").asDouble(0), 0));
                }
            }
            case "wallet" -> {
                BalanceStore store = balances;
                if (store == null) return;
                for (JsonNode acc : n.path("data"))
                    for (JsonNode c : acc.path("coin")) {
                        double[] fl = freeLocked(c, category);
                        store.set(c.path("coin").asText(), fl[0], fl[1]);
                    }
                store.markSynced();
                walletSeen = true;
            }
            case "position" -> {
                PositionStore ps = positions;
                if (ps == null) return;
                for (JsonNode p : n.path("data")) {
                    if (!"linear".equals(p.path("category").asText("linear"))) continue;
                    double size = p.path("size").asDouble(0);
                    ps.set(p.path("symbol").asText(), "Sell".equals(p.path("side").asText()) ? -size : size, p.path("entryPrice").asDouble(0));
                }
            }
            default -> { }
        }
    }

    /**
     * Свободно и занято по монете кошелька. Спот: занято = locked (в ордерах).
     * Перпы: занято = начальная маржа позиций и ордеров (totalPositionIM + totalOrderIM).
     */
    static double[] freeLocked(JsonNode c, String category) {
        double total = c.path("walletBalance").asDouble(0);
        double locked = category.equals("linear")
                ? c.path("totalPositionIM").asDouble(0) + c.path("totalOrderIM").asDouble(0)
                : c.path("locked").asDouble(0);
        return new double[]{Math.max(0, total - locked), locked};
    }

    /** Статус Bybit в наш. */
    static String status(String s) {
        return switch (s) {
            case "New", "Untriggered" -> "NEW";
            case "PartiallyFilled" -> "PARTIALLY_FILLED";
            case "Filled" -> "FILLED";
            case "Cancelled", "PartiallyFilledCanceled", "Deactivated" -> "CANCELED";
            case "Rejected" -> "REJECTED";
            default -> s.toUpperCase();
        };
    }
}
