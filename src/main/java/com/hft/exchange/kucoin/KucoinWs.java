package com.hft.exchange.kucoin;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hft.model.OrderEnums.Side;
import com.hft.model.OrderResult;
import com.hft.rest.ApiException;
import com.hft.rest.WsRpcChannel;
import com.hft.rest.WsRpcChannel.Msg;
import com.hft.store.BalanceStore;
import com.hft.util.BoundedMap;
import com.hft.util.Hmac;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * KuCoin по WebSocket.
 *
 * Торговый сокет (Pro WS API, wss://wsapi.kucoin.com/v1/private):
 *  - ключ в адресе: apikey, timestamp, sign = base64(HMAC(secret, apikey + timestamp)),
 *    passphrase = base64(HMAC(secret, passphrase));
 *  - сервер присылает приветствие {sessionId, data:"welcome", …}; клиент отвечает текстом
 *    base64(HMAC(secret, приветствие)); второе сообщение с sessionId — сессия готова;
 *  - запрос {id, op:"spot.order"|"spot.cancel", args:{…}}, ответ {id, op, code:"200000", data:{…}};
 *  - пинг {"id":…,"op":"ping","timestamp":…}.
 *
 * Приватный поток (classic, через POST /api/v1/bullet-private): /spotMarket/tradeOrdersV2 —
 * изменения ордеров, /account/balance — балансы.
 *
 * ВНИМАНИЕ: формат взят из документации KuCoin без доступа к живому API. Счёт в режиме UTA
 * торгует через uta.order — здесь не поддерживается, используйте classic-счёт или wsTrade=false.
 */
final class KucoinWs {

    /** Разбор и сборка JSON. */
    private final ObjectMapper mapper = new ObjectMapper();
    /** API-ключ и секрет. */
    private final String apiKey, secret, passphrase;
    /** Номера запросов. */
    private final AtomicLong seq = new AtomicLong();
    /** Последнее состояние ордеров из потока (числовой id -> результат). */
    final Map<Long, OrderResult> streamed;
    /** Исполненная сумма в котируемой валюте по id ордера — для средней цены. */
    private final Map<String, double[]> fills = BoundedMap.create(10_000);
    /** Строковый id биржи -> числовой (из клиента). */
    private final java.util.function.ToLongFunction<String> ids;
    /** Куда пишутся балансы. */
    volatile BalanceStore balances;
    /** Баланс хотя бы раз пришёл по WS. */
    volatile boolean balanceSeen;

    final Trade trade;
    final Private priv;

    /**
     * @param tradeUrl адрес торгового сокета (без параметров)
     * @param bullet получение адреса приватного потока (bullet-private)
     */
    KucoinWs(String tradeUrl, Callable<String> bullet, String apiKey, String secret, String passphrase,
             Map<Long, OrderResult> streamed, java.util.function.ToLongFunction<String> ids) {
        this.apiKey = apiKey;
        this.secret = secret;
        this.passphrase = passphrase;
        this.streamed = streamed;
        this.ids = ids;
        this.trade = new Trade(tradeUrl);
        this.priv = new Private(bullet);
    }

    /** Новый id запроса. */
    String nextId() { return "k" + seq.incrementAndGet(); }

    /** Запрос торгового сокета. */
    String request(String id, String op, ObjectNode args) {
        ObjectNode m = mapper.createObjectNode().put("id", id).put("op", op);
        m.set("args", args);
        return m.toString();
    }

    /** data из ответа; code не 200000 — ApiException с кодом KuCoin. */
    JsonNode result(String reply) throws Exception {
        JsonNode n = mapper.readTree(reply);
        String code = n.path("code").asText("");
        if (!code.equals("200000") && !code.equals("0")) {
            throw new ApiException(200, code.isEmpty() ? "?" : code, n.path("msg").asText(reply), code.equals("429000"));
        }
        return n.path("data");
    }

    /** Новый объект JSON. */
    ObjectNode obj() { return mapper.createObjectNode(); }

    /** URL-кодирование. */
    private static String enc(String s) { return URLEncoder.encode(s, StandardCharsets.UTF_8); }

    /** Торговый сокет. */
    final class Trade implements WsRpcChannel.Protocol {
        /** Адрес без параметров. */
        private final String base;
        Trade(String base) { this.base = base; }

        @Override public String url() {
            String ts = Long.toString(System.currentTimeMillis());
            return base + (base.contains("?") ? "&" : "?") + "apikey=" + enc(apiKey) + "&timestamp=" + ts
                    + "&sign=" + enc(Hmac.sha256Base64(secret, apiKey + ts))
                    + "&passphrase=" + enc(Hmac.sha256Base64(secret, passphrase));
        }
        @Override public List<String> login() { return List.of(); }
        @Override public boolean serverInitiatedLogin() { return true; }
        @Override public List<String> subscriptions() { return List.of(); }
        @Override public String ping() { return "{\"id\":\"ping-" + seq.incrementAndGet() + "\",\"op\":\"ping\",\"timestamp\":\"" + System.currentTimeMillis() + "\"}"; }
        @Override public long pingIntervalMs() { return 15_000; }

        @Override public Msg parse(String text) throws Exception {
            JsonNode n = mapper.readTree(text);
            if (n.has("sessionId")) {
                if ("welcome".equals(n.path("data").asText())) return Msg.answer(Hmac.sha256Base64(secret, text));
                return Msg.loginOk();                          // сессия подтверждена
            }
            String id = n.path("id").asText("");
            if (id.startsWith("ping-") || "pong".equals(n.path("op").asText())) return Msg.ignore();
            if (!id.isEmpty()) return Msg.reply(id, text);
            String code = n.path("code").asText("");
            if (!code.isEmpty() && !code.equals("200000") && !code.equals("0"))
                throw new IllegalStateException("KuCoin WS API: " + code + " " + n.path("msg").asText());
            return Msg.ignore();
        }
    }

    /** Приватный поток ордеров и балансов. */
    final class Private implements WsRpcChannel.Protocol {
        /** Получение адреса с токеном (POST /api/v1/bullet-private). */
        private final Callable<String> bullet;
        Private(Callable<String> bullet) { this.bullet = bullet; }

        @Override public String url() {
            try { return bullet.call(); }
            catch (Exception e) { throw new IllegalStateException("KuCoin bullet-private: " + e.getMessage(), e); }
        }
        @Override public List<String> login() { return List.of(); }
        @Override public boolean serverInitiatedLogin() { return true; }   // ждём welcome
        @Override public List<String> subscriptions() {
            return List.of(sub("/spotMarket/tradeOrdersV2"), sub("/account/balance"));
        }
        /** Подписка на приватную тему. */
        private String sub(String topic) {
            return mapper.createObjectNode().put("id", "sub" + seq.incrementAndGet()).put("type", "subscribe")
                    .put("topic", topic).put("privateChannel", true).put("response", true).toString();
        }
        @Override public String ping() { return "{\"id\":\"p" + seq.incrementAndGet() + "\",\"type\":\"ping\"}"; }
        @Override public long pingIntervalMs() { return 15_000; }

        @Override public Msg parse(String text) throws Exception {
            JsonNode n = mapper.readTree(text);
            switch (n.path("type").asText()) {
                case "welcome": return Msg.loginOk();
                case "message": return Msg.event(text);
                case "error": throw new IllegalStateException("KuCoin private: " + n.path("code").asText() + " " + n.path("data").asText());
                default: return Msg.ignore();                   // ack, pong
            }
        }
    }

    /** Событие приватного потока: ордер или баланс. */
    void onEvent(String text) throws Exception {
        JsonNode n = mapper.readTree(text);
        String topic = n.path("topic").asText();
        JsonNode d = n.path("data");
        if (topic.startsWith("/spotMarket/tradeOrders")) {
            String venueId = d.path("orderId").asText();
            long id = ids.applyAsLong(venueId);
            double[] f = fills.computeIfAbsent(venueId, k -> new double[2]);   // [объём, сумма]
            if ("match".equals(d.path("type").asText())) {
                double q = d.path("matchSize").asDouble(0), p = d.path("matchPrice").asDouble(0);
                synchronized (f) { f[0] += q; f[1] += q * p; }
            }
            double exec = d.path("filledSize").asDouble(0);
            double avg;
            synchronized (f) { avg = f[0] > 0 ? f[1] / f[0] : 0; }
            String type = d.path("type").asText(), status = d.path("status").asText();
            String st = switch (type) {
                case "filled" -> "FILLED";
                case "canceled" -> "CANCELED";
                default -> "done".equals(status) ? (exec > 0 ? "FILLED" : "CANCELED") : exec > 0 ? "PARTIALLY_FILLED" : "NEW";
            };
            streamed.put(id, new OrderResult(id, d.path("clientOid").asText(""), d.path("symbol").asText().replace("-", ""),
                    "sell".equals(d.path("side").asText()) ? Side.SELL : Side.BUY, st, d.path("size").asDouble(0), exec, avg, 0));
        } else if (topic.startsWith("/account/balance")) {
            if (!d.path("relationEvent").asText("trade.").startsWith("trade")) return;   // только торговый счёт
            BalanceStore store = balances;
            if (store == null) return;
            store.set(d.path("currency").asText(), d.path("available").asDouble(0), d.path("hold").asDouble(0));
            store.markSynced();
            balanceSeen = true;
        }
    }
}
