package com.hft.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hft.model.OrderEnums.Side;
import com.hft.model.OrderResult;
import com.hft.rest.WsRpcChannel.Msg;
import com.hft.store.BalanceStore;
import com.hft.util.BoundedMap;
import com.hft.util.Signer;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;


/**
 * <b>Binance Spot WebSocket API (ws-api/v3)</b>: ордера, отмены и статусы по одному сокету, плюс события
 * аккаунта (executionReport, outboundAccountPosition) по подписке userDataStream.subscribe.signature
 * на том же соединении. REST остаётся запасным каналом — это решает {@link BinanceRestClient}.
 * <p>
 * <b>Подпись:</b> параметры по алфавиту "k=v&k=v" (вместе с apiKey и timestamp) -> HMAC-SHA256 hex.
 * <b>Ответ:</b> {"id":…,"status":200,"result":…} или {"id":…,"status":4xx,"error":{"code":…,"msg":…}}.
 * <b>Событие:</b> {"subscriptionId":N,"event":{"e":"executionReport",…}}.
 * Сервер шлёт ping-кадры, JDK отвечает на них сам — прикладной пинг не нужен.
 * <p>
 * <b>ВНИМАНИЕ:</b> формат взят из документации Binance без доступа к живому API — проверяйте в testnet.
 */
final class BinanceWsApi implements WsRpcChannel.Protocol {

    /** Основная сеть. */
    static final String MAINNET = "wss://ws-api.binance.com:443/ws-api/v3";
    /** Тестовая сеть. */
    static final String TESTNET = "wss://ws-api.testnet.binance.vision/ws-api/v3";

    /** Разбор и сборка JSON. */
    private final ObjectMapper mapper = new ObjectMapper();
    /** Адрес сокета. */
    private final String url;
    /** API-ключ. */
    private final String apiKey;
    /** Подпись HMAC-SHA256. */
    private final Signer signer;
    /** Время с поправкой на часы биржи. */
    private final LongSupplier clock;
    /** Номера запросов. */
    private final AtomicLong seq = new AtomicLong();
    /** Последнее состояние ордеров из событий (orderId -> результат). */
    final Map<Long, OrderResult> streamed = BoundedMap.create(10_000);
    /** Куда пишутся балансы из событий. */
    volatile BalanceStore balances;
    /** Баланс хотя бы раз пришёл событием. */
    volatile boolean accountSeen;

    /**
     * @param url адрес ws-api
     * @param apiKey API-ключ
     * @param signer подпись
     * @param clock время с поправкой на часы биржи
     */
    BinanceWsApi(String url, String apiKey, Signer signer, LongSupplier clock) {
        this.url = url;
        this.apiKey = apiKey;
        this.signer = signer;
        this.clock = clock;
    }

    /** Новый id запроса. */
    String nextId() { return "b" + seq.incrementAndGet(); }

    /** Подписанный запрос: к params добавляются apiKey, timestamp и signature. */
    String request(String id, String method, Map<String, String> params) {
        TreeMap<String, String> p = new TreeMap<>(params);
        p.put("apiKey", apiKey);
        p.put("timestamp", Long.toString(clock.getAsLong()));
        StringBuilder q = new StringBuilder();
        for (var e : p.entrySet()) {
            if (q.length() > 0) q.append('&');
            q.append(e.getKey()).append('=').append(e.getValue());
        }
        ObjectNode m = mapper.createObjectNode().put("id", id).put("method", method);
        ObjectNode ps = m.putObject("params");
        p.forEach(ps::put);
        ps.put("signature", signer.sign(q.toString()));
        return m.toString();
    }

    /** result из ответа; ошибка биржи — ExchangeException с её кодом и текстом. */
    JsonNode result(String reply) throws Exception {
        JsonNode n = mapper.readTree(reply);
        int status = n.path("status").asInt(0);
        if (status != 200) {
            int http = status == 0 ? 400 : status;
            throw new BinanceRestClient.ExchangeException(http, n.path("error").toString());
        }
        return n.path("result");
    }

    // ------------------------------------------------------------ Protocol

    @Override public String url() { return url; }

    /** Логина нет: каждый запрос подписан. */
    @Override public List<String> login() { return List.of(); }

    /** Подписка на события аккаунта этим же ключом. */
    @Override public List<String> subscriptions() {
        return List.of(request("sub" + seq.incrementAndGet(), "userDataStream.subscribe.signature", Map.of()));
    }

    @Override public Msg parse(String text) throws Exception {
        JsonNode n = mapper.readTree(text);
        if (n.has("event")) return Msg.event(text);
        if (n.has("id")) {
            String id = n.path("id").asText();
            if (id.startsWith("sub")) {
                if (n.path("status").asInt() != 200) throw new IllegalStateException("Binance: подписка на события аккаунта отклонена: " + n.path("error"));
                return Msg.ignore();
            }
            return Msg.reply(id, text);
        }
        throw new IllegalStateException("Binance WS: неожиданное сообщение " + (text.length() > 120 ? text.substring(0, 120) : text));
    }

    // ------------------------------------------------------------ события

    /** executionReport — состояние ордера; outboundAccountPosition — балансы изменившихся активов. */
    void onEvent(String text) throws Exception {
        JsonNode e = mapper.readTree(text).path("event");
        switch (e.path("e").asText()) {
            case "executionReport" -> {
                long id = e.path("i").asLong();
                double exec = e.path("z").asDouble(), quote = e.path("Z").asDouble();
                streamed.put(id, new OrderResult(id, e.path("c").asText(""), e.path("s").asText(),
                        "BUY".equals(e.path("S").asText()) ? Side.BUY : Side.SELL, e.path("X").asText(),
                        e.path("q").asDouble(), exec, exec > 0 ? quote / exec : 0, 0));
            }
            case "outboundAccountPosition" -> {
                BalanceStore store = balances;
                if (store == null) return;
                for (JsonNode b : e.path("B")) store.set(b.path("a").asText(), b.path("f").asDouble(), b.path("l").asDouble());
                store.markSynced();
                accountSeen = true;
            }
            default -> { }   // balanceUpdate, listStatus и пр. — не нужны
        }
    }
}
