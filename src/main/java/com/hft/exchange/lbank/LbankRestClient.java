package com.hft.exchange.lbank;

import com.fasterxml.jackson.databind.JsonNode;
import com.hft.config.Credentials;
import com.hft.config.ExchangeConfig;
import com.hft.model.OrderEnums.Side;
import com.hft.model.OrderEnums.Type;
import com.hft.model.OrderResult;
import com.hft.rest.ApiException;
import com.hft.rest.SignedCexClient;
import com.hft.store.BalanceStore;
import com.hft.store.SymbolFilters;
import com.hft.util.Hmac;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.SecureRandom;
import java.util.HexFormat;
import java.util.Map;
import java.util.TreeMap;

/**
 * LBank v2, спот. Не проверялся на живой бирже; схема подписи записана по памяти
 * и самая рискованная из всех клиентов — если биржа отвечает «неверная подпись»,
 * сверяйте с актуальной документацией LBank, а не с этим кодом.
 *
 * Подпись: параметры (api_key, signature_method, timestamp, echostr + бизнес-параметры)
 * сортируются по ключу, склеиваются «k=v&…», берётся MD5 в верхнем регистре, затем
 * HMAC-SHA256 этой строки секретом (hex, нижний регистр) -> sign.
 * Все запросы — POST с application/x-www-form-urlencoded.
 *
 * Тип ордера кодируется в поле type: buy / sell (лимит), buy_ioc, buy_fok, buy_market, sell_market.
 * Важно: у market BUY сумма в котируемой валюте передаётся в price, а не в amount.
 * Статусы: -1 отменён, 0 активен, 1 частично исполнен, 2 исполнен.
 */
public final class LbankRestClient extends SignedCexClient {

    /** Логгер. */
    private static final Logger log = LoggerFactory.getLogger(LbankRestClient.class);
    /** Источник случайной строки echostr. */
    private static final SecureRandom RND = new SecureRandom();
    /** Алфавит echostr. */
    private static final String ALNUM = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    /**
     * @param config подключение и параметры биржи
     * @param credentials ключи из окружения
     * @param filters правила символов
     */
    public LbankRestClient(ExchangeConfig config, Credentials credentials, SymbolFilters filters) {
        super("lbank", config, credentials, filters);
    }

    /** Имя символа на бирже. */
    static String pair(String symbol) {
        return (BalanceStore.baseAsset(symbol) + "_" + BalanceStore.quoteAsset(symbol)).toLowerCase();
    }

    /** Случайная строка 35 символов для подписи LBank. */
    private static String echostr() {
        StringBuilder sb = new StringBuilder(35);
        for (int i = 0; i < 35; i++) sb.append(ALNUM.charAt(RND.nextInt(ALNUM.length())));
        return sb.toString();
    }

    /** Подпись набора параметров (без sign). Вынесено отдельно, чтобы тестировать. */
    static String sign(Map<String, String> params, String secret) {
        StringBuilder sb = new StringBuilder();
        for (var e : new TreeMap<>(params).entrySet()) {
            if (sb.length() > 0) sb.append('&');
            sb.append(e.getKey()).append('=').append(e.getValue());
        }
        try {
            byte[] md5 = MessageDigest.getInstance("MD5").digest(sb.toString().getBytes(StandardCharsets.UTF_8));
            String prepared = HexFormat.of().withUpperCase().formatHex(md5);
            return Hmac.sha256Hex(secret, prepared);
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /** Публичный GET. */
    private JsonNode publicGet(String path) throws Exception {
        return exec(req(baseUrl + path).GET().build(), false);
    }

    /** Подписанный POST (параметры формой). */
    private JsonNode signedPost(String path, Map<String, String> p, boolean order) throws Exception {
        credentials.require();
        String ts = String.valueOf(System.currentTimeMillis()), echo = echostr();
        p.put("api_key", credentials.apiKey());
        p.put("signature_method", "HmacSHA256");
        p.put("timestamp", ts);
        p.put("echostr", echo);
        p.put("sign", sign(p, credentials.apiSecret()));
        HttpRequest r = req(baseUrl + path)
                .header("Content-Type", "application/x-www-form-urlencoded")
                .header("timestamp", ts).header("signature_method", "HmacSHA256").header("echostr", echo)
                .POST(HttpRequest.BodyPublishers.ofString(query(p))).build();
        return exec(r, order);
    }

    /** Ошибка в HTTP-коде или теле ответа — ApiException (лимит — с признаком rateLimit). */
    @Override
    protected void checkError(int http, JsonNode body) {
        if (http == 429) throw new ApiException(http, "429", "слишком часто", true);
        String result = body.path("result").asText("");
        int code = body.path("error_code").asInt(0);
        if (http >= 400 || "false".equalsIgnoreCase(result) || (code != 0 && !result.equalsIgnoreCase("true"))) {
            throw new ApiException(http, String.valueOf(code), body.path("msg").asText(body.toString()), code == 10008);
        }
    }

    // ------------------------------------------------------------ ордера

    /** Отправить ордер (по WebSocket, если можно, иначе REST); исход ждёт вызывающий. */
    @Override
    protected OrderResult placeRaw(Order o) throws Exception {
        Map<String, String> p = params();
        p.put("symbol", pair(o.symbol()));
        p.put("custom_id", o.clientId());
        String dir = o.side() == Side.BUY ? "buy" : "sell";
        int qs = filters.quantityScale(o.symbol()), ps = filters.priceScale(o.symbol());
        if (o.type() == Type.MARKET) {
            p.put("type", dir + "_market");
            if (o.side() == Side.BUY) {
                if (!o.qtyIsQuote()) throw new IllegalArgumentException("LBank: market BUY задаётся суммой в котируемой валюте (buyMarketForQuote)");
                p.put("price", plain(o.qty(), 8));
            } else {
                p.put("amount", plain(o.qty(), qs));
            }
        } else {
            p.put("type", switch (o.tif()) {
                case GTC -> dir;
                case IOC -> dir + "_ioc";
                case FOK -> dir + "_fok";
            });
            p.put("price", plain(o.price(), ps));
            p.put("amount", plain(o.qty(), qs));
        }
        JsonNode r = signedPost("/v2/supplement/create_order.do", p, true);
        String id = r.path("data").path("order_id").asText("");
        if (id.isEmpty()) throw new ApiException(200, "NO_ID", "нет order_id в ответе: " + r, false);
        return new OrderResult(registerId(id), o.clientId(), o.symbol(), o.side(), "NEW",
                o.qtyIsQuote() ? 0 : o.qty(), 0, 0, 0);
    }

    /** Статус ордера: из WS-потока, если есть, иначе запрос к бирже. */
    @Override
    public OrderResult orderStatus(String symbol, long orderId) throws Exception {
        Map<String, String> p = params();
        p.put("symbol", pair(symbol));
        p.put("order_id", venueId(orderId));
        JsonNode o = signedPost("/v2/supplement/orders_info.do", p, false).path("data").path("orders").path(0);
        if (o.isMissingNode()) throw new IllegalStateException("Ордер не найден");
        double exec = d(o, "deal_amount");
        String status = switch (o.path("status").asInt(0)) {
            case 0 -> "NEW";
            case 1 -> "PARTIALLY_FILLED";
            case 2 -> "FILLED";
            case -1 -> "CANCELED";
            case 4 -> "CANCELED";
            default -> "NEW";
        };
        Side side = o.path("type").asText("").startsWith("buy") ? Side.BUY : Side.SELL;
        return new OrderResult(orderId, o.path("custom_id").asText(""), symbol.toUpperCase(), side, status,
                d(o, "amount"), exec, d(o, "avg_price"), 0);
    }

    /** Отменить ордер. */
    @Override
    public void cancelOrder(String symbol, long orderId) throws Exception {
        Map<String, String> p = params();
        p.put("symbol", pair(symbol));
        p.put("orderId", venueId(orderId));
        signedPost("/v2/supplement/cancel_order.do", p, true);
        log.info("[lbank] ордер {} по {} отменён", orderId, symbol);
    }

    /** Отменить все открытые ордера символа; возвращает их число. */
    @Override
    public int cancelAll(String symbol) throws Exception {
        Map<String, String> p = params();
        p.put("symbol", pair(symbol));
        JsonNode d = signedPost("/v2/supplement/cancel_order_by_symbol.do", p, true).path("data");
        int n = d.isArray() ? d.size() : 0;
        log.info("[lbank] отменено {} ордеров по {}", n, symbol);
        return n;
    }

    // ------------------------------------------------------------ правила и баланс

    /** /v2/accuracy.do: quantityAccuracy и priceAccuracy — количество знаков после запятой. */
    @Override
    public void loadFilters(Iterable<String> symbols) throws Exception {
        JsonNode list = publicGet("/v2/accuracy.do").path("data");
        int loaded = 0;
        for (JsonNode s : list) {
            String sym = s.path("symbol").asText().replace("_", "").toUpperCase();
            boolean wanted = false;
            for (String w : symbols) if (w.equalsIgnoreCase(sym)) { wanted = true; break; }
            if (!wanted) continue;
            double minQty = d(s, "minTranQua");
            filters.put(sym, new SymbolFilters.Filter(minQty, Double.MAX_VALUE,
                    Math.pow(10, -s.path("quantityAccuracy").asInt(6)), 0, 0,
                    Math.pow(10, -s.path("priceAccuracy").asInt(8)), 5.0));
            loaded++;
        }
        if (loaded == 0) throw new IllegalStateException("LBank: правила торговли не найдены для " + symbols);
        log.info("[lbank] правила загружены для {} символов (minNotional по умолчанию 5.0)", loaded);
    }

    /** Загрузить балансы. */
    @Override
    public void loadBalances(BalanceStore store) throws Exception {
        JsonNode r = signedPost("/v2/supplement/user_info_account.do", params(), false);
        for (JsonNode b : r.path("data").path("balances")) {
            double free = d(b, "free"), locked = d(b, "locked");
            if (free > 0 || locked > 0) store.set(b.path("asset").asText(), free, locked);
        }
        store.markSynced();
    }
}
