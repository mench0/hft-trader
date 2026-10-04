package com.hft.exchange.bingx;

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
import java.util.Map;

/**
 * BingX спот v1. Не проверялся на живой бирже.
 *
 * Особенности:
 *  - ключ в X-BX-APIKEY, подпись HMAC-SHA256 (hex) от строки параметров, параметры в query;
 *  - символ пишется через дефис: BTC-USDT;
 *  - ответ {code, msg, data}; успех — code 0;
 *  - market BUY задаётся суммой (quoteOrderQty), market SELL — объёмом (quantity).
 */
public final class BingxRestClient extends SignedCexClient {

    /** Логгер. */
    private static final Logger log = LoggerFactory.getLogger(BingxRestClient.class);

    /**
     * @param config подключение и параметры биржи
     * @param credentials ключи из окружения
     * @param filters правила символов
     */
    public BingxRestClient(ExchangeConfig config, Credentials credentials, SymbolFilters filters) {
        super("bingx", config, credentials, filters);
    }

    /** Имя символа на бирже. */
    static String sym(String symbol) {
        return BalanceStore.baseAsset(symbol) + "-" + BalanceStore.quoteAsset(symbol);
    }

    // ------------------------------------------------------------ HTTP

    /** Публичный GET. */
    private JsonNode publicGet(String path) throws Exception {
        return exec(req(baseUrl + path).GET().build(), false);
    }

    /** Подписанный запрос. */
    private JsonNode signed(String method, String path, Map<String, String> p, boolean order) throws Exception {
        credentials.require();
        p.put("timestamp", String.valueOf(System.currentTimeMillis()));
        p.put("recvWindow", String.valueOf(config.recvWindowMs()));
        String q = query(p);
        String url = baseUrl + path + "?" + q + "&signature=" + Hmac.sha256Hex(credentials.apiSecret(), q);
        HttpRequest r = req(url).header("X-BX-APIKEY", credentials.apiKey())
                .method(method, HttpRequest.BodyPublishers.noBody()).build();
        return exec(r, order);
    }

    /** Ошибка в HTTP-коде или теле ответа — ApiException (лимит — с признаком rateLimit). */
    @Override
    protected void checkError(int http, JsonNode body) {
        if (http == 429) throw new ApiException(http, "429", "слишком часто", true);
        int code = body.path("code").asInt(0);
        if (http >= 400 || code != 0) {
            String msg = body.path("msg").asText(body.toString());
            boolean rl = code == 100410 || msg.toLowerCase().contains("rate limit");
            throw new ApiException(http, String.valueOf(code), msg, rl);
        }
    }

    // ------------------------------------------------------------ ордера

    /** Отправить ордер (по WebSocket, если можно, иначе REST); исход ждёт вызывающий. */
    @Override
    protected OrderResult placeRaw(Order o) throws Exception {
        Map<String, String> p = params();
        p.put("symbol", sym(o.symbol()));
        p.put("side", o.side() == Side.BUY ? "BUY" : "SELL");
        p.put("newClientOrderId", o.clientId());
        if (o.type() == Type.MARKET) {
            p.put("type", "MARKET");
            if (o.qtyIsQuote()) p.put("quoteOrderQty", plain(o.qty(), 8));
            else p.put("quantity", plain(o.qty(), filters.quantityScale(o.symbol())));
        } else {
            p.put("type", "LIMIT");
            p.put("timeInForce", o.tif().name());
            p.put("quantity", plain(o.qty(), filters.quantityScale(o.symbol())));
            p.put("price", plain(o.price(), filters.priceScale(o.symbol())));
        }
        JsonNode d = signed("POST", "/openApi/spot/v1/trade/order", p, true).path("data");
        String id = d.path("orderId").asText("");
        if (id.isEmpty()) throw new ApiException(200, "NO_ID", "нет orderId в ответе: " + d, false);
        return new OrderResult(registerId(id), o.clientId(), o.symbol(), o.side(), "NEW",
                o.qtyIsQuote() ? 0 : o.qty(), 0, 0, 0);
    }

    /** Статус ордера: из WS-потока, если есть, иначе запрос к бирже. */
    @Override
    public OrderResult orderStatus(String symbol, long orderId) throws Exception {
        Map<String, String> p = params();
        p.put("symbol", sym(symbol));
        p.put("orderId", venueId(orderId));
        JsonNode o = signed("GET", "/openApi/spot/v1/trade/query", p, false).path("data");
        double exec = d(o, "executedQty"), quoteSum = d(o, "cummulativeQuoteQty");
        String status = switch (o.path("status").asText()) {
            case "NEW", "PENDING" -> "NEW";
            case "PARTIALLY_FILLED" -> "PARTIALLY_FILLED";
            case "FILLED" -> "FILLED";
            case "CANCELED", "CANCELLED", "FAILED" -> "CANCELED";
            default -> o.path("status").asText().toUpperCase();
        };
        Side side = "BUY".equals(o.path("side").asText()) ? Side.BUY : Side.SELL;
        return new OrderResult(orderId, o.path("clientOrderID").asText(""), symbol.toUpperCase(), side, status,
                d(o, "origQty"), exec, exec > 0 ? quoteSum / exec : 0, 0);
    }

    /** Отменить ордер. */
    @Override
    public void cancelOrder(String symbol, long orderId) throws Exception {
        Map<String, String> p = params();
        p.put("symbol", sym(symbol));
        p.put("orderId", venueId(orderId));
        signed("POST", "/openApi/spot/v1/trade/cancel", p, true);
        log.info("[bingx] ордер {} по {} отменён", orderId, symbol);
    }

    /** Отменить все открытые ордера символа; возвращает их число. */
    @Override
    public int cancelAll(String symbol) throws Exception {
        Map<String, String> p = params();
        p.put("symbol", sym(symbol));
        JsonNode d = signed("POST", "/openApi/spot/v1/trade/cancelOpenOrders", p, true).path("data");
        JsonNode list = d.path("orders");
        int n = list.isArray() ? list.size() : 0;
        log.info("[bingx] отменено {} ордеров по {}", n, symbol);
        return n;
    }

    // ------------------------------------------------------------ правила и баланс

    /** Загрузить правила торговли символов (шаги объёма и цены, минимальная сумма). */
    @Override
    public void loadFilters(Iterable<String> symbols) throws Exception {
        JsonNode list = publicGet("/openApi/spot/v1/common/symbols").path("data").path("symbols");
        int loaded = 0;
        for (JsonNode s : list) {
            String name = s.path("symbol").asText().replace("-", "");
            boolean wanted = false;
            for (String w : symbols) if (w.equalsIgnoreCase(name)) { wanted = true; break; }
            if (!wanted) continue;
            double max = d(s, "maxQty");
            filters.put(name, new SymbolFilters.Filter(d(s, "minQty"), max > 0 ? max : Double.MAX_VALUE,
                    d(s, "stepSize"), 0, 0, d(s, "tickSize"), Math.max(d(s, "minNotional"), 1.0)));
            loaded++;
        }
        if (loaded == 0) throw new IllegalStateException("BingX: правила торговли не найдены для " + symbols);
        log.info("[bingx] правила загружены для {} символов", loaded);
    }

    /** Загрузить балансы. */
    @Override
    public void loadBalances(BalanceStore store) throws Exception {
        JsonNode r = signed("GET", "/openApi/spot/v1/account/balance", params(), false);
        for (JsonNode b : r.path("data").path("balances")) {
            double free = d(b, "free"), locked = d(b, "locked");
            if (free > 0 || locked > 0) store.set(b.path("asset").asText(), free, locked);
        }
        store.markSynced();
    }
}
