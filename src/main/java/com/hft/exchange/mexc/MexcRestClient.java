package com.hft.exchange.mexc;

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
 * MEXC спот v3 — по устройству почти Binance. Не проверялся на живой бирже.
 *
 * Отличия, учтённые в коде:
 *  - ключ в заголовке X-MEXC-APIKEY, подпись HMAC-SHA256 (hex) от query string;
 *  - все параметры идут в query, даже у POST/DELETE;
 *  - IOC и FOK — это значения type (IMMEDIATE_OR_CANCEL, FILL_OR_KILL), а не timeInForce;
 *  - orderId строковый и не обязательно числовой — наружу отдаём числовой псевдоним;
 *  - ошибка приходит как {code, msg} с HTTP 4xx.
 */
public final class MexcRestClient extends SignedCexClient {

    /** Логгер. */
    private static final Logger log = LoggerFactory.getLogger(MexcRestClient.class);

    /**
     * @param config подключение и параметры биржи
     * @param credentials ключи из окружения
     * @param filters правила символов
     */
    public MexcRestClient(ExchangeConfig config, Credentials credentials, SymbolFilters filters) {
        super("mexc", config, credentials, filters);
    }

    // ------------------------------------------------------------ HTTP

    /** Публичный GET. */
    private JsonNode publicGet(String path, Map<String, String> p) throws Exception {
        String q = p.isEmpty() ? "" : "?" + query(p);
        return exec(req(baseUrl + path + q).GET().build(), false);
    }

    /** Подписанный запрос. */
    private JsonNode signed(String method, String path, Map<String, String> p, boolean order) throws Exception {
        credentials.require();
        p.put("recvWindow", String.valueOf(config.recvWindowMs()));
        p.put("timestamp", String.valueOf(System.currentTimeMillis()));
        String q = query(p);
        String url = baseUrl + path + "?" + q + "&signature=" + Hmac.sha256Hex(credentials.apiSecret(), q);
        HttpRequest r = req(url).header("X-MEXC-APIKEY", credentials.apiKey())
                .header("Content-Type", "application/json")
                .method(method, HttpRequest.BodyPublishers.noBody()).build();
        return exec(r, order);
    }

    /** Ошибка в HTTP-коде или теле ответа — ApiException (лимит — с признаком rateLimit). */
    @Override
    protected void checkError(int http, JsonNode body) {
        if (http == 429 || body.path("code").asInt(0) == 429) {
            throw new ApiException(http, "429", body.path("msg").asText("слишком часто"), true);
        }
        if (http >= 400) throw new ApiException(http, body.path("code").asText("?"), body.path("msg").asText(body.toString()), false);
        // у части ответов успех приходит с HTTP 200, но с отрицательным code
        if (body.isObject() && body.has("code") && body.path("code").asInt(0) != 0 && body.path("code").asInt(0) != 200) {
            throw new ApiException(http, body.path("code").asText(), body.path("msg").asText(), false);
        }
    }

    // ------------------------------------------------------------ ордера

    /** Отправить ордер (по WebSocket, если можно, иначе REST); исход ждёт вызывающий. */
    @Override
    protected OrderResult placeRaw(Order o) throws Exception {
        Map<String, String> p = params();
        p.put("symbol", o.symbol());
        p.put("side", o.side() == Side.BUY ? "BUY" : "SELL");
        p.put("newClientOrderId", o.clientId());
        if (o.type() == Type.MARKET) {
            p.put("type", "MARKET");
            if (o.qtyIsQuote()) p.put("quoteOrderQty", plain(o.qty(), 8));
            else p.put("quantity", plain(o.qty(), filters.quantityScale(o.symbol())));
        } else {
            p.put("type", switch (o.tif()) {
                case GTC -> "LIMIT";
                case IOC -> "IMMEDIATE_OR_CANCEL";
                case FOK -> "FILL_OR_KILL";
            });
            p.put("quantity", plain(o.qty(), filters.quantityScale(o.symbol())));
            p.put("price", plain(o.price(), filters.priceScale(o.symbol())));
        }
        JsonNode r = signed("POST", "/api/v3/order", p, true);
        String id = r.path("orderId").asText("");
        if (id.isEmpty()) throw new ApiException(200, "NO_ID", "нет orderId в ответе: " + r, false);
        return new OrderResult(registerId(id), o.clientId(), o.symbol(), o.side(), "NEW",
                o.qtyIsQuote() ? 0 : o.qty(), 0, 0, 0);
    }

    /** Статус ордера: из WS-потока, если есть, иначе запрос к бирже. */
    @Override
    public OrderResult orderStatus(String symbol, long orderId) throws Exception {
        Map<String, String> p = params();
        p.put("symbol", symbol.toUpperCase());
        p.put("orderId", venueId(orderId));
        JsonNode o = signed("GET", "/api/v3/order", p, false);
        double exec = d(o, "executedQty");
        double quoteSum = d(o, "cummulativeQuoteQty");
        String status = switch (o.path("status").asText()) {
            case "NEW" -> "NEW";
            case "PARTIALLY_FILLED" -> "PARTIALLY_FILLED";
            case "FILLED" -> "FILLED";
            case "CANCELED", "PARTIALLY_CANCELED" -> "CANCELED";
            case "REJECTED" -> "REJECTED";
            default -> o.path("status").asText().toUpperCase();
        };
        Side side = "BUY".equals(o.path("side").asText()) ? Side.BUY : Side.SELL;
        return new OrderResult(orderId, o.path("clientOrderId").asText(""), symbol.toUpperCase(), side, status,
                d(o, "origQty"), exec, exec > 0 ? quoteSum / exec : 0, 0);
    }

    /** Отменить ордер. */
    @Override
    public void cancelOrder(String symbol, long orderId) throws Exception {
        Map<String, String> p = params();
        p.put("symbol", symbol.toUpperCase());
        p.put("orderId", venueId(orderId));
        signed("DELETE", "/api/v3/order", p, true);
        log.info("[mexc] ордер {} по {} отменён", orderId, symbol);
    }

    /** Отменить все открытые ордера символа; возвращает их число. */
    @Override
    public int cancelAll(String symbol) throws Exception {
        Map<String, String> p = params();
        p.put("symbol", symbol.toUpperCase());
        JsonNode r = signed("DELETE", "/api/v3/openOrders", p, true);
        int n = r.isArray() ? r.size() : 0;
        log.info("[mexc] отменено {} ордеров по {}", n, symbol);
        return n;
    }

    // ------------------------------------------------------------ правила и баланс

    /**
     * В exchangeInfo MEXC нет явного stepSize: шаг объёма берём из baseSizePrecision
     * (строка «0.0001») или 10^-baseAssetPrecision, шаг цены — 10^-quoteAssetPrecision.
     * Если у символа формат другой, правила загрузятся неточными — сверьте с биржей.
     */
    @Override
    public void loadFilters(Iterable<String> symbols) throws Exception {
        int loaded = 0;
        for (String want : symbols) {
            Map<String, String> p = params();
            p.put("symbol", want.toUpperCase());
            JsonNode s = publicGet("/api/v3/exchangeInfo", p).path("symbols").path(0);
            if (s.isMissingNode()) continue;
            double step = d(s, "baseSizePrecision");
            if (step <= 0 || step >= 1 && s.path("baseSizePrecision").asText().matches("\\d+")) {
                step = Math.pow(10, -s.path("baseAssetPrecision").asInt(6));
            }
            double tick = Math.pow(10, -s.path("quoteAssetPrecision").asInt(s.path("quotePrecision").asInt(8)));
            double minNotional = d(s, "quoteAmountPrecision");
            filters.put(want.toUpperCase(), new SymbolFilters.Filter(0, Double.MAX_VALUE, step, 0, 0, tick,
                    minNotional > 0 ? minNotional : 1.0));
            loaded++;
        }
        if (loaded == 0) throw new IllegalStateException("MEXC: правила торговли не найдены");
        log.info("[mexc] правила загружены для {} символов", loaded);
    }

    /** Загрузить балансы. */
    @Override
    public void loadBalances(BalanceStore store) throws Exception {
        JsonNode r = signed("GET", "/api/v3/account", params(), false);
        for (JsonNode b : r.path("balances")) {
            double free = d(b, "free"), locked = d(b, "locked");
            if (free > 0 || locked > 0) store.set(b.path("asset").asText(), free, locked);
        }
        store.markSynced();
    }
}
