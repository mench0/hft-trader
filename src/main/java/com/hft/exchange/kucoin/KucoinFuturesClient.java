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

import java.net.http.HttpRequest;
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
 *   <li>Ордера, баланс, позиции — REST (исполнение рыночного ордера дочитывается статусом).</li>
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
        JsonNode d = signed("POST", "/api/v1/orders", b.toString(), true);
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

    @Override
    public OrderResult orderStatus(String symbol, long orderId) throws Exception {
        return parse(signed("GET", "/api/v1/orders/" + venueId(orderId), "", false), orderId, symbol);
    }

    @Override
    public void cancelOrder(String symbol, long orderId) throws Exception {
        signed("DELETE", "/api/v1/orders/" + venueId(orderId), "", true);
    }

    @Override
    public int cancelAll(String symbol) throws Exception {
        JsonNode d = signed("DELETE", "/api/v1/orders?symbol=" + instrument(symbol), "", true);
        int n = d.path("cancelledOrderIds").size();
        log.info("[kucoin] фьючерсы: отменено {} ордеров по {}", n, symbol);
        return n;
    }
}
