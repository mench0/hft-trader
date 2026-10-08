package com.hft.exchange.gate;

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

import java.net.URI;
import java.net.http.HttpRequest;
import java.util.HashSet;
import java.util.Set;

/**
 * Gate USDT-фьючерсы (API v4, /api/v4/futures/usdt), market=perp. Не проверялся на живой бирже.
 *
 * <ul>
 *   <li>Контракт BTC_USDT; объём — целое число контрактов со знаком (+ покупка, − продажа),
 *       контракт = quanto_multiplier монет. Бот считает в монетах и пересчитывает сам.</li>
 *   <li>Рыночный ордер — price "0" и tif "ioc"; reduce_only — закрытие.</li>
 *   <li>Подпись та же, что у спота: HMAC-SHA512(method\npath\nquery\nsha512(body)\nts) в заголовках KEY/SIGN/Timestamp.</li>
 *   <li>Ордера, баланс, позиции — REST (исполнение рыночного ордера дочитывается статусом).</li>
 * </ul>
 */
public final class GateFuturesClient extends SignedCexClient {

    /** Логгер. */
    private static final Logger log = LoggerFactory.getLogger(GateFuturesClient.class);
    /** Префикс путей. */
    private static final String API = "/api/v4";
    /** Расчётная валюта. */
    private static final String SETTLE = "/futures/usdt";

    /**
     * @param config подключение (restUrl — api.gateio.ws или fx-api-testnet)
     * @param credentials GATE_API_KEY/_SECRET
     * @param filters правила символов
     */
    public GateFuturesClient(ExchangeConfig config, Credentials credentials, SymbolFilters filters) {
        super(Exchange.GATE.id(), config, credentials, filters);
    }

    @Override public boolean isPerp() { return true; }

    /** Имя контракта: BTCUSDT -> BTC_USDT. */
    private static String contract(String symbol) { return ContractSizes.instrument(Exchange.GATE, symbol); }

    /** Размер контракта в монетах. */
    private double mult(String symbol) { return ContractSizes.get(Exchange.GATE, baseUrl, contract(symbol)); }

    // ------------------------------------------------------------ HTTP

    /** Публичный GET. */
    private JsonNode publicGet(String path) throws Exception {
        return exec(req(baseUrl + API + path).header("Accept", "application/json").GET().build(), false);
    }

    /** Подписанный запрос. */
    private JsonNode signed(String method, String path, String query, String body, boolean order) throws Exception {
        credentials.require();
        String ts = String.valueOf(System.currentTimeMillis() / 1000);
        String payload = method + "\n" + API + path + "\n" + query + "\n" + Hmac.sha512HexOf(body) + "\n" + ts;
        String url = baseUrl + API + path + (query.isEmpty() ? "" : "?" + query);
        HttpRequest.Builder b = HttpRequest.newBuilder(URI.create(url)).timeout(java.time.Duration.ofSeconds(10))
                .header("KEY", credentials.apiKey())
                .header("SIGN", Hmac.sha512Hex(credentials.apiSecret(), payload))
                .header("Timestamp", ts)
                .header("Accept", "application/json")
                .header("Content-Type", "application/json");
        b.method(method, body.isEmpty() ? HttpRequest.BodyPublishers.noBody() : HttpRequest.BodyPublishers.ofString(body));
        return exec(b.build(), order);
    }

    /** Ошибка Gate: {label, message}. */
    @Override
    protected void checkError(int http, JsonNode body) {
        String label = body.path("label").asText("");
        if (http == 429 || "TOO_MANY_REQUESTS".equals(label)) throw new ApiException(http, label, body.path("message").asText("слишком часто"), true);
        if (http >= 400 || !label.isEmpty()) throw new ApiException(http, label.isEmpty() ? "?" : label, body.path("message").asText(body.toString()), false);
    }

    // ------------------------------------------------------------ правила, баланс, позиции, плечо

    /** Правила: шаг объёма = 1 контракт (quanto_multiplier монет), шаг цены = order_price_round. */
    @Override
    public void loadFilters(Iterable<String> symbols) throws Exception {
        Set<String> want = new HashSet<>();
        for (String s : symbols) want.add(contract(s.toUpperCase()));
        int loaded = 0;
        for (JsonNode c : publicGet(SETTLE + "/contracts")) {
            String name = c.path("name").asText();
            double m = d(c, "quanto_multiplier");
            ContractSizes.put(Exchange.GATE, name, m);
            if (!want.contains(name)) continue;
            double max = d(c, "order_size_max");
            filters.put(name.replace("_", ""), new SymbolFilters.Filter(d(c, "order_size_min") * m, max > 0 ? max * m : Double.MAX_VALUE,
                    m, 0, 0, d(c, "order_price_round"), 1.0));
            loaded++;
        }
        if (loaded == 0) throw new IllegalStateException("Gate futures: контракты не найдены для " + symbols);
        log.info("[gate] фьючерсы: правила загружены для {} контрактов", loaded);
    }

    /** Баланс фьючерсного счёта: available — свободно, остальное — маржа. */
    @Override
    public void loadBalances(BalanceStore store) throws Exception {
        JsonNode a = signed("GET", SETTLE + "/accounts", "", "", false);
        double total = d(a, "total"), free = d(a, "available");
        store.set(a.path("currency").asText("USDT"), free, Math.max(0, total - free));
        store.markSynced();
    }

    /** Плечо контракта (0 у Gate — кросс-маржа, поэтому задаём явно). */
    @Override
    public void setLeverage(String symbol, int leverage) throws Exception {
        signed("POST", SETTLE + "/positions/" + contract(symbol) + "/leverage", "leverage=" + leverage, "", false);
        log.info("[gate] плечо {}x для {}", leverage, symbol);
    }

    /** Открытые позиции: size — контракты со знаком. */
    @Override
    public void loadPositions(PositionStore store) throws Exception {
        Set<String> seen = new HashSet<>();
        for (JsonNode p : signed("GET", SETTLE + "/positions", "", "", false)) {
            double size = d(p, "size");
            if (size == 0) continue;
            String c = p.path("contract").asText(), sym = c.replace("_", "");
            store.set(sym, size * ContractSizes.get(Exchange.GATE, baseUrl, c), d(p, "entry_price"));
            seen.add(sym);
        }
        for (String s : store.snapshot().keySet()) if (!seen.contains(s)) store.set(s, 0, 0);
    }

    // ------------------------------------------------------------ ордера

    /** Монеты -> целое число контрактов со знаком стороны. */
    private long contracts(Order o) {
        long n = (long) Math.floor(o.qty() / mult(o.symbol()) + 1e-9);
        if (n <= 0) throw new IllegalArgumentException("Gate futures: объём меньше одного контракта");
        return o.side() == Side.BUY ? n : -n;
    }

    @Override
    protected OrderResult placeRaw(Order o) throws Exception {
        if (o.qtyIsQuote()) throw new IllegalArgumentException("Gate futures: ордер на сумму не поддерживается — задайте объём");
        ObjectNode b = mapper.createObjectNode()
                .put("contract", contract(o.symbol()))
                .put("size", contracts(o))
                .put("text", "t-" + o.clientId());
        if (o.type() == Type.MARKET) {
            b.put("price", "0").put("tif", "ioc");
        } else {
            b.put("price", plain(o.price(), filters.priceScale(o.symbol())))
             .put("tif", switch (o.tif()) { case GTC -> "gtc"; case IOC -> "ioc"; case FOK -> "fok"; });
        }
        if (o.reduceOnly()) b.put("reduce_only", true);
        JsonNode r = signed("POST", SETTLE + "/orders", "", b.toString(), true);
        return parse(r, o.symbol());
    }

    /** Ответ об ордере: size/left — контракты; finished + left 0 — исполнен. */
    private OrderResult parse(JsonNode r, String symbol) {
        long id = r.path("id").asLong();
        double m = mult(symbol);
        double size = Math.abs(d(r, "size")), left = Math.abs(d(r, "left"));
        double exec = (size - left) * m;
        String status;
        if ("finished".equals(r.path("status").asText())) status = left == 0 ? "FILLED" : "CANCELED";
        else status = exec > 0 ? "PARTIALLY_FILLED" : "NEW";
        Side side = d(r, "size") < 0 ? Side.SELL : Side.BUY;
        String text = r.path("text").asText("");
        return new OrderResult(id, text.startsWith("t-") ? text.substring(2) : text, symbol.toUpperCase(), side, status,
                size * m, exec, d(r, "fill_price"), 0);
    }

    @Override
    public OrderResult orderStatus(String symbol, long orderId) throws Exception {
        return parse(signed("GET", SETTLE + "/orders/" + orderId, "", "", false), symbol);
    }

    @Override
    public void cancelOrder(String symbol, long orderId) throws Exception {
        signed("DELETE", SETTLE + "/orders/" + orderId, "", "", true);
    }

    @Override
    public int cancelAll(String symbol) throws Exception {
        JsonNode r = signed("DELETE", SETTLE + "/orders", "contract=" + contract(symbol), "", true);
        int n = r.isArray() ? r.size() : 0;
        log.info("[gate] фьючерсы: отменено {} ордеров по {}", n, symbol);
        return n;
    }
}
