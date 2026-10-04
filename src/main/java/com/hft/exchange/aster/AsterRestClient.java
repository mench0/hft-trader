package com.hft.exchange.aster;

import com.fasterxml.jackson.databind.JsonNode;
import com.hft.config.Credentials;
import com.hft.config.ExchangeConfig;
import com.hft.crypto.EvmCrypto;
import com.hft.crypto.Hex;
import com.hft.crypto.Web3jCrypto;
import com.hft.model.OrderEnums.Side;
import com.hft.model.OrderEnums.Type;
import com.hft.model.OrderResult;
import com.hft.rest.ApiException;
import com.hft.rest.SignedCexClient;
import com.hft.store.BalanceStore;
import com.hft.store.SymbolFilters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.net.http.HttpRequest;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Aster спот, API v3 (sapi.asterdex.com/api/v3). Формат запросов и ответов — как у Binance,
 * подпись — EIP-712 кошельком-агентом. Не проверялся на живой бирже.
 *
 * Ключи: ASTER_API_KEY — адрес основного кошелька (user), ASTER_API_SECRET — приватный ключ
 * API-кошелька (signer, агент без права вывода). Адрес агента вычисляется из ключа.
 *
 * Подпись (документация Aster, «Auth v3»): к параметрам запроса добавляются nonce (микросекунды,
 * монотонно), user и signer; строка параметров подписывается как EIP-712 Message{string msg}
 * с доменом AsterSignTransaction / "1" / chainId 1666 (testnet 714) / нулевой контракт;
 * подпись 0x r‖s‖v добавляется параметром signature.
 */
public final class AsterRestClient extends SignedCexClient {

    private static final Logger log = LoggerFactory.getLogger(AsterRestClient.class);
    private static final String API = "/api/v3";

    private final EvmCrypto crypto;
    private final AtomicLong lastNonce = new AtomicLong();
    private byte[] domainSeparator;

    public AsterRestClient(ExchangeConfig config, Credentials credentials, SymbolFilters filters) {
        this(config, credentials, filters, credentials.isPresent() ? new Web3jCrypto(credentials.apiSecret()) : null);
    }

    public AsterRestClient(ExchangeConfig config, Credentials credentials, SymbolFilters filters, EvmCrypto crypto) {
        super("aster", config, credentials, filters);
        this.crypto = crypto;
    }

    // ------------------------------------------------------------ подпись

    private synchronized byte[] domainSeparator() {
        if (domainSeparator == null) {
            long chainId = config.testnet() ? 714 : 1666;
            domainSeparator = crypto.keccak256(Hex.concat(
                    crypto.keccak256(utf8("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)")),
                    crypto.keccak256(utf8("AsterSignTransaction")),
                    crypto.keccak256(utf8("1")),
                    uint256(BigInteger.valueOf(chainId)),
                    new byte[32]));                                   // verifyingContract = 0x0
        }
        return domainSeparator;
    }

    /** EIP-712 подпись строки параметров: 0x + r + s + v. */
    String sign(String msg) {
        byte[] struct = crypto.keccak256(Hex.concat(crypto.keccak256(utf8("Message(string msg)")), crypto.keccak256(utf8(msg))));
        byte[] digest = crypto.keccak256(Hex.concat(new byte[]{0x19, 0x01}, domainSeparator(), struct));
        EvmCrypto.Signature s = crypto.sign(digest);
        return "0x" + Hex.enc(s.r()) + Hex.enc(s.s()) + String.format("%02x", s.v());
    }

    /** Микросекунды, строго возрастающие (два запроса в одну микросекунду не получат одинаковый nonce). */
    private long nonce() {
        long now = System.currentTimeMillis() * 1000;
        return lastNonce.updateAndGet(prev -> Math.max(prev + 1, now));
    }

    private static byte[] utf8(String s) { return s.getBytes(StandardCharsets.UTF_8); }

    private static byte[] uint256(BigInteger v) {
        byte[] raw = v.toByteArray(), out = new byte[32];
        System.arraycopy(raw, Math.max(0, raw.length - 32), out, 32 - Math.min(32, raw.length), Math.min(32, raw.length));
        return out;
    }

    // ------------------------------------------------------------ HTTP

    private JsonNode publicGet(String path, Map<String, String> p) throws Exception {
        String q = p.isEmpty() ? "" : "?" + query(p);
        return exec(req(baseUrl + API + path + q).GET().build(), false);
    }

    private JsonNode signed(String method, String path, Map<String, String> p, boolean order) throws Exception {
        credentials.require();
        p.put("recvWindow", String.valueOf(config.recvWindowMs()));
        p.put("timestamp", String.valueOf(System.currentTimeMillis()));
        p.put("nonce", String.valueOf(nonce()));
        p.put("user", credentials.apiKey());
        p.put("signer", crypto.address());
        String q = query(p);
        String url = baseUrl + API + path + "?" + q + "&signature=" + sign(q);
        HttpRequest r = req(url).header("Content-Type", "application/x-www-form-urlencoded")
                .method(method, HttpRequest.BodyPublishers.noBody()).build();
        return exec(r, order);
    }

    @Override
    protected void checkError(int http, JsonNode body) {
        int code = body.path("code").asInt(0);
        if (http == 429 || code == -1003) throw new ApiException(http, String.valueOf(code), body.path("msg").asText("слишком часто"), true);
        if (http >= 400) throw new ApiException(http, body.path("code").asText("?"), body.path("msg").asText(body.toString()), false);
        if (body.isObject() && code < 0) throw new ApiException(http, String.valueOf(code), body.path("msg").asText(), false);
    }

    // ------------------------------------------------------------ ордера

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
            p.put("type", "LIMIT");
            p.put("timeInForce", o.tif().name());
            p.put("quantity", plain(o.qty(), filters.quantityScale(o.symbol())));
            p.put("price", plain(o.price(), filters.priceScale(o.symbol())));
        }
        JsonNode r = signed("POST", "/order", p, true);
        String id = r.path("orderId").asText("");
        if (id.isEmpty()) throw new ApiException(200, "NO_ID", "нет orderId в ответе: " + r, false);
        OrderResult parsed = fromOrder(r, registerId(id), o.symbol());
        return new OrderResult(parsed.orderId(), o.clientId(), o.symbol(), o.side(), parsed.status(),
                o.qtyIsQuote() ? 0 : o.qty(), parsed.executedQty(), parsed.avgPrice(), 0);
    }

    @Override
    public OrderResult orderStatus(String symbol, long orderId) throws Exception {
        Map<String, String> p = params();
        p.put("symbol", symbol.toUpperCase());
        p.put("orderId", venueId(orderId));
        return fromOrder(signed("GET", "/order", p, false), orderId, symbol.toUpperCase());
    }

    /** Ответ в формате Binance; сумма в котируемой валюте — cummulativeQuoteQty (спот) или cumQuote. */
    static OrderResult fromOrder(JsonNode o, long id, String symbol) {
        double exec = d(o, "executedQty");
        double quote = d(o, "cummulativeQuoteQty");
        if (quote == 0) quote = d(o, "cumQuote");
        double avg = d(o, "avgPrice");
        if (avg == 0 && exec > 0) avg = quote / exec;
        String status = switch (o.path("status").asText("NEW")) {
            case "PARTIALLY_FILLED" -> "PARTIALLY_FILLED";
            case "FILLED" -> "FILLED";
            case "CANCELED" -> "CANCELED";
            case "REJECTED" -> "REJECTED";
            case "EXPIRED" -> "EXPIRED";
            default -> "NEW";
        };
        Side side = "SELL".equals(o.path("side").asText()) ? Side.SELL : Side.BUY;
        return new OrderResult(id, o.path("clientOrderId").asText(""), symbol, side, status, d(o, "origQty"), exec, avg, 0);
    }

    @Override
    public void cancelOrder(String symbol, long orderId) throws Exception {
        Map<String, String> p = params();
        p.put("symbol", symbol.toUpperCase());
        p.put("orderId", venueId(orderId));
        signed("DELETE", "/order", p, true);
        log.info("[aster] ордер {} по {} отменён", orderId, symbol);
    }

    @Override
    public int cancelAll(String symbol) throws Exception {
        Map<String, String> p = params();
        p.put("symbol", symbol.toUpperCase());
        JsonNode r = signed("DELETE", "/allOpenOrders", p, true);
        int n = r.isArray() ? r.size() : 0;
        log.info("[aster] отменены открытые ордера по {}", symbol);
        return n;
    }

    // ------------------------------------------------------------ правила и баланс

    /** exchangeInfo в формате Binance: LOT_SIZE, PRICE_FILTER, MIN_NOTIONAL/NOTIONAL. */
    @Override
    public void loadFilters(Iterable<String> symbols) throws Exception {
        JsonNode all = publicGet("/exchangeInfo", params()).path("symbols");
        int loaded = 0;
        for (String want : symbols) {
            String w = want.toUpperCase();
            for (JsonNode s : all) {
                if (!w.equals(s.path("symbol").asText())) continue;
                double minQty = 0, maxQty = Double.MAX_VALUE, step = 0, minPrice = 0, maxPrice = 0, tick = 0, minNotional = 0;
                for (JsonNode f : s.path("filters")) {
                    switch (f.path("filterType").asText()) {
                        case "LOT_SIZE" -> { minQty = d(f, "minQty"); maxQty = d(f, "maxQty"); step = d(f, "stepSize"); }
                        case "PRICE_FILTER" -> { minPrice = d(f, "minPrice"); maxPrice = d(f, "maxPrice"); tick = d(f, "tickSize"); }
                        case "MIN_NOTIONAL", "NOTIONAL" -> minNotional = Math.max(d(f, "minNotional"), d(f, "notional"));
                        default -> {}
                    }
                }
                filters.put(w, new SymbolFilters.Filter(minQty, maxQty, step, minPrice, maxPrice, tick, minNotional > 0 ? minNotional : 1.0));
                loaded++;
            }
        }
        if (loaded == 0) throw new IllegalStateException("Aster: правила торговли не найдены");
        log.info("[aster] правила загружены для {} символов", loaded);
    }

    @Override
    public void loadBalances(BalanceStore store) throws Exception {
        JsonNode r = signed("GET", "/account", params(), false);
        for (JsonNode b : r.path("balances")) {
            double free = d(b, "free"), locked = d(b, "locked");
            if (free > 0 || locked > 0) store.set(b.path("asset").asText(), free, locked);
        }
        store.markSynced();
    }
}
