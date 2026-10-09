package com.hft.exchange.uniswapv2;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hft.config.Credentials;
import com.hft.config.ExchangeConfig;
import com.hft.crypto.EvmCrypto;
import com.hft.crypto.Web3jCrypto;
import com.hft.exchange.Exchange;
import com.hft.model.OrderEnums.Side;
import com.hft.model.OrderEnums.TimeInForce;
import com.hft.model.OrderEnums.Type;
import com.hft.model.OrderResult;
import com.hft.rest.ApiException;
import com.hft.rest.SignedClient;
import com.hft.rest.WsRpcChannel;
import com.hft.store.BalanceStore;
import com.hft.store.SymbolFilters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.hft.util.BoundedMap;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Uniswap V2 и совместимые (PancakeSwap V2, SushiSwap): свопы через Router02. Не проверялся на живой сети.
 *
 * Это не биржа с книгой ордеров, поэтому методы отвечают так:
 *   рыночные покупка/продажа и лимитные IOC/FOK — реальный своп с защитой по цене (amountOutMin / amountInMax);
 *   лимитная заявка GTC и отмена ордера — невозможны (нет книги), бросают UnsupportedOperationException;
 *   cancelAll — всегда 0 (в сети нечего снимать).
 * Исполнение атомарно: транзакция либо проходит целиком, либо откатывается (газ при откате теряется).
 *
 * Конфигурация:
 *   UNISWAPV2_API_KEY / UNISWAPV2_API_SECRET (окружение) — адрес кошелька (метка) и его приватный ключ
 *                                              (отдельный горячий кошелёк с небольшой суммой, не основной!)
 *   uniRouter          — параметр биржи: адрес Router02 в нужной сети
 *   uniTokens          — параметр биржи: "WETH=0xАДРЕС:18;USDC=0xАДРЕС:6"
 *   uniSlippagePercent — параметр биржи: допустимое проскальзывание, по умолчанию 0.5
 *   restUrl биржи — RPC-узел (для защиты от сэндвичей берите приватный RPC, напр. Flashbots Protect)
 *
 * Символ WETHUSDC: base — WETH, quote — USDC. Газ платится в ETH — следите за его балансом.
 * Цена исполнения учитывает комиссию пула 0.3% автоматически (она внутри getAmountsOut).
 */
public final class UniswapV2Client extends SignedClient {

    /** Логгер. */
    private static final Logger log = LoggerFactory.getLogger(UniswapV2Client.class);

    /** Токен: адрес контракта и число знаков. */
    private record Token(String address, int decimals) {}

    /** Подпись транзакций кошельком. */
    private final EvmCrypto crypto;
    /** Адрес Router02 (параметр uniRouter). */
    private final String router;
    /** Допустимое проскальзывание свопа, доля (параметр uniSlippagePercent). */
    private final double slippage;
    /** Токены по тикеру (параметр uniTokens). */
    private final Map<String, Token> tokens = new LinkedHashMap<>();
    /** Результаты свопов (последние 10 000). */
    private final Map<Long, OrderResult> results = BoundedMap.create(MAX_TRACKED_ORDERS);
    /** Chain id сети (из eth_chainId; -1 — ещё не запрошен). */
    private volatile long chainId = -1;

    /**
     * @param config подключение и параметры биржи
     * @param credentials ключи из окружения
     * @param filters правила символов
     */
    public UniswapV2Client(ExchangeConfig config, Credentials credentials, SymbolFilters filters) {
        this(config, credentials, filters, credentials.isPresent() ? new Web3jCrypto(credentials.apiSecret()) : null,
                config.params().uniRouter(), config.params().uniTokens(), String.valueOf(config.params().uniSlippagePercent()));
    }

    public UniswapV2Client(ExchangeConfig config, Credentials credentials, SymbolFilters filters, EvmCrypto crypto,
                           String router, String tokenSpec, String slippagePct) {
        super(Exchange.UNISWAPV2.id(), config, credentials, filters);
        this.crypto = crypto;
        if (router == null || router.isBlank()) throw new IllegalStateException("Задайте параметр биржи uniRouter (адрес Router02)");
        this.router = router.toLowerCase();
        this.slippage = (slippagePct == null || slippagePct.isBlank() ? 0.5 : Double.parseDouble(slippagePct)) / 100.0;
        if (tokenSpec == null || tokenSpec.isBlank()) throw new IllegalStateException("Задайте параметр биржи uniTokens=\"WETH=0x..:18;USDC=0x..:6\"");
        for (String item : tokenSpec.split(";")) {
            String[] kv = item.trim().split("=");
            String[] f = kv[1].split(":");
            tokens.put(kv[0].trim().toUpperCase(), new Token(f[0].toLowerCase(), Integer.parseInt(f[1])));
        }
    }

    /** Токен по тикеру; неизвестный — IllegalArgumentException. */
    private Token token(String asset) {
        Token t = tokens.get(asset.toUpperCase());
        if (t == null) throw new IllegalStateException("Нет токена " + asset + " в UNISWAPV2_TOKENS");
        return t;
    }

    // ------------------------------------------------------------ JSON-RPC

    /** Номера RPC-запросов. */
    private final java.util.concurrent.atomic.AtomicLong rpcSeq = new java.util.concurrent.atomic.AtomicLong();
    /** Адрес WebSocket ноды вместо стандартного (тесты). */
    private volatile String wsUrlOverride;

    /** Адрес WebSocket ноды вместо стандартного (тесты). */
    public void setWsUrl(String url) { this.wsUrlOverride = url; }

    /** WebSocket к ноде для всех JSON-RPC вызовов; без него (или при сбое) — HTTP. Повтор по HTTP безопасен:
     *  чтения идемпотентны, а eth_sendRawTransaction с тем же raw даст тот же хеш. */
    @Override
    public void startStreams(BalanceStore store) {
        if (!wsTradeAllowed() || wsChannel != null) return;
        String url = wsUrlOverride != null ? wsUrlOverride
                : (config.wsUrl() != null && !config.wsUrl().isBlank() ? config.wsUrl() : baseUrl.replaceFirst("^http", "ws"));
        WsRpcChannel ch = new WsRpcChannel(Exchange.UNISWAPV2.id(), new RpcProtocol(url));
        wsChannel = ch;
        ch.start();
    }

    /** JSON-RPC по WebSocket ноды: без логина, ответы по id. */
    private static final class RpcProtocol implements WsRpcChannel.Protocol {
        /** Адрес WebSocket ноды. */
        private final String url;
        /** Разбор ответов RPC. */
        private final com.fasterxml.jackson.databind.ObjectMapper om = new com.fasterxml.jackson.databind.ObjectMapper();
        RpcProtocol(String url) { this.url = url; }
        @Override public String url() { return url; }
        @Override public java.util.List<String> login() { return java.util.List.of(); }
        @Override public java.util.List<String> subscriptions() { return java.util.List.of(); }
        @Override public String ping() { return "{\"jsonrpc\":\"2.0\",\"id\":\"hb\",\"method\":\"eth_blockNumber\",\"params\":[]}"; }
        @Override public long pingIntervalMs() { return 15_000; }
        @Override public WsRpcChannel.Msg parse(String text) throws Exception {
            JsonNode n = om.readTree(text);
            if (!n.has("id")) return WsRpcChannel.Msg.ignore();
            String id = n.path("id").asText();
            return id.equals("hb") ? WsRpcChannel.Msg.ignore() : WsRpcChannel.Msg.reply(id, text);
        }
    }

    /** JSON-RPC вызов: по WebSocket, если готов, иначе HTTP; ошибка RPC — исключение. */
    private JsonNode rpc(String method, Object... params) throws Exception {
        WsRpcChannel ch = wsChannel;
        if (ch != null && ch.isReady()) {
            String id = "r" + rpcSeq.incrementAndGet();
            ObjectNode m = mapper.createObjectNode();
            m.put("jsonrpc", "2.0").put("id", id).put("method", method);
            var pa = m.putArray("params");
            for (Object p : params) pa.add(mapper.valueToTree(p));
            if (method.equals("eth_sendRawTransaction")) orderLimiter.acquire(); else callLimiter.acquire();
            try {
                JsonNode r = mapper.readTree(ch.call(id, m.toString(), 8000));
                checkError(200, r);
                return r.path("result");
            } catch (WsRpcChannel.WsNotReadyException | WsRpcChannel.WsUnknownOutcomeException e) {
                wsFallbacks.incrementAndGet();      // ниже — тот же вызов по HTTP
            }
        }
        ObjectNode b = mapper.createObjectNode();
        b.put("jsonrpc", "2.0").put("id", 1).put("method", method);
        var arr = b.putArray("params");
        for (Object p : params) arr.add(mapper.valueToTree(p));
        return exec(req(baseUrl).header("Content-Type", "application/json")
                .POST(java.net.http.HttpRequest.BodyPublishers.ofString(b.toString())).build(), method.equals("eth_sendRawTransaction"))
                .path("result");
    }

    /** Ошибка в HTTP-коде или теле ответа — ApiException (лимит — с признаком rateLimit). */
    @Override
    protected void checkError(int http, JsonNode body) {
        if (http == 429) throw new ApiException(http, "429", "слишком часто", true);
        if (http >= 400) throw new ApiException(http, "?", body.toString(), false);
        JsonNode err = body.path("error");
        if (!err.isMissingNode() && !err.isNull()) {
            String msg = err.path("message").asText(err.toString());
            throw new ApiException(http, err.path("code").asText("RPC"), msg, msg.toLowerCase().contains("rate"));
        }
    }

    /** eth_call к контракту, результат hex. */
    private String ethCall(String to, String data) throws Exception {
        Map<String, String> call = new LinkedHashMap<>();
        call.put("to", to);
        call.put("data", data);
        return rpc("eth_call", call, "latest").asText();
    }

    /** hex (с 0x) в число. */
    private static BigInteger big(String hex) {
        String h = hex.startsWith("0x") ? hex.substring(2) : hex;
        return h.isEmpty() ? BigInteger.ZERO : new BigInteger(h, 16);
    }

    /** Chain id сети (запрашивается один раз). */
    private long chainId() throws Exception {
        if (chainId < 0) chainId = big(rpc("eth_chainId").asText()).longValueExact();
        return chainId;
    }

    // ------------------------------------------------------------ котировки и баланс

    /** Баланс токена кошелька в минимальных единицах. */
    BigInteger balanceOf(Token t) throws Exception {
        return big(ethCall(t.address, UniswapV2Abi.call("70a08231", crypto.address())));
    }

    /** Сколько получим за in (getAmountsOut роутера). */
    private BigInteger amountOut(BigInteger in, Token from, Token to) throws Exception {
        return lastWord(ethCall(router, UniswapV2Abi.call("d06ca61f", in, List.of(from.address, to.address))));
    }

    /** Сколько нужно отдать за out (getAmountsIn роутера). */
    private BigInteger amountIn(BigInteger out, Token from, Token to) throws Exception {
        return firstWord(ethCall(router, UniswapV2Abi.call("1f00ca74", out, List.of(from.address, to.address))));
    }

    /** Возврат uint[]: [offset, length, e0, e1, …] — берём последний элемент. */
    private static BigInteger lastWord(String hex) {
        String h = hex.startsWith("0x") ? hex.substring(2) : hex;
        int n = big(h.substring(64, 128)).intValueExact();
        return big(h.substring(128 + 64 * (n - 1), 128 + 64 * n));
    }

    /** Первое 32-байтовое слово ответа как число. */
    private static BigInteger firstWord(String hex) {
        String h = hex.startsWith("0x") ? hex.substring(2) : hex;
        return big(h.substring(128, 192));
    }

    /** Количество в минимальные единицы токена. */
    private static BigInteger units(double v, int decimals) {
        return BigDecimal.valueOf(v).movePointRight(decimals).setScale(0, RoundingMode.DOWN).toBigInteger();
    }

    /** Минимальные единицы в количество. */
    private static double human(BigInteger v, int decimals) {
        return new BigDecimal(v).movePointLeft(decimals).doubleValue();
    }

    /** Значение плюс проскальзывание (максимум к оплате). */
    private BigInteger plus(BigInteger v) { return new BigDecimal(v).multiply(BigDecimal.valueOf(1 + slippage)).setScale(0, RoundingMode.UP).toBigInteger(); }
    /** Значение минус проскальзывание (минимум к получению). */
    private BigInteger minus(BigInteger v) { return new BigDecimal(v).multiply(BigDecimal.valueOf(1 - slippage)).setScale(0, RoundingMode.DOWN).toBigInteger(); }

    // ------------------------------------------------------------ ордера

    /** Отправить ордер (по WebSocket, если можно, иначе REST); исход ждёт вызывающий. */
    @Override
    protected OrderResult placeRaw(Order o) throws Exception {
        if (crypto == null) credentials.require();
        if (o.type() == Type.LIMIT && o.tif() == TimeInForce.GTC) {
            throw new UnsupportedOperationException("AMM: лимитная заявка GTC невозможна (нет книги ордеров). Используйте IOC/FOK или рыночный ордер.");
        }
        Token base = token(BalanceStore.baseAsset(o.symbol())), quote = token(BalanceStore.quoteAsset(o.symbol()));
        boolean buy = o.side() == Side.BUY;
        Token from = buy ? quote : base, to = buy ? base : quote;

        boolean exactIn;           // true: swapExactTokensForTokens, false: swapTokensForExactTokens
        BigInteger amountA, amountB;   // exactIn: (in, minOut); иначе (out, maxIn)
        if (buy && o.qtyIsQuote()) {
            amountA = units(o.qty(), quote.decimals);
            BigInteger out = amountOut(amountA, from, to);
            exactIn = true;
            amountB = minus(out);
        } else if (buy) {
            amountA = units(o.qty(), base.decimals);
            BigInteger need = amountIn(amountA, from, to);
            exactIn = false;
            amountB = plus(need);
            if (o.type() == Type.LIMIT) {
                BigInteger cap = units(o.qty() * o.price(), quote.decimals);
                if (need.compareTo(cap) > 0) return expired(o);
                amountB = amountB.min(cap);
            }
        } else {
            amountA = units(o.qty(), base.decimals);
            BigInteger out = amountOut(amountA, from, to);
            exactIn = true;
            amountB = minus(out);
            if (o.type() == Type.LIMIT) {
                BigInteger floor = units(o.qty() * o.price(), quote.decimals);
                if (out.compareTo(floor) < 0) return expired(o);
                amountB = amountB.max(floor);
            }
        }
        BigInteger spend = exactIn ? amountA : amountB;       // сколько from максимум уйдёт

        BigInteger baseBefore = balanceOf(base), quoteBefore = balanceOf(quote);
        ensureAllowance(from, spend);
        long deadline = System.currentTimeMillis() / 1000 + 120;
        String data = exactIn
                ? UniswapV2Abi.call("38ed1739", amountA, amountB, List.of(from.address, to.address), crypto.address(), BigInteger.valueOf(deadline))
                : UniswapV2Abi.call("8803dbee", amountA, amountB, List.of(from.address, to.address), crypto.address(), BigInteger.valueOf(deadline));
        String txHash = sendTx(router, data);
        waitReceipt(txHash);

        double baseDelta = Math.abs(human(balanceOf(base).subtract(baseBefore), base.decimals));
        double quoteDelta = Math.abs(human(balanceOf(quote).subtract(quoteBefore), quote.decimals));
        double avg = baseDelta > 0 ? quoteDelta / baseDelta : 0;
        long id = registerId(txHash);
        OrderResult r = new OrderResult(id, o.clientId(), o.symbol(), o.side(), "FILLED",
                o.qtyIsQuote() ? baseDelta : o.qty(), baseDelta, avg, 0);
        results.put(id, r);
        log.info("[uniswapv2] своп {} {} {} @ {} (tx {})", o.side(), baseDelta, o.symbol(), avg, txHash);
        return r;
    }

    /** Своп не выполнен — результат EXPIRED без исполнения. */
    private OrderResult expired(Order o) {
        return new OrderResult(0, o.clientId(), o.symbol(), o.side(), "EXPIRED", o.qtyIsQuote() ? 0 : o.qty(), 0, 0, 0);
    }

    /** Разрешить роутеру тратить токен (approve), если разрешения не хватает. */
    private void ensureAllowance(Token t, BigInteger need) throws Exception {
        BigInteger have = big(ethCall(t.address, UniswapV2Abi.call("dd62ed3e", crypto.address(), router)));
        if (have.compareTo(need) >= 0) return;
        log.info("[uniswapv2] approve {} на сумму {}", t.address, need);
        waitReceipt(sendTx(t.address, UniswapV2Abi.call("095ea7b3", router, need)));   // ровно нужная сумма, не безлимит
    }

    /** Подписать и отправить транзакцию; возвращает хеш. */
    private String sendTx(String to, String data) throws Exception {
        String from = crypto.address();
        BigInteger nonce = big(rpc("eth_getTransactionCount", from, "pending").asText());
        BigInteger gasPrice = new BigDecimal(big(rpc("eth_gasPrice").asText())).multiply(BigDecimal.valueOf(1.1)).toBigInteger();
        BigInteger gas;
        try {
            Map<String, String> call = new LinkedHashMap<>();
            call.put("from", from);
            call.put("to", to);
            call.put("data", data);
            gas = new BigDecimal(big(rpc("eth_estimateGas", call).asText())).multiply(BigDecimal.valueOf(1.3)).toBigInteger();
        } catch (ApiException e) {
            throw new ApiException(200, "ESTIMATE", "транзакция не пройдёт (оценка газа не удалась): " + e.getMessage(), false);
        }
        String raw = crypto.signTransaction(chainId(), nonce, gasPrice, gas, to, BigInteger.ZERO, data);
        return rpc("eth_sendRawTransaction", raw).asText();
    }

    /** Дождаться квитанции транзакции; revert — исключение. */
    private void waitReceipt(String txHash) throws Exception {
        for (int i = 0; i < 90; i++) {
            JsonNode r = rpc("eth_getTransactionReceipt", txHash);
            if (!r.isMissingNode() && !r.isNull()) {
                if (!"0x1".equals(r.path("status").asText())) throw new ApiException(200, "REVERT", "транзакция откатилась: " + txHash, false);
                return;
            }
            Thread.sleep(1000);
        }
        throw new ApiException(200, "TIMEOUT", "транзакция не подтверждена за 90 с: " + txHash, false);
    }

    /** Статус ордера: из WS-потока, если есть, иначе запрос к бирже. */
    @Override
    public OrderResult orderStatus(String symbol, long orderId) {
        OrderResult r = results.get(orderId);
        if (r == null) throw new IllegalStateException("Ордер не найден");
        return r;
    }

    /** Отменить ордер. */
    @Override
    public void cancelOrder(String symbol, long orderId) {
        throw new UnsupportedOperationException("AMM: отменять нечего — своп уже подтверждён или отклонён");
    }

    /** Отменить все открытые ордера символа; возвращает их число. */
    @Override
    public int cancelAll(String symbol) { return 0; }

    // ------------------------------------------------------------ правила и баланс

    /** Загрузить правила торговли символов (шаги объёма и цены, минимальная сумма). */
    @Override
    public void loadFilters(Iterable<String> symbols) {
        int loaded = 0;
        for (String s : symbols) {
            Token b = token(BalanceStore.baseAsset(s));
            token(BalanceStore.quoteAsset(s));
            filters.put(s.toUpperCase(), new SymbolFilters.Filter(Math.pow(10, -Math.min(b.decimals, 8)), Double.MAX_VALUE,
                    Math.pow(10, -Math.min(b.decimals, 8)), 0, 0, 1e-9, 5.0));
            loaded++;
        }
        if (loaded == 0) throw new IllegalStateException("Uniswap: нет символов");
    }

    /** Загрузить балансы. */
    @Override
    public void loadBalances(BalanceStore store) throws Exception {
        store.set("ETH", human(big(rpc("eth_getBalance", crypto.address(), "latest").asText()), 18), 0);
        for (var e : tokens.entrySet()) store.set(e.getKey(), human(balanceOf(e.getValue()), e.getValue().decimals), 0);
        store.markSynced();
    }
}
