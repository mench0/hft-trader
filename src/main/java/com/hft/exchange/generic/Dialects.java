package com.hft.exchange.generic;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hft.exchange.Exchange;
import com.hft.exchange.generic.BookDialect.ParsedBook;
import com.hft.store.BalanceStore;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URI;
import java.net.http.HttpRequest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Диалекты публичных стаканов. ВАЖНО: форматы ответов записаны по памяти из
 * документации бирж и ни разу не сверялись с живым API (хосты были закрыты
 * сетевой политикой). Каждый диалект нужно прогнать на реальном ответе,
 * прежде чем верить цифрам. Парсеры намеренно строгие: при неожиданной
 * структуре кидают исключение, а не молча отдают пустой стакан.
 */
public final class Dialects {

    /** Утилитный класс — экземпляры не создаются. */
    private Dialects() {}

    /** Сборка и разбор JSON (только для редких сообщений; стакан — потоково). */
    private static final ObjectMapper JSON = new ObjectMapper();
    /** Таймаут REST-запроса стакана. */
    private static final Duration TIMEOUT = Duration.ofSeconds(3);

    /** Диалект биржи с её параметрами (для Uniswap — пулы из uniPools). */
    public static BookDialect forExchange(String id, com.hft.config.ExchangeConfig cfg) {
        if (cfg.params().isPerp()) {
            if (Exchange.BINANCE.is(id)) return new Aster("/fapi/v1/depth");
            if (Exchange.OKX.is(id)) return new Okx(true, cfg.restUrl());
        }
        return Exchange.UNISWAPV2.is(id) ? new UniswapV2(cfg.params().uniPools()) : forExchange(id);
    }

    /** Диалект REST-стакана биржи (без параметров: для Uniswap пулы пусты). */
    public static BookDialect forExchange(String id) {
        return switch (Exchange.find(id).orElse(null)) {
            case OKX -> new Okx(false, "");
            case MEXC -> new Mexc();
            case GATE -> new Gate();
            case HYPERLIQUID -> new Hyperliquid();
            case UNISWAPV2 -> new UniswapV2("");
            case KUCOIN -> new Kucoin();
            case ASTER -> new Aster("/api/v3/depth");
            case null, default -> throw new IllegalArgumentException("Нет диалекта для биржи: " + id);
        };
    }

    // ---------------------------------------------------------------- helpers

    /** GET с таймаутом и Accept: application/json. */
    private static HttpRequest get(String url) {
        return HttpRequest.newBuilder(URI.create(url)).timeout(TIMEOUT)
                .header("Accept", "application/json").GET().build();
    }

    /** Базовая валюта символа. */
    private static String base(String symbol) { return BalanceStore.baseAsset(symbol); }
    /** Котируемая валюта символа. */
    private static String quote(String symbol) { return BalanceStore.quoteAsset(symbol); }

    /** Уровни из массива: либо [[px, qty, ...]], либо [{pxKey:..., qtyKey:...}]. */
    private static double[][] levels(JsonNode arr, String pxKey, String qtyKey, boolean descending, int max) {
        if (arr == null || !arr.isArray()) throw new IllegalStateException("нет массива уровней");
        List<double[]> rows = new ArrayList<>();
        for (JsonNode lvl : arr) {
            double px, qty;
            if (pxKey == null) {
                px = Double.parseDouble(lvl.get(0).asText());
                qty = Double.parseDouble(lvl.get(1).asText());
            } else {
                px = Double.parseDouble(lvl.get(pxKey).asText());
                qty = Double.parseDouble(lvl.get(qtyKey).asText());
            }
            if (qty > 0 && px > 0) rows.add(new double[]{px, qty});
        }
        rows.sort((a, b) -> descending ? Double.compare(b[0], a[0]) : Double.compare(a[0], b[0]));
        int n = Math.min(max, rows.size());
        double[] p = new double[n], q = new double[n];
        for (int i = 0; i < n; i++) { p[i] = rows.get(i)[0]; q[i] = rows.get(i)[1]; }
        return new double[][]{p, q};
    }

    /** Стакан из отсортированных уровней. */
    private static ParsedBook book(double[][] bids, double[][] asks, long ts) {
        return new ParsedBook(bids[0], bids[1], asks[0], asks[1], ts);
    }

    /** Сколько уровней стакана берётся из REST-ответа. */
    private static final int MAX = 50;

    // -------------------------------------------------------------------- CEX

    /** GET /api/v5/market/books?instId=BTC-USDT&sz=20 -> {"code":"0","data":[{asks,bids,ts}]} */
    static final class Okx implements BookDialect {
        /** Перпы: инструмент -SWAP, объёмы в контрактах. */
        private final boolean swap;
        /** REST для справочника контрактов. */
        private final String restUrl;
        Okx(boolean swap, String restUrl) { this.swap = swap; this.restUrl = restUrl; }
        /** Имя инструмента. */
        private String inst(String s) { return swap ? com.hft.exchange.okx.OkxContracts.instId(s) : base(s) + "-" + quote(s); }
        /** Запрос стакана символа. */
        public HttpRequest request(String b, String s, int d) {
            return get(b + "/api/v5/market/books?instId=" + inst(s) + "&sz=" + Math.min(d, 400));
        }
        /** Разобрать ответ в стакан; ошибка биржи — исключение. */
        public ParsedBook parse(String body, String s) throws Exception {
            JsonNode r = JSON.readTree(body);
            if (!"0".equals(r.path("code").asText())) throw new IllegalStateException("OKX: " + body);
            JsonNode d = r.path("data").get(0);
            double[][] bids = levels(d.get("bids"), null, null, true, MAX), asks = levels(d.get("asks"), null, null, false, MAX);
            if (swap) {                                                  // контракты -> базовая валюта
                double ct = com.hft.exchange.okx.OkxContracts.ctVal(restUrl, inst(s));
                for (int i = 0; i < bids[1].length; i++) bids[1][i] *= ct;
                for (int i = 0; i < asks[1].length; i++) asks[1][i] *= ct;
            }
            return book(bids, asks, d.path("ts").asLong(System.currentTimeMillis()));
        }
    }

    /** GET /api/v3/depth?symbol=BTCUSDT&limit=20 -> {lastUpdateId,bids,asks} */
    static final class Mexc implements BookDialect {
        /** Запрос стакана символа. */
        public HttpRequest request(String b, String s, int d) {
            return get(b + "/api/v3/depth?symbol=" + s + "&limit=" + Math.min(d, 100));
        }
        /** Разобрать ответ в стакан; ошибка биржи — исключение. */
        public ParsedBook parse(String body, String s) throws Exception {
            JsonNode r = JSON.readTree(body);
            return book(levels(r.get("bids"), null, null, true, MAX),
                    levels(r.get("asks"), null, null, false, MAX), System.currentTimeMillis());
        }
    }

    /** GET /api/v1/market/orderbook/level2_20?symbol=BTC-USDT -> {code:"200000",data:{time,bids,asks}} */
    static final class Kucoin implements BookDialect {
        /** Запрос стакана символа. */
        public HttpRequest request(String b, String s, int d) {
            return get(b + "/api/v1/market/orderbook/" + (d <= 20 ? "level2_20" : "level2_100") + "?symbol=" + base(s) + "-" + quote(s));
        }
        /** Разобрать ответ в стакан; ошибка биржи — исключение. */
        public ParsedBook parse(String body, String s) throws Exception {
            JsonNode r = JSON.readTree(body);
            if (!"200000".equals(r.path("code").asText())) throw new IllegalStateException("KuCoin: " + body);
            JsonNode d = r.get("data");
            return book(levels(d.get("bids"), null, null, true, MAX),
                    levels(d.get("asks"), null, null, false, MAX), d.path("time").asLong(System.currentTimeMillis()));
        }
    }

    /** Aster спот: GET /api/v3/depth?symbol=BTCUSDT&limit=20 -> {lastUpdateId,E?,bids,asks} (формат Binance) */
    static final class Aster implements BookDialect {
        /** Путь стакана: /api/v3/depth (Aster) или /fapi/v1/depth (Binance USDⓈ-M). */
        private final String path;
        Aster(String path) { this.path = path; }
        /** Запрос стакана символа. */
        public HttpRequest request(String b, String s, int d) {
            int limit = d <= 5 ? 5 : d <= 10 ? 10 : d <= 20 ? 20 : d <= 50 ? 50 : 100;
            return get(b + path + "?symbol=" + s + "&limit=" + limit);
        }
        /** Разобрать ответ в стакан; ошибка биржи — исключение. */
        public ParsedBook parse(String body, String s) throws Exception {
            JsonNode r = JSON.readTree(body);
            if (r.has("code") && r.path("code").asInt(0) < 0) throw new IllegalStateException("Aster: " + body);
            return book(levels(r.get("bids"), null, null, true, MAX),
                    levels(r.get("asks"), null, null, false, MAX), r.path("E").asLong(System.currentTimeMillis()));
        }
    }

    /** GET /api/v4/spot/order_book?currency_pair=BTC_USDT&limit=20 -> {current(ms),asks,bids} */
    static final class Gate implements BookDialect {
        /** Запрос стакана символа. */
        public HttpRequest request(String b, String s, int d) {
            return get(b + "/api/v4/spot/order_book?currency_pair=" + base(s) + "_" + quote(s) + "&limit=" + Math.min(d, 100));
        }
        /** Разобрать ответ в стакан; ошибка биржи — исключение. */
        public ParsedBook parse(String body, String s) throws Exception {
            JsonNode r = JSON.readTree(body);
            return book(levels(r.get("bids"), null, null, true, MAX),
                    levels(r.get("asks"), null, null, false, MAX), r.path("current").asLong(System.currentTimeMillis()));
        }
    }

    // ------------------------------------------------------------------ perp DEX

    /** POST /info {"type":"l2Book","coin":"BTC"} -> {time, levels:[[bids],[asks]]}, уровни {px,sz,n} */
    static final class Hyperliquid implements BookDialect {
        /** Запрос стакана символа. */
        public HttpRequest request(String b, String s, int d) {
            String body = "{\"type\":\"l2Book\",\"coin\":\"" + base(s) + "\"}";
            return HttpRequest.newBuilder(URI.create(b + "/info")).timeout(TIMEOUT)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body)).build();
        }
        /** Разобрать ответ в стакан; ошибка биржи — исключение. */
        public ParsedBook parse(String body, String s) throws Exception {
            JsonNode r = JSON.readTree(body);
            JsonNode lv = r.get("levels");
            if (lv == null || lv.size() != 2) throw new IllegalStateException("Hyperliquid: " + body);
            return book(levels(lv.get(0), "px", "sz", true, MAX),
                    levels(lv.get(1), "px", "sz", false, MAX), r.path("time").asLong(System.currentTimeMillis()));
        }
    }

    // ------------------------------------------------------------------ AMM

    /**
     * Пул Uniswap V2: читаем getReserves() через eth_call и строим синтетический
     * стакан по формуле x*y=k. Пулы задаются параметром биржи uniPools
     * "WETHUSDC=0xPAIR:true:18:6;WBTCUSDC=0x...:false:8:6", где
     * поля — адрес пары, base это token0?, decimals base, decimals quote.
     * Газ и MEV не учитываются: это только оценка цены.
     */
    static final class UniswapV2 implements BookDialect {
        /** Пул: адрес пары, база — token0, десятичные базы и котируемой. */
        record Pool(String pair, boolean baseIsToken0, int decBase, int decQuote) {}
        /** Комиссия пула Uniswap V2 (0.3%) — учитывается в ценах синтетического стакана. */
        private static final double FEE = 0.003;
        /** Уровней синтетического стакана на сторону. */
        private static final int STEPS = 10;
        /** Пулы по тикеру. */
        final Map<String, Pool> pools = new HashMap<>();

        /** @param raw пулы из параметра uniPools (пусто — нет пулов) */
        UniswapV2(String raw) {
            if (raw == null || raw.isBlank()) return;
            for (String item : raw.split(";")) {
                String[] kv = item.trim().split("=");
                String[] f = kv[1].split(":");
                pools.put(kv[0].trim().toUpperCase(),
                        new Pool(f[0], Boolean.parseBoolean(f[1]), Integer.parseInt(f[2]), Integer.parseInt(f[3])));
            }
        }

        /** Запрос стакана символа. */
        public HttpRequest request(String b, String s, int d) {
            Pool p = pools.get(s.toUpperCase());
            if (p == null) throw new IllegalArgumentException("Нет пула для " + s + " в параметре uniPools");
            String body = "{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"eth_call\",\"params\":[{\"to\":\""
                    + p.pair + "\",\"data\":\"0x0902f1ac\"},\"latest\"]}";
            return HttpRequest.newBuilder(URI.create(b)).timeout(TIMEOUT)
                    .header("Content-Type", "application/json")
                    .POST(HttpRequest.BodyPublishers.ofString(body)).build();
        }

        /** Разобрать ответ в стакан; ошибка биржи — исключение. */
        public ParsedBook parse(String body, String s) throws Exception {
            Pool p = pools.get(s.toUpperCase());
            JsonNode r = JSON.readTree(body);
            String hex = r.path("result").asText("");
            if (!hex.startsWith("0x") || hex.length() < 2 + 128) throw new IllegalStateException("RPC: " + body);
            String h = hex.substring(2);
            return fromReserves(p, new BigInteger(h.substring(0, 64), 16), new BigInteger(h.substring(64, 128), 16));
        }

        /** Синтетический стакан по резервам пула (x*y=k). Общий для REST-опроса и WS-подписки на Sync. */
        ParsedBook fromReserves(Pool p, BigInteger r0, BigInteger r1) {
            BigInteger rb = p.baseIsToken0 ? r0 : r1, rq = p.baseIsToken0 ? r1 : r0;
            double base = new BigDecimal(rb).movePointLeft(p.decBase).doubleValue();
            double quote = new BigDecimal(rq).movePointLeft(p.decQuote).doubleValue();
            if (base <= 0 || quote <= 0) throw new IllegalStateException("пустой пул");

            double[] ap = new double[STEPS], aq = new double[STEPS], bp = new double[STEPS], bq = new double[STEPS];
            double prevBase = 0, prevQuote = 0;
            for (int i = 1; i <= STEPS; i++) {            // покупка base за quote
                double qIn = quote * 0.002 * i;
                double out = base - base * quote / (quote + qIn * (1 - FEE));
                ap[i - 1] = (qIn - prevQuote) / (out - prevBase);
                aq[i - 1] = out - prevBase;
                prevBase = out; prevQuote = qIn;
            }
            prevBase = 0; prevQuote = 0;
            for (int i = 1; i <= STEPS; i++) {            // продажа base за quote
                double bIn = base * 0.002 * i;
                double out = quote - base * quote / (base + bIn * (1 - FEE)) ;
                bp[i - 1] = (out - prevQuote) / (bIn - prevBase);
                bq[i - 1] = bIn - prevBase;
                prevBase = bIn; prevQuote = out;
            }
            return new ParsedBook(bp, bq, ap, aq, System.currentTimeMillis());
        }
    }

}
