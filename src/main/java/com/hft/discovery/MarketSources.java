package com.hft.discovery;

import com.fasterxml.jackson.databind.JsonNode;
import com.hft.exchange.catalog.ExchangeCatalog;
import com.hft.exchange.catalog.ExchangeInfo;

import java.util.*;

/**
 * Источники сводок 24ч и минутных свечей по биржам. Одна сводка — один запрос на биржу;
 * свечи запрашиваются только для лучших кандидатов, с мягким лимитом.
 *
 * Сводки берутся по REST: это разовый снимок раз в N минут, а не поток (WS-каналы «все тикеры»
 * есть не у всех бирж и держали бы постоянные подписки на сотни символов ради редкого пересчёта).
 *
 * ВНИМАНИЕ: форматы ответов записаны по документации по памяти и на живых биржах не проверялись
 * (сеть сборки закрыта). Ошибка разбора одной биржи не мешает остальным — она видна в статусе.
 */
public final class MarketSources {

    private MarketSources() {}

    /** Котируемые валюты, которые берём в подбор. */
    static final List<String> QUOTES = quotes();

    private static List<String> quotes() {
        String env = System.getenv("DISCOVERY_QUOTES");
        if (env == null || env.isBlank()) return List.of("USDT", "USDC", "USD");
        List<String> out = new ArrayList<>();
        for (String q : env.split(",")) if (!q.isBlank()) out.add(q.trim().toUpperCase());
        return out;
    }

    public static List<String> supported() {
        return List.of("binance", "bybit", "okx", "gate", "mexc", "bingx", "lbank", "kucoin", "aster", "hyperliquid", "dydx");
    }

    public static Optional<MarketSource> create(String id) {
        String base = switch (id) {
            case "binance" -> "https://api.binance.com";
            case "bybit" -> "https://api.bybit.com";
            default -> ExchangeCatalog.find(id).map(ExchangeInfo::restUrl).orElse("");
        };
        return create(id, base);
    }

    public static Optional<MarketSource> create(String id, String baseUrl) {
        double rps = ExchangeCatalog.find(id).map(i -> Math.max(0.5, Math.min(2.0, i.maxRequestsPerSec() / 2))).orElse(1.0);
        Http http = new Http(id, rps);
        String b = baseUrl.replaceAll("/+$", "");
        return Optional.ofNullable(switch (id) {
            case "binance" -> new BinanceLike(id, b, http, "/api/v3/ticker/24hr", "/api/v3/klines");
            case "mexc", "aster" -> new BinanceLike(id, b, http, "/api/v3/ticker/24hr", "/api/v3/klines");
            case "kucoin" -> new Kucoin(b, http);
            case "bybit" -> new Bybit(b, http);
            case "okx" -> new Okx(b, http);
            case "gate" -> new Gate(b, http);
            case "bingx" -> new Bingx(b, http);
            case "lbank" -> new Lbank(b, http);
            case "hyperliquid" -> new Hyperliquid(b, http);
            case "dydx" -> new Dydx(b, http);
            default -> null;
        });
    }

    // ───────────────────────── helpers ─────────────────────────

    static double d(JsonNode n, String f) {
        JsonNode v = n.get(f);
        if (v == null || v.isNull()) return Double.NaN;
        if (v.isNumber()) return v.asDouble();
        String s = v.asText("");
        if (s.isEmpty()) return Double.NaN;
        try { return Double.parseDouble(s); } catch (NumberFormatException e) { return Double.NaN; }
    }

    static double d(JsonNode arr, int i) {
        JsonNode v = arr.get(i);
        if (v == null || v.isNull()) return Double.NaN;
        if (v.isNumber()) return v.asDouble();
        try { return Double.parseDouble(v.asText()); } catch (NumberFormatException e) { return Double.NaN; }
    }

    /** "BTCUSDT" / "BTC-USDT" / "btc_usdt" -> {BTC, USDT}; null — котировка не из списка. */
    static String[] split(String venue) {
        String v = venue.toUpperCase(Locale.ROOT);
        int sep = Math.max(v.indexOf('-'), v.indexOf('_'));
        if (sep > 0) {
            String b = v.substring(0, sep), q = v.substring(sep + 1);
            return QUOTES.contains(q) ? new String[]{b, q} : null;
        }
        for (String q : QUOTES) if (v.endsWith(q) && v.length() > q.length()) return new String[]{v.substring(0, v.length() - q.length()), q};
        return null;
    }

    static TickerSnapshot spot(String ex, String venue, double last, double bid, double ask, double hi, double lo, double open, double qv, long trades) {
        String[] bq = split(venue);
        if (bq == null) return null;
        return new TickerSnapshot(ex, bq[0] + bq[1], venue, bq[0], bq[1], false, last, bid, ask, hi, lo, open, qv, trades);
    }

    static double[] closes(List<double[]> rows) {
        rows.sort(Comparator.comparingDouble(r -> r[0]));
        double[] out = new double[rows.size()];
        for (int i = 0; i < out.length; i++) out[i] = rows.get(i)[1];
        return out;
    }

    // ───────────────────────── Binance / MEXC ─────────────────────────

    static final class BinanceLike implements MarketSource {
        final String id, base, tickerPath, klinePath; final Http http;
        BinanceLike(String id, String base, Http http, String tickerPath, String klinePath) { this.id = id; this.base = base; this.http = http; this.tickerPath = tickerPath; this.klinePath = klinePath; }
        public String exchange() { return id; }
        public List<TickerSnapshot> tickers() throws Exception {
            List<TickerSnapshot> out = new ArrayList<>();
            for (JsonNode t : http.get(base + tickerPath)) {
                double last = d(t, "lastPrice");
                double open = d(t, "openPrice");
                if (Double.isNaN(open)) { double ch = d(t, "priceChangePercent"); if (!Double.isNaN(ch)) open = last / (1 + ch / 100); }
                TickerSnapshot s = spot(id, t.path("symbol").asText(), last, d(t, "bidPrice"), d(t, "askPrice"),
                        d(t, "highPrice"), d(t, "lowPrice"), open, d(t, "quoteVolume"), t.path("count").asLong(-1));
                if (s != null) out.add(s);
            }
            return out;
        }
        public double[] closes1m(TickerSnapshot t, int limit) throws Exception {
            List<double[]> rows = new ArrayList<>();
            for (JsonNode k : http.get(base + klinePath + "?symbol=" + t.venueSymbol() + "&interval=1m&limit=" + limit))
                rows.add(new double[]{d(k, 0), d(k, 4)});
            return closes(rows);
        }
    }

    // ───────────────────────── KuCoin ─────────────────────────

    /** GET /api/v1/market/allTickers -> data.ticker[{symbol:"BTC-USDT",buy,sell,last,high,low,changeRate,volValue}];
     *  свечи GET /api/v1/market/candles?type=1min&symbol=BTC-USDT -> [[time(с),open,close,high,low,vol,turnover]], новые первыми. */
    static final class Kucoin implements MarketSource {
        final String base; final Http http;
        Kucoin(String base, Http http) { this.base = base; this.http = http; }
        public String exchange() { return "kucoin"; }
        public List<TickerSnapshot> tickers() throws Exception {
            List<TickerSnapshot> out = new ArrayList<>();
            for (JsonNode t : http.get(base + "/api/v1/market/allTickers").path("data").path("ticker")) {
                double last = d(t, "last"), ch = d(t, "changeRate");
                double open = Double.isNaN(ch) ? Double.NaN : last / (1 + ch);
                TickerSnapshot s = spot("kucoin", t.path("symbol").asText(), last, d(t, "buy"), d(t, "sell"),
                        d(t, "high"), d(t, "low"), open, d(t, "volValue"), -1);
                if (s != null) out.add(s);
            }
            return out;
        }
        public double[] closes1m(TickerSnapshot t, int limit) throws Exception {
            long end = System.currentTimeMillis() / 1000, start = end - limit * 60L;
            List<double[]> rows = new ArrayList<>();
            for (JsonNode k : http.get(base + "/api/v1/market/candles?type=1min&symbol=" + t.venueSymbol() + "&startAt=" + start + "&endAt=" + end).path("data"))
                rows.add(new double[]{d(k, 0), d(k, 2)});
            return closes(rows);
        }
    }

    // ───────────────────────── Bybit ─────────────────────────

    static final class Bybit implements MarketSource {
        final String base; final Http http;
        Bybit(String base, Http http) { this.base = base; this.http = http; }
        public String exchange() { return "bybit"; }
        public List<TickerSnapshot> tickers() throws Exception {
            List<TickerSnapshot> out = new ArrayList<>();
            for (JsonNode t : http.get(base + "/v5/market/tickers?category=spot").path("result").path("list")) {
                TickerSnapshot s = spot("bybit", t.path("symbol").asText(), d(t, "lastPrice"), d(t, "bid1Price"), d(t, "ask1Price"),
                        d(t, "highPrice24h"), d(t, "lowPrice24h"), d(t, "prevPrice24h"), d(t, "turnover24h"), -1);
                if (s != null) out.add(s);
            }
            return out;
        }
        public double[] closes1m(TickerSnapshot t, int limit) throws Exception {
            List<double[]> rows = new ArrayList<>();
            for (JsonNode k : http.get(base + "/v5/market/kline?category=spot&symbol=" + t.venueSymbol() + "&interval=1&limit=" + Math.min(limit, 1000)).path("result").path("list"))
                rows.add(new double[]{d(k, 0), d(k, 4)});
            return closes(rows);
        }
    }

    // ───────────────────────── OKX ─────────────────────────

    static final class Okx implements MarketSource {
        final String base; final Http http;
        Okx(String base, Http http) { this.base = base; this.http = http; }
        public String exchange() { return "okx"; }
        public List<TickerSnapshot> tickers() throws Exception {
            List<TickerSnapshot> out = new ArrayList<>();
            for (JsonNode t : http.get(base + "/api/v5/market/tickers?instType=SPOT").path("data")) {
                TickerSnapshot s = spot("okx", t.path("instId").asText(), d(t, "last"), d(t, "bidPx"), d(t, "askPx"),
                        d(t, "high24h"), d(t, "low24h"), d(t, "open24h"), d(t, "volCcy24h"), -1);
                if (s != null) out.add(s);
            }
            return out;
        }
        public double[] closes1m(TickerSnapshot t, int limit) throws Exception {
            List<double[]> rows = new ArrayList<>();
            for (JsonNode k : http.get(base + "/api/v5/market/candles?instId=" + t.venueSymbol() + "&bar=1m&limit=" + Math.min(limit, 300)).path("data"))
                rows.add(new double[]{d(k, 0), d(k, 4)});
            return closes(rows);
        }
    }

    // ───────────────────────── Gate ─────────────────────────

    static final class Gate implements MarketSource {
        final String base; final Http http;
        Gate(String base, Http http) { this.base = base; this.http = http; }
        public String exchange() { return "gate"; }
        public List<TickerSnapshot> tickers() throws Exception {
            List<TickerSnapshot> out = new ArrayList<>();
            for (JsonNode t : http.get(base + "/api/v4/spot/tickers")) {
                double last = d(t, "last"), ch = d(t, "change_percentage");
                TickerSnapshot s = spot("gate", t.path("currency_pair").asText(), last, d(t, "highest_bid"), d(t, "lowest_ask"),
                        d(t, "high_24h"), d(t, "low_24h"), Double.isNaN(ch) ? Double.NaN : last / (1 + ch / 100), d(t, "quote_volume"), -1);
                if (s != null) out.add(s);
            }
            return out;
        }
        public double[] closes1m(TickerSnapshot t, int limit) throws Exception {
            List<double[]> rows = new ArrayList<>();
            for (JsonNode k : http.get(base + "/api/v4/spot/candlesticks?currency_pair=" + t.venueSymbol() + "&interval=1m&limit=" + Math.min(limit, 1000)))
                rows.add(new double[]{d(k, 0), d(k, 2)});           // [t, объём в котировке, close, high, low, open, …]
            return closes(rows);
        }
    }

    // ───────────────────────── BingX ─────────────────────────

    static final class Bingx implements MarketSource {
        final String base; final Http http;
        Bingx(String base, Http http) { this.base = base; this.http = http; }
        public String exchange() { return "bingx"; }
        public List<TickerSnapshot> tickers() throws Exception {
            List<TickerSnapshot> out = new ArrayList<>();
            JsonNode r = http.get(base + "/openApi/spot/v1/ticker/24hr?timestamp=" + System.currentTimeMillis());
            if (r.path("code").asInt(0) != 0) throw new IllegalStateException("BingX: " + r.path("msg").asText());
            for (JsonNode t : r.path("data")) {
                TickerSnapshot s = spot("bingx", t.path("symbol").asText(), d(t, "lastPrice"), d(t, "bidPrice"), d(t, "askPrice"),
                        d(t, "highPrice"), d(t, "lowPrice"), d(t, "openPrice"), d(t, "quoteVolume"), -1);
                if (s != null) out.add(s);
            }
            return out;
        }
        public double[] closes1m(TickerSnapshot t, int limit) throws Exception {
            List<double[]> rows = new ArrayList<>();
            JsonNode r = http.get(base + "/openApi/spot/v2/market/kline?symbol=" + t.venueSymbol() + "&interval=1m&limit=" + Math.min(limit, 1000));
            for (JsonNode k : r.path("data")) rows.add(new double[]{d(k, 0), d(k, 4)});
            return closes(rows);
        }
    }

    // ───────────────────────── LBank ─────────────────────────

    static final class Lbank implements MarketSource {
        final String base; final Http http;
        Lbank(String base, Http http) { this.base = base; this.http = http; }
        public String exchange() { return "lbank"; }
        public List<TickerSnapshot> tickers() throws Exception {
            List<TickerSnapshot> out = new ArrayList<>();
            for (JsonNode t : http.get(base + "/v2/ticker/24hr.do?symbol=all").path("data")) {
                JsonNode k = t.path("ticker");
                double last = d(k, "latest"), ch = d(k, "change");
                TickerSnapshot s = spot("lbank", t.path("symbol").asText(), last, Double.NaN, Double.NaN,
                        d(k, "high"), d(k, "low"), Double.isNaN(ch) ? Double.NaN : last / (1 + ch / 100), d(k, "turnover"), -1);
                if (s != null) out.add(s);
            }
            return out;
        }
        public double[] closes1m(TickerSnapshot t, int limit) throws Exception {
            long from = System.currentTimeMillis() / 1000 - limit * 60L;
            List<double[]> rows = new ArrayList<>();
            for (JsonNode k : http.get(base + "/v2/kline.do?symbol=" + t.venueSymbol() + "&size=" + limit + "&type=minute1&time=" + from).path("data"))
                rows.add(new double[]{d(k, 0), d(k, 4)});
            return closes(rows);
        }
    }

    // ───────────────────────── Hyperliquid (перпы) ─────────────────────────

    static final class Hyperliquid implements MarketSource {
        final String base; final Http http;
        Hyperliquid(String base, Http http) { this.base = base; this.http = http; }
        public String exchange() { return "hyperliquid"; }
        public List<TickerSnapshot> tickers() throws Exception {
            JsonNode r = http.post(base + "/info", "{\"type\":\"metaAndAssetCtxs\"}");
            JsonNode universe = r.path(0).path("universe"), ctxs = r.path(1);
            List<TickerSnapshot> out = new ArrayList<>();
            for (int i = 0; i < universe.size() && i < ctxs.size(); i++) {
                String coin = universe.get(i).path("name").asText();
                JsonNode c = ctxs.get(i);
                double mid = d(c, "midPx"), mark = d(c, "markPx");
                double last = Double.isNaN(mid) ? mark : mid;
                JsonNode imp = c.path("impactPxs");                  // цены удара на стандартный объём — оценка bid/ask
                double bid = imp.isArray() ? d(imp, 0) : Double.NaN, ask = imp.isArray() ? d(imp, 1) : Double.NaN;
                out.add(new TickerSnapshot("hyperliquid", coin + "USDC", coin, coin, "USDC", true, last, bid, ask,
                        Double.NaN, Double.NaN, d(c, "prevDayPx"), d(c, "dayNtlVlm"), -1));
            }
            return out;
        }
        public double[] closes1m(TickerSnapshot t, int limit) throws Exception {
            long now = System.currentTimeMillis();
            String body = "{\"type\":\"candleSnapshot\",\"req\":{\"coin\":\"" + t.venueSymbol() + "\",\"interval\":\"1m\",\"startTime\":"
                    + (now - limit * 60_000L) + ",\"endTime\":" + now + "}}";
            List<double[]> rows = new ArrayList<>();
            for (JsonNode k : http.post(base + "/info", body)) rows.add(new double[]{d(k, "t"), d(k, "c")});
            return closes(rows);
        }
    }

    // ───────────────────────── dYdX v4 (перпы) ─────────────────────────

    static final class Dydx implements MarketSource {
        final String base; final Http http;
        Dydx(String base, Http http) { this.base = base; this.http = http; }
        public String exchange() { return "dydx"; }
        public List<TickerSnapshot> tickers() throws Exception {
            List<TickerSnapshot> out = new ArrayList<>();
            var it = http.get(base + "/v4/perpetualMarkets").path("markets").fields();
            while (it.hasNext()) {
                var e = it.next();
                JsonNode m = e.getValue();
                if (!"ACTIVE".equals(m.path("status").asText("ACTIVE"))) continue;
                String[] bq = split(e.getKey());
                if (bq == null) continue;
                double last = d(m, "oraclePrice"), ch = d(m, "priceChange24H");      // изменение в деньгах, не в %
                out.add(new TickerSnapshot("dydx", bq[0] + bq[1], e.getKey(), bq[0], bq[1], true, last, Double.NaN, Double.NaN,
                        Double.NaN, Double.NaN, Double.isNaN(ch) ? Double.NaN : last - ch, d(m, "volume24H"), m.path("trades24H").asLong(-1)));
            }
            return out;
        }
        public double[] closes1m(TickerSnapshot t, int limit) throws Exception {
            List<double[]> rows = new ArrayList<>();
            for (JsonNode k : http.get(base + "/v4/candles/perpetualMarkets/" + t.venueSymbol() + "?resolution=1MIN&limit=" + Math.min(limit, 100)).path("candles")) {
                double ts = java.time.Instant.parse(k.path("startedAt").asText()).toEpochMilli();
                rows.add(new double[]{ts, d(k, "close")});
            }
            return closes(rows);
        }
    }
}
