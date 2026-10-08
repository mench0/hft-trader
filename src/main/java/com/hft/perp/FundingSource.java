package com.hft.perp;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hft.exchange.Exchange;
import com.hft.rest.RateBudget;
import com.hft.store.BalanceStore;
import com.hft.store.FundingStore.Funding;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Ставки funding по публичному REST биржи (ключи не нужны — работает и в бумажном режиме).
 *
 * Почему REST, а не WebSocket: ставка меняется медленно и списывается раз в 1–8 часов, опрос раз в
 * {@code fundingPollSec} (по умолчанию 30 с) одним запросом на все символы даёт ту же точность без
 * ещё одного сокета на биржу. У OKX ставка запрашивается по символу.
 */
public abstract class FundingSource {

    /** Разбор JSON. */
    protected static final ObjectMapper JSON = new ObjectMapper();
    /** HTTP-клиент на все источники. */
    private static final HttpClient HTTP = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();

    /** Биржа. */
    protected final String exchangeId;
    /** REST-адрес фьючерсов. */
    protected final String baseUrl;
    /** Общий бюджет запросов биржи. */
    private final RateBudget budget;

    /**
     * @param exchangeId биржа
     * @param baseUrl REST-адрес фьючерсов
     */
    protected FundingSource(String exchangeId, String baseUrl) {
        this.exchangeId = exchangeId;
        this.baseUrl = baseUrl;
        this.budget = RateBudget.of(exchangeId);
    }

    /** Источник ставок биржи; null — у биржи нет фьючерсов. */
    public static FundingSource forExchange(String id, String baseUrl) {
        return switch (Exchange.find(id).orElse(null)) {
            case BINANCE -> new Binance(Exchange.BINANCE, baseUrl);
            case ASTER -> new Binance(Exchange.ASTER, baseUrl);            // формат Binance USDⓈ-M
            case GATE -> new Gate(baseUrl);
            case KUCOIN -> new Kucoin(baseUrl);
            case MEXC -> new Mexc(baseUrl);
            case BYBIT -> new Bybit(baseUrl);
            case OKX -> new Okx(baseUrl);
            case HYPERLIQUID -> new Hyperliquid(baseUrl);
            case null, default -> null;
        };
    }

    /** Ставки по нашим символам (BTCUSDT). Символы, которых нет у биржи, пропускаются. */
    public abstract Map<String, Funding> fetch(Collection<String> symbols) throws Exception;

    /** GET и разбор JSON; HTTP-ошибка — исключение. */
    protected JsonNode get(String pathAndQuery) throws Exception {
        return send(HttpRequest.newBuilder(URI.create(baseUrl + pathAndQuery)).timeout(Duration.ofSeconds(10)).GET().build());
    }

    /** POST JSON и разбор ответа. */
    protected JsonNode post(String path, String body) throws Exception {
        return send(HttpRequest.newBuilder(URI.create(baseUrl + path)).timeout(Duration.ofSeconds(10))
                .header("Content-Type", "application/json").POST(HttpRequest.BodyPublishers.ofString(body)).build());
    }

    /** Отправить с учётом бюджета лимитов биржи. */
    private JsonNode send(HttpRequest r) throws Exception {
        budget.acquire(RateBudget.Kind.PUBLIC, 1, 5000);
        HttpResponse<String> resp = HTTP.send(r, HttpResponse.BodyHandlers.ofString());
        budget.onResponse(resp.statusCode(), resp.headers());
        if (resp.statusCode() >= 400) throw new IllegalStateException(exchangeId + " funding: HTTP " + resp.statusCode() + " " + abbreviate(resp.body()));
        return JSON.readTree(resp.body());
    }

    /** Начало текста для сообщения об ошибке. */
    private static String abbreviate(String s) { return s == null ? "" : s.length() > 200 ? s.substring(0, 200) + "…" : s; }

    /** Число из поля (строка или число); NaN — нет. */
    protected static double num(JsonNode n, String f) {
        String s = n.path(f).asText("");
        if (s.isEmpty()) return Double.NaN;
        try { return Double.parseDouble(s); } catch (NumberFormatException e) { return Double.NaN; }
    }

    // ------------------------------------------------------------------ биржи

    /** Binance USDⓈ-M: GET /fapi/v1/premiumIndex (все символы) + /fapi/v1/fundingInfo (нестандартные периоды). */
    static final class Binance extends FundingSource {
        /** Символы с периодом не 8 ч; читается раз в час. */
        private volatile Map<String, Double> intervals = Map.of();
        /** Когда читались периоды. */
        private long intervalsAt;
        Binance(Exchange ex, String baseUrl) { super(ex.id(), baseUrl); }

        @Override public Map<String, Funding> fetch(Collection<String> symbols) throws Exception {
            long now = System.currentTimeMillis();
            if (now - intervalsAt > 3_600_000) {
                try {
                    Map<String, Double> m = new HashMap<>();
                    for (JsonNode x : get("/fapi/v1/fundingInfo")) m.put(x.path("symbol").asText(), x.path("fundingIntervalHours").asDouble(8));
                    intervals = m;
                } catch (Exception ignore) { /* у testnet эндпоинта может не быть — период 8 ч */ }
                intervalsAt = now;
            }
            Map<String, Funding> out = new HashMap<>();
            for (JsonNode x : get("/fapi/v1/premiumIndex")) {
                String s = x.path("symbol").asText();
                if (!symbols.contains(s)) continue;
                out.put(s, new Funding(num(x, "lastFundingRate"), intervals.getOrDefault(s, 8.0),
                        x.path("nextFundingTime").asLong(0), num(x, "markPrice"), now));
            }
            return out;
        }
    }

    /** Gate USDT-фьючерсы: GET /api/v4/futures/usdt/contracts — funding_rate, funding_next_apply (с), funding_interval (с). */
    static final class Gate extends FundingSource {
        Gate(String baseUrl) { super(Exchange.GATE.id(), baseUrl); }

        @Override public Map<String, Funding> fetch(Collection<String> symbols) throws Exception {
            Map<String, String> byInst = new HashMap<>();
            for (String s : symbols) byInst.put(com.hft.exchange.generic.ContractSizes.instrument(Exchange.GATE, s), s);
            Map<String, Funding> out = new HashMap<>();
            long now = System.currentTimeMillis();
            for (JsonNode c : get("/api/v4/futures/usdt/contracts")) {
                String s = byInst.get(c.path("name").asText());
                if (s == null) continue;
                double interval = c.path("funding_interval").asDouble(28_800) / 3600;
                out.put(s, new Funding(num(c, "funding_rate"), interval > 0 ? interval : 8,
                        c.path("funding_next_apply").asLong(0) * 1000, num(c, "mark_price"), now));
            }
            return out;
        }
    }

    /**
     * KuCoin Futures: GET /api/v1/contracts/active — fundingFeeRate, fundingRateGranularity (мс),
     * nextFundingRateDateTime (мс; у старых ответов — nextFundingRateTime, мс до списания), markPrice.
     */
    static final class Kucoin extends FundingSource {
        Kucoin(String baseUrl) { super(Exchange.KUCOIN.id(), baseUrl); }

        @Override public Map<String, Funding> fetch(Collection<String> symbols) throws Exception {
            Map<String, String> byInst = new HashMap<>();
            for (String s : symbols) byInst.put(com.hft.exchange.generic.ContractSizes.instrument(Exchange.KUCOIN, s), s);
            JsonNode r = get("/api/v1/contracts/active");
            if (!"200000".equals(r.path("code").asText())) throw new IllegalStateException("kucoin funding: " + r.path("msg").asText());
            Map<String, Funding> out = new HashMap<>();
            long now = System.currentTimeMillis();
            for (JsonNode c : r.path("data")) {
                String s = byInst.get(c.path("symbol").asText());
                if (s == null) continue;
                double hours = c.path("fundingRateGranularity").asDouble(28_800_000) / 3_600_000;
                long next = c.path("nextFundingRateDateTime").asLong(0);
                if (next == 0 && c.has("nextFundingRateTime")) next = now + c.path("nextFundingRateTime").asLong(0);
                out.put(s, new Funding(num(c, "fundingFeeRate"), hours > 0 ? hours : 8, next, num(c, "markPrice"), now));
            }
            return out;
        }
    }

    /** MEXC Contract: GET /api/v1/contract/funding_rate/BTC_USDT — fundingRate, nextSettleTime (мс), collectCycle (ч). */
    static final class Mexc extends FundingSource {
        Mexc(String baseUrl) { super(Exchange.MEXC.id(), baseUrl); }

        @Override public Map<String, Funding> fetch(Collection<String> symbols) throws Exception {
            Map<String, Funding> out = new HashMap<>();
            long now = System.currentTimeMillis();
            for (String s : symbols) {
                JsonNode r = get("/api/v1/contract/funding_rate/" + com.hft.exchange.generic.ContractSizes.instrument(Exchange.MEXC, s));
                if (!r.path("success").asBoolean(false)) continue;               // контракта нет
                JsonNode d = r.path("data");
                double hours = d.path("collectCycle").asDouble(8);
                out.put(s, new Funding(num(d, "fundingRate"), hours > 0 ? hours : 8, d.path("nextSettleTime").asLong(0), Double.NaN, now));
            }
            return out;
        }
    }

    /** Bybit linear: GET /v5/market/tickers?category=linear (все символы). */
    static final class Bybit extends FundingSource {
        Bybit(String baseUrl) { super(Exchange.BYBIT.id(), baseUrl); }

        @Override public Map<String, Funding> fetch(Collection<String> symbols) throws Exception {
            JsonNode r = get("/v5/market/tickers?category=linear");
            if (r.path("retCode").asInt(-1) != 0) throw new IllegalStateException("bybit funding: " + r.path("retMsg").asText());
            Map<String, Funding> out = new HashMap<>();
            long now = System.currentTimeMillis();
            for (JsonNode x : r.path("result").path("list")) {
                String s = x.path("symbol").asText();
                if (!symbols.contains(s)) continue;
                double h = num(x, "fundingIntervalHour");
                out.put(s, new Funding(num(x, "fundingRate"), Double.isNaN(h) || h <= 0 ? 8 : h,
                        x.path("nextFundingTime").asLong(0), num(x, "markPrice"), now));
            }
            return out;
        }
    }

    /** OKX SWAP: GET /api/v5/public/funding-rate?instId=BTC-USDT-SWAP (по символу). */
    static final class Okx extends FundingSource {
        Okx(String baseUrl) { super(Exchange.OKX.id(), baseUrl); }

        @Override public Map<String, Funding> fetch(Collection<String> symbols) throws Exception {
            Map<String, Funding> out = new HashMap<>();
            long now = System.currentTimeMillis();
            for (String s : symbols) {
                JsonNode r = get("/api/v5/public/funding-rate?instId=" + BalanceStore.baseAsset(s) + "-" + BalanceStore.quoteAsset(s) + "-SWAP");
                if (!"0".equals(r.path("code").asText())) continue;           // инструмента нет
                JsonNode x = r.path("data").path(0);
                long t = x.path("fundingTime").asLong(0), next = x.path("nextFundingTime").asLong(0);
                double h = t > 0 && next > t ? (next - t) / 3_600_000.0 : 8;
                out.put(s, new Funding(num(x, "fundingRate"), h, t, Double.NaN, now));
            }
            return out;
        }
    }

    /** Hyperliquid: POST /info {"type":"metaAndAssetCtxs"} — ставка за час, списание каждый час. */
    static final class Hyperliquid extends FundingSource {
        Hyperliquid(String baseUrl) { super(Exchange.HYPERLIQUID.id(), baseUrl); }

        @Override public Map<String, Funding> fetch(Collection<String> symbols) throws Exception {
            JsonNode r = post("/info", "{\"type\":\"metaAndAssetCtxs\"}");
            JsonNode universe = r.path(0).path("universe"), ctxs = r.path(1);
            Map<String, String> byCoin = new HashMap<>();
            for (String s : symbols) byCoin.put(BalanceStore.baseAsset(s), s);
            Map<String, Funding> out = new HashMap<>();
            long now = System.currentTimeMillis();
            long nextHour = (now / 3_600_000 + 1) * 3_600_000;
            for (int i = 0; i < universe.size() && i < ctxs.size(); i++) {
                String s = byCoin.get(universe.get(i).path("name").asText());
                if (s == null) continue;
                JsonNode c = ctxs.get(i);
                out.put(s, new Funding(num(c, "funding"), 1, nextHour, num(c, "markPx"), now));
            }
            return out;
        }
    }
}
