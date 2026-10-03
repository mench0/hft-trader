package com.hft.exchange.binance;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hft.config.ExchangeConfig;
import com.hft.engine.TickPipeline;
import com.hft.metrics.Latency;
import com.hft.net.AbstractWsFeed;
import com.hft.store.MarketDataStore;
import com.hft.store.OrderBook;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Поток рыночных данных Binance. Netty/reconnect/ping — общий код
 * в {@link AbstractWsFeed}, здесь только формат URL и разбор JSON.
 *
 * Один комбинированный стрим на все символы:
 *   <symbol>@trade         — каждая сделка
 *   <symbol>@depth20@100ms — стакан на 20 уровней
 */
public final class BinanceMarketDataFeed extends AbstractWsFeed {

    private final ExchangeConfig config;
    private final MarketDataStore store;
    private final TickPipeline pipeline;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Latency parseLatency = new Latency("[binance] Парсинг сообщения");

    private final double[] bidPrices;
    private final double[] bidQtys;
    private final double[] askPrices;
    private final double[] askQtys;

    // Символы, на которые подписаны прямо сейчас. Изменяется через
    // addSymbol/removeSymbol, применяется при следующем (пере)подключении.
    private final List<String> activeSymbols;

    public BinanceMarketDataFeed(ExchangeConfig config, MarketDataStore store, TickPipeline pipeline) {
        this.config = config;
        this.store = store;
        this.pipeline = pipeline;
        this.activeSymbols = new CopyOnWriteArrayList<>(config.symbols());
        int d = config.bookDepth();
        this.bidPrices = new double[d];
        this.bidQtys = new double[d];
        this.askPrices = new double[d];
        this.askQtys = new double[d];
    }

    @Override
    protected String name() { return "binance"; }

    @Override
    protected URI buildUri() throws Exception {
        StringBuilder streams = new StringBuilder();
        for (String s : activeSymbols) {
            String lower = s.toLowerCase();
            if (streams.length() > 0) streams.append('/');
            streams.append(lower).append("@trade")
                   .append('/').append(lower).append("@depth").append(depthParam()).append("@100ms");
        }
        return new URI(config.wsUrl() + "/stream?streams=" + streams);
    }

    private String depthParam() {
        int d = config.bookDepth();
        if (d <= 5) return "5";
        if (d <= 10) return "10";
        return "20";
    }

    @Override
    protected void onText(String json, long receivedNanos) {
        try {
            JsonNode root = mapper.readTree(json);
            JsonNode data = root.has("data") ? root.get("data") : root;
            String stream = root.path("stream").asText("");

            if (stream.contains("@trade") || "trade".equals(data.path("e").asText())) {
                handleTrade(data, receivedNanos);
            } else if (stream.contains("@depth") || data.has("bids")) {
                handleDepth(stream, data);
            }
            parseLatency.recordSince(receivedNanos);
        } catch (Exception e) {
            log.error("Ошибка разбора сообщения Binance", e);
        }
    }

    private void handleTrade(JsonNode d, long receivedNanos) {
        String symbol = d.get("s").asText();
        double price = d.get("p").asDouble();
        double qty = d.get("q").asDouble();
        boolean buyerIsMaker = d.get("m").asBoolean();
        long eventTime = d.get("T").asLong();
        pipeline.publish(symbol, price, qty, buyerIsMaker, eventTime, receivedNanos);
    }

    private void handleDepth(String stream, JsonNode d) {
        String symbol = symbolFromStream(stream);
        if (symbol == null) return;

        OrderBook book = store.book(symbol);
        if (book == null) return;

        JsonNode bids = d.has("bids") ? d.get("bids") : d.get("b");
        JsonNode asks = d.has("asks") ? d.get("asks") : d.get("a");
        if (bids == null || asks == null) return;

        int depth = config.bookDepth();
        int bn = Math.min(bids.size(), depth);
        int an = Math.min(asks.size(), depth);

        for (int i = 0; i < bn; i++) {
            JsonNode lvl = bids.get(i);
            bidPrices[i] = lvl.get(0).asDouble();
            bidQtys[i] = lvl.get(1).asDouble();
        }
        for (int i = 0; i < an; i++) {
            JsonNode lvl = asks.get(i);
            askPrices[i] = lvl.get(0).asDouble();
            askQtys[i] = lvl.get(1).asDouble();
        }

        long updateId = d.path("lastUpdateId").asLong(0);
        book.applySnapshot(bidPrices, bidQtys, bn, askPrices, askQtys, an,
                updateId, System.currentTimeMillis());
    }

    private String symbolFromStream(String stream) {
        int at = stream.indexOf('@');
        return at > 0 ? stream.substring(0, at).toUpperCase() : null;
    }

    /** Добавить символ в подписку. Подписка обновится при следующем реконнекте. */
    public void addSymbol(String symbol) {
        String s = symbol.toUpperCase();
        if (!activeSymbols.contains(s)) {
            activeSymbols.add(s);
            log.info("Символ {} добавлен, применится при переподключении", s);
        }
    }

    public void removeSymbol(String symbol) {
        activeSymbols.remove(symbol.toUpperCase());
    }

    public List<String> activeSymbols() { return List.copyOf(activeSymbols); }
    public Latency parseLatency() { return parseLatency; }
}
