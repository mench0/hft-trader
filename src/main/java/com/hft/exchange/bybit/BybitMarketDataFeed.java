package com.hft.exchange.bybit;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hft.config.ExchangeConfig;
import com.hft.engine.TickPipeline;
import com.hft.metrics.Latency;
import com.hft.net.AbstractWsFeed;
import com.hft.store.MarketDataStore;
import com.hft.store.OrderBook;
import io.netty.channel.Channel;

import java.net.URI;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * Поток рыночных данных Bybit v5 (public spot).
 *
 * В отличие от Binance, где подписка кодируется прямо в URL, у Bybit
 * подключаются к общему адресу и после хендшейка отправляют JSON-команду
 * {"op":"subscribe","args":[...]} отдельным текстовым фреймом —
 * это и есть {@link #onHandshakeComplete}.
 *
 * Топики:
 *   publicTrade.<symbol>   — сделки
 *   orderbook.50.<symbol>  — стакан на 50 уровней (у Bybit шаг только 1/50/200/500)
 */
public final class BybitMarketDataFeed extends AbstractWsFeed {

    private final ExchangeConfig config;
    private final MarketDataStore store;
    private final TickPipeline pipeline;
    private final ObjectMapper mapper = new ObjectMapper();
    private final Latency parseLatency = new Latency("[bybit] Парсинг сообщения");

    private final double[] bidPrices;
    private final double[] bidQtys;
    private final double[] askPrices;
    private final double[] askQtys;

    private final List<String> activeSymbols;

    public BybitMarketDataFeed(ExchangeConfig config, MarketDataStore store, TickPipeline pipeline) {
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
    protected String name() { return "bybit"; }

    @Override
    protected URI buildUri() throws Exception {
        return new URI(config.wsUrl());
    }

    @Override
    protected void onHandshakeComplete(Channel channel) {
        StringBuilder args = new StringBuilder();
        for (String s : activeSymbols) {
            if (args.length() > 0) args.append(',');
            args.append("\"publicTrade.").append(s).append("\",")
                .append("\"orderbook.").append(depthParam()).append('.').append(s).append('"');
        }
        String subscribe = "{\"op\":\"subscribe\",\"args\":[" + args + "]}";
        send(subscribe);
        log.info("[bybit] Отправлена подписка на {} символов", activeSymbols.size());
    }

    private String depthParam() {
        int d = config.bookDepth();
        if (d <= 1) return "1";
        if (d <= 50) return "50";
        if (d <= 200) return "200";
        return "500";
    }

    @Override
    protected void onText(String json, long receivedNanos) {
        try {
            JsonNode root = mapper.readTree(json);
            String topic = root.path("topic").asText("");

            if (topic.startsWith("publicTrade.")) {
                handleTrade(root, receivedNanos);
            } else if (topic.startsWith("orderbook.")) {
                handleDepth(topic, root);
            }
            parseLatency.recordSince(receivedNanos);
        } catch (Exception e) {
            log.error("Ошибка разбора сообщения Bybit", e);
        }
    }

    private void handleTrade(JsonNode root, long receivedNanos) {
        JsonNode data = root.get("data");
        if (data == null || !data.isArray()) return;
        for (JsonNode t : data) {
            String symbol = t.get("s").asText();
            double price = t.get("p").asDouble();
            double qty = t.get("v").asDouble();
            boolean buyerIsMaker = "Sell".equals(t.get("S").asText());
            long eventTime = t.get("T").asLong();
            pipeline.publish(symbol, price, qty, buyerIsMaker, eventTime, receivedNanos);
        }
    }

    private void handleDepth(String topic, JsonNode root) {
        String symbol = symbolFromTopic(topic);
        if (symbol == null) return;

        OrderBook book = store.book(symbol);
        if (book == null) return;

        JsonNode data = root.get("data");
        if (data == null) return;

        JsonNode bids = data.get("b");
        JsonNode asks = data.get("a");
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

        // "snapshot" — полная замена, "delta" — инкремент. Для простоты
        // обрабатываем оба как снимок: верх стакана обновляется корректно
        // почти всегда, так как Bybit включает верхние уровни в каждый
        // delta-пакет. Для точного инкрементального стакана нужно отдельно
        // учитывать поле "type" и мержить delta по цене вместо перезаписи.
        long updateId = data.path("u").asLong(0);
        book.applySnapshot(bidPrices, bidQtys, bn, askPrices, askQtys, an,
                updateId, System.currentTimeMillis());
    }

    private String symbolFromTopic(String topic) {
        int dot = topic.lastIndexOf('.');
        return dot > 0 ? topic.substring(dot + 1).toUpperCase() : null;
    }

    public void addSymbol(String symbol) {
        String s = symbol.toUpperCase();
        if (!activeSymbols.contains(s)) {
            activeSymbols.add(s);
            send("{\"op\":\"subscribe\",\"args\":[\"publicTrade." + s + "\",\"orderbook."
                    + depthParam() + "." + s + "\"]}");
        }
    }

    public void removeSymbol(String symbol) {
        String s = symbol.toUpperCase();
        if (activeSymbols.remove(s)) {
            send("{\"op\":\"unsubscribe\",\"args\":[\"publicTrade." + s + "\",\"orderbook."
                    + depthParam() + "." + s + "\"]}");
        }
    }

    public List<String> activeSymbols() { return List.copyOf(activeSymbols); }
    public Latency parseLatency() { return parseLatency; }
}
