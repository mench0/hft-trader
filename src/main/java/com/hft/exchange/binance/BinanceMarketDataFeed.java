package com.hft.exchange.binance;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.hft.exchange.Exchange;
import com.hft.exchange.generic.FastJson;
import com.hft.config.ExchangeConfig;
import com.hft.engine.TickPipeline;
import com.hft.metrics.Latency;
import com.hft.net.AbstractWsFeed;
import com.hft.store.MarketDataStore;
import com.hft.store.OrderBook;

import java.io.IOException;
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

    /** Подключение и параметры биржи. */
    private final ExchangeConfig config;
    /** Куда записываются стаканы. */
    private final MarketDataStore store;
    /** Конвейер, куда уходят сделки. */
    private final TickPipeline pipeline;
    /** Время разбора сообщения. */
    private final Latency parseLatency = new Latency("[binance] Парсинг сообщения");

    /** Буфер публикации верха стакана. */
    private final double[] bidPrices;
    /** Буфер публикации верха стакана. */
    private final double[] bidQtys;
    /** Буфер публикации верха стакана. */
    private final double[] askPrices;
    /** Буфер публикации верха стакана. */
    private final double[] askQtys;

    // Символы, на которые подписаны.
    private final List<String> activeSymbols;

    /**
     * @param config подключение и параметры
     * @param store рыночные данные
     * @param pipeline конвейер тиков
     */
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

    /** Имя для логов и бюджета лимитов. */
    @Override
    protected String name() { return Exchange.BINANCE.id(); }

    /** Адрес WebSocket (у Binance — с подпиской на потоки в URL). */
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

    /** Глубина подписки, которую поддерживает биржа. */
    private String depthParam() {
        int d = config.bookDepth();
        if (d <= 5) return "5";
        if (d <= 10) return "10";
        return "20";
    }

    // Разбор одного сообщения — только поток WS, поэтому поля можно переиспользовать
    private String msgSymbol;          // символ из "stream" (для стакана) или "s" (для сделки)
    /** В сообщении сделка / стакан. */
    private boolean isTrade, hasBook;
    /** Цена и объём сделки. */
    private double tradePrice, tradeQty;
    /** Агрессор — продавец. */
    private boolean buyerIsMaker;
    /** Время сделки и номер обновления стакана. */
    private long tradeTime, updateId;
    /** Уровней бидов и асков. */
    private int bn, an;

    /**
     * Потоковый разбор без дерева узлов: числа читаются прямо из буфера парсера ({@link FastJson#num}),
     * символ сопоставляется с подпиской без создания строки. Порядок полей не важен.
     */
    @Override
    protected void onText(String json, long receivedNanos) {
        try (JsonParser p = FastJson.F.createParser(json)) {
            msgSymbol = null; isTrade = false; hasBook = false; bn = 0; an = 0; updateId = 0;
            if (p.nextToken() != JsonToken.START_OBJECT) return;
            while (p.nextToken() == JsonToken.FIELD_NAME) {
                String f = p.currentName();
                p.nextToken();
                switch (f) {
                    case "stream" -> { String s = symbolFromStream(p); if (s != null) msgSymbol = s; }
                    case "data" -> { if (p.currentToken() == JsonToken.START_OBJECT) parseData(p); else p.skipChildren(); }
                    default -> parseField(f, p);           // несоставное сообщение (без обёртки stream/data)
                }
            }
            if (msgSymbol == null) return;
            if (isTrade) {
                pipeline.publish(msgSymbol, tradePrice, tradeQty, buyerIsMaker, tradeTime, receivedNanos);
            } else if (hasBook) {
                OrderBook book = store.book(msgSymbol);
                if (book != null) {
                    book.applySnapshot(bidPrices, bidQtys, bn, askPrices, askQtys, an, updateId, System.currentTimeMillis());
                    onBook.accept(msgSymbol);
                }
            }
            parseLatency.recordSince(receivedNanos);
        } catch (Exception e) {
            log.error("Ошибка разбора сообщения Binance", e);
        }
    }

    /** Разобрать объект data. */
    private void parseData(JsonParser p) throws IOException {
        while (p.nextToken() == JsonToken.FIELD_NAME) {
            String f = p.currentName();
            p.nextToken();
            parseField(f, p);
        }
    }

    /** Разобрать одно поле сообщения Binance. */
    private void parseField(String f, JsonParser p) throws IOException {
        switch (f) {
            case "e" -> isTrade = FastJson.textIs(p, "trade");
            case "s" -> { String s = symbolOf(p.getTextCharacters(), p.getTextOffset(), p.getTextLength(), false); if (s != null) msgSymbol = s; }
            case "p" -> tradePrice = FastJson.num(p);
            case "q" -> tradeQty = FastJson.num(p);
            case "m" -> buyerIsMaker = p.currentToken() == JsonToken.VALUE_TRUE;
            case "T" -> tradeTime = FastJson.longOf(p, 0);
            case "lastUpdateId" -> updateId = FastJson.longOf(p, 0);
            case "bids", "b" -> { hasBook = true; bn = levels(p, bidPrices, bidQtys); }
            case "asks", "a" -> { hasBook = true; an = levels(p, askPrices, askQtys); }
            default -> p.skipChildren();
        }
    }

    /** [[цена, объём], ...] в массивы; уровни глубже bookDepth пропускаются. */
    private static int levels(JsonParser p, double[] px, double[] qty) throws IOException {
        if (p.currentToken() != JsonToken.START_ARRAY) { p.skipChildren(); return 0; }
        int n = 0;
        while (p.nextToken() == JsonToken.START_ARRAY) {
            p.nextToken(); double price = FastJson.num(p);
            p.nextToken(); double q = FastJson.num(p);
            while (p.nextToken() != JsonToken.END_ARRAY) p.skipChildren();
            if (n < px.length) { px[n] = price; qty[n] = q; n++; }
        }
        return n;
    }

    /** "btcusdt@depth20@100ms" -> "BTCUSDT" из подписки (без новой строки). */
    private String symbolFromStream(JsonParser p) throws IOException {
        char[] c = p.getTextCharacters(); int off = p.getTextOffset(), len = p.getTextLength();
        int at = 0;
        while (at < len && c[off + at] != '@') at++;
        boolean trade = at < len && len - at == 6 && c[off + at + 1] == 't';   // "@trade"
        if (trade) isTrade = true;
        return symbolOf(c, off, at, true);
    }

    /** Символ подписки, совпадающий с c[off..off+len) (без учёта регистра, если ignoreCase). */
    private String symbolOf(char[] c, int off, int len, boolean ignoreCase) {
        for (String s : activeSymbols) {
            if (s.length() != len) continue;
            int i = 0;
            for (; i < len; i++) {
                char x = c[off + i];
                if (ignoreCase && x >= 'a' && x <= 'z') x -= 32;
                if (x != s.charAt(i)) break;
            }
            if (i == len) return s;
        }
        return null;
    }

    /** Символы подписки. */
    public List<String> activeSymbols() { return List.copyOf(activeSymbols); }
    /** Время разбора сообщения. */
    public Latency parseLatency() { return parseLatency; }
}
