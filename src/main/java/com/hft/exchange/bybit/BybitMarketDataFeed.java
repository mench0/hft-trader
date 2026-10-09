package com.hft.exchange.bybit;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.hft.config.ExchangeConfig;
import com.hft.exchange.Exchange;
import com.hft.exchange.generic.FastJson;
import com.hft.exchange.generic.LocalBook;
import com.hft.engine.TickPipeline;
import com.hft.metrics.Latency;
import com.hft.net.AbstractWsFeed;
import com.hft.store.MarketDataStore;
import com.hft.store.OrderBook;
import io.netty.channel.Channel;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
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
 *
 * Стакан приходит снимком (type=snapshot), затем изменениями (type=delta): изменения применяются
 * к локальному {@link LocalBook} символа, в {@link OrderBook} публикуются верхние bookDepth уровней.
 */
public final class BybitMarketDataFeed extends AbstractWsFeed {

    /** Подключение и параметры биржи. */
    private final ExchangeConfig config;
    /** Куда записываются стаканы. */
    private final MarketDataStore store;
    /** Конвейер, куда уходят сделки. */
    private final TickPipeline pipeline;
    /** Время разбора сообщения. */
    private final Latency parseLatency = new Latency("[bybit] Парсинг сообщения");

    /** Буфер публикации верха стакана. */
    private final double[] bidPrices;
    /** Буфер публикации верха стакана. */
    private final double[] bidQtys;
    /** Буфер публикации верха стакана. */
    private final double[] askPrices;
    /** Буфер публикации верха стакана. */
    private final double[] askQtys;

    /** Символы подписки. */
    private final List<String> activeSymbols;
    /** Локальный стакан каждого символа — пишет только поток WS. */
    private final Map<String, LocalBook> localBooks = new ConcurrentHashMap<>();

    /**
     * @param config подключение и параметры
     * @param store рыночные данные
     * @param pipeline конвейер тиков
     */
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

    /** Имя для логов и бюджета лимитов. */
    @Override
    protected String name() { return Exchange.BYBIT.id(); }

    /** Адрес WebSocket (у Binance — с подпиской на потоки в URL). */
    @Override
    protected URI buildUri() throws Exception {
        return new URI(config.wsUrl());
    }

    /** После рукопожатия — подписаться на сделки и стаканы. */
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

    /** Глубина подписки, которую поддерживает биржа. */
    private String depthParam() {
        int d = config.bookDepth();
        if (d <= 1) return "1";
        if (d <= 50) return "50";
        if (d <= 200) return "200";
        return "500";
    }

    // Разбор одного сообщения — только поток WS
    private String msgSymbol;
    /** Сообщение — снимок / в нём стакан. */
    private boolean snapshot, hasBook;
    /** Номер обновления стакана. */
    private long updateId;
    // изменения уровней из сообщения: применяются после разбора, когда известен type
    private double[] lbp = new double[64], lbq = new double[64], lap = new double[64], laq = new double[64];
    /** Уровней бидов и асков в сообщении. */
    private int lbn, lan;

    /** Потоковый разбор без дерева узлов; сделки публикуются по мере чтения массива data. */
    @Override
    protected void onText(String json, long receivedNanos) {
        try (JsonParser p = FastJson.F.createParser(json)) {
            msgSymbol = null; snapshot = false; hasBook = false; updateId = 0; lbn = 0; lan = 0;
            if (p.nextToken() != JsonToken.START_OBJECT) return;
            while (p.nextToken() == JsonToken.FIELD_NAME) {
                String f = p.currentName();
                p.nextToken();
                switch (f) {
                    case "type" -> snapshot = FastJson.textIs(p, "snapshot");
                    case "data" -> {
                        if (p.currentToken() == JsonToken.START_ARRAY) parseTrades(p, receivedNanos);
                        else if (p.currentToken() == JsonToken.START_OBJECT) parseBook(p);
                        else p.skipChildren();
                    }
                    default -> p.skipChildren();
                }
            }
            if (hasBook && msgSymbol != null) applyBook();
            parseLatency.recordSince(receivedNanos);
        } catch (Exception e) {
            log.error("Ошибка разбора сообщения Bybit", e);
        }
    }

    /** Сделки из массива data — сразу в конвейер. */
    private void parseTrades(JsonParser p, long receivedNanos) throws IOException {
        while (p.nextToken() == JsonToken.START_OBJECT) {
            String symbol = null;
            double price = 0, qty = 0;
            boolean buyerIsMaker = false;
            long time = 0;
            while (p.nextToken() == JsonToken.FIELD_NAME) {
                String f = p.currentName();
                p.nextToken();
                switch (f) {
                    case "s" -> symbol = symbolOf(p);
                    case "p" -> price = FastJson.num(p);
                    case "v" -> qty = FastJson.num(p);
                    case "S" -> buyerIsMaker = FastJson.textIs(p, "Sell");
                    case "T" -> time = FastJson.longOf(p, 0);
                    default -> p.skipChildren();
                }
            }
            if (symbol != null) pipeline.publish(symbol, price, qty, buyerIsMaker, time, receivedNanos);
        }
    }

    /** Стакан из объекта data. */
    private void parseBook(JsonParser p) throws IOException {
        hasBook = true;
        while (p.nextToken() == JsonToken.FIELD_NAME) {
            String f = p.currentName();
            p.nextToken();
            switch (f) {
                case "s" -> msgSymbol = symbolOf(p);
                case "u" -> updateId = FastJson.longOf(p, 0);
                case "b" -> lbn = levels(p, true);
                case "a" -> lan = levels(p, false);
                default -> p.skipChildren();
            }
        }
    }

    /** Уровни в буферы сообщения; возвращает их число. */
    private int levels(JsonParser p, boolean bid) throws IOException {
        if (p.currentToken() != JsonToken.START_ARRAY) { p.skipChildren(); return 0; }
        int n = 0;
        while (p.nextToken() == JsonToken.START_ARRAY) {
            p.nextToken(); double price = FastJson.num(p);
            p.nextToken(); double q = FastJson.num(p);
            while (p.nextToken() != JsonToken.END_ARRAY) p.skipChildren();
            if (bid) {
                if (n == lbp.length) { lbp = Arrays.copyOf(lbp, n * 2); lbq = Arrays.copyOf(lbq, n * 2); }
                lbp[n] = price; lbq[n] = q;
            } else {
                if (n == lap.length) { lap = Arrays.copyOf(lap, n * 2); laq = Arrays.copyOf(laq, n * 2); }
                lap[n] = price; laq[n] = q;
            }
            n++;
        }
        return n;
    }

    /** Снимок заменяет, изменения правят локальный стакан; верх публикуется. */
    private void applyBook() {
        OrderBook book = store.book(msgSymbol);
        if (book == null) return;
        LocalBook lb = localBooks.computeIfAbsent(msgSymbol, k -> new LocalBook(LocalBook.levelsFor(config.bookDepth())));
        if (snapshot) lb.clear();
        for (int i = 0; i < lbn; i++) lb.applyBid(lbp[i], lbq[i]);
        for (int i = 0; i < lan; i++) lb.applyAsk(lap[i], laq[i]);
        if (!lb.isReady()) return;                        // delta до первого снимка — ждём снимок
        int bn = lb.topBids(bidPrices, bidQtys);
        int an = lb.topAsks(askPrices, askQtys);
        book.applySnapshot(bidPrices, bidQtys, bn, askPrices, askQtys, an, updateId, System.currentTimeMillis());
        onBook.accept(msgSymbol);
    }

    /** Символ подписки, совпадающий с текущей строкой (без создания новой строки). */
    private String symbolOf(JsonParser p) throws IOException {
        char[] c = p.getTextCharacters(); int off = p.getTextOffset(), len = p.getTextLength();
        for (String s : activeSymbols) {
            if (s.length() != len) continue;
            int i = 0;
            while (i < len && c[off + i] == s.charAt(i)) i++;
            if (i == len) return s;
        }
        return null;
    }

    /** Символы подписки. */
    public List<String> activeSymbols() { return List.copyOf(activeSymbols); }
    /** Время разбора сообщения. */
    public Latency parseLatency() { return parseLatency; }
}
