package com.hft.exchange.generic;

import com.hft.config.ExchangeConfig;
import com.hft.exchange.catalog.ExchangeInfo;
import com.hft.rest.RateBudget;
import com.hft.rest.WsSender;
import com.hft.store.MarketDataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Стакан по WebSocket для биржи с диалектом {@link WsDialect}. Использует WebSocket из JDK
 * (без Netty), поэтому не добавляет зависимостей.
 *
 * Жизненный цикл: подключиться → подписаться → читать; при обрыве, ошибке разбора подряд или
 * тишине дольше порога — переподключиться с экспоненциальной паузой (до 30 с). После
 * {@value #MAX_FAILURES} неудачных подключений подряд источник сдаётся и вызывает onGiveUp.
 * Счётчик неудач обнуляется, как только пришёл первый корректный стакан.
 *
 * Стакан в памяти — {@link LocalBook}: снимок заменяет всё, обновление правит уровни.
 * Перекрещённый стакан (bid ≥ ask) не публикуется; если так долго, подписка на символ
 * обновляется заново.
 *
 * Горячий путь без мусора: текст собирается в переиспользуемый char[], диалект разбирает его
 * потоково в переиспользуемый {@link BookBatch}, стакан — на массивах примитивов, тик уходит
 * в {@link TickSink} без объекта. Отправка — через {@link WsSender}: поток чтения никогда не ждёт сеть.
 */
public final class WsBookFeed implements BookFeed {

    private static final Logger log = LoggerFactory.getLogger(WsBookFeed.class);
    static final int MAX_FAILURES = 15;
    private static final int MAX_CONSECUTIVE_PARSE_ERRORS = 5;
    private static final int MAX_CROSSED_EVENTS = 200;

    private final ExchangeInfo info;
    private final ExchangeConfig cfg;
    private final WsDialect dialect;
    private final MarketDataStore market;
    private final TickSink onTick;
    private final Consumer<String> onBook;
    private final Runnable onGiveUp;
    private volatile Runnable onStateChange = () -> {};

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final List<String> symbols = new CopyOnWriteArrayList<>();
    private final Map<String, String> venueToInternal = new ConcurrentHashMap<>();
    /** Состояние символа: локальный стакан и буферы публикации — только поток WS. */
    private final Map<String, SymbolState> states = new ConcurrentHashMap<>();

    private static final class SymbolState {
        final String internal;
        final LocalBook book;
        final double[] bp, bq, ap, aq;
        int crossed;
        SymbolState(String internal, int depth) {
            this.internal = internal;
            this.book = new LocalBook(LocalBook.levelsFor(depth));
            bp = new double[depth]; bq = new double[depth]; ap = new double[depth]; aq = new double[depth];
        }
    }

    private volatile boolean running;
    private volatile boolean gaveUp;
    private volatile WebSocket socket;
    private volatile WsSender sender;
    private volatile BookBatch currentBatch;
    private volatile boolean open;
    private volatile long lastFrameMs;
    private volatile int failures;
    private volatile int parseErrorsInRow;
    private volatile boolean abortRequested;
    private volatile String lastError = "";
    private volatile long staleMs;
    private volatile long baseBackoffMs = 500;
    private Thread thread;

    private final AtomicLong messages = new AtomicLong();
    private final AtomicLong bookUpdates = new AtomicLong();
    private final AtomicLong reconnects = new AtomicLong();
    private final AtomicLong parseErrors = new AtomicLong();

    public WsBookFeed(ExchangeInfo info, ExchangeConfig cfg, WsDialect dialect, MarketDataStore market,
                      TickSink onTick, Consumer<String> onBook, Runnable onGiveUp) {
        this.info = info;
        this.cfg = cfg;
        this.dialect = dialect;
        this.market = market;
        this.onTick = onTick;
        this.onBook = onBook;
        this.onGiveUp = onGiveUp;
        this.staleMs = dialect.pingMessage() != null ? Math.max(30_000, dialect.pingIntervalMs() * 3) : 60_000;
        cfg.symbols().forEach(this::register);
    }

    /** Для тестов. */
    public WsBookFeed tune(long staleMs, long baseBackoffMs) {
        this.staleMs = staleMs;
        this.baseBackoffMs = baseBackoffMs;
        return this;
    }

    /** Вызывается при подключении/обрыве/сдаче — гибридный фид переключается сразу, без опроса. */
    void onStateChange(Runnable r) { this.onStateChange = r; }

    private void fireState() { try { onStateChange.run(); } catch (Exception ignored) { /* наблюдатель не должен ронять фид */ } }

    private void register(String s) {
        if (!symbols.contains(s)) symbols.add(s);
        try {
            String v = dialect.venueSymbol(s);
            venueToInternal.put(v, s);
            states.put(v, new SymbolState(s, Math.max(1, cfg.bookDepth())));
        } catch (IllegalArgumentException e) {            // нет описания пула и т.п.: по WS символ не слушаем, остаётся опрос
            lastError = e.getMessage();
            log.warn("[{}] {} не подписан по WS: {}", info.id(), s, e.getMessage());
        }
    }

    private String venueOf(String internal) {
        for (var e : venueToInternal.entrySet()) if (e.getValue().equals(internal)) return e.getKey();
        return null;
    }

    String url() {
        return cfg.wsUrl() == null || cfg.wsUrl().isBlank() ? dialect.defaultUrl(cfg.testnet(), cfg.restUrl()) : cfg.wsUrl();
    }

    // ───────────────────────── BookFeed ─────────────────────────

    @Override public synchronized void start() {
        if (running) return;
        running = true;
        gaveUp = false;
        failures = 0;
        thread = new Thread(this::loop, "ws-" + info.id());
        thread.setDaemon(true);
        thread.start();
    }

    @Override public synchronized void stop() {
        running = false;
        WebSocket w = socket;
        if (w != null) w.abort();
        if (thread != null) thread.interrupt();
    }

    @Override public List<String> activeSymbols() { return List.copyOf(symbols); }

    @Override public boolean isConnected() {
        return running && !gaveUp && open && System.currentTimeMillis() - lastFrameMs < staleMs;
    }

    @Override public boolean isRealtime() { return isConnected(); }
    @Override public boolean hasGivenUp() { return gaveUp; }
    @Override public long messageCount() { return messages.get(); }

    @Override public Map<String, Object> stats() {
        var m = new LinkedHashMap<String, Object>();
        m.put("transport", "websocket");
        m.put("url", url());
        m.put("connected", isConnected());
        m.put("messages", messages.get());
        m.put("bookUpdates", bookUpdates.get());
        m.put("reconnects", reconnects.get());
        m.put("parseErrors", parseErrors.get());
        m.put("consecutiveFailures", failures);
        m.put("sendQueue", sender == null ? 0 : sender.queued());
        m.put("lastFrameAgeMs", lastFrameMs == 0 ? -1 : System.currentTimeMillis() - lastFrameMs);
        m.put("lastError", lastError);
        return m;
    }

    // ───────────────────────── подключение ─────────────────────────

    private void loop() {
        while (running) {
            try {
                connectAndServe();
            } catch (InterruptedException e) {
                return;
            } catch (Exception e) {
                lastError = e.getClass().getSimpleName() + ": " + e.getMessage();
                log.warn("[{}] WS: {}", info.id(), lastError);
            }
            open = false;
            fireState();
            if (!running) return;
            if (++failures >= MAX_FAILURES) { giveUp(); return; }
            reconnects.incrementAndGet();
            long pause = Math.min(30_000, baseBackoffMs << Math.min(failures, 10));
            try { Thread.sleep(pause); } catch (InterruptedException e) { return; }
        }
    }

    private void connectAndServe() throws Exception {
        CompletableFuture<Void> closed = new CompletableFuture<>();
        Listener listener = new Listener(closed);
        RateBudget budget = RateBudget.of(info.id());
        budget.acquire(RateBudget.Kind.WS_CONNECT, 1, 60_000);   // лимит подключений на IP
        WebSocket w = http.newWebSocketBuilder().connectTimeout(Duration.ofSeconds(5))
                .buildAsync(URI.create(dialect.connectUrl(url(), cfg.restUrl().isBlank() ? info.restUrl() : cfg.restUrl())), listener)
                .get(10, TimeUnit.SECONDS);
        socket = w;
        WsSender snd = new WsSender(w, 1000);
        sender = snd;
        for (SymbolState st : states.values()) { st.book.clear(); st.crossed = 0; }
        parseErrorsInRow = 0;
        abortRequested = false;
        lastFrameMs = System.currentTimeMillis();
        open = true;
        try {
            List<String> venues = List.copyOf(venueToInternal.keySet());
            listener.batch.setKnown(venues);
            currentBatch = listener.batch;
            if (!venues.isEmpty()) {
                CompletableFuture<?> last = null;
                for (String m : dialect.subscribe(venues, cfg.bookDepth())) {
                    budget.acquire(RateBudget.Kind.WS_MESSAGE, 1, 60_000);   // лимит исходящих сообщений
                    last = snd.send(m);
                }
                last.get(10, TimeUnit.SECONDS);                // подписка ушла (без блокировки потока чтения)
            }
            long lastPing = System.currentTimeMillis();
            while (running && !closed.isDone() && !abortRequested) {
                try { closed.get(500, TimeUnit.MILLISECONDS); } catch (java.util.concurrent.TimeoutException ignored) {}
                long now = System.currentTimeMillis();
                if (now - lastFrameMs > staleMs) throw new IllegalStateException("тишина " + (now - lastFrameMs) + " мс — переподключаюсь");
                String ping = dialect.pingMessage();
                if (ping != null && now - lastPing >= dialect.pingIntervalMs()) { lastPing = now; snd.send(ping); }
            }
            if (closed.isCompletedExceptionally()) closed.get();
            if (abortRequested) throw new IllegalStateException("повторные ошибки разбора — соединение сброшено");
            if (running) throw new IllegalStateException("сервер закрыл соединение");
        } finally {
            open = false;
            w.abort();
        }
    }

    private void giveUp() {
        gaveUp = true;
        running = false;
        open = false;
        log.error("[{}] WS: {} неудачных подключений подряд — источник остановлен ({})", info.id(), MAX_FAILURES, lastError);
        fireState();
        try { onGiveUp.run(); } catch (Exception e) { log.error("onGiveUp: {}", e.toString()); }
    }

    // ───────────────────────── обработка сообщений (поток чтения WS) ─────────────────────────

    private void handle(WebSocket w, char[] buf, int len, BookBatch batch) {
        messages.incrementAndGet();
        try {
            String reply = dialect.parse(buf, len, batch);
            if (reply != null) { WsSender snd = sender; if (snd != null) snd.send(reply); }
            if (batch.venue != null) apply(w, batch);
            parseErrorsInRow = 0;
        } catch (Exception e) {
            lastError = "parse: " + e.getMessage();
            parseErrors.incrementAndGet();
            log.warn("[{}] WS: {}", info.id(), lastError);
            if (++parseErrorsInRow >= MAX_CONSECUTIVE_PARSE_ERRORS) {
                log.warn("[{}] WS: {} ошибок разбора подряд — переподключение", info.id(), parseErrorsInRow);
                abortRequested = true;     // abort() не вызывает onClose/onError, поэтому говорим циклу сами
                w.abort();
            }
        }
    }

    private void apply(WebSocket w, BookBatch b) {
        SymbolState st = states.get(b.venue);
        if (st == null) return;                          // отписались или чужой символ
        boolean wasReady = st.book.isReady();
        if (b.snapshot) st.book.clear();
        st.book.apply(b);
        if (!st.book.isReady()) return;
        if (st.book.isCrossed()) {
            if (++st.crossed >= MAX_CROSSED_EVENTS) {
                log.warn("[{}] {} перекрещён {} обновлений подряд — подписываюсь заново", info.id(), st.internal, st.crossed);
                st.crossed = 0;
                st.book.clear();
                WsSender snd = sender;
                List<String> unsub = dialect.unsubscribe(List.of(b.venue), cfg.bookDepth());
                List<String> sub = dialect.subscribe(List.of(b.venue), cfg.bookDepth());
                // поток чтения не ждёт: нет бюджета на сообщения — переподпишемся на следующем перекрещивании
                if (snd != null && RateBudget.of(info.id()).tryAcquire(RateBudget.Kind.WS_MESSAGE, unsub.size() + sub.size())) {
                    for (String m : unsub) snd.send(m);
                    for (String m : sub) snd.send(m);
                }
            }
            return;
        }
        st.crossed = 0;
        int bn = st.book.topBids(st.bp, st.bq), an = st.book.topAsks(st.ap, st.aq);
        long now = System.currentTimeMillis();
        market.book(st.internal).applySnapshot(st.bp, st.bq, bn, st.ap, st.aq, an, now, now);
        if (failures != 0) failures = 0;
        bookUpdates.incrementAndGet();
        if (!wasReady) fireState();
        onTick.onTick(st.internal, (st.bp[0] + st.ap[0]) / 2, 0, false, b.tsMs, System.nanoTime());
        onBook.accept(st.internal);
    }

    private final class Listener implements WebSocket.Listener {
        private final CompletableFuture<Void> closed;
        final BookBatch batch = new BookBatch();
        private char[] buf = new char[8192];
        private int len;
        private ByteBuffer bin = ByteBuffer.allocate(0);

        Listener(CompletableFuture<Void> closed) { this.closed = closed; }

        @Override public void onOpen(WebSocket w) { w.request(1); }

        private void append(CharSequence data) {
            int n = data.length();
            if (len + n > buf.length) buf = java.util.Arrays.copyOf(buf, Math.max(buf.length * 2, len + n));
            if (data instanceof String s) s.getChars(0, n, buf, len);
            else for (int i = 0; i < n; i++) buf[len + i] = data.charAt(i);
            len += n;
        }

        @Override public CompletionStage<?> onText(WebSocket w, CharSequence data, boolean last) {
            lastFrameMs = System.currentTimeMillis();
            append(data);
            if (last) { handle(w, buf, len, batch); len = 0; }
            w.request(1);
            return null;
        }

        @Override public CompletionStage<?> onBinary(WebSocket w, ByteBuffer data, boolean last) {
            lastFrameMs = System.currentTimeMillis();
            ByteBuffer merged = ByteBuffer.allocate(bin.remaining() + data.remaining());
            merged.put(bin).put(data).flip();
            bin = merged;
            if (last) {
                byte[] bytes = new byte[bin.remaining()];
                bin.get(bytes);
                bin = ByteBuffer.allocate(0);
                try {
                    len = 0;
                    append(dialect.decodeBinary(bytes));
                    handle(w, buf, len, batch);
                    len = 0;
                } catch (Exception e) {
                    parseErrors.incrementAndGet();
                    lastError = "binary: " + e;
                }
            }
            w.request(1);
            return null;
        }

        @Override public CompletionStage<?> onPing(WebSocket w, ByteBuffer m) {
            lastFrameMs = System.currentTimeMillis();
            w.request(1);
            return null;       // pong JDK отправляет сам
        }

        @Override public CompletionStage<?> onPong(WebSocket w, ByteBuffer m) {
            lastFrameMs = System.currentTimeMillis();
            w.request(1);
            return null;
        }

        @Override public CompletionStage<?> onClose(WebSocket w, int code, String reason) {
            lastError = "close " + code + " " + reason;
            closed.complete(null);
            return null;
        }

        @Override public void onError(WebSocket w, Throwable e) {
            lastError = "ws error: " + e;
            closed.completeExceptionally(e);
        }
    }
}
