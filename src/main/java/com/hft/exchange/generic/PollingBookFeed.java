package com.hft.exchange.generic;

import com.hft.config.ExchangeConfig;
import com.hft.exchange.catalog.ExchangeInfo;
import com.hft.exchange.generic.BookDialect.ParsedBook;
import com.hft.store.MarketDataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Универсальный источник стакана для бирж без собственного WS-адаптера:
 * по кругу опрашивает REST, соблюдая свой лимит запросов.
 *
 * Защита от бана:
 *  - между любыми двумя запросами к бирже проходит не меньше 1/maxRequestsPerSec;
 *  - на 429/418/403 запросы встают на паузу (60 c, удваивается до 10 мин);
 *  - после maxFailures ошибок подряд источник сдаётся и вызывает onGiveUp
 *    (владелец отменяет заявки и останавливает торговлю).
 *
 * Опрос на сотни миллисекунд — это не HFT, а мониторинг и paper-торговля.
 */
public final class PollingBookFeed implements BookFeed {

    private static final Logger log = LoggerFactory.getLogger(PollingBookFeed.class);
    private static final int MAX_FAILURES = 15;

    private final ExchangeInfo info;
    private final ExchangeConfig cfg;
    private final BookDialect dialect;
    private final MarketDataStore market;
    private final TickSink onTick;
    private final Consumer<String> onBook;
    private final Runnable onGiveUp;

    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
    private final List<String> symbols = new CopyOnWriteArrayList<>();
    private final long minGapNanos;

    private volatile boolean running;
    private volatile boolean gaveUp;
    private volatile long blockedUntilMs;
    private volatile long backoffMs = 60_000;
    private volatile long lastSuccessMs;
    private volatile int consecutiveFailures;
    private volatile String lastError = "";
    private Thread thread;

    private final AtomicLong requests = new AtomicLong();
    private final AtomicLong errors = new AtomicLong();
    private final AtomicLong rateLimited = new AtomicLong();
    private final AtomicLong totalLatencyNanos = new AtomicLong();
    private final AtomicLong maxLatencyNanos = new AtomicLong();
    private long nextSlotNanos;

    public PollingBookFeed(ExchangeInfo info, ExchangeConfig cfg, BookDialect dialect, MarketDataStore market,
                           TickSink onTick, Consumer<String> onBook, Runnable onGiveUp) {
        this.info = info;
        this.cfg = cfg;
        this.dialect = dialect;
        this.market = market;
        this.onTick = onTick;
        this.onBook = onBook;
        this.onGiveUp = onGiveUp;
        this.symbols.addAll(cfg.symbols());
        this.minGapNanos = (long) (1_000_000_000d / Math.max(0.1, info.maxRequestsPerSec()));
    }

    private volatile boolean paused;
    /** Пока WS жив, опрос молчит (не тратит лимит). */
    public void pause() { paused = true; }
    public void resume() { paused = false; }
    public boolean isPaused() { return paused; }

    /** REST-опрос — никогда не «реальное время». */
    @Override public boolean isRealtime() { return false; }

    @Override public synchronized void start() {
        if (running) return;
        running = true;
        gaveUp = false;
        thread = new Thread(this::loop, "poll-" + info.id());
        thread.setDaemon(true);
        thread.start();
    }

    @Override public synchronized void stop() {
        running = false;
        if (thread != null) thread.interrupt();
    }

    @Override public void addSymbol(String s) { if (!symbols.contains(s)) symbols.add(s); }
    @Override public void removeSymbol(String s) { symbols.remove(s); }
    @Override public List<String> activeSymbols() { return List.copyOf(symbols); }

    @Override public boolean isConnected() {
        return running && !gaveUp && System.currentTimeMillis() - lastSuccessMs < 10_000;
    }
    @Override public boolean hasGivenUp() { return gaveUp; }
    @Override public long messageCount() { return requests.get(); }

    /** Метрики запросов для админки и /metrics. */
    @Override public java.util.Map<String, Object> stats() {
        long n = requests.get();
        var m = new java.util.LinkedHashMap<String, Object>();
        m.put("requests", n);
        m.put("errors", errors.get());
        m.put("rateLimited", rateLimited.get());
        m.put("avgLatencyMs", n == 0 ? 0 : totalLatencyNanos.get() / 1e6 / n);
        m.put("maxLatencyMs", maxLatencyNanos.get() / 1e6);
        m.put("blockedForMs", Math.max(0, blockedUntilMs - System.currentTimeMillis()));
        m.put("consecutiveFailures", consecutiveFailures);
        m.put("lastError", lastError);
        return m;
    }

    private void loop() {
        while (running) {
            try {
                if (symbols.isEmpty() || paused) { Thread.sleep(200); continue; }
                boolean anyOk = false;
                for (String s : symbols) {                 // итератор CopyOnWriteArrayList — снимок без копирования
                    if (!running) break;
                    long wait = blockedUntilMs - System.currentTimeMillis();
                    if (wait > 0) { Thread.sleep(Math.min(wait, 1000)); break; }
                    paceRequests();
                    anyOk |= pollOne(s);
                }
                if (anyOk) consecutiveFailures = 0;
                else if (++consecutiveFailures >= MAX_FAILURES) { giveUp(); return; }
                else Thread.sleep(Math.min(30_000, 250L << Math.min(consecutiveFailures, 7)));
            } catch (InterruptedException e) {
                return;
            } catch (Exception e) {
                lastError = String.valueOf(e.getMessage());
                log.warn("[{}] ошибка цикла опроса: {}", info.id(), e.toString());
            }
        }
    }

    private void paceRequests() throws InterruptedException {
        long now = System.nanoTime();
        if (nextSlotNanos > now) {
            long ns = nextSlotNanos - now;
            Thread.sleep(ns / 1_000_000, (int) (ns % 1_000_000));
        }
        nextSlotNanos = Math.max(now, nextSlotNanos) + minGapNanos;
    }

    private boolean pollOne(String symbol) throws InterruptedException {
        long t0 = System.nanoTime();
        try {
            HttpRequest req = dialect.request(cfg.restUrl().isBlank() ? info.restUrl() : cfg.restUrl(), symbol, cfg.bookDepth());
            requests.incrementAndGet();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            long dt = System.nanoTime() - t0;
            totalLatencyNanos.addAndGet(dt);
            maxLatencyNanos.accumulateAndGet(dt, Math::max);

            int code = resp.statusCode();
            if (code == 429 || code == 418 || code == 403) {
                rateLimited.incrementAndGet();
                blockedUntilMs = System.currentTimeMillis() + backoffMs;
                lastError = "HTTP " + code + ": пауза " + backoffMs / 1000 + " c";
                log.warn("[{}] {} — запросы приостановлены на {} c", info.id(), lastError, backoffMs / 1000);
                backoffMs = Math.min(backoffMs * 2, 600_000);
                return false;
            }
            if (code / 100 != 2) throw new IllegalStateException("HTTP " + code);

            ParsedBook b = dialect.parse(resp.body(), symbol);
            if (b.isEmpty()) throw new IllegalStateException("пустой стакан");
            if (b.bp()[0] >= b.ap()[0]) throw new IllegalStateException("перекрещённый стакан — отбрасываю");

            long now = System.currentTimeMillis();
            market.book(symbol).applySnapshot(b.bp(), b.bq(), b.bp().length, b.ap(), b.aq(), b.ap().length, now, now);
            lastSuccessMs = now;
            backoffMs = 60_000;

            onTick.onTick(symbol, (b.bp()[0] + b.ap()[0]) / 2, 0, false, b.tsMs(), System.nanoTime());
            onBook.accept(symbol);
            return true;
        } catch (InterruptedException e) {
            throw e;
        } catch (Exception e) {
            errors.incrementAndGet();
            lastError = e.getClass().getSimpleName() + ": " + e.getMessage();
            log.warn("[{}] {} {}", info.id(), symbol, lastError);
            return false;
        }
    }

    private void giveUp() {
        gaveUp = true;
        running = false;
        log.error("[{}] {} ошибок подряд — источник данных остановлен", info.id(), MAX_FAILURES);
        try { onGiveUp.run(); } catch (Exception e) { log.error("onGiveUp: {}", e.toString()); }
    }
}
