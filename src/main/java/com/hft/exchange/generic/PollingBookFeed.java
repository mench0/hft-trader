package com.hft.exchange.generic;

import com.hft.config.ExchangeConfig;
import com.hft.exchange.catalog.ExchangeInfo;
import com.hft.exchange.generic.BookDialect.ParsedBook;
import com.hft.store.MarketDataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpClient;
import com.hft.rest.ApiException;
import com.hft.rest.RateBudget;
import com.hft.rest.RateLimits;
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

    /** Логгер. */
    private static final Logger log = LoggerFactory.getLogger(PollingBookFeed.class);
    /** Неудачных циклов опроса подряд, после которых источник сдаётся (параметр feedMaxFailures). */
    private volatile int maxFailures = 15;

    /** Описание биржи из каталога. */
    private final ExchangeInfo info;
    /** Подключение и параметры биржи. */
    private final ExchangeConfig cfg;
    /** Формат REST-стакана биржи. */
    private final BookDialect dialect;
    /** Куда записываются стаканы. */
    private final MarketDataStore market;
    /** Куда уходит тик после обновления стакана (конвейер). */
    private final TickSink onTick;
    /** Вызывается после обновления стакана символа (бумажный движок). */
    private final Consumer<String> onBook;
    /** Вызывается, когда источник сдался. */
    private final Runnable onGiveUp;

    /** HTTP-клиент. */
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).build();
    /** Символы подписки (наш формат). */
    private final List<String> symbols = new CopyOnWriteArrayList<>();
    /** Минимальный интервал между запросами по каталогу, нс. */
    private final long minGapNanos;
    /** Общий бюджет биржи: опрос не должен съесть лимит, нужный торговому клиенту. */
    private final RateBudget budget;

    /** Фид запущен. */
    private volatile boolean running;
    /** Источник сдался. */
    private volatile boolean gaveUp;
    /** До какого момента опрос приостановлен после «слишком часто». */
    private volatile long blockedUntilMs;
    /** Текущая пауза после «слишком часто» (удваивается при повторах). */
    private volatile long backoffMs = 60_000;
    /** Начальная пауза после «слишком часто» (параметр pollBackoffMs). */
    private volatile long baseBackoffMs = 60_000;
    /** Время последнего успешного ответа. */
    private volatile long lastSuccessMs;
    /** Неудачных циклов подряд. */
    private volatile int consecutiveFailures;
    /** Последняя ошибка (для метрик). */
    private volatile String lastError = "";
    /** Поток фида. */
    private Thread thread;

    /** Запросов. */
    private final AtomicLong requests = new AtomicLong();
    /** Ошибок. */
    private final AtomicLong errors = new AtomicLong();
    /** Ответов «слишком часто». */
    private final AtomicLong rateLimited = new AtomicLong();
    /** Суммарная задержка ответов, нс. */
    private final AtomicLong totalLatencyNanos = new AtomicLong();
    /** Максимальная задержка ответа, нс. */
    private final AtomicLong maxLatencyNanos = new AtomicLong();
    /** Когда можно отправить следующий запрос (nanoTime). */
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
        this.budget = RateBudget.of(info.id());
    }

    /** Задать, после скольких неудачных циклов подряд источник сдаётся. */
    public PollingBookFeed maxFailures(int n) { this.maxFailures = n; return this; }

    /** Задать начальную паузу после ответа «слишком часто», мс. */
    public PollingBookFeed backoff(long ms) { this.baseBackoffMs = ms; this.backoffMs = ms; return this; }

    /** Опрос на паузе, пока WS жив. */
    private volatile boolean paused;
    /** Пока WS жив, опрос молчит (не тратит лимит). */
    public void pause() { paused = true; }
    /** Возобновить опрос. */
    public void resume() { paused = false; }
    /** Опрос на паузе. */
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

    /** Подключаться и переподключаться с нарастающей паузой; после feedMaxFailures неудач — сдаться. */
    private void loop() {
        while (running) {
            try {
                if (symbols.isEmpty() || paused) { Thread.sleep(200); continue; }
                boolean anyOk = false;
                for (String s : symbols) {                 // итератор CopyOnWriteArrayList — снимок без копирования
                    if (!running) break;
                    long wait = Math.max(blockedUntilMs - System.currentTimeMillis(), budget.blockedForMs());
                    if (wait > 0) { Thread.sleep(Math.min(wait, 1000)); break; }
                    paceRequests();
                    anyOk |= pollOne(s);
                }
                if (anyOk) consecutiveFailures = 0;
                else if (++consecutiveFailures >= maxFailures) { giveUp(); return; }
                else Thread.sleep(Math.min(30_000, 250L << Math.min(consecutiveFailures, 7)));
            } catch (InterruptedException e) {
                return;
            } catch (Exception e) {
                lastError = String.valueOf(e.getMessage());
                log.warn("[{}] ошибка цикла опроса: {}", info.id(), e.toString());
            }
        }
    }

    /** Выдержать минимальный интервал между запросами. */
    private void paceRequests() throws InterruptedException {
        long now = System.nanoTime();
        if (nextSlotNanos > now) {
            long ns = nextSlotNanos - now;
            Thread.sleep(ns / 1_000_000, (int) (ns % 1_000_000));
        }
        nextSlotNanos = Math.max(now, nextSlotNanos) + minGapNanos;
    }

    /** Один запрос стакана: бюджет лимитов, ответ, запись в стакан, тик. */
    private boolean pollOne(String symbol) throws InterruptedException {
        long t0 = System.nanoTime();
        try {
            HttpRequest req = dialect.request(cfg.restUrl().isBlank() ? info.restUrl() : cfg.restUrl(), symbol, cfg.bookDepth());
            double weight = "hyperliquid".equals(info.id()) ? 2            // l2Book у Hyperliquid весит 2
                    : RateLimits.weight(info.id(), req.method(), req.uri().getPath(), req.uri().getRawQuery());
            budget.acquire(RateBudget.Kind.PUBLIC, weight, 5_000);
            requests.incrementAndGet();
            HttpResponse<String> resp = http.send(req, HttpResponse.BodyHandlers.ofString());
            budget.onResponse(resp.statusCode(), resp.headers());
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
            backoffMs = baseBackoffMs;

            onTick.onTick(symbol, (b.bp()[0] + b.ap()[0]) / 2, 0, false, b.tsMs(), System.nanoTime());
            onBook.accept(symbol);
            return true;
        } catch (InterruptedException e) {
            throw e;
        } catch (ApiException e) {
            if (!"LOCAL".equals(e.code())) throw e;
            lastError = e.getMessage();                 // свой бюджет исчерпан — это не ошибка биржи
            return true;
        } catch (Exception e) {
            errors.incrementAndGet();
            lastError = e.getClass().getSimpleName() + ": " + e.getMessage();
            log.warn("[{}] {} {}", info.id(), symbol, lastError);
            return false;
        }
    }

    /** Сдаться: остановиться и вызвать onGiveUp. */
    private void giveUp() {
        gaveUp = true;
        running = false;
        log.error("[{}] {} ошибок подряд — источник данных остановлен", info.id(), maxFailures);
        try { onGiveUp.run(); } catch (Exception e) { log.error("onGiveUp: {}", e.toString()); }
    }
}
