package com.hft.discovery;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Подбор тикеров под стратегии: при старте бота (и затем раз в N минут) берёт сводки 24ч со всех
 * бирж, прогоняет их через профили стратегий и хранит результат для админки
 * (GET /discovery). Сеть — только публичные эндпоинты, ключи не нужны; лимиты соблюдаются
 * (одна сводка на биржу + свечи лишь для лучших кандидатов).
 */
public final class DiscoveryService {

    /** Логгер. */
    private static final Logger log = LoggerFactory.getLogger(DiscoveryService.class);

    /** Источники сводок бирж. */
    private final List<MarketSource> sources;
    /** Профили стратегий. */
    private final List<StrategyProfile> profiles;
    /** Период пересчёта, мин. */
    private final long refreshMinutes;
    /** Пересчёт идёт. */
    private final AtomicBoolean running = new AtomicBoolean();
    /** Поток пересчёта по расписанию (демон). */
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "discovery");
        t.setDaemon(true);
        return t;
    });
    /** Результат последнего пересчёта. */
    private volatile Map<String, Object> last = Map.of("status", "ещё не запускался");
    /** Расписание запущено. */
    private volatile boolean started;

    /**
     * @param sources источники сводок
     * @param profiles профили стратегий
     * @param refreshMinutes период пересчёта, мин
     */
    public DiscoveryService(List<MarketSource> sources, List<StrategyProfile> profiles, long refreshMinutes) {
        this.sources = sources;
        this.profiles = profiles;
        this.refreshMinutes = refreshMinutes;
    }

    /**
     * Подбор по настройкам процесса: биржи (discoveryExchanges, пусто — все с источником сводок),
     * котируемые валюты, период пересчёта, бюджет свечей и пороги профилей.
     *
     * @param mrParams параметры бэктеста возврата к среднему для биржи (null — по умолчанию)
     * @param g        настройки процесса
     */
    public static DiscoveryService createDefault(java.util.function.Function<String, MeanReversionBacktest.Params> mrParams,
                                                 com.hft.config.GlobalParams g) {
        MarketSources.setQuotes(Arrays.stream(g.discoveryQuotes().split(",")).map(String::trim).filter(q -> !q.isEmpty()).toList());
        List<String> ids = g.discoveryExchanges().isBlank() ? MarketSources.supported()
                : Arrays.stream(g.discoveryExchanges().split(",")).map(String::trim).toList();
        List<MarketSource> src = new ArrayList<>();
        for (String id : ids) MarketSources.create(id).ifPresent(src::add);
        DiscoveryService d = new DiscoveryService(src, List.of(new Profiles.MeanReversion(mrParams, g), new Profiles.CrossExchange(g),
                new Profiles.SpreadCapture(g)), g.discoveryRefreshMin());
        d.klineBudget = g.discoveryKlineBudget();
        return d;
    }

    /** Сколько запросов свечей на биржу за один пересчёт. */
    private volatile int klineBudget = 12;

    /** Запустить пересчёт сейчас и далее по расписанию. */
    public synchronized void start() {
        if (started) return;
        started = true;
        scheduler.scheduleWithFixedDelay(this::runOnce, 0, Math.max(1, refreshMinutes), TimeUnit.MINUTES);
        log.info("Подбор тикеров: {} бирж, обновление раз в {} мин", sources.size(), refreshMinutes);
    }

    /** Остановить расписание. */
    public void stop() { scheduler.shutdownNow(); }

    /** Запустить пересчёт сейчас (в фоне). false — уже идёт. */
    public boolean refreshAsync() {
        if (running.get()) return false;
        scheduler.execute(this::runOnce);
        return true;
    }

    /** Пересчёт идёт. */
    public boolean isRunning() { return running.get(); }

    /** Результат последнего пересчёта и признак «идёт». */
    public Map<String, Object> result() {
        Map<String, Object> m = new LinkedHashMap<>(last);
        m.put("running", running.get());
        return m;
    }

    /** Один полный прогон (синхронно). */
    public Map<String, Object> runOnce() {
        if (!running.compareAndSet(false, true)) return last;
        long t0 = System.currentTimeMillis();
        try {
            Map<String, Object> exStatus = new LinkedHashMap<>();
            Map<String, List<TickerSnapshot>> byExchange = new LinkedHashMap<>();
            Map<String, MarketSource> srcById = new HashMap<>();
            ExecutorService pool = Executors.newFixedThreadPool(Math.max(1, Math.min(sources.size(), 8)), r -> {
                Thread t = new Thread(r, "discovery-fetch");
                t.setDaemon(true);
                return t;
            });
            try {
                Map<String, Future<List<TickerSnapshot>>> futures = new LinkedHashMap<>();
                for (MarketSource s : sources) { srcById.put(s.exchange(), s); futures.put(s.exchange(), pool.submit(s::tickers)); }
                for (var e : futures.entrySet()) {
                    Map<String, Object> st = new LinkedHashMap<>();
                    long s0 = System.currentTimeMillis();
                    try {
                        List<TickerSnapshot> l = e.getValue().get(60, TimeUnit.SECONDS);
                        byExchange.put(e.getKey(), l);
                        st.put("ok", true);
                        st.put("tickers", l.size());
                    } catch (Exception ex) {
                        Throwable c = ex instanceof ExecutionException ee && ee.getCause() != null ? ee.getCause() : ex;
                        st.put("ok", false);
                        st.put("error", c.getClass().getSimpleName() + ": " + c.getMessage());
                        log.warn("[discovery] {}: {}", e.getKey(), c.toString());
                    }
                    st.put("waitMs", System.currentTimeMillis() - s0);
                    exStatus.put(e.getKey(), st);
                }
            } finally {
                pool.shutdownNow();
            }

            Map<String, Integer> budget = new ConcurrentHashMap<>();
            Map<String, double[]> cache = new ConcurrentHashMap<>();
            int perExchangeBudget = klineBudget;
            StrategyProfile.ClosesProvider closes = (t, limit) -> {
                String key = t.exchange() + ":" + t.symbol();
                double[] c = cache.get(key);
                if (c != null) return c;
                if (budget.merge(t.exchange(), 1, Integer::sum) > perExchangeBudget) return null;
                MarketSource s = srcById.get(t.exchange());
                if (s == null) return null;
                try {
                    c = s.closes1m(t, limit);
                    cache.put(key, c);
                    return c;
                } catch (Exception e) {
                    log.warn("[discovery] свечи {} {}: {}", t.exchange(), t.symbol(), e.toString());
                    Object st = exStatus.get(t.exchange());
                    if (st instanceof Map<?, ?> m) {
                        @SuppressWarnings("unchecked") Map<String, Object> mm = (Map<String, Object>) m;
                        mm.put("klineError", e.getClass().getSimpleName() + ": " + e.getMessage());
                    }
                    return null;
                }
            };

            List<Map<String, Object>> strategies = new ArrayList<>();
            for (StrategyProfile p : profiles) {
                Map<String, Object> sm = new LinkedHashMap<>();
                sm.put("id", p.id());
                sm.put("title", p.title());
                sm.put("description", p.description());
                sm.put("executable", p.executable());
                sm.put("criteria", p.criteria());
                try {
                    List<Candidate> c = p.screen(byExchange, closes);
                    sm.put("candidates", c);
                    sm.put("suitable", c.stream().filter(Candidate::suitable).count());
                } catch (Exception e) {
                    log.error("[discovery] профиль {}: {}", p.id(), e.toString());
                    sm.put("candidates", List.of());
                    sm.put("error", e.toString());
                }
                strategies.add(sm);
            }

            Map<String, Object> r = new LinkedHashMap<>();
            r.put("updatedAt", System.currentTimeMillis());
            r.put("durationMs", System.currentTimeMillis() - t0);
            r.put("refreshMinutes", refreshMinutes);
            r.put("exchanges", exStatus);
            r.put("strategies", strategies);
            r.put("note", "Форматы публичных API бирж записаны по документации и не проверены на живых биржах; ошибки видны в exchanges.");
            last = r;
            log.info("[discovery] готово за {} мс: {}", r.get("durationMs"),
                    strategies.stream().map(s -> s.get("id") + "=" + s.get("suitable")).toList());
            return r;
        } catch (Exception e) {
            log.error("[discovery] сбой: {}", e.toString());
            return last;
        } finally {
            running.set(false);
        }
    }
}
