package com.hft.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.http.HttpHeaders;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Общий бюджет запросов к одной бирже на весь процесс.
 *
 * Биржа считает лимит на IP или ключ, а не на объект в программе: торговый клиент, REST-опрос стакана,
 * подбор тикеров и WebSocket-подписки тратят один и тот же лимит. Поэтому бюджет один на биржу
 * ({@link #of(String)}), и все компоненты берут разрешение у него.
 *
 * Устройство: набор «вёдер» из {@link RateLimits} — у каждого ёмкость (с запасом 20% от официального
 * лимита), окно и виды запросов, которые в него входят. Запрос с весом w ждёт, пока во всех его вёдрах
 * наберётся w жетонов (жетоны можно занять заранее — запросы выстраиваются в очередь без гонки).
 * Если ждать дольше maxWaitMs — ApiException(429, LOCAL), поток не зависает.
 *
 * Ответы биржи: 429/418/403 и коды лимита останавливают все запросы к бирже на время из Retry-After
 * (или по умолчанию), повторные — вдвое дольше. Заголовки с израсходованным весом (Binance/Aster),
 * остатком и временем сброса (Bybit, Gate, KuCoin) подтягивают наш счёт к счёту биржи.
 */
public final class RateBudget {

    /** Логгер. */
    private static final Logger log = LoggerFactory.getLogger(RateBudget.class);
    /** Бюджеты всех бирж процесса. */
    private static final Map<String, RateBudget> ALL = new ConcurrentHashMap<>();

    /** Вид запроса: в какие вёдра он попадает, решает {@link RateLimits}. */
    public enum Kind { PUBLIC, PRIVATE, ORDER, WS_MESSAGE, WS_CONNECT }

    /** Ведро: capacity жетонов, полностью восполняется за windowMs. */
    public record Limit(String name, double capacity, long windowMs, Set<Kind> kinds) {
        /** Ведро по официальному лимиту (ёмкость умножается на долю SAFETY); без видов — для всех видов. */
        public static Limit of(String name, double officialCapacity, long windowMs, Kind... kinds) {
            return new Limit(name, officialCapacity * SAFETY, windowMs, kinds.length == 0 ? EnumSet.allOf(Kind.class) : EnumSet.of(kinds[0], kinds));
        }
    }

    /** Доля официального лимита, которую мы себе позволяем (настройка rateLimitSafety). */
    static volatile double SAFETY = 0.8;

    /** Задать долю лимита. Действует на бюджеты, созданные после вызова (контроллер вызывает при старте). */
    public static void setSafety(double safety) { SAFETY = Math.max(0.1, Math.min(1.0, safety)); }

    /** Биржа. */
    private final String exchange;
    /** Вёдра лимитов. */
    private final Bucket[] buckets;
    /** До какого момента все запросы к бирже запрещены. */
    private volatile long blockedUntilMs;
    /** Почему запрещены (для логов и метрик). */
    private volatile String blockReason = "";
    /** Когда биржа последний раз сообщала о превышении. */
    private long lastLimitHitMs;
    /** Сколько превышений подряд (за 5 минут) — для удвоения паузы. */
    private int consecutiveHits;
    /** Ответов биржи о превышении лимита. */
    private final AtomicLong rateLimitedResponses = new AtomicLong();
    /** Запросов, не отправленных из-за своего бюджета. */
    private final AtomicLong localRejects = new AtomicLong();
    /** Суммарное ожидание в очереди, мс. */
    private final AtomicLong waitedMs = new AtomicLong();

    /** Ведро жетонов одного лимита. */
    private static final class Bucket {
        /** Лимит ведра. */
        final Limit limit;
        /** Скорость восполнения, жетонов в мс. */
        final double refillPerMs;
        double tokens;          // может уйти в минус: это уже занятые будущие жетоны
        /** Когда ведро последний раз восполнялось. */
        long lastMs;
        Bucket(Limit l) { limit = l; refillPerMs = l.capacity() / l.windowMs(); tokens = l.capacity(); lastMs = System.currentTimeMillis(); }
        /** Восполнить жетоны пропорционально прошедшему времени. */
        void refill(long now) {
            if (now > lastMs) { tokens = Math.min(limit.capacity(), tokens + (now - lastMs) * refillPerMs); lastMs = now; }
        }
    }

    /**
     * @param exchange биржа
     * @param limits вёдра лимитов
     */
    RateBudget(String exchange, List<Limit> limits) {
        this.exchange = exchange;
        this.buckets = limits.stream().map(Bucket::new).toArray(Bucket[]::new);
    }

    /** Бюджет биржи (один на процесс). */
    public static RateBudget of(String exchange) {
        return ALL.computeIfAbsent(exchange, id -> new RateBudget(id, RateLimits.forExchange(id)));
    }

    /** Все созданные бюджеты (для метрик). */
    public static Map<String, RateBudget> all() { return Map.copyOf(ALL); }

    /** Биржа. */
    public String exchange() { return exchange; }

    // ------------------------------------------------------------ получение разрешения

    /**
     * Дождаться разрешения на запрос вида kind весом weight; ApiException(LOCAL), если ждать дольше maxWaitMs или запросы приостановлены надолго.
     */
    public void acquire(Kind kind, double weight, long maxWaitMs) throws InterruptedException {
        long blocked = blockedForMs();
        if (blocked > 0) {
            if (blocked > maxWaitMs) {
                localRejects.incrementAndGet();
                throw new ApiException(429, "LOCAL", exchange + ": запросы приостановлены ещё на " + blocked + " мс (" + blockReason + ")", true);
            }
            Thread.sleep(blocked);
        }
        long waitMs;
        synchronized (this) {
            long now = System.currentTimeMillis();
            waitMs = 0;
            for (Bucket b : buckets) {
                if (!b.limit.kinds().contains(kind)) continue;
                b.refill(now);
                double w = Math.min(weight, b.limit.capacity());
                double deficit = w - b.tokens;
                if (deficit > 0) waitMs = Math.max(waitMs, (long) Math.ceil(deficit / b.refillPerMs));
            }
            if (waitMs > maxWaitMs) {
                localRejects.incrementAndGet();
                throw new ApiException(429, "LOCAL", exchange + ": очередь запросов переполнена (ждать " + waitMs + " мс)", true);
            }
            for (Bucket b : buckets) if (b.limit.kinds().contains(kind)) b.tokens -= Math.min(weight, b.limit.capacity());
        }
        if (waitMs > 0) { waitedMs.addAndGet(waitMs); Thread.sleep(waitMs); }
    }

    /** Взять без ожидания; false — сейчас нельзя (для фоновых задач, которые могут подождать следующего цикла). */
    public synchronized boolean tryAcquire(Kind kind, double weight) {
        if (blockedForMs() > 0) return false;
        long now = System.currentTimeMillis();
        for (Bucket b : buckets) {
            if (!b.limit.kinds().contains(kind)) continue;
            b.refill(now);
            if (b.tokens < Math.min(weight, b.limit.capacity())) return false;
        }
        for (Bucket b : buckets) if (b.limit.kinds().contains(kind)) b.tokens -= Math.min(weight, b.limit.capacity());
        return true;
    }

    /** Полоса для одного вида запросов — удобная обёртка для клиентов. */
    public Lane lane(Kind kind, long maxWaitMs) { return new Lane(kind, maxWaitMs); }

    /** Полоса одного вида запросов с фиксированным пределом ожидания. */
    public final class Lane {
        /** Вид запросов полосы. */
        private final Kind kind;
        /** Предел ожидания полосы, мс. */
        private final long maxWaitMs;
        /**
         * @param kind вид запросов
         * @param maxWaitMs предел ожидания
         */
        private Lane(Kind kind, long maxWaitMs) { this.kind = kind; this.maxWaitMs = maxWaitMs; }
        /** Разрешение на запрос весом 1. */
        public void acquire() throws InterruptedException { RateBudget.this.acquire(kind, 1, maxWaitMs); }
        /** Разрешение на запрос указанного веса. */
        public void acquire(double weight) throws InterruptedException { RateBudget.this.acquire(kind, weight, maxWaitMs); }
        /** Приостановить все запросы к бирже на ms. */
        public void blockFor(long ms) { RateBudget.this.blockFor(ms, "локально"); }
        /** Сколько ещё мс запросы приостановлены. */
        public long blockedForMs() { return RateBudget.this.blockedForMs(); }
    }

    // ------------------------------------------------------------ ответы биржи

    /** Приостановить все запросы к бирже на ms с причиной. */
    public void blockFor(long ms, String reason) {
        long until = System.currentTimeMillis() + ms;
        if (until > blockedUntilMs) { blockedUntilMs = until; blockReason = reason; }
    }

    /** Сколько ещё мс запросы приостановлены. */
    public long blockedForMs() { return Math.max(0, blockedUntilMs - System.currentTimeMillis()); }

    /**
     * Учесть ответ биржи. Возвращает паузу в мс, если ответ означает превышение лимита (0 — нет).
     * Вызывать на каждый ответ, в том числе успешный: заголовки подтягивают счёт к счёту биржи.
     */
    public long onResponse(int httpStatus, HttpHeaders headers) {
        if (headers != null) syncFromHeaders(headers);
        if (httpStatus == 429 || httpStatus == 418 || httpStatus == 403) {
            OptionalLong retryAfter = headers == null ? OptionalLong.empty() : headers.firstValueAsLong("Retry-After");
            long base = switch (httpStatus) { case 418 -> 120_000; case 403 -> 60_000; default -> 10_000; };
            return limitHit("HTTP " + httpStatus, retryAfter.isPresent() ? retryAfter.getAsLong() * 1000 : base);
        }
        return 0;
    }

    /** Биржа сообщила о лимите в теле ответа (код ошибки) — та же пауза, что и для 429. */
    public long onLimitError(String what) { return limitHit(what, 10_000); }

    /** Превышение лимита: пауза (удваивается при повторах в течение 5 минут, до 30 минут) и опустошение вёдер. */
    private synchronized long limitHit(String what, long pauseMs) {
        long now = System.currentTimeMillis();
        consecutiveHits = now - lastLimitHitMs < 300_000 ? consecutiveHits + 1 : 1;
        lastLimitHitMs = now;
        long pause = Math.min(pauseMs * (1L << Math.min(consecutiveHits - 1, 6)), 30 * 60_000L);
        rateLimitedResponses.incrementAndGet();
        blockFor(pause, what);
        for (Bucket b : buckets) { b.refill(now); b.tokens = Math.min(b.tokens, 0); }   // после паузы — с пустых вёдер
        log.warn("[{}] {} — превышен лимит запросов, все запросы к бирже приостановлены на {} с (повтор №{})",
                exchange, what, pause / 1000, consecutiveHits);
        return pause;
    }

    /** Подтянуть счёт к счёту биржи по заголовкам ответа (Binance/Aster, Bybit, Gate, KuCoin). */
    private void syncFromHeaders(HttpHeaders h) {
        // Binance / Aster: израсходованный вес за минуту и число ордеров за 10 с / сутки
        h.firstValueAsLong("X-MBX-USED-WEIGHT-1M").ifPresent(used -> syncUsed("weight", used));
        h.firstValueAsLong("X-MBX-ORDER-COUNT-10S").ifPresent(used -> syncUsed("orders10s", used));
        h.firstValueAsLong("X-MBX-ORDER-COUNT-1D").ifPresent(used -> syncUsed("orders1d", used));
        // Bybit: остаток и момент сброса (мс эпохи)
        remainReset(h, "X-Bapi-Limit-Status", "X-Bapi-Limit-Reset-Timestamp", true);
        // Gate: остаток и момент сброса (мс эпохи)
        remainReset(h, "X-Gate-RateLimit-Requests-Remain", "X-Gate-RateLimit-Reset-Timestamp", true);
        // KuCoin: остаток в пуле и мс до сброса
        remainReset(h, "gw-ratelimit-remaining", "gw-ratelimit-reset", false);
    }

    /** Биржа говорит, сколько уже израсходовано: если это больше нашего счёта — уменьшаем остаток. */
    private synchronized void syncUsed(String bucket, long usedOfficial) {
        long now = System.currentTimeMillis();
        for (Bucket b : buckets) {
            if (!b.limit.name().equals(bucket)) continue;
            b.refill(now);
            double official = b.limit.capacity() / SAFETY;
            double left = b.limit.capacity() - Math.min(usedOfficial, official);   // запас 20% сохраняется
            if (left < b.tokens) b.tokens = left;
        }
    }

    /** Остаток почти исчерпан (≤ 2) — приостановить запросы до момента сброса из заголовка. */
    private void remainReset(HttpHeaders h, String remainHeader, String resetHeader, boolean resetIsEpoch) {
        OptionalLong remain = h.firstValueAsLong(remainHeader);
        if (remain.isEmpty() || remain.getAsLong() > 2) return;
        OptionalLong reset = h.firstValueAsLong(resetHeader);
        long waitMs = reset.isEmpty() ? 1_000 : resetIsEpoch ? reset.getAsLong() - System.currentTimeMillis() : reset.getAsLong();
        if (waitMs > 0) blockFor(Math.min(waitMs, 60_000), remainHeader + "=" + remain.getAsLong());
    }

    // ------------------------------------------------------------ метрики

    /** Заполненность каждого ведра (для метрик). */
    public synchronized List<Map<String, Object>> buckets() {
        long now = System.currentTimeMillis();
        List<Map<String, Object>> out = new ArrayList<>();
        for (Bucket b : buckets) {
            b.refill(now);
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("name", b.limit.name());
            m.put("capacity", b.limit.capacity());
            m.put("windowMs", b.limit.windowMs());
            m.put("usedRatio", Math.max(0, Math.min(1, 1 - b.tokens / b.limit.capacity())));
            out.add(m);
        }
        return out;
    }

    /** Метрики бюджета для админки. */
    public Map<String, Object> stats() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("blockedForMs", blockedForMs());
        m.put("blockReason", blockedForMs() > 0 ? blockReason : "");
        m.put("rateLimitedResponses", rateLimitedResponses.get());
        m.put("localRejects", localRejects.get());
        m.put("waitedMs", waitedMs.get());
        m.put("buckets", buckets());
        return m;
    }

    /** Ответов о превышении лимита. */
    public long rateLimitedResponses() { return rateLimitedResponses.get(); }
    /** Отказов своего бюджета. */
    public long localRejects() { return localRejects.get(); }
    /** Суммарное ожидание, мс. */
    public long waitedMs() { return waitedMs.get(); }
}
