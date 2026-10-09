package com.hft.perp;

import com.hft.config.TradingSettings;
import com.hft.engine.OrderService;
import com.hft.model.OrderResult;
import com.hft.store.MarketDataStore;
import com.hft.store.OrderBook;
import com.hft.store.PositionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Сторож позиций по перпам — раз в секунду:
 * <ul>
 *   <li><b>стоп на бирже</b> приводится к позиции ({@link OrderService#syncStop}): позиция изменилась мимо бота,
 *       стоп сработал, прошлая постановка не удалась — всё это чинится здесь;</li>
 *   <li><b>«ничьи» позиции</b> — открытые на бирже, но без стратегии-владельца (остались после перезапуска бота,
 *       открыты вручную, стратегия выключена) — берутся под защиту (guardOrphans): стоп-лосс orphanStopLossPercent
 *       от цены входа и, если задан, таймаут orphanMaxHoldMinutes с момента, как бот их заметил.</li>
 * </ul>
 * Закрытие — рыночным reduceOnly-ордером: проходит и при остановленной торговле.
 */
public final class PositionGuard {

    /** Логгер. */
    private static final Logger log = LoggerFactory.getLogger(PositionGuard.class);

    /** Биржа (для логов). */
    private final String id;
    /** Позиции биржи. */
    private final PositionStore positions;
    /** Ордера и владельцы позиций. */
    private final OrderService orders;
    /** Стаканы (текущая цена). */
    private final MarketDataStore market;
    /** Параметры биржи. */
    private final TradingSettings settings;
    /** Когда бот заметил «ничью» позицию по символу, мс. */
    private final Map<String, Long> orphanSince = new ConcurrentHashMap<>();
    /** Закрыто «ничьих» позиций. */
    private final AtomicLong closed = new AtomicLong();
    /** Поток проверок. */
    private volatile ScheduledExecutorService scheduler;

    /**
     * @param id биржа
     * @param positions позиции
     * @param orders сервис ордеров
     * @param market стаканы
     * @param settings параметры биржи
     */
    public PositionGuard(String id, PositionStore positions, OrderService orders, MarketDataStore market, TradingSettings settings) {
        this.id = id;
        this.positions = positions;
        this.orders = orders;
        this.market = market;
        this.settings = settings;
    }

    /** Запустить проверки раз в секунду. */
    public synchronized void start() {
        if (scheduler != null) return;
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "guard-" + id);
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(() -> {
            try { check(); } catch (Exception e) { log.error("[{}] сторож позиций: {}", id, e.toString()); }
        }, 1, 1, TimeUnit.SECONDS);
    }

    /** Остановить проверки. */
    public synchronized void stop() {
        if (scheduler != null) scheduler.shutdownNow();
        scheduler = null;
    }

    /** Одна проверка всех позиций (вызывается по таймеру; публично — для тестов). */
    public void check() {
        var p = settings.get();
        Set<String> symbols = new HashSet<>(positions.snapshot().keySet());
        symbols.addAll(orders.stopSymbols());
        long now = System.currentTimeMillis();
        for (String s : symbols) {
            orders.syncStop(s);
            PositionStore.Position pos = positions.get(s);
            if (pos.isFlat() || orders.owner(s) != null) { orphanSince.remove(s); continue; }
            if (!p.guardOrphans()) continue;
            Long since = orphanSince.get(s);
            if (since == null) {
                orphanSince.put(s, now);
                log.warn("[{}] позиция {} {} @ {} без стратегии — беру под защиту: стоп-лосс {}%{}", id, s, pos.qty(), pos.entryPrice(),
                        p.orphanStopLossPercent(), p.orphanMaxHoldMinutes() > 0 ? ", закрытие через " + p.orphanMaxHoldMinutes() + " мин" : "");
                since = now;
            }
            double price = price(s);
            if (!(price > 0) || !(pos.entryPrice() > 0)) continue;           // нет свежей цены — ждём
            double pnlPct = Math.signum(pos.qty()) * (price - pos.entryPrice()) / pos.entryPrice() * 100;
            String why = pnlPct <= -p.orphanStopLossPercent() ? "стоп-лосс " + String.format("%.2f", pnlPct) + "%"
                    : p.orphanMaxHoldMinutes() > 0 && now - since >= p.orphanMaxHoldMinutes() * 60_000 ? "таймаут" : null;
            if (why == null) continue;
            log.warn("[{}] закрываю позицию {} без стратегии ({})", id, s, why);
            OrderResult r = orders.closePosition(s);
            if (r != null && r.executedQty() > 0) { closed.incrementAndGet(); orphanSince.remove(s); }
        }
    }

    /** Середина свежего стакана; NaN — стакана нет. */
    private double price(String symbol) {
        OrderBook b = market.book(symbol);
        return b == null || !b.isReady() ? Double.NaN : b.midPrice();
    }

    /** Состояние для админки. */
    public Map<String, Object> stats() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("orphans", Set.copyOf(orphanSince.keySet()));
        m.put("orphansClosed", closed.get());
        Map<String, Object> st = new LinkedHashMap<>();
        for (String s : orders.stopSymbols()) st.put(s, orders.stopPrice(s));
        m.put("exchangeStops", st);
        return m;
    }
}
