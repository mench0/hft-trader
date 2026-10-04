package com.hft.store;

import com.hft.model.Tick;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Всё состояние рынка в оперативной памяти. Ничего не пишется на диск,
 * ничего не читается из базы — только RAM.
 *<p>
 * Что хранится по каждому инструменту:
 *   - стакан (OrderBook) с заданной глубиной
 *   - скользящее окно цен (PriceWindow) для статистики и z-score
 *   - последняя цена, объём, счётчики
 *<p>
 * Потокобезопасность: карты — ConcurrentHashMap, потому что читать могут
 * несколько потоков (стратегия, HTTP-админка). Сами OrderBook и PriceWindow
 * не синхронизированы: пишет в них только поток Disruptor.
 * Для 5 мс бюджета этого достаточно — читатель может увидеть слегка
 * устаревшее значение, но не повреждённое.
 */
public final class MarketDataStore {

    /** Глубина стаканов. */
    private final int bookDepth;
    /** Размер окна цен. */
    private final int windowSize;

    /** Стакан по символу. */
    private final Map<String, OrderBook> books = new ConcurrentHashMap<>();
    /** Окно цен по символу. */
    private final Map<String, PriceWindow> windows = new ConcurrentHashMap<>();
    /** Статистика тиков по символу. */
    private final Map<String, SymbolStats> stats = new ConcurrentHashMap<>();

    /**
     * @param bookDepth глубина стакана, уровней
     * @param windowSize окно цен, тиков
     */
    public MarketDataStore(int bookDepth, int windowSize) {
        this.bookDepth = bookDepth;
        this.windowSize = windowSize;
    }

    /** Статистика по одному символу. Mutable, обновляется на каждом тике. */
    public static final class SymbolStats {
        /** Цена последнего тика. */
        volatile double lastPrice;
        /** Сколько тиков получено. */
        volatile long tickCount;

        /** Цена последнего тика. */
        public double lastPrice() { return lastPrice; }
        /** Сколько тиков получено. */
        public long tickCount() { return tickCount; }
    }

    /** Регистрация инструмента. Вызывается на старте для каждого символа. */
    public void register(String symbol) {
        String key = symbol.toUpperCase();
        books.computeIfAbsent(key, s -> new OrderBook(s, bookDepth));
        windows.computeIfAbsent(key, s -> new PriceWindow(windowSize));
        stats.computeIfAbsent(key, s -> new SymbolStats());
    }

    /**
     * Обновление по сделке. Вызывается из потока Disruptor —
     * ровно один поток, поэтому синхронизация не нужна.
     */
    public void onTick(Tick tick) {
        String symbol = tick.symbol();

        PriceWindow window = windows.get(symbol);
        if (window != null) {
            window.add(tick.price());
        }

        SymbolStats st = stats.get(symbol);
        if (st != null) {
            st.lastPrice = tick.price();
            st.tickCount++;
        }
    }

    /** Стакан символа или null. */
    public OrderBook book(String symbol) {
        return books.get(symbol.toUpperCase());
    }

    /** Окно цен символа или null. */
    public PriceWindow window(String symbol) {
        return windows.get(symbol.toUpperCase());
    }

    /** Статистика символа или null. */
    public SymbolStats stats(String symbol) {
        return stats.get(symbol.toUpperCase());
    }

    /** Зарегистрированные символы. */
    public Set<String> symbols() {
        return Collections.unmodifiableSet(books.keySet());
    }

    /**
     * Лучшая доступная цена для оценки: микроцена из стакана,
     * если стакан пуст — последняя цена сделки.
     */
    public double referencePrice(String symbol) {
        OrderBook b = book(symbol);
        if (b != null && b.isReady()) {
            double micro = b.microPrice();
            if (!Double.isNaN(micro)) return micro;
        }
        SymbolStats st = stats(symbol);
        return st != null ? st.lastPrice : Double.NaN;
    }

    /** Есть ли по символу свежие данные (стакан не старше maxAgeMs). */
    public boolean isFresh(String symbol, long maxAgeMs) {
        OrderBook b = book(symbol);
        return b != null && b.isReady() && b.ageMs() <= maxAgeMs;
    }
}
