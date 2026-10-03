package com.hft.store;

import com.hft.model.Tick;
import org.agrona.collections.Object2ObjectHashMap;

import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Всё состояние рынка в оперативной памяти. Ничего не пишется на диск,
 * ничего не читается из базы — только RAM.
 *
 * Что хранится по каждому инструменту:
 *   - стакан (OrderBook) с заданной глубиной
 *   - скользящее окно цен (PriceWindow) для статистики и z-score
 *   - последняя цена, объём, счётчики
 *
 * Потокобезопасность: карты — ConcurrentHashMap, потому что читать могут
 * несколько потоков (стратегия, HTTP-админка). Сами OrderBook и PriceWindow
 * не синхронизированы: пишет в них только поток Disruptor.
 * Для 5 мс бюджета этого достаточно — читатель может увидеть слегка
 * устаревшее значение, но не повреждённое.
 */
public final class MarketDataStore {

    private final int bookDepth;
    private final int windowSize;

    private final Map<String, OrderBook> books = new ConcurrentHashMap<>();
    private final Map<String, PriceWindow> windows = new ConcurrentHashMap<>();
    private final Map<String, SymbolStats> stats = new ConcurrentHashMap<>();

    public MarketDataStore(int bookDepth, int windowSize) {
        this.bookDepth = bookDepth;
        this.windowSize = windowSize;
    }

    /** Статистика по одному символу. Mutable, обновляется на каждом тике. */
    public static final class SymbolStats {
        volatile double lastPrice;
        volatile double lastQty;
        volatile long tickCount;
        volatile long lastTickMs;
        volatile double volumeBuy;   // объём агрессивных покупок
        volatile double volumeSell;  // объём агрессивных продаж

        public double lastPrice() { return lastPrice; }
        public double lastQty() { return lastQty; }
        public long tickCount() { return tickCount; }
        public long lastTickMs() { return lastTickMs; }
        public double volumeBuy() { return volumeBuy; }
        public double volumeSell() { return volumeSell; }

        /**
         * Дельта объёма: покупки минус продажи.
         * Положительная — давление вверх, отрицательная — вниз.
         */
        public double volumeDelta() { return volumeBuy - volumeSell; }

        /** Доля покупок в общем объёме: 0.5 = равновесие. */
        public double buyRatio() {
            double total = volumeBuy + volumeSell;
            return total == 0 ? 0.5 : volumeBuy / total;
        }

        void reset() {
            volumeBuy = 0;
            volumeSell = 0;
        }
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
            st.lastQty = tick.quantity();
            st.tickCount++;
            st.lastTickMs = tick.exchangeTimeMs();
            // buyerIsMaker == true означает, что агрессором был продавец
            if (tick.buyerIsMaker()) {
                st.volumeSell += tick.quantity();
            } else {
                st.volumeBuy += tick.quantity();
            }
        }
    }

    public OrderBook book(String symbol) {
        return books.get(symbol.toUpperCase());
    }

    public PriceWindow window(String symbol) {
        return windows.get(symbol.toUpperCase());
    }

    public SymbolStats stats(String symbol) {
        return stats.get(symbol.toUpperCase());
    }

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

    /** Сброс счётчиков объёма — вызывать периодически, например раз в минуту. */
    public void resetVolumeCounters() {
        stats.values().forEach(SymbolStats::reset);
    }

    /** Есть ли по символу свежие данные (стакан не старше maxAgeMs). */
    public boolean isFresh(String symbol, long maxAgeMs) {
        OrderBook b = book(symbol);
        return b != null && b.isReady() && b.ageMs() <= maxAgeMs;
    }
}
