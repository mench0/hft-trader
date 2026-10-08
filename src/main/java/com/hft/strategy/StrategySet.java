package com.hft.strategy;

import com.hft.engine.OrderService;
import com.hft.config.TradingSettings;
import com.hft.model.Tick;
import com.hft.store.MarketDataStore;
import com.lmax.disruptor.EventHandler;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BooleanSupplier;

/**
 * Все стратегии одной биржи. Каждая сама проверяет свой флаг в параметрах биржи
 * (meanReversionEnabled, triangularEnabled, statArbEnabled), поэтому включать и выключать их
 * можно из админки на лету; enable()/disable() здесь — общий выключатель торговли.
 */
public final class StrategySet {

    /** Возврат к среднему. */
    private final MeanReversionStrategy meanReversion;
    /** Треугольный арбитраж. */
    private final TriangularArbStrategy triangular;
    /** Статистический арбитраж. */
    private final StatArbStrategy statArb;
    /** Все стратегии в порядке вызова. */
    private final List<Strategy> all;

    /**
     * @param market рыночные данные биржи
     * @param orders сервис ордеров
     * @param exchangeId биржа (для имён потоков и логов)
     * @param settings параметры биржи
     */
    public StrategySet(MarketDataStore market, OrderService orders, String exchangeId, TradingSettings settings) {
        this.meanReversion = new MeanReversionStrategy(market, orders, exchangeId, settings);
        this.triangular = new TriangularArbStrategy(market, orders, exchangeId, settings);
        this.statArb = new StatArbStrategy(market, orders, exchangeId, settings);
        this.all = List.of(meanReversion, triangular, statArb);
    }

    /** Стратегия возврата к среднему. */
    public MeanReversionStrategy meanReversion() { return meanReversion; }
    /** Стратегия треугольного арбитража. */
    public TriangularArbStrategy triangular() { return triangular; }
    /** Стратегия статистического арбитража. */
    public StatArbStrategy statArb() { return statArb; }
    /** Все стратегии. */
    public List<Strategy> all() { return all; }

    /** Обработчики для конвейера тиков (идут после записи данных в память). */
    @SuppressWarnings("unchecked")
    public EventHandler<Tick>[] handlers() { return all.toArray(new EventHandler[0]); }

    /** Источник признака «данные в реальном времени»: на REST-запасе новых входов нет. */
    public void setRealtimeSource(BooleanSupplier s) {
        meanReversion.setRealtimeSource(s);
        triangular.setRealtimeSource(s);
        statArb.setRealtimeSource(s);
    }

    /** Разрешить торговлю всем стратегиям (каждая ещё проверяет свой флаг в параметрах). */
    public void enable() { all.forEach(Strategy::enable); }
    /** Запретить торговлю всем стратегиям. */
    public void disable() { all.forEach(Strategy::disable); }
    /** Торговля разрешена. */
    public boolean isEnabled() { return meanReversion.isEnabled(); }

    /** Закрыть позиции всех стратегий (треугольный арбитраж позиций не держит — только дождаться круга). */
    public void closeAll() {
        triangular.drain(15_000);
        meanReversion.closeAll();
        statArb.closeAll();
    }

    /** Открытых позиций у всех стратегий вместе. */
    public int openPositions() { return meanReversion.openPositions() + statArb.openPositions(); }

    /** Состояние стратегий для GET /strategies. */
    public Map<String, Object> stats() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("meanReversion", Map.of("openPositions", meanReversion.openPositions(), "orderExecutor", meanReversion.executor().stats()));
        m.put("triangular", triangular.stats());
        m.put("statArb", statArb.stats());
        return m;
    }
}
