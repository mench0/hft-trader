package com.hft.engine;

import com.hft.model.OrderResult;
import com.hft.model.Tick;
import com.hft.store.MarketDataStore;
import com.hft.store.OrderBook;
import com.hft.store.PriceWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Пример рабочей стратегии: возврат к среднему по z-score.
 *
 * Логика:
 *   - цена ушла вниз больше чем на entryZ сигм от скользящего среднего
 *   - стакан подтверждает: покупателей больше продавцов (imbalance > 0)
 *   - открываем лонг
 *   - закрываем, когда z-score вернулся к exitZ или сработал стоп
 *
 * Ордера уходят через {@link OrderExecutor}: поток конвейера никогда не ждёт биржу.
 * Пока по символу ордер «в полёте», новых решений по нему нет; позиция появляется/исчезает
 * в колбэке после ответа биржи. Вход — только по свежему стакану (не старше maxBookAgeMs)
 * и только если фид в реальном времени (не на REST-запасе).
 *
 * Это не готовая для денег стратегия, а демонстрация того, как
 * связаны хранилище, стакан и сервис ордеров. Параметры нужно
 * подбирать на исторических данных.
 */
public final class MeanReversionStrategy extends Strategy {

    private static final Logger log = LoggerFactory.getLogger(MeanReversionStrategy.class);

    // Параметры — вынесены в поля, чтобы менять из админки без пересборки
    private volatile double entryZ = 2.0;       // порог входа в сигмах
    private volatile double exitZ = 0.3;        // порог выхода
    private volatile double stopLossPercent = 0.5;
    private volatile double minImbalance = 0.15; // минимальный перевес бидов
    private volatile double orderQuote = 20.0;   // размер сделки в USDT

    /** Открытые позиции: символ -> детали входа. */
    private final Map<String, Position> positions = new ConcurrentHashMap<>();

    private record Position(double entryPrice, double quantity, long openedAtMs) {}

    private final OrderExecutor executor;
    private final double[] top = new double[4];          // только поток конвейера
    private volatile long maxBookAgeMs = 2_000;
    /** Фид в реальном времени? (на REST-запасе стакан старый — новые входы запрещены, выходы разрешены) */
    private volatile java.util.function.BooleanSupplier realtime = () -> true;

    public MeanReversionStrategy(MarketDataStore market, OrderService orders) {
        this(market, orders, "default");
    }

    public MeanReversionStrategy(MarketDataStore market, OrderService orders, String exchangeId) {
        super("mean-reversion", market, orders);
        this.executor = new OrderExecutor(exchangeId, 4);
    }

    /** Источник признака «данные в реальном времени» (фид биржи). */
    public void setRealtimeSource(java.util.function.BooleanSupplier s) { this.realtime = s; }

    public OrderExecutor executor() { return executor; }

    @Override
    protected void onTick(Tick tick) {
        String symbol = tick.symbol();

        PriceWindow window = market.window(symbol);
        OrderBook book = market.book(symbol);
        if (window == null || book == null) return;
        if (!window.isWarmedUp() || !book.isReady()) return;

        if (executor.isBusy(symbol)) return;              // ордер по символу ещё в полёте
        double z = window.currentZScore();
        Position pos = positions.get(symbol);

        if (pos == null) {
            checkEntry(symbol, z, book);
        } else {
            checkExit(symbol, z, tick.price(), pos);
        }
    }

    private void checkEntry(String symbol, double z, OrderBook book) {
        // Ищем только перепроданность — лонг от низа
        if (z > -entryZ) return;
        if (book.ageMs() > maxBookAgeMs || !realtime.getAsBoolean()) return;   // старые данные — не входим

        // Стакан должен подтверждать: покупателей больше
        double imbalance = book.imbalance(5);
        if (imbalance < minImbalance) return;

        // Спред не должен быть аномально широким — признак низкой ликвидности
        if (!book.readTop(top)) return;                  // bid/ask из одного снимка
        double mid = (top[0] + top[2]) / 2;
        double spread = (top[2] - top[0]) / mid * 100.0;
        if (spread > 0.1) return;

        double price = top[2];
        double qty = orderQuote / price;

        log.info("Сигнал входа {}: z={} imbalance={} spread={}%",
                symbol, String.format("%.2f", z),
                String.format("%.2f", imbalance), String.format("%.3f", spread));

        executor.submit(symbol, () -> {
            OrderResult result = orders.buyMarket(symbol, qty);
            if (result.executedQty() > 0) {
                positions.put(symbol, new Position(
                        result.avgPrice(), result.executedQty(), System.currentTimeMillis()));
                log.info("Позиция открыта: {} {} @ {}", result.executedQty(), symbol, result.avgPrice());
            }
        });
    }

    private void checkExit(String symbol, double z, double currentPrice, Position pos) {
        double pnlPercent = (currentPrice - pos.entryPrice()) / pos.entryPrice() * 100.0;

        boolean takeProfit = z >= -exitZ;
        boolean stopLoss = pnlPercent <= -stopLossPercent;
        // Страховка от зависших позиций: закрыть через час в любом случае
        boolean timeout = System.currentTimeMillis() - pos.openedAtMs() > 3_600_000;

        if (!takeProfit && !stopLoss && !timeout) return;

        String reason = stopLoss ? "стоп-лосс" : takeProfit ? "тейк-профит" : "таймаут";
        log.info("Сигнал выхода {} ({}): z={} pnl={}%",
                symbol, reason, String.format("%.2f", z), String.format("%.3f", pnlPercent));

        executor.submit(symbol, () -> closePosition(symbol, pos));
    }

    private void closePosition(String symbol, Position pos) {
        OrderResult result = orders.sellMarket(symbol, pos.quantity());
        if (result.executedQty() <= 0) return;
        double left = pos.quantity() - result.executedQty();
        if (left > pos.quantity() * 1e-6) positions.put(symbol, new Position(pos.entryPrice(), left, pos.openedAtMs()));
        else positions.remove(symbol);
        double realized = (result.avgPrice() - pos.entryPrice()) * result.executedQty();
        log.info("Позиция закрыта: {} {} @ {}, результат {} USDT",
                result.executedQty(), symbol, result.avgPrice(), String.format("%.4f", realized));
    }

    /** Закрыть все позиции — вызывается при остановке бота. */
    public void closeAll() {
        // сначала дождаться ордеров в полёте: иначе позиция, открытая прямо сейчас, останется незакрытой
        if (!executor.drain(15_000)) log.warn("Не все ордера завершились за 15 с — закрываю то, что известно");
        positions.forEach((symbol, pos) -> {
            log.info("Закрываю позицию по {} при остановке", symbol);
            closePosition(symbol, pos);
        });
    }

    // Параметры доступны на изменение через админку
    public void setEntryZ(double v) { this.entryZ = v; }
    public void setExitZ(double v) { this.exitZ = v; }
    public void setStopLossPercent(double v) { this.stopLossPercent = v; }
    public void setMinImbalance(double v) { this.minImbalance = v; }
    public void setOrderQuote(double v) { this.orderQuote = v; }
    public void setMaxBookAgeMs(long v) { this.maxBookAgeMs = v; }

    public double entryZ() { return entryZ; }
    public double exitZ() { return exitZ; }
    public double stopLossPercent() { return stopLossPercent; }
    public double minImbalance() { return minImbalance; }
    public double orderQuote() { return orderQuote; }
    public int openPositions() { return positions.size(); }
}
