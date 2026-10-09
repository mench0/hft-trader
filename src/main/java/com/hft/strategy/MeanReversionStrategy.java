package com.hft.strategy;

import com.hft.engine.OrderService;
import com.hft.config.TradingParams;
import com.hft.config.TradingSettings;
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
 * Отправка ордеров вне потока конвейера тиков.
 *
 * <p>Стратегия работает в потоке Disruptor. Если ждать в нём ответа биржи
 * (REST до 10 с, WebSocket до 5 с, паузы лимитера, дочитывание статуса),
 * тики всех символов биржи встанут в очередь, и стратегия начнёт торговать
 * по устаревшим ценам.
 *
 * <p>Поэтому стратегия только ставит задачу в этот исполнитель и сразу
 * возвращается.
 *
 * <p>Правила:
 * <ul>
 *   <li>
 *     <b>Один символ — одна задача одновременно.</b>
 *     Не более одного «ордера в полёте» на символ. Пока ответ не получен,
 *     стратегия не принимает по этому символу новых торговых решений.
 *   </li>
 *   <li>
 *     <b>Разные символы выполняются параллельно.</b>
 *     Используется небольшой пул потоков, чтобы медленный ответ по одному
 *     символу не блокировал обработку других.
 *   </li>
 *   <li>
 *     <b>Потоки завершаются после простоя.</b>
 *     После перезапуска биржи пул не накапливает неиспользуемые потоки.
 *   </li>
 * </ul>
 */
public final class MeanReversionStrategy extends Strategy {

    /** Логгер. */
    private static final Logger log = LoggerFactory.getLogger(MeanReversionStrategy.class);

    /** Параметры биржи из админки; читаются на каждом тике, меняются без перезапуска. */
    private final TradingSettings settings;

    /** Открытые позиции: символ -> детали входа. */
    private final Map<String, Position> positions = new ConcurrentHashMap<>();
    /** Имя владельца позиций в OrderService. */
    private static final String OWNER = "mean-reversion";

    /** Открытая позиция: цена входа, объём, время открытия, направление (шорт — только на перпах). */
    private record Position(double entryPrice, double quantity, long openedAtMs, boolean isLong) {}

    /** Отправка ордеров вне потока конвейера. */
    private final OrderExecutor executor;
    private final double[] top = new double[4];          // только поток конвейера
    /** Фид в реальном времени? (на REST-запасе стакан старый — новые входы запрещены, выходы разрешены) */
    private volatile java.util.function.BooleanSupplier realtime = () -> true;

    /**
     * @param market рыночные данные
     * @param orders сервис ордеров
     * @param exchangeId биржа (имя потоков)
     * @param settings параметры биржи
     */
    public MeanReversionStrategy(MarketDataStore market, OrderService orders, String exchangeId, TradingSettings settings) {
        super("mean-reversion", market, orders);
        this.settings = settings;
        this.executor = new OrderExecutor(exchangeId, settings.get().orderThreads());
    }

    /** Источник признака «данные в реальном времени» (фид биржи). */
    public void setRealtimeSource(java.util.function.BooleanSupplier s) { this.realtime = s; }

    /** Исполнитель ордеров (для метрик). */
    public OrderExecutor executor() { return executor; }

    /** Решение по тику: вход, если позиции нет, иначе проверка выхода. */
    @Override
    protected void onTick(Tick tick) {
        if (!settings.get().meanReversionEnabled()) return;
        String symbol = tick.symbol();

        PriceWindow window = market.window(symbol);
        OrderBook book = market.book(symbol);
        if (window == null || book == null) return;
        if (!window.isWarmedUp() || !book.isReady()) return;

        if (executor.isBusy(symbol)) return;              // ордер по символу ещё в полёте
        double z = window.currentZScore();
        Position pos = positions.get(symbol);

        TradingParams p = settings.get();
        if (pos != null) pos = reconcile(symbol, pos);    // позиция могла измениться мимо стратегии
        if (pos == null) {
            checkEntry(symbol, z, book, window, p);
        } else {
            checkExit(symbol, z, tick.price(), pos, p);
        }
    }

    /**
     * Вход: лонг при перепроданности (z ≤ −entryZ, перевес бидов), на перпах ещё и шорт при перекупленности
     * (z ≥ entryZ, перевес асков). Плюс узкий спред, свежий стакан и данные в реальном времени.
     */
    private void checkEntry(String symbol, double z, OrderBook book, PriceWindow window, TradingParams p) {
        boolean isLong = z <= -p.entryZ();
        boolean isShort = !isLong && orders.isPerp() && z >= p.entryZ();   // шорт — только на фьючерсах
        if (!isLong && !isShort) return;
        if (book.ageMs() > p.maxBookAgeMs() || !realtime.getAsBoolean()) return;   // старые данные — не входим

        // Стакан должен подтверждать направление: для лонга покупателей больше, для шорта — продавцов
        double imbalance = book.imbalance(p.imbalanceLevels());
        if (isLong ? imbalance < p.minImbalance() : -imbalance < p.minImbalance()) return;

        // Спред не должен быть аномально широким — признак низкой ликвидности
        if (!book.readTop(top)) return;                  // bid/ask из одного снимка
        double mid = (top[0] + top[2]) / 2;
        double spread = (top[2] - top[0]) / mid * 100.0;
        if (spread > p.maxSpreadPercent()) return;

        double price = isLong ? top[2] : top[0];
        double qty = p.orderQuote() / price;

        // Ожидаемый ход до выхода: от |z| до exitZ сигм. Он должен окупать комиссии входа и выхода
        // (2 × takerFeePercent), спред, который платим при входе по рынку, и фиксированные издержки
        double expectedPct = (Math.abs(z) - p.exitZ()) * window.stdDev() / price * 100.0;
        double costPct = orders.roundTripCostPercent(p.orderQuote()) + spread;
        if (!(expectedPct > costPct)) return;

        log.info("Сигнал входа {} {}: z={} imbalance={} spread={}%", isLong ? "в лонг" : "в шорт",
                symbol, String.format("%.2f", z),
                String.format("%.2f", imbalance), String.format("%.3f", spread));

        if (!orders.claim(symbol, OWNER)) return;         // позицией символа управляет другая стратегия
        boolean submitted = executor.submit(symbol, () -> {
            OrderResult result = isLong ? orders.buyMarket(symbol, qty) : orders.sellMarket(symbol, qty);
            if (result.executedQty() > 0) {
                positions.put(symbol, new Position(
                        result.avgPrice(), result.executedQty(), System.currentTimeMillis(), isLong));
                log.info("Позиция {} открыта: {} {} @ {}", isLong ? "лонг" : "шорт", result.executedQty(), symbol, result.avgPrice());
            } else {
                orders.release(symbol, OWNER);
            }
        });
        if (!submitted) orders.release(symbol, OWNER);
    }

    /** Выход: z вернулся, стоп-лосс или таймаут. */
    private void checkExit(String symbol, double z, double currentPrice, Position pos, TradingParams p) {
        double dir = pos.isLong() ? 1 : -1;
        double pnlPercent = dir * (currentPrice - pos.entryPrice()) / pos.entryPrice() * 100.0;

        boolean takeProfit = pos.isLong() ? z >= -p.exitZ() : z <= p.exitZ();
        boolean stopLoss = pnlPercent <= -p.stopLossPercent();
        // Страховка от зависших позиций: закрыть по таймауту в любом случае
        boolean timeout = System.currentTimeMillis() - pos.openedAtMs() > p.positionTimeoutMs();

        if (!takeProfit && !stopLoss && !timeout) return;

        String reason = stopLoss ? "стоп-лосс" : takeProfit ? "тейк-профит" : "таймаут";
        log.info("Сигнал выхода {} ({}): z={} pnl={}%",
                symbol, reason, String.format("%.2f", z), String.format("%.3f", pnlPercent));

        executor.submit(symbol, () -> closePosition(symbol, pos));
    }

    /** Закрыть позицию (лонг — продажей, шорт — покупкой; на перпах — reduceOnly); результат после комиссий — в дневной PnL. */
    private void closePosition(String symbol, Position pos) {
        OrderResult result = orders.reduceMarket(symbol, pos.isLong() ? com.hft.model.OrderEnums.Side.SELL : com.hft.model.OrderEnums.Side.BUY, pos.quantity());
        if (result.executedQty() <= 0) return;
        double left = pos.quantity() - result.executedQty();
        if (left > pos.quantity() * 1e-6) positions.put(symbol, new Position(pos.entryPrice(), left, pos.openedAtMs(), pos.isLong()));
        else { positions.remove(symbol); orders.release(symbol, OWNER); }
        // комиссия тейкера на обеих ногах — в риск идёт чистый результат, по нему считается дневной лимит убытка
        // в бумаге комиссия уже в цене исполнения — не вычитаем второй раз
        // комиссии входа и выхода (в бумаге — уже в ценах) и фиксированные издержки двух сделок
        double fee = orders.costOf(pos.entryPrice() * result.executedQty()) + orders.costOf(result.avgPrice() * result.executedQty());
        double realized = (pos.isLong() ? 1 : -1) * (result.avgPrice() - pos.entryPrice()) * result.executedQty() - fee;
        orders.risk().recordPnl(realized);
        log.info("Позиция закрыта: {} {} @ {}, результат {} USDT",
                result.executedQty(), symbol, result.avgPrice(), String.format("%.4f", realized));
    }

    /**
     * Сверить свою позицию с фактической: на перпах — с позицией биржи (ликвидация, ручное закрытие, сработавший
     * стоп на бирже), на споте — с остатком монеты. Меньше — уменьшить запись, нет или другой знак — забыть.
     * @return позиция после сверки; null — позиции больше нет
     */
    private Position reconcile(String symbol, Position pos) {
        double actual;
        if (orders.isPerp()) {
            double q = orders.positions().qty(symbol);
            actual = pos.isLong() ? Math.max(0, q) : Math.max(0, -q);
        } else {
            actual = orders.balances().total(com.hft.store.BalanceStore.baseAsset(symbol));
        }
        if (actual >= pos.quantity() * (1 - 1e-6)) return pos;
        if (actual <= pos.quantity() * 1e-6) {
            log.warn("Позиция {} {} закрыта мимо стратегии (ликвидация, стоп на бирже или вручную) — снимаю с учёта", symbol, pos.isLong() ? "лонг" : "шорт");
            positions.remove(symbol);
            orders.release(symbol, OWNER);
            return null;
        }
        log.warn("Позиция {} уменьшилась мимо стратегии: {} -> {}", symbol, pos.quantity(), actual);
        Position fixed = new Position(pos.entryPrice(), actual, pos.openedAtMs(), pos.isLong());
        positions.put(symbol, fixed);
        return fixed;
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

    /** Открытых позиций. */
    public int openPositions() { return positions.size(); }
}
