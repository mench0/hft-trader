package com.hft.risk;

import com.hft.config.AppConfig;
import com.hft.model.OrderRequest;
import com.hft.model.OrderResult;
import com.hft.store.MarketDataStore;
import com.hft.store.OrderBook;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.DoubleAdder;

/**
 * Проверки перед отправкой ордера.
 *
 * Это самая важная часть для реальных денег. Стратегия может быть
 * посредственной и всё равно не разорить счёт, если риск-менеджмент
 * работает. Обратное неверно.
 *
 * Что проверяется:
 *   - глобальный выключатель торговли (kill switch)
 *   - размер позиции в деньгах
 *   - дневной лимит убытка
 *   - ожидаемое проскальзывание для рыночных ордеров
 *   - свежесть рыночных данных
 *   - частота отправки ордеров
 */
public final class RiskManager {

    private static final Logger log = LoggerFactory.getLogger(RiskManager.class);

    private final AppConfig config;
    private final MarketDataStore market;
    private final String exchangeId;

    private final AtomicBoolean killSwitch = new AtomicBoolean(false);
    private final DoubleAdder dailyPnl = new DoubleAdder();
    private final AtomicInteger ordersThisMinute = new AtomicInteger();
    private final AtomicLong minuteWindowStart = new AtomicLong(System.currentTimeMillis());
    private final AtomicLong rejectedCount = new AtomicLong();
    private final AtomicLong acceptedCount = new AtomicLong();

    private volatile String lastRejectReason = "";

    public RiskManager(AppConfig config, MarketDataStore market, String exchangeId) {
        this.config = config;
        this.market = market;
        this.exchangeId = exchangeId;
    }

    /** Результат проверки: разрешено или отказ с причиной. */
    public record Decision(boolean allowed, String reason) {
        public static final Decision OK = new Decision(true, "");
        public static Decision deny(String reason) { return new Decision(false, reason); }
    }

    /**
     * Главная проверка. Вызывается перед каждым ордером.
     * Порядок проверок от дешёвых к дорогим.
     */
    public Decision check(OrderRequest request, double resolvedQty, double estimatedPrice) {
        if (killSwitch.get()) {
            return reject("Торговля остановлена (kill switch)");
        }

        if (!config.tradingEnabled()) {
            return reject("Торговля выключена в конфигурации");
        }

        // Лимит частоты — защита от бага в стратегии, который начнёт слать ордера в цикле
        if (!allowRate()) {
            return reject("Превышен лимит " + config.maxOrdersPerMinute() + " ордеров в минуту");
        }

        // Дневной убыток
        double pnl = dailyPnl.sum();
        if (pnl < -config.maxDailyLossQuote()) {
            killSwitch.set(true);
            log.error("[{}] Достигнут дневной лимит убытка {}. Торговля остановлена.", exchangeId, pnl);
            return reject("Достигнут дневной лимит убытка");
        }

        // Свежесть данных: торговать по протухшему стакану опасно
        if (!market.isFresh(request.symbol(), 5000)) {
            return reject("Нет свежих данных по " + request.symbol());
        }

        // Размер позиции в деньгах
        double notional = resolvedQty * estimatedPrice;
        if (notional > config.maxPositionQuote()) {
            return reject(String.format("Размер позиции %.2f превышает лимит %.2f",
                    notional, config.maxPositionQuote()));
        }

        // Проскальзывание для рыночных ордеров
        if (request.type() == com.hft.model.OrderEnums.Type.MARKET) {
            OrderBook book = market.book(request.symbol());
            if (book != null && book.isReady()) {
                boolean isBuy = request.side() == com.hft.model.OrderEnums.Side.BUY;
                double slip = book.estimateSlippagePercent(resolvedQty, isBuy);
                if (!Double.isNaN(slip) && slip > config.maxSlippagePercent()) {
                    return reject(String.format(
                            "Ожидаемое проскальзывание %.3f%% превышает лимит %.3f%% — стакан слишком тонкий",
                            slip, config.maxSlippagePercent()));
                }
                if (Double.isNaN(slip)) {
                    return reject("В стакане недостаточно объёма для этого ордера");
                }
            }
        }

        acceptedCount.incrementAndGet();
        return Decision.OK;
    }

    private Decision reject(String reason) {
        rejectedCount.incrementAndGet();
        lastRejectReason = reason;
        log.warn("[{}] Ордер отклонён риск-менеджером: {}", exchangeId, reason);
        return Decision.deny(reason);
    }

    private boolean allowRate() {
        long now = System.currentTimeMillis();
        long windowStart = minuteWindowStart.get();
        if (now - windowStart > 60_000) {
            if (minuteWindowStart.compareAndSet(windowStart, now)) {
                ordersThisMinute.set(0);
            }
        }
        return ordersThisMinute.incrementAndGet() <= config.maxOrdersPerMinute();
    }

    /** Учёт результата сделки. Положительное значение — прибыль. */
    public void recordPnl(double quotePnl) {
        dailyPnl.add(quotePnl);
    }

    /** Учёт исполненного ордера для грубой оценки PnL по средней цене. */
    public void onOrderFilled(OrderResult result, double referencePrice) {
        if (result.executedQty() <= 0 || referencePrice <= 0) return;
        double diff = result.side() == com.hft.model.OrderEnums.Side.BUY
                ? (referencePrice - result.avgPrice())
                : (result.avgPrice() - referencePrice);
        recordPnl(diff * result.executedQty());
    }

    // ---------- Управление ----------

    /** Немедленная остановка всей торговли. */
    public void stopTrading(String reason) {
        killSwitch.set(true);
        log.error("[{}] KILL SWITCH активирован: {}", exchangeId, reason);
    }

    public void resumeTrading() {
        killSwitch.set(false);
        log.info("Торговля возобновлена");
    }

    public boolean isStopped() {
        return killSwitch.get();
    }

    /** Сброс дневных счётчиков — вызывать в начале суток. */
    public void resetDaily() {
        dailyPnl.reset();
        log.info("Дневные счётчики сброшены");
    }

    public double dailyPnl() { return dailyPnl.sum(); }
    public long rejectedCount() { return rejectedCount.get(); }
    public long acceptedCount() { return acceptedCount.get(); }
    public String lastRejectReason() { return lastRejectReason; }
    public int ordersThisMinute() { return ordersThisMinute.get(); }
}
