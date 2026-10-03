package com.hft.engine;

import com.hft.config.AppConfig;
import com.hft.metrics.Latency;
import com.hft.model.OrderEnums.Side;
import com.hft.model.OrderEnums.TimeInForce;
import com.hft.model.OrderEnums.Type;
import com.hft.model.OrderRequest;
import com.hft.model.OrderResult;
import com.hft.rest.ExchangeOrderApi;
import com.hft.risk.RiskManager;
import com.hft.store.BalanceStore;
import com.hft.store.MarketDataStore;
import com.hft.store.OrderBook;
import com.hft.store.SymbolFilters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Единая точка входа для всех торговых операций.
 *
 * Здесь происходит то, чего нет в голом REST-клиенте:
 *   - "весь баланс" превращается в конкретное число с учётом комиссии
 *   - работают проверки риск-менеджера
 *   - балансы корректируются локально после исполнения
 *   - замеряется латентность
 *
 * Стратегия должна вызывать этот класс, а не REST-клиент напрямую.
 */
public final class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    private final ExchangeOrderApi rest;
    private final MarketDataStore market;
    private final BalanceStore balances;
    private final SymbolFilters filters;
    private final RiskManager risk;
    private final AppConfig config;

    private final Latency orderLatency = new Latency("Латентность ордера");

    public OrderService(ExchangeOrderApi rest, MarketDataStore market, BalanceStore balances,
                        SymbolFilters filters, RiskManager risk, AppConfig config) {
        this.rest = rest;
        this.market = market;
        this.balances = balances;
        this.filters = filters;
        this.risk = risk;
        this.config = config;
    }

    // ======================= УДОБНЫЕ МЕТОДЫ =======================
    // Четыре комбинации, которые вам нужны, плюс варианты с долей баланса.

    /** Лимитная покупка заданного объёма. */
    public OrderResult buyLimit(String symbol, double qty, double price) {
        return execute(OrderRequest.limit(symbol, Side.BUY, price).quantity(qty));
    }

    /** Лимитная покупка на весь доступный баланс котируемой валюты. */
    public OrderResult buyLimitAll(String symbol, double price) {
        return execute(OrderRequest.limit(symbol, Side.BUY, price).fullBalance());
    }

    /** Лимитная продажа заданного объёма. */
    public OrderResult sellLimit(String symbol, double qty, double price) {
        return execute(OrderRequest.limit(symbol, Side.SELL, price).quantity(qty));
    }

    /** Лимитная продажа всего доступного объёма базовой валюты. */
    public OrderResult sellLimitAll(String symbol, double price) {
        return execute(OrderRequest.limit(symbol, Side.SELL, price).fullBalance());
    }

    /** Рыночная покупка заданного объёма базовой валюты. */
    public OrderResult buyMarket(String symbol, double qty) {
        return execute(OrderRequest.market(symbol, Side.BUY).quantity(qty));
    }

    /** Рыночная покупка на весь свободный баланс котируемой валюты. */
    public OrderResult buyMarketAll(String symbol) {
        return execute(OrderRequest.market(symbol, Side.BUY).fullBalance());
    }

    /** Рыночная продажа заданного объёма. */
    public OrderResult sellMarket(String symbol, double qty) {
        return execute(OrderRequest.market(symbol, Side.SELL).quantity(qty));
    }

    /** Рыночная продажа всего доступного объёма базовой валюты. */
    public OrderResult sellMarketAll(String symbol) {
        return execute(OrderRequest.market(symbol, Side.SELL).fullBalance());
    }

    /** Покупка на долю баланса: portion = 0.25 означает четверть. */
    public OrderResult buyMarketPortion(String symbol, double portion) {
        return execute(OrderRequest.market(symbol, Side.BUY).balancePortion(portion));
    }

    public OrderResult sellMarketPortion(String symbol, double portion) {
        return execute(OrderRequest.market(symbol, Side.SELL).balancePortion(portion));
    }

    /**
     * Лимитный ордер по лучшей цене стакана — пассивный вход.
     * offsetTicks сдвигает цену вглубь стакана: 0 — встать на лучшую цену,
     * 1 — на тик хуже (выше шанс постоять в очереди, но не перебить).
     */
    public OrderResult buyLimitAtBid(String symbol, double qty, int offsetTicks) {
        OrderBook book = market.book(symbol);
        if (book == null || !book.isReady()) {
            return failed(symbol, Side.BUY, "Нет данных стакана");
        }
        SymbolFilters.Filter f = filters.get(symbol);
        double tick = f != null ? f.tickSize() : 0;
        double price = book.bestBid() - tick * offsetTicks;
        return buyLimit(symbol, qty, price);
    }

    public OrderResult sellLimitAtAsk(String symbol, double qty, int offsetTicks) {
        OrderBook book = market.book(symbol);
        if (book == null || !book.isReady()) {
            return failed(symbol, Side.SELL, "Нет данных стакана");
        }
        SymbolFilters.Filter f = filters.get(symbol);
        double tick = f != null ? f.tickSize() : 0;
        double price = book.bestAsk() + tick * offsetTicks;
        return sellLimit(symbol, qty, price);
    }

    // ======================= ОСНОВНОЙ МЕТОД =======================

    /**
     * Выполнение любого ордера. Здесь собрана вся логика:
     * разрешение объёма, проверки риска, отправка, обновление балансов.
     */
    public OrderResult execute(OrderRequest request) {
        String symbol = request.symbol();

        try {
            // 1. Оценочная цена — нужна и для проверок, и для расчёта объёма
            double refPrice = estimatePrice(request);
            if (Double.isNaN(refPrice) || refPrice <= 0) {
                return failed(symbol, request.side(), "Не удалось определить цену " + symbol);
            }

            // 2. Разрешаем "весь баланс" в конкретный объём
            double qty = resolveQuantity(request, refPrice);
            if (qty <= 0) {
                return failed(symbol, request.side(), "Недостаточно средств для ордера");
            }

            // 3. Риск-менеджер
            RiskManager.Decision decision = risk.check(request, qty, refPrice);
            if (!decision.allowed()) {
                return failed(symbol, request.side(), decision.reason());
            }

            // 4. Отправка
            long start = System.nanoTime();
            OrderResult result = send(request, qty);
            orderLatency.recordSince(start);

            // 5. Обновление локальных балансов
            if (result.executedQty() > 0) {
                applyToBalances(result);
                risk.onOrderFilled(result, refPrice);
            }

            log.info("{} -> {}", request, result);
            return result;

        } catch (Exception e) {
            if (e instanceof com.hft.rest.RateLimited rl && rl.isRateLimit()) {
                risk.stopTrading("Превышен лимит запросов биржи, нужна пауза");
            }
            log.error("Ошибка при отправке ордера {}", request, e);
            return failed(symbol, request.side(), e.getMessage());
        }
    }

    /**
     * Превращение "весь баланс" в конкретный объём.
     *
     * Для BUY: берём свободные котируемые средства (USDT), вычитаем резерв
     * под комиссию, делим на цену — получаем количество базовой валюты.
     *
     * Для SELL: берём свободную базовую валюту (BTC) напрямую.
     */
    private double resolveQuantity(OrderRequest request, double price) {
        if (!request.isFullBalance()) {
            return filters.roundQuantity(request.symbol(), request.rawQuantity());
        }

        String[] assets = BalanceStore.splitSymbol(request.symbol());
        String base = assets[0];
        String quote = assets[1];
        double reserve = 1.0 - config.feeReservePercent() / 100.0;

        double qty;
        if (request.side() == Side.BUY) {
            double quoteFree = balances.free(quote) * request.quotePortion() * reserve;
            qty = quoteFree / price;
            log.debug("Весь баланс BUY: {} {} / {} = {} {}", quoteFree, quote, price, qty, base);
        } else {
            qty = balances.free(base) * request.quotePortion() * reserve;
            log.debug("Весь баланс SELL: {} {}", qty, base);
        }

        return filters.roundQuantity(request.symbol(), qty);
    }

    /** Оценочная цена исполнения: для LIMIT — своя цена, для MARKET — из стакана. */
    private double estimatePrice(OrderRequest request) {
        if (request.type() == Type.LIMIT) {
            return request.price();
        }
        OrderBook book = market.book(request.symbol());
        if (book != null && book.isReady()) {
            return request.side() == Side.BUY ? book.bestAsk() : book.bestBid();
        }
        return market.referencePrice(request.symbol());
    }

    /**
     * Известная межбиржевая тонкость: у Binance rest.buyMarket(qty) всегда
     * означает объём в базовой валюте, а quoteOrderQty — отдельный опциональный
     * параметр. У Bybit market BUY на споте по умолчанию тоже принимает qty
     * в базовой валюте при явном marketUnit=baseCoin (это и делает наш
     * BybitRestClient.buyMarket) — так что оба клиента здесь ведут себя
     * одинаково для явного объёма. buyMarketForQuote нужен отдельно только
     * для сценария "потратить ровно N USDT" — см. execute() с quoteOrderQty.
     */
    private OrderResult send(OrderRequest request, double qty) throws Exception {
        TimeInForce tif = request.timeInForce();
        if (request.type() == Type.LIMIT) {
            return request.side() == Side.BUY
                    ? rest.buyLimit(request.symbol(), qty, request.price(), tif)
                    : rest.sellLimit(request.symbol(), qty, request.price(), tif);
        }
        return request.side() == Side.BUY
                ? rest.buyMarket(request.symbol(), qty)
                : rest.sellMarket(request.symbol(), qty);
    }

    /**
     * Локальная корректировка балансов после исполнения.
     * Это позволяет сразу отправить следующий ордер, не дожидаясь
     * синхронизации с биржей.
     */
    private void applyToBalances(OrderResult r) {
        String[] assets = BalanceStore.splitSymbol(r.symbol());
        double quoteAmount = r.executedQty() * r.avgPrice();
        if (r.side() == Side.BUY) {
            balances.adjust(assets[0], r.executedQty());
            balances.adjust(assets[1], -quoteAmount);
        } else {
            balances.adjust(assets[0], -r.executedQty());
            balances.adjust(assets[1], quoteAmount);
        }
    }

    private OrderResult failed(String symbol, Side side, String reason) {
        log.warn("Ордер не отправлен по {}: {}", symbol, reason);
        return new OrderResult(0, "", symbol, side, "REJECTED_LOCAL", 0, 0, 0, 0);
    }

    // ======================= ОТМЕНА =======================

    public void cancel(String symbol, long orderId) {
        try {
            rest.cancelOrder(symbol, orderId);
        } catch (Exception e) {
            log.error("Не удалось отменить ордер {}", orderId, e);
        }
    }

    /** Отменить все открытые ордера по символу. */
    public int cancelAll(String symbol) {
        try {
            return rest.cancelAll(symbol);
        } catch (Exception e) {
            log.error("Не удалось отменить ордера по {}", symbol, e);
            return 0;
        }
    }

    /** Аварийный выход: отменить всё и остановить торговлю. */
    public void panicClose(String reason) {
        log.error("АВАРИЙНОЕ ЗАКРЫТИЕ: {}", reason);
        risk.stopTrading(reason);
        for (String symbol : market.symbols()) {
            cancelAll(symbol);
        }
    }

    public Latency latency() { return orderLatency; }
}
