package com.hft.rest;

import com.hft.model.OrderEnums.TimeInForce;
import com.hft.model.OrderEnums.Side;
import com.hft.model.OrderResult;
import com.hft.store.PositionStore;

/**
 * Минимальный набор методов, которым пользуется {@link com.hft.engine.OrderService}.
 * Реализуют {@code BinanceRestClient} и {@code BybitRestClient} — каждый
 * по-своему подписывает запросы и разбирает ответ, но наружу отдаёт
 * одинаковый {@link OrderResult}.
 * <p>
 * Ввести этот интерфейс — единственный способ, которым OrderService
 * остаётся биржо-независимым: он вызывает buyLimit/sellMarket и не знает,
 * что творится внутри (HMAC в query у Binance или в заголовках у Bybit).
 */
public interface ExchangeOrderApi {

    /** Лимитная покупка qty по price с указанным временем жизни. */
    OrderResult buyLimit(String symbol, double qty, double price, TimeInForce tif) throws Exception;

    /** Лимитная продажа qty по price. */
    OrderResult sellLimit(String symbol, double qty, double price, TimeInForce tif) throws Exception;

    /** Рыночная покупка qty базовой валюты. */
    OrderResult buyMarket(String symbol, double qty) throws Exception;

    /** Рыночная продажа qty базовой валюты. */
    OrderResult sellMarket(String symbol, double qty) throws Exception;

    /**
     * Рыночная покупка на сумму в котируемой валюте, а не на объём базовой.
     * У Binance это отдельный опциональный параметр (quoteOrderQty),
     * у Bybit — единственный способ купить на споте маркет-ордером,
     * поэтому метод вынесен в общий интерфейс, а не оставлен деталью Binance.
     */
    OrderResult buyMarketForQuote(String symbol, double quoteAmount) throws Exception;

    /** Отменить ордер по id. */
    void cancelOrder(String symbol, long orderId) throws Exception;

    /** Отменить все открытые ордера по символу; возвращает число отменённых. */
    int cancelAll(String symbol) throws Exception;

    // ---------------- бессрочные контракты (перпы); у спотовых клиентов — значения по умолчанию

    /** Клиент торгует перпами (позиции, плечо, funding), а не спотом. */
    default boolean isPerp() { return false; }

    /**
     * Рыночный ордер, который может только уменьшить позицию (reduceOnly) — закрытие лонга (SELL)
     * или шорта (BUY). На споте — обычный рыночный ордер.
     */
    default OrderResult reduceMarket(String symbol, Side side, double qty) throws Exception {
        return side == Side.BUY ? buyMarket(symbol, qty) : sellMarket(symbol, qty);
    }

    /** Установить плечо символа (перпы). */
    default void setLeverage(String symbol, int leverage) throws Exception {}

    /** Загрузить открытые позиции с биржи (перпы). */
    default void loadPositions(PositionStore store) throws Exception {}

    /**
     * Защитный стоп на бирже (перпы): когда цена маркировки дойдёт до {@code stopPrice}, биржа сама закроет позицию
     * рыночным reduceOnly-ордером. {@code side} — сторона закрывающего ордера (SELL закрывает лонг, BUY — шорт),
     * {@code qty} — объём позиции в монетах.
     * @return id стопа на бирже (для отмены); null — биржа или клиент стопы не поддерживают
     */
    default String placeStopLoss(String symbol, Side side, double qty, double stopPrice) throws Exception { return null; }

    /** Снять защитный стоп, поставленный {@link #placeStopLoss}. */
    default void cancelStopLoss(String symbol, String stopId) throws Exception {}
}
