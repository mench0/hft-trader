package com.hft.rest;

import com.hft.model.OrderEnums.TimeInForce;
import com.hft.model.OrderResult;

/**
 * Минимальный набор методов, которым пользуется {@link com.hft.engine.OrderService}.
 * Реализуют {@code BinanceRestClient} и {@code BybitRestClient} — каждый
 * по-своему подписывает запросы и разбирает ответ, но наружу отдаёт
 * одинаковый {@link OrderResult}.
 *
 * Ввести этот интерфейс — единственный способ, которым OrderService
 * остаётся биржо-независимым: он вызывает buyLimit/sellMarket и не знает,
 * что творится внутри (HMAC в query у Binance или в заголовках у Bybit).
 */
public interface ExchangeOrderApi {

    OrderResult buyLimit(String symbol, double qty, double price, TimeInForce tif) throws Exception;

    OrderResult sellLimit(String symbol, double qty, double price, TimeInForce tif) throws Exception;

    OrderResult buyMarket(String symbol, double qty) throws Exception;

    OrderResult sellMarket(String symbol, double qty) throws Exception;

    /**
     * Рыночная покупка на сумму в котируемой валюте, а не на объём базовой.
     * У Binance это отдельный опциональный параметр (quoteOrderQty),
     * у Bybit — единственный способ купить на споте маркет-ордером,
     * поэтому метод вынесен в общий интерфейс, а не оставлен деталью Binance.
     */
    OrderResult buyMarketForQuote(String symbol, double quoteAmount) throws Exception;

    void cancelOrder(String symbol, long orderId) throws Exception;

    int cancelAll(String symbol) throws Exception;
}
