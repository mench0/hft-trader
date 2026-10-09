package com.hft.rest;

import com.hft.store.BalanceStore;

import java.util.Map;

/**
 * Клиент биржи для режима LIVE: ордера ({@link ExchangeOrderApi}) плюс то, что нужно классу биржи на старте
 * и в работе — правила, баланс, приватные WS-каналы, метрики. Реализуют {@link SignedCexClient},
 * {@link BinanceRestClient} и BybitRestClient.
 */
public interface TradingClient extends ExchangeOrderApi {

    /** Загрузить правила торговли символов. */
    void loadFilters(Iterable<String> symbols) throws Exception;

    /** Загрузить балансы. */
    void loadBalances(BalanceStore store) throws Exception;

    /** Поднять приватные WS-каналы (ордера, исполнения, балансы). */
    void startStreams(BalanceStore store) throws Exception;

    /** Подождать готовности WS-каналов, но не дольше ms. */
    void awaitStreams(long ms) throws InterruptedException;

    /** Остановить приватные WS-каналы. */
    void stopStreams();

    /** Метрики запросов и сокетов — для админки. */
    Map<String, Object> stats();

    /** Синхронизировать часы с биржей (нужно, если биржа проверяет время подписи). */
    default void syncTime() throws Exception {}
}
