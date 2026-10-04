package com.hft.exchange.generic;

import java.util.List;
import java.util.Map;

/** Источник стакана биржи: WebSocket, REST-опрос или связка из них. */
public interface BookFeed {
    /** Подключиться и начать получать стакан. */
    void start();
    /** Отключиться. */
    void stop();
    /** Символы, по которым идут данные. */
    List<String> activeSymbols();
    /** Данные идут. */
    boolean isConnected();

    /** Данные в реальном времени (WS)? На REST-опросе стакан старше на сотни мс — новые входы запрещаются. */
    default boolean isRealtime() { return isConnected(); }
    /** Источник сдался после многих неудач подряд (будет вызван onGiveUp). */
    boolean hasGivenUp();
    /** Сколько сообщений/ответов получено. */
    long messageCount();
    /** Метрики для админки. */
    Map<String, Object> stats();
}
