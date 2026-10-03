package com.hft.exchange.generic;

import java.util.List;
import java.util.Map;

/** Источник стакана биржи: WebSocket, REST-опрос или связка из них. */
public interface BookFeed {
    void start();
    void stop();
    void addSymbol(String symbol);
    void removeSymbol(String symbol);
    List<String> activeSymbols();
    boolean isConnected();

    /** Данные в реальном времени (WS)? На REST-опросе стакан старше на сотни мс — новые входы запрещаются. */
    default boolean isRealtime() { return isConnected(); }
    boolean hasGivenUp();
    long messageCount();
    Map<String, Object> stats();
}
