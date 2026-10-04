package com.hft.exchange.generic;

import java.util.Map;

/** Биржа, умеющая показать режим (LIVE/PAPER) и метрики запросов — для админки. */
public interface RequestStatsSource {
    /** Метрики запросов к бирже для админки (режим, фид, ордера). */
    Map<String, Object> requestStats();
    /** Ордера идут на биржу (LIVE), а не в бумажный движок. */
    boolean isLive();
}
