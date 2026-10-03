package com.hft.exchange.generic;

import java.util.Map;

/** Биржа, умеющая показать режим (LIVE/PAPER) и метрики запросов — для админки. */
public interface RequestStatsSource {
    Map<String, Object> requestStats();
    boolean isLive();
}
