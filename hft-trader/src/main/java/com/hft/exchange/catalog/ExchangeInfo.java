package com.hft.exchange.catalog;

/**
 * Описание биржи в каталоге: что это, как к ней ходить и насколько
 * можно доверять нашему адаптеру.
 *
 * @param adapter  NATIVE_LIVE — полноценный адаптер (REST+WS+ордера), написан раньше;
 *                 LIVE_UNVERIFIED — REST-клиент с реальными ордерами по документации, не проверен;
 *                 PAPER_BLIND — только рыночные данные (REST-опрос) и бумажная торговля.
 *                 Формат ответов API взят из документации по памяти и НЕ проверен
 *                 против живой биржи: сеть песочницы закрывала хосты бирж.
 */
public record ExchangeInfo(
        String id,
        String title,
        Kind kind,
        Adapter adapter,
        String restUrl,
        double maxRequestsPerSec,   // наш консервативный лимит публичных запросов
        double makerFeePct,
        double takerFeePct,
        String defaultQuote,        // в какой валюте считаем бумажный баланс
        String symbolHint,          // как писать тикер в админке
        String notes
) {
    public enum Kind { CEX_TIER1, CEX_TIER3, PERP_DEX, AMM_DEX }

    public enum Adapter { NATIVE_LIVE, LIVE_UNVERIFIED, PAPER_BLIND, NOT_IMPLEMENTED }
}
