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
        String notes,
        String wsUrl,               // WebSocket рыночных данных (null — адрес знает диалект/фид биржи)
        String testnetRestUrl,      // REST тестовой сети; null — у биржи нет testnet
        String testnetWsUrl         // WebSocket тестовой сети
) {
    /** Биржа без отдельных адресов WebSocket и testnet (их знает диалект или их нет). */
    public ExchangeInfo(String id, String title, Kind kind, Adapter adapter, String restUrl, double maxRequestsPerSec,
                        double makerFeePct, double takerFeePct, String defaultQuote, String symbolHint, String notes) {
        this(id, title, kind, adapter, restUrl, maxRequestsPerSec, makerFeePct, takerFeePct, defaultQuote, symbolHint, notes, null, null, null);
    }

    /** Биржа как enum. */
    public com.hft.exchange.Exchange exchange() { return com.hft.exchange.Exchange.of(id); }

    /** Есть ли у биржи тестовая сеть. */
    public boolean hasTestnet() { return testnetRestUrl != null; }

    /** REST-адрес с учётом testnet. */
    public String restUrl(boolean testnet) { return testnet && hasTestnet() ? testnetRestUrl : restUrl; }

    /** WebSocket-адрес с учётом testnet (null — по умолчанию диалекта/фида). */
    public String wsUrl(boolean testnet) { return testnet && hasTestnet() ? testnetWsUrl : wsUrl; }

    /** Тип площадки: крупная/мелкая CEX, перп-DEX, DEX со стаканом, AMM. */
    public enum Kind { CEX_TIER1, CEX_TIER3, PERP_DEX, ORDERBOOK_DEX, AMM_DEX }

    /** Степень готовности адаптера (см. описание record'а). */
    public enum Adapter { NATIVE_LIVE, LIVE_UNVERIFIED, PAPER_BLIND, NOT_IMPLEMENTED }
}
