package com.hft.config;

import java.util.List;

/**
 * Подключение к одной бирже на время работы, собранное контроллером при /control/start:
 * адреса — из параметров биржи или каталога (с учётом testnet), символы — из выбора,
 * остальное — из {@link TradingParams} этой биржи.
 *
 * @param id              идентификатор биржи ("binance", "bybit", …)
 * @param testnet         тестовая сеть
 * @param restUrl         REST-адрес (для Uniswap — RPC ноды)
 * @param wsUrl           WebSocket-адрес; пусто — по умолчанию диалекта/фида
 * @param recvWindowMs    окно годности подписанного запроса, мс
 * @param symbols         выбранные символы
 * @param bookDepth       глубина стакана в памяти
 * @param priceWindowSize окно цен для среднего и сигмы
 * @param params          параметры биржи на момент старта (настройки фидов, Uniswap, live, бумажный баланс)
 */
public record ExchangeConfig(
        String id,
        boolean testnet,
        String restUrl,
        String wsUrl,
        int recvWindowMs,
        List<String> symbols,
        int bookDepth,
        int priceWindowSize,
        TradingParams params
) {
    /** Подключение с параметрами по умолчанию (тесты и вспомогательные места). */
    public ExchangeConfig(String id, boolean testnet, String restUrl, String wsUrl, int recvWindowMs,
                          List<String> symbols, int bookDepth, int priceWindowSize) {
        this(id, testnet, restUrl, wsUrl, recvWindowMs, symbols, bookDepth, priceWindowSize, TradingParams.DEFAULTS);
    }
}
