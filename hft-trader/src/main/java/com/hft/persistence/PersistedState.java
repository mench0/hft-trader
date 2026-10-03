package com.hft.persistence;

import java.util.List;
import java.util.Map;

/**
 * Всё состояние, которое должно сохраняться между перезапусками процесса.
 *
 * Без этого файла при рестарте приложение снова стартовало бы с чистого
 * application.yml, и любая настройка, сделанная через админку (выбор бирж,
 * тикеров, параметры риска, параметры стратегии), терялась бы.
 */
public record PersistedState(
        /** Биржа -> список тикеров, как было выбрано через /control/select. */
        Map<String, List<String>> selection,

        /** Если true — при следующем запуске процесса биржи поднимутся сами,
         *  без ручного вызова /control/start. Управляется через /control/autostart. */
        boolean autoStart,

        /** Если true — вместе с автозапуском сразу включится и торговля
         *  (эквивалент вызова /trading/start после /control/start). */
        boolean autoTrade,

        RiskSnapshot risk,

        /** Биржа -> последние применённые параметры стратегии на ней. */
        Map<String, StrategyParams> strategyParams
) {

    public record RiskSnapshot(
            double maxPositionQuote,
            double maxDailyLossQuote,
            double maxSlippagePercent,
            double feeReservePercent,
            int maxOrdersPerMinute,
            boolean tradingEnabled
    ) {}

    public record StrategyParams(
            double entryZ,
            double exitZ,
            double stopLossPercent,
            double minImbalance,
            double orderQuote
    ) {}
}
