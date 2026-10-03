package com.hft.persistence;

import java.util.List;
import java.util.Map;

/**
 * Всё состояние, которое должно сохраняться между перезапусками процесса.
 *
 * Без этого при рестарте любая настройка, сделанная через админку (выбор бирж,
 * тикеров, торговые параметры каждой биржи), терялась бы.
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

        /** Биржа -> торговые параметры ({@link com.hft.config.TradingParams#toStringMap()}).
         *  Хранятся как ключ-значение: новый параметр в будущей версии получит значение
         *  по умолчанию, а не сломает чтение старого состояния. */
        Map<String, Map<String, String>> trading
) {}
