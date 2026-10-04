package com.hft.config;

/**
 * Текущие торговые параметры одной биржи. Один экземпляр на биржу живёт всё время работы процесса
 * и передаётся риск-менеджеру, сервису ордеров и стратегии; админка подменяет значение целиком,
 * и новые параметры подхватываются на следующем тике без перезапуска.
 */
public final class TradingSettings {

    /** Текущий набор; volatile — изменение из админки сразу видно потоку стратегии. */
    private volatile TradingParams params;

    /** @param params начальные параметры биржи */
    public TradingSettings(TradingParams params) { this.params = params; }

    /** Текущие параметры (одно volatile-чтение). */
    public TradingParams get() { return params; }

    /** Подменить параметры целиком; видно потоку стратегии сразу. */
    public void set(TradingParams params) { this.params = params; }
}
