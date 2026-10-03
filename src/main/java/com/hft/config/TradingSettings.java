package com.hft.config;

/**
 * Текущие торговые параметры одной биржи. Один экземпляр на биржу живёт всё время работы процесса
 * и передаётся риск-менеджеру, сервису ордеров и стратегии; админка подменяет значение целиком,
 * и новые параметры подхватываются на следующем тике без перезапуска.
 */
public final class TradingSettings {

    private volatile TradingParams params;

    public TradingSettings(TradingParams params) { this.params = params; }

    public TradingParams get() { return params; }

    public void set(TradingParams params) { this.params = params; }
}
