package com.hft.config;

import java.util.List;

/**
 * Настройки одной биржи. Раньше это было частью общего AppConfig,
 * теперь у каждой биржи своя копия, потому что URL, ключи и даже
 * набор символов может отличаться между Binance и Bybit.
 */
public record ExchangeConfig(
        String id,              // "binance", "bybit"
        boolean enabled,
        boolean testnet,
        String restUrl,
        String wsUrl,
        int recvWindowMs,
        List<String> symbols,
        int bookDepth,
        int priceWindowSize
) {
    public String baseCredentialsEnvPrefix() {
        return id.toUpperCase(); // BINANCE_API_KEY, BYBIT_API_KEY
    }
}
