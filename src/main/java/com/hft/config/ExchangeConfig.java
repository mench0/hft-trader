package com.hft.config;

import java.util.List;

/**
 * Подключение к одной бирже на время работы: адреса и окно подписи — из application.yml/окружения,
 * символы и размеры стакана/окна цен — из выбора и торговых параметров в админке.
 */
public record ExchangeConfig(
        String id,              // "binance", "bybit"
        boolean testnet,
        String restUrl,
        String wsUrl,
        int recvWindowMs,
        List<String> symbols,
        int bookDepth,
        int priceWindowSize
) {}
