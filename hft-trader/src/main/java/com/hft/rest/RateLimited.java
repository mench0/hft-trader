package com.hft.rest;

/**
 * Реализуют исключения бирж, сигнализирующие о превышении лимита запросов
 * (HTTP 429 у Binance, retCode 10006 у Bybit и т.д.). OrderService ловит
 * общий RuntimeException и проверяет instanceof RateLimited — так не нужно
 * знать в общем коде, какая конкретно биржа кинула ошибку.
 */
public interface RateLimited {
    boolean isRateLimit();
}
