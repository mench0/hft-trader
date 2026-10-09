package com.hft.rest;

/** Ошибка биржи или лимитера. isRateLimit() понимает OrderService, не зная, какая это биржа. */
public final class ApiException extends RuntimeException implements RateLimited {
    /** HTTP-код ответа биржи (429 и т.п.). Отказ своего ограничителя — не ApiException, а {@link LocalThrottleException}. */
    private final int httpStatus;
    /** Код ошибки биржи. */
    private final String code;
    /** Биржа сообщила о превышении лимита. */
    private final boolean rateLimit;

    /**
     * @param httpStatus HTTP-код
     * @param code код ошибки биржи
     * @param message текст ошибки
     * @param rateLimit это превышение лимита запросов
     */
    public ApiException(int httpStatus, String code, String message, boolean rateLimit) {
        super("HTTP " + httpStatus + " code " + code + ": " + message);
        this.httpStatus = httpStatus;
        this.code = code;
        this.rateLimit = rateLimit;
    }

    /** HTTP-код ответа. */
    public int httpStatus() { return httpStatus; }

    /** Код ошибки биржи. */
    public String code() { return code; }

    /** Ошибка означает превышение лимита запросов. */
    @Override
    public boolean isRateLimit() { return rateLimit || httpStatus == 429 || httpStatus == 418; }
}
