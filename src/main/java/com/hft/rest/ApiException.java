package com.hft.rest;

/** Ошибка биржи или лимитера. isRateLimit() понимает OrderService, не зная, какая это биржа. */
public final class ApiException extends RuntimeException implements RateLimited {
    private final int httpStatus;
    private final String code;
    private final boolean rateLimit;

    public ApiException(int httpStatus, String code, String message, boolean rateLimit) {
        super("HTTP " + httpStatus + " code " + code + ": " + message);
        this.httpStatus = httpStatus;
        this.code = code;
        this.rateLimit = rateLimit;
    }

    public int httpStatus() { return httpStatus; }
    public String code() { return code; }

    @Override
    public boolean isRateLimit() { return rateLimit || httpStatus == 429 || httpStatus == 418; }
}
