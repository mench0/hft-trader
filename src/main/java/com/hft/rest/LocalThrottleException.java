package com.hft.rest;

/**
 * Отказ своего ограничителя запросов — до биржи запрос не дошёл, и это не ответ биржи (не HTTP 429).
 * Не {@link RateLimited}: OrderService отклоняет ордер, но торговлю (kill switch) не останавливает —
 * настоящий лимит биржи приходит её ответом ({@link ApiException} с 429/418/кодом лимита).
 */
public final class LocalThrottleException extends RuntimeException {

    /** Почему запрос не отправлен. */
    public enum Reason {
        /** Очередь своего лимитера переполнена: ждать слота дольше допустимого. */
        QUEUE_FULL,
        /** Запросы к бирже приостановлены после её ответа о превышении лимита (Retry-After). */
        PAUSED
    }

    /** Причина. */
    private final Reason reason;
    /** Сколько пришлось бы ждать, мс. */
    private final long waitMs;

    /**
     * @param reason причина
     * @param waitMs сколько пришлось бы ждать, мс
     * @param message текст
     */
    public LocalThrottleException(Reason reason, long waitMs, String message) {
        super(message, null, false, false);                 // без стека: ожидаемый и частый отказ
        this.reason = reason;
        this.waitMs = waitMs;
    }

    /** Причина. */
    public Reason reason() { return reason; }

    /** Сколько пришлось бы ждать, мс. */
    public long waitMs() { return waitMs; }
}
