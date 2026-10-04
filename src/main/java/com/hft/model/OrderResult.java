package com.hft.model;

/**
 * Что биржа ответила на наш ордер.
 *
 * @param orderId        ID на бирже, нужен для отмены и проверки статуса
 * @param clientOrderId  наш собственный ID, помогает сопоставить ответ с запросом
 * @param status         NEW / PARTIALLY_FILLED / FILLED / CANCELED / REJECTED / EXPIRED
 * @param executedQty    сколько реально исполнилось (может быть меньше запрошенного)
 * @param requestedQty   сколько просили
 * @param avgPrice       средневзвешенная цена исполнения (0 если ничего не исполнилось)
 * @param latencyNanos   сколько заняло: от отправки до получения ответа
 */
public record OrderResult(
        long orderId,
        String clientOrderId,
        String symbol,
        OrderEnums.Side side,
        String status,
        double requestedQty,
        double executedQty,
        double avgPrice,
        long latencyNanos
) {

    /** Исполнен полностью. */
    public boolean isFilled() {
        return "FILLED".equals(status);
    }

    /** Исполнен частично. */
    public boolean isPartial() {
        return "PARTIALLY_FILLED".equals(status);
    }

    /** Отклонён биржей или истёк без исполнения. */
    public boolean isRejected() {
        return "REJECTED".equals(status) || "EXPIRED".equals(status);
    }

    /** Кратко для логов. */
    @Override
    public String toString() {
        return String.format("Order[%d] %s %s %s: %.8f/%.8f @ %.2f (%.1f ms)",
                orderId, status, side, symbol, executedQty, requestedQty, avgPrice,
                latencyNanos / 1_000_000.0);
    }
}
