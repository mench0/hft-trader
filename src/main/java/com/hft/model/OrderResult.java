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

    public boolean isFilled() {
        return "FILLED".equals(status);
    }

    public boolean isPartial() {
        return "PARTIALLY_FILLED".equals(status);
    }

    public boolean isRejected() {
        return "REJECTED".equals(status) || "EXPIRED".equals(status);
    }

    /** Остаток, который не исполнился. */
    public double remainingQty() {
        return requestedQty - executedQty;
    }

    /** Доля исполнения: 1.0 = полностью, 0.3 = исполнилось 30%. */
    public double fillRatio() {
        return requestedQty > 0 ? executedQty / requestedQty : 0;
    }

    @Override
    public String toString() {
        return String.format("Order[%d] %s %s %s: %.8f/%.8f @ %.2f (%.1f ms)",
                orderId, status, side, symbol, executedQty, requestedQty, avgPrice,
                latencyNanos / 1_000_000.0);
    }
}
