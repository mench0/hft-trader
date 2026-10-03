package com.hft.config;

/**
 * API-ключи одной биржи. Читаются из переменных окружения с префиксом
 * по имени биржи:
 *
 *   BINANCE_API_KEY / BINANCE_API_SECRET
 *   BYBIT_API_KEY / BYBIT_API_SECRET
 *
 * Так ключи разных бирж не путаются и не конфликтуют по именам.
 */
public record Credentials(String apiKey, String apiSecret) {

    public static Credentials fromEnv(String exchangeId) {
        String prefix = exchangeId.toUpperCase();
        String key = System.getenv(prefix + "_API_KEY");
        String secret = System.getenv(prefix + "_API_SECRET");
        if (key == null || key.isBlank() || secret == null || secret.isBlank()) {
            return new Credentials(null, null);
        }
        return new Credentials(key.trim(), secret.trim());
    }

    public boolean isPresent() {
        return apiKey != null && apiSecret != null;
    }

    public void require() {
        if (!isPresent()) {
            throw new IllegalStateException(
                    "Нужны API-ключи в переменных окружения (например, BINANCE_API_KEY / BINANCE_API_SECRET)");
        }
    }

    @Override
    public String toString() {
        return isPresent()
                ? "Credentials[key=" + apiKey.substring(0, Math.min(6, apiKey.length())) + "...]"
                : "Credentials[не заданы]";
    }
}
