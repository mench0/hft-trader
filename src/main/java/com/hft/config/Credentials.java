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

    /** Ключи биржи из окружения: ID_API_KEY и ID_API_SECRET (например BINANCE_API_KEY). */
    public static Credentials fromEnv(String exchangeId) {
        String prefix = exchangeId.toUpperCase();
        String key = Env.get(prefix + "_API_KEY");
        String secret = Env.get(prefix + "_API_SECRET");
        if (key == null || key.isBlank() || secret == null || secret.isBlank()) {
            return new Credentials(null, null);
        }
        return new Credentials(key.trim(), secret.trim());
    }

    /** Заданы ли оба ключа. */
    public boolean isPresent() {
        return apiKey != null && apiSecret != null;
    }

    /** Бросает IllegalStateException, если ключей нет: подписанный запрос без них невозможен. */
    public void require() {
        if (!isPresent()) {
            throw new IllegalStateException(
                    "Нужны API-ключи в переменных окружения (например, BINANCE_API_KEY / BINANCE_API_SECRET)");
        }
    }

    /** Без секрета: ключи не должны попадать в логи. */
    @Override
    public String toString() {
        return isPresent()
                ? "Credentials[key=" + apiKey.substring(0, Math.min(6, apiKey.length())) + "...]"
                : "Credentials[не заданы]";
    }
}
