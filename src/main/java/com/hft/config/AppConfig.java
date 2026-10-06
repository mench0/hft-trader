package com.hft.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Минимальная конфигурация, необходимая для запуска админки:
 * включена ли админка, её порт и токен доступа.
 *
 * <p>Вся остальная конфигурация — выбор бирж, параметры подключения
 * (testnet, адреса), риск-менеджмент, стратегии и фоновые задачи —
 * задаётся через админку и хранится в SQLite:
 * {@link TradingParams}, {@link GlobalParams}.
 *
 * <p>Источники конфигурации в порядке приоритета:
 * <ol>
 *   <li>
 *     Переменные {@code ADMIN_ENABLED}, {@code ADMIN_PORT}, {@code ADMIN_TOKEN}
 *     из файла {@code .env} ({@link Env}).
 *   </li>
 *   <li>
 *     Те же переменные окружения процесса.
 *   </li>
 *   <li>
 *     Значения по умолчанию.
 *   </li>
 * </ol>
 *
 * <p>API-ключи бирж ({@code <EXCHANGE>_API_KEY}/{@code <EXCHANGE>_API_SECRET})
 * берутся оттуда же и не сохраняются в базу данных.
 */
public final class AppConfig {

    /** Логгер конфигурации. */
    private static final Logger log = LoggerFactory.getLogger(AppConfig.class);

    /** Включена ли админка. */
    private boolean adminEnabled = true;
    /** Порт HTTP-админки. */
    private int adminPort = 8080;
    /** Токен админки; пусто — без авторизации. */
    private String adminToken = "";

    /** Создаётся через load() или defaults(). */
    private AppConfig() {}

    /** Прочитать ADMIN_* из .env и окружения. */
    public static AppConfig load() {
        AppConfig cfg = new AppConfig();
        cfg.adminEnabled = bool(env("ADMIN_ENABLED"), cfg.adminEnabled);
        cfg.adminPort = intOf(env("ADMIN_PORT"), cfg.adminPort);
        cfg.adminToken = str(env("ADMIN_TOKEN"), cfg.adminToken);
        log.info("Админка: {}", cfg.adminEnabled ? "порт " + cfg.adminPort + (cfg.adminToken.isBlank() ? " (без токена)" : " (с токеном)") : "выключена");
        return cfg;
    }

    /** Значения по умолчанию без .env и окружения (тесты). */
    public static AppConfig defaults() { return new AppConfig(); }

    /** Переменная из .env или окружения; null, если не задана или пуста. */
    private static String env(String key) {
        String v = Env.get(key);
        return v == null || v.isBlank() ? null : v;
    }

    /** Строка или значение по умолчанию. */
    private static String str(Object v, String def) { return v == null ? def : String.valueOf(v); }
    /** Флаг из строки или значение по умолчанию. */
    private static boolean bool(Object v, boolean def) { return v == null ? def : Boolean.parseBoolean(String.valueOf(v)); }
    /** Целое из строки; при ошибке — значение по умолчанию. */
    private static int intOf(Object v, int def) {
        if (v == null) return def;
        try { return Integer.parseInt(String.valueOf(v).trim()); } catch (NumberFormatException e) { return def; }
    }

    /** Админка включена (без неё некому запустить биржи). */
    public boolean adminEnabled() { return adminEnabled; }

    /** Порт HTTP-админки. */
    public int adminPort() { return adminPort; }

    /** Токен админки; пусто — без авторизации. */
    public String adminToken() { return adminToken; }
}
