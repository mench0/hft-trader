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
 *     Переменные {@code ADMIN_ENABLED}, {@code ADMIN_PORT}, {@code ADMIN_TOKEN}, {@code ADMIN_BIND}, {@code ADMIN_CORS_ORIGINS}
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
    /** Токен админки; без него API не работает (пустой при старте — генерируется и пишется в .env). */
    private String adminToken = "";
    /** Адрес, на котором слушает админка: по умолчанию только этот компьютер (доступ снаружи — SSH-туннель). */
    private String adminBind = "127.0.0.1";
    /** Адреса веб-админки, которым разрешён доступ из браузера (CORS); пусто — только localhost и файл. */
    private java.util.List<String> adminCorsOrigins = java.util.List.of();

    /** Создаётся через load() или defaults(). */
    private AppConfig() {}

    /** Прочитать ADMIN_* из .env и окружения. */
    public static AppConfig load() {
        AppConfig cfg = new AppConfig();
        cfg.adminEnabled = bool(env("ADMIN_ENABLED"), cfg.adminEnabled);
        cfg.adminPort = intOf(env("ADMIN_PORT"), cfg.adminPort);
        cfg.adminToken = str(env("ADMIN_TOKEN"), cfg.adminToken);
        cfg.adminBind = str(env("ADMIN_BIND"), cfg.adminBind).trim();
        String origins = env("ADMIN_CORS_ORIGINS");
        if (origins != null) cfg.adminCorsOrigins = java.util.Arrays.stream(origins.split(","))
                .map(String::trim).filter(o -> !o.isEmpty()).map(o -> o.replaceAll("/+$", "")).toList();
        if (cfg.adminEnabled && cfg.adminToken.isBlank()) cfg.adminToken = generateToken();
        if (cfg.adminEnabled && !isLoopback(cfg.adminBind))
            log.warn("Админка слушает {} — доступна по сети. Нужен ли этот доступ? Безопаснее ADMIN_BIND=127.0.0.1 и SSH-туннель", cfg.adminBind);
        log.info("Админка: {}", cfg.adminEnabled ? cfg.adminBind + ":" + cfg.adminPort + " (с токеном)" : "выключена");
        return cfg;
    }

    /**
     * ADMIN_TOKEN не задан — сгенерировать случайный (32 байта) и записать в .env: без токена любой процесс
     * или сайт в браузере на этом компьютере мог бы управлять ботом. Сам токен в лог не пишется.
     */
    private static String generateToken() {
        byte[] b = new byte[32];
        new java.security.SecureRandom().nextBytes(b);
        String token = java.util.HexFormat.of().formatHex(b);
        try {
            Env.set(java.util.Map.of("ADMIN_TOKEN", token));
            log.warn("ADMIN_TOKEN не был задан — сгенерирован и записан в {} (укажите его в админке)", Env.file());
        } catch (Exception e) {
            throw new IllegalStateException("ADMIN_TOKEN не задан, а записать сгенерированный в " + Env.file()
                    + " не удалось (" + e.getMessage() + "). Задайте ADMIN_TOKEN в .env или окружении.", e);
        }
        return token;
    }

    /** Адрес только этого компьютера (127.0.0.1, ::1, localhost). */
    public static boolean isLoopback(String host) {
        return host.equals("127.0.0.1") || host.equals("::1") || host.equalsIgnoreCase("localhost") || host.startsWith("127.");
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

    /** Токен админки. */
    public String adminToken() { return adminToken; }

    /** Адрес, на котором слушает админка. */
    public String adminBind() { return adminBind; }

    /** Разрешённые адреса веб-админки для CORS; пусто — только localhost и файл. */
    public java.util.List<String> adminCorsOrigins() { return adminCorsOrigins; }

    /** Задать токен (тесты). */
    public AppConfig withToken(String token) { this.adminToken = token; return this; }

    /** Задать порт (тесты). */
    public AppConfig withPort(int port) { this.adminPort = port; return this; }

    /** Задать разрешённые адреса CORS (тесты). */
    public AppConfig withCorsOrigins(java.util.List<String> origins) { this.adminCorsOrigins = origins; return this; }
}
