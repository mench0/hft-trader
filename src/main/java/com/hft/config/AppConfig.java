package com.hft.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.util.Map;

/**
 * Только то, без чего админка не может запуститься: включена ли она, порт и токен.
 *
 * Всё остальное — выбор бирж, их подключение (testnet, адреса), риск, стратегии, фоновые задачи —
 * задаётся через админку и хранится в SQLite ({@link TradingParams}, {@link GlobalParams}).
 *
 * Источники, от высшего к низшему: переменные окружения ADMIN_ENABLED/ADMIN_PORT/ADMIN_TOKEN,
 * блок admin в application.yml рядом с jar (или -Dconfig.file=…), значения по умолчанию.
 * API-ключи бирж — только в окружении (ID_API_KEY/_SECRET), в файлы и базу не попадают.
 */
public final class AppConfig {

    /** Логгер конфигурации. */
    private static final Logger log = LoggerFactory.getLogger(AppConfig.class);
    /** Разбор YAML. */
    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());

    /** Включена ли админка. */
    private boolean adminEnabled = true;
    /** Порт HTTP-админки. */
    private int adminPort = 8080;
    /** Токен админки; пусто — без авторизации. */
    private String adminToken = "";

    /** Создаётся через load() или defaults(). */
    private AppConfig() {}

    /** Прочитать application.yml (если есть) и переменные окружения. */
    public static AppConfig load() {
        AppConfig cfg = new AppConfig();
        Map<String, Object> yaml = readYaml();
        if (yaml != null) cfg.applyAdminYaml(yaml);
        cfg.applyEnv();
        log.info("Админка: {}", cfg.adminEnabled ? "порт " + cfg.adminPort + (cfg.adminToken.isBlank() ? " (без токена)" : " (с токеном)") : "выключена");
        return cfg;
    }

    /** Значения по умолчанию без файлов и окружения (тесты). */
    public static AppConfig defaults() { return new AppConfig(); }

    /** application.yml рядом с jar (или -Dconfig.file), иначе из ресурсов jar; null — файла нет. */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> readYaml() {
        String path = System.getProperty("config.file", "application.yml");
        File external = new File(path);
        try {
            if (external.exists()) {
                log.info("Читаю конфигурацию из файла: {}", external.getAbsolutePath());
                return YAML.readValue(external, Map.class);
            }
            try (InputStream in = AppConfig.class.getResourceAsStream("/application.yml")) {
                if (in != null) return YAML.readValue(in, Map.class);
            }
        } catch (Exception e) {
            log.warn("Не удалось прочитать YAML: {}", e.getMessage());
        }
        return null;
    }

    /** Блок admin из YAML; про устаревшие блоки risk и exchanges — предупреждение в лог. */
    @SuppressWarnings("unchecked")
    private void applyAdminYaml(Map<String, Object> root) {
        for (String legacy : new String[]{"risk", "exchanges"}) {
            if (root.containsKey(legacy))
                log.warn("Блок «{}» в application.yml больше не читается: эти настройки задаются через админку", legacy);
        }
        Map<String, Object> admin = (Map<String, Object>) root.getOrDefault("admin", Map.of());
        adminEnabled = bool(admin.get("enabled"), adminEnabled);
        adminPort = intOf(admin.get("port"), adminPort);
        adminToken = str(admin.get("token"), adminToken);
    }

    /** Переменные окружения ADMIN_* важнее YAML. */
    private void applyEnv() {
        adminEnabled = bool(env("ADMIN_ENABLED"), adminEnabled);
        adminPort = intOf(env("ADMIN_PORT"), adminPort);
        adminToken = str(env("ADMIN_TOKEN"), adminToken);
    }

    /** Переменная окружения или null, если не задана или пуста. */
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
