package com.hft.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Инфраструктурная конфигурация: порт и токен админки, адреса бирж и testnet.
 *
 * Торговых параметров (риск, стратегия, символы, глубина стакана) здесь нет —
 * они задаются для каждой биржи через админку и хранятся в SQLite, см. {@link TradingParams}.
 *
 * Приоритет источников, от высшего к низшему:
 *   1. Переменные окружения (переопределяют конкретную биржу по префиксу)
 *   2. Внешний YAML рядом с jar
 *   3. application.yml внутри jar
 *   4. Значения по умолчанию в коде
 */
public final class AppConfig {

    private static final Logger log = LoggerFactory.getLogger(AppConfig.class);
    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());

    private final List<ExchangeConfig> exchanges;

    // Админка
    private boolean adminEnabled = true;
    private int adminPort = 8080;
    private String adminToken = "";

    private AppConfig(List<ExchangeConfig> exchanges) {
        this.exchanges = exchanges;
    }

    public static AppConfig load() {
        Map<String, Object> yaml = readYaml();
        List<ExchangeConfig> exchanges = parseExchanges(yaml);
        AppConfig cfg = new AppConfig(exchanges);
        if (yaml != null) cfg.applyAdminYaml(yaml);
        cfg.applyEnv();

        log.info("Конфигурация загружена. Бирж с явными адресами: {}", exchanges.size());
        for (ExchangeConfig ex : exchanges) {
            log.info("  [{}] testnet={} rest={} ws={}", ex.id(), ex.testnet(), ex.restUrl(), ex.wsUrl());
        }
        log.info("Админка: {}", cfg.adminEnabled ? "порт " + cfg.adminPort : "выключена");
        return cfg;
    }

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
                if (in != null) {
                    log.info("Читаю конфигурацию из ресурсов jar");
                    return YAML.readValue(in, Map.class);
                }
            }
        } catch (Exception e) {
            log.warn("Не удалось прочитать YAML: {}", e.getMessage());
        }
        return null;
    }

    /** Блок exchanges в YAML — только адреса/testnet; список может быть пустым. */
    @SuppressWarnings("unchecked")
    private static List<ExchangeConfig> parseExchanges(Map<String, Object> root) {
        List<ExchangeConfig> result = new ArrayList<>();
        Object rawList = root == null ? null : root.get("exchanges");
        if (!(rawList instanceof List<?> list)) return result;

        for (Object item : list) {
            Map<String, Object> m = (Map<String, Object>) item;
            String id = str(m.get("id"), "binance").toLowerCase();
            boolean testnet = bool(m.get("testnet"), true);
            ExchangeConfig ec = new ExchangeConfig(
                    id,
                    testnet,
                    str(m.get("rest-url"), defaultRestUrl(id, testnet)),
                    str(m.get("ws-url"), defaultWsUrl(id, testnet)),
                    intOf(m.get("recv-window-ms"), 5000),
                    List.of(), 0, 0);
            // Переменные окружения переопределяют конкретную биржу по префиксу: BINANCE_TESTNET, BYBIT_REST_URL ...
            result.add(applyExchangeEnv(ec));
        }
        return result;
    }

    private static ExchangeConfig applyExchangeEnv(ExchangeConfig ec) {
        String prefix = ec.id().toUpperCase();
        boolean testnet = bool(env(prefix + "_TESTNET"), ec.testnet());
        String restUrl = str(env(prefix + "_REST_URL"), ec.restUrl());
        String wsUrl = str(env(prefix + "_WS_URL"), ec.wsUrl());
        return new ExchangeConfig(ec.id(), testnet, restUrl, wsUrl, ec.recvWindowMs(), List.of(), 0, 0);
    }

    private static String defaultRestUrl(String id, boolean testnet) {
        return switch (id) {
            case "binance" -> testnet ? "https://testnet.binance.vision" : "https://api.binance.com";
            case "bybit" -> testnet ? "https://api-testnet.bybit.com" : "https://api.bybit.com";
            default -> com.hft.exchange.catalog.ExchangeCatalog.find(id)
                    .map(com.hft.exchange.catalog.ExchangeInfo::restUrl).orElse("");
        };
    }

    private static String defaultWsUrl(String id, boolean testnet) {
        return switch (id) {
            case "binance" -> testnet ? "wss://testnet.binance.vision/ws" : "wss://stream.binance.com:9443/ws";
            case "bybit" -> testnet ? "wss://stream-testnet.bybit.com/v5/public/spot" : "wss://stream.bybit.com/v5/public/spot";
            default -> "";
        };
    }

    @SuppressWarnings("unchecked")
    private void applyAdminYaml(Map<String, Object> root) {
        Map<String, Object> admin = (Map<String, Object>) root.getOrDefault("admin", Map.of());
        adminEnabled = bool(admin.get("enabled"), adminEnabled);
        adminPort = intOf(admin.get("port"), adminPort);
        adminToken = str(admin.get("token"), adminToken);
    }

    private void applyEnv() {
        adminEnabled = bool(env("ADMIN_ENABLED"), adminEnabled);
        adminPort = intOf(env("ADMIN_PORT"), adminPort);
        adminToken = str(env("ADMIN_TOKEN"), adminToken);
    }

    private static String env(String key) {
        String v = System.getenv(key);
        return v == null || v.isBlank() ? null : v;
    }

    private static String str(Object v, String def) { return v == null ? def : String.valueOf(v); }
    private static boolean bool(Object v, boolean def) { return v == null ? def : Boolean.parseBoolean(String.valueOf(v)); }
    private static int intOf(Object v, int def) {
        if (v == null) return def;
        try { return Integer.parseInt(String.valueOf(v).trim()); } catch (NumberFormatException e) { return def; }
    }

    // ---------- Геттеры ----------

    public List<ExchangeConfig> exchanges() { return List.copyOf(exchanges); }

    /**
     * Подключение по умолчанию, если в application.yml для биржи нет записи
     * (в том числе когда YAML нет вообще). testnet=true по умолчанию — так безопаснее;
     * переменные окружения ID_TESTNET/ID_REST_URL/ID_WS_URL действуют и здесь.
     */
    public ExchangeConfig defaultExchangeConfig(String id) {
        return applyExchangeEnv(new ExchangeConfig(id, true,
                defaultRestUrl(id, true), defaultWsUrl(id, true), 5000, List.of(), 0, 0));
    }

    public boolean adminEnabled() { return adminEnabled; }
    public int adminPort() { return adminPort; }
    public String adminToken() { return adminToken; }
}
