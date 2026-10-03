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
 * Конфигурация всего приложения: список бирж + общие для всех риск-параметры.
 *
 * Раньше был один блок "exchange" на одну биржу. Теперь "exchanges" —
 * список, у каждой записи свои url, ключи и символы. Риск и админка
 * остаются общими: лимит дневного убытка, например, разумно считать
 * суммарно по всем биржам, а не отдельно на каждую.
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

    // Риск — общий для всех бирж
    private double maxPositionQuote = 100.0;
    private double maxDailyLossQuote = 50.0;
    private double maxSlippagePercent = 0.3;
    private double feeReservePercent = 0.2;
    private int maxOrdersPerMinute = 30;
    private boolean tradingEnabled = false;

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
        if (yaml != null) cfg.applyRiskYaml(yaml);
        cfg.applyEnv();
        cfg.validate();

        log.info("Конфигурация загружена. Бирж: {}", exchanges.size());
        for (ExchangeConfig ex : exchanges) {
            log.info("  [{}] enabled={} testnet={} символы={}",
                    ex.id(), ex.enabled(), ex.testnet(), ex.symbols());
        }
        log.info("Торговля: {}, админка: {}", cfg.tradingEnabled,
                cfg.adminEnabled ? "порт " + cfg.adminPort : "выключена");
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

    @SuppressWarnings("unchecked")
    private static List<ExchangeConfig> parseExchanges(Map<String, Object> root) {
        List<ExchangeConfig> result = new ArrayList<>();
        if (root == null) {
            result.add(defaultBinance());
            return result;
        }

        Object rawList = root.get("exchanges");
        if (!(rawList instanceof List<?> list) || list.isEmpty()) {
            result.add(defaultBinance());
            return result;
        }

        for (Object item : list) {
            Map<String, Object> m = (Map<String, Object>) item;
            String id = str(m.get("id"), "binance").toLowerCase();

            List<String> symbols = new ArrayList<>();
            Object syms = m.get("symbols");
            if (syms instanceof List<?> sl) {
                sl.forEach(s -> symbols.add(String.valueOf(s).toUpperCase()));
            }
            if (symbols.isEmpty()) symbols.add("BTCUSDT");

            boolean testnet = bool(m.get("testnet"), true);
            ExchangeConfig ec = new ExchangeConfig(
                    id,
                    bool(m.get("enabled"), true),
                    testnet,
                    str(m.get("rest-url"), defaultRestUrl(id, testnet)),
                    str(m.get("ws-url"), defaultWsUrl(id, testnet)),
                    intOf(m.get("recv-window-ms"), 5000),
                    symbols,
                    intOf(m.get("book-depth"), 20),
                    intOf(m.get("price-window"), 1000)
            );

            // Переменные окружения переопределяют конкретную биржу по префиксу:
            // BINANCE_TESTNET, BINANCE_SYMBOLS, BYBIT_TESTNET, BYBIT_SYMBOLS ...
            result.add(applyExchangeEnv(ec));
        }
        return result;
    }

    private static ExchangeConfig applyExchangeEnv(ExchangeConfig ec) {
        String prefix = ec.id().toUpperCase();
        boolean testnet = bool(env(prefix + "_TESTNET"), ec.testnet());
        String restUrl = str(env(prefix + "_REST_URL"), ec.restUrl());
        String wsUrl = str(env(prefix + "_WS_URL"), ec.wsUrl());
        boolean enabled = bool(env(prefix + "_ENABLED"), ec.enabled());

        List<String> symbols = ec.symbols();
        String envSymbols = env(prefix + "_SYMBOLS");
        if (envSymbols != null && !envSymbols.isBlank()) {
            List<String> parsed = new ArrayList<>();
            for (String s : envSymbols.split(",")) {
                String t = s.trim().toUpperCase();
                if (!t.isEmpty()) parsed.add(t);
            }
            if (!parsed.isEmpty()) symbols = parsed;
        }

        return new ExchangeConfig(ec.id(), enabled, testnet, restUrl, wsUrl,
                ec.recvWindowMs(), symbols, ec.bookDepth(), ec.priceWindowSize());
    }

    private static ExchangeConfig defaultBinance() {
        return new ExchangeConfig("binance", true, true,
                defaultRestUrl("binance", true), defaultWsUrl("binance", true),
                5000, List.of("BTCUSDT"), 20, 1000);
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
    private void applyRiskYaml(Map<String, Object> root) {
        Map<String, Object> risk = (Map<String, Object>) root.getOrDefault("risk", Map.of());
        maxPositionQuote = dbl(risk.get("max-position-quote"), maxPositionQuote);
        maxDailyLossQuote = dbl(risk.get("max-daily-loss-quote"), maxDailyLossQuote);
        maxSlippagePercent = dbl(risk.get("max-slippage-percent"), maxSlippagePercent);
        feeReservePercent = dbl(risk.get("fee-reserve-percent"), feeReservePercent);
        maxOrdersPerMinute = intOf(risk.get("max-orders-per-minute"), maxOrdersPerMinute);
        tradingEnabled = bool(risk.get("trading-enabled"), tradingEnabled);

        Map<String, Object> admin = (Map<String, Object>) root.getOrDefault("admin", Map.of());
        adminEnabled = bool(admin.get("enabled"), adminEnabled);
        adminPort = intOf(admin.get("port"), adminPort);
        adminToken = str(admin.get("token"), adminToken);
    }

    private void applyEnv() {
        maxPositionQuote = dbl(env("RISK_MAX_POSITION_QUOTE"), maxPositionQuote);
        maxDailyLossQuote = dbl(env("RISK_MAX_DAILY_LOSS_QUOTE"), maxDailyLossQuote);
        maxSlippagePercent = dbl(env("RISK_MAX_SLIPPAGE_PERCENT"), maxSlippagePercent);
        feeReservePercent = dbl(env("RISK_FEE_RESERVE_PERCENT"), feeReservePercent);
        maxOrdersPerMinute = intOf(env("RISK_MAX_ORDERS_PER_MINUTE"), maxOrdersPerMinute);
        tradingEnabled = bool(env("TRADING_ENABLED"), tradingEnabled);

        adminEnabled = bool(env("ADMIN_ENABLED"), adminEnabled);
        adminPort = intOf(env("ADMIN_PORT"), adminPort);
        adminToken = str(env("ADMIN_TOKEN"), adminToken);
    }

    private void validate() {
        if (exchanges.isEmpty()) {
            throw new IllegalStateException("Не задано ни одной биржи");
        }
        boolean anyEnabled = exchanges.stream().anyMatch(ExchangeConfig::enabled);
        if (!anyEnabled) {
            throw new IllegalStateException("Все биржи выключены (enabled: false)");
        }
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
    private static double dbl(Object v, double def) {
        if (v == null) return def;
        try { return Double.parseDouble(String.valueOf(v).trim()); } catch (NumberFormatException e) { return def; }
    }

    // ---------- Геттеры ----------

    public List<ExchangeConfig> exchanges() { return List.copyOf(exchanges); }

    public ExchangeConfig exchange(String id) {
        return exchanges.stream().filter(e -> e.id().equalsIgnoreCase(id)).findFirst()
                .orElseThrow(() -> new IllegalArgumentException("Биржа не найдена в конфиге: " + id));
    }

    /**
     * Конфигурация биржи по умолчанию, если в application.yml для неё нет
     * записи — например, если бот запущен вообще без YAML и вся настройка
     * идёт через админку. testnet=true по умолчанию — так безопаснее.
     */
    public ExchangeConfig defaultExchangeConfig(String id) {
        return new ExchangeConfig(id, true, true,
                defaultRestUrl(id, true), defaultWsUrl(id, true),
                5000, List.of("BTCUSDT"), 20, 1000);
    }

    public double maxPositionQuote() { return maxPositionQuote; }
    public void setMaxPositionQuote(double v) { this.maxPositionQuote = v; }

    public double maxDailyLossQuote() { return maxDailyLossQuote; }
    public void setMaxDailyLossQuote(double v) { this.maxDailyLossQuote = v; }

    public double maxSlippagePercent() { return maxSlippagePercent; }
    public void setMaxSlippagePercent(double v) { this.maxSlippagePercent = v; }

    public double feeReservePercent() { return feeReservePercent; }
    public void setFeeReservePercent(double v) { this.feeReservePercent = v; }

    public int maxOrdersPerMinute() { return maxOrdersPerMinute; }
    public void setMaxOrdersPerMinute(int v) { this.maxOrdersPerMinute = v; }

    public boolean tradingEnabled() { return tradingEnabled; }
    public void setTradingEnabled(boolean v) { this.tradingEnabled = v; }
    public boolean adminEnabled() { return adminEnabled; }
    public int adminPort() { return adminPort; }
    public String adminToken() { return adminToken; }
}
