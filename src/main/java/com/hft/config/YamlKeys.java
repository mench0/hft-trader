package com.hft.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.InputStream;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * API-ключи бирж из блока keys в application.yml. Где ищется: файл рядом с jar / в папке запуска
 * (или -Dconfig.file), иначе src/main/resources/application.yml, собранный в jar. Внешний файл
 * перечитывается при изменении; из ресурсов — читается один раз (поменять — пересобрать jar).
 * Формат:
 *
 * <pre>
 * keys:
 *   binance:
 *     api-key: ...
 *     api-secret: ...
 *   okx:
 *     api-key: ...
 *     api-secret: ...
 *     passphrase: ...
 * </pre>
 *
 * Блок admin (enabled, port, token) тоже отдаётся как ADMIN_*, чтобы админка видела его источник.
 * Превращается в те же имена, что и переменные окружения: BINANCE_API_KEY, OKX_PASSPHRASE и т.д.
 * Файл перечитывается при изменении — новые ключи применяются при следующем /control/start.
 */
final class YamlKeys {

    /** Логгер. */
    private static final Logger log = LoggerFactory.getLogger(YamlKeys.class);
    /** Разбор YAML. */
    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory());
    /** Ключи из файла в виде переменных окружения. */
    private static volatile Map<String, String> vars = Map.of();
    /** Время изменения файла при последнем чтении; -1 — файла не было. */
    private static long loadedMtime = Long.MIN_VALUE;

    private YamlKeys() {}

    /** Файл конфигурации. */
    static File file() { return new File(System.getProperty("config.file", "application.yml")).getAbsoluteFile(); }

    /** Ключи из application.yml (после перечитывания). */
    static Map<String, String> vars() { refresh(); return vars; }

    /** Перечитать файл, если он изменился (или появился/исчез). */
    private static synchronized void refresh() {
        File f = file();
        long mtime = f.isFile() ? f.lastModified() : -1;
        if (mtime == loadedMtime) return;
        loadedMtime = mtime;
        try {
            if (mtime >= 0) {
                vars = Collections.unmodifiableMap(flatten(YAML.readValue(f, Map.class)));
                if (!vars.isEmpty()) log.info("Прочитаны API-ключи из {}: {}", f, vars.keySet());
                return;
            }
            // внешнего файла нет — application.yml из ресурсов (src/main/resources, внутри jar)
            try (InputStream in = YamlKeys.class.getResourceAsStream("/application.yml")) {
                vars = in == null ? Map.of() : Collections.unmodifiableMap(flatten(YAML.readValue(in, Map.class)));
            }
            if (!vars.isEmpty()) log.info("Прочитаны API-ключи из application.yml в ресурсах: {}", vars.keySet());
        } catch (Exception e) {
            log.error("Не удалось прочитать блок keys из application.yml: {}", e.getMessage());
        }
    }

    /** keys.&lt;биржа&gt;.&lt;поле&gt; -> БИРЖА_ПОЛЕ (api-key -> API_KEY); пустые значения пропускаются. */
    @SuppressWarnings("unchecked")
    static Map<String, String> flatten(Map<String, Object> root) {
        Map<String, String> out = new LinkedHashMap<>();
        if (root == null) return out;
        if (root.get("admin") instanceof Map<?, ?> admin)       // admin.token -> ADMIN_TOKEN и т.д.
            for (String f : new String[]{"enabled", "port", "token"})
                if (admin.get(f) != null && !String.valueOf(admin.get(f)).isBlank())
                    out.put("ADMIN_" + f.toUpperCase(Locale.ROOT), String.valueOf(admin.get(f)).trim());
        if (!(root.get("keys") instanceof Map<?, ?> keys)) return out;
        for (var ex : ((Map<String, Object>) keys).entrySet()) {
            if (!(ex.getValue() instanceof Map<?, ?> fields)) continue;
            String prefix = ex.getKey().trim().toUpperCase(Locale.ROOT);
            for (var fe : ((Map<String, Object>) fields).entrySet()) {
                if (fe.getValue() == null) continue;
                String v = String.valueOf(fe.getValue()).trim();
                if (v.isEmpty()) continue;
                String name = fe.getKey().trim().toUpperCase(Locale.ROOT).replace('-', '_');
                out.put(prefix + "_" + name, v);
            }
        }
        return out;
    }
}
