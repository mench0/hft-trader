package com.hft.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.dataformat.yaml.YAMLGenerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Настройки процесса из application.yml: админка, путь к базе и API-ключи бирж.
 *
 * <pre>
 * admin:
 *   enabled: true
 *   port: 8080
 *   token: "..."
 * storage:
 *   state-db: data/state.db
 * keys:
 *   binance:
 *     api-key: "..."
 *     api-secret: "..."
 *   okx:
 *     api-key: "..."
 *     api-secret: "..."
 *     passphrase: "..."
 * </pre>
 *
 * Внутри значения доступны под плоскими именами: ADMIN_TOKEN, STATE_DB, BINANCE_API_KEY, OKX_PASSPHRASE…
 * Где ищется файл: рядом с jar / в папке запуска (или -Dconfig.file=…), иначе application.yml
 * из ресурсов (src/main/resources, внутри jar). Внешний файл перечитывается при изменении —
 * новые ключи применяются при следующем /control/start; файл из ресурсов меняется только пересборкой.
 * Переменная окружения с тем же именем используется, только если в application.yml значения нет.
 */
public final class Env {

    /** Логгер. */
    private static final Logger log = LoggerFactory.getLogger(Env.class);
    /** Разбор и запись YAML. */
    private static final ObjectMapper YAML = new ObjectMapper(new YAMLFactory()
            .disable(YAMLGenerator.Feature.WRITE_DOC_START_MARKER)
            .enable(YAMLGenerator.Feature.MINIMIZE_QUOTES));
    /** Значения из файла под плоскими именами. */
    private static volatile Map<String, String> vars = Map.of();
    /** Откуда прочитано: путь к файлу, "ресурсы" или "" — ниоткуда. */
    private static volatile String loadedFrom = "";
    /** Время изменения внешнего файла при последнем чтении; -1 — файла не было. */
    private static long loadedMtime = Long.MIN_VALUE;

    private Env() {}

    /** Значение: application.yml, иначе переменная окружения с тем же именем; пустое — null. */
    public static String get(String key) {
        refresh();
        String v = vars.get(key);
        if (v == null || v.isBlank()) v = System.getenv(key);
        return v == null || v.isBlank() ? null : v.trim();
    }

    /** Откуда берётся значение: "application.yml", "окружение" или "" — не задано. */
    public static String source(String key) {
        refresh();
        if (notBlank(vars.get(key))) return "application.yml";
        if (notBlank(System.getenv(key))) return "окружение";
        return "";
    }

    /** Внешний файл application.yml (существует он или нет). */
    public static File file() { return new File(System.getProperty("config.file", "application.yml")).getAbsoluteFile(); }

    /** Откуда сейчас прочитаны значения: путь к файлу, "ресурсы (внутри jar)" или "". */
    public static String loadedFrom() { refresh(); return loadedFrom; }

    /**
     * Записать значения во внешний application.yml (нет файла — создаётся; был только в ресурсах —
     * берётся за основу). Пустое значение удаляет поле. Комментарии файла при записи не сохраняются,
     * поэтому прежняя версия кладётся рядом как application.yml.bak. Запись атомарная, права 600.
     */
    @SuppressWarnings("unchecked")
    public static synchronized void set(Map<String, String> updates) throws IOException {
        for (var e : updates.entrySet()) {
            if (path(e.getKey()) == null) throw new IllegalArgumentException("Нельзя задать в application.yml: " + e.getKey());
            if (e.getValue() != null && (e.getValue().contains("\n") || e.getValue().contains("\r")))
                throw new IllegalArgumentException("Значение не может содержать перевод строки");
        }
        File f = file();
        Map<String, Object> root = readRoot(f);
        for (var e : updates.entrySet()) {
            String[] p = path(e.getKey());
            Map<String, Object> node = root;
            for (int i = 0; i < p.length - 1; i++) {
                Object child = node.get(p[i]);
                if (!(child instanceof Map)) { child = new LinkedHashMap<String, Object>(); node.put(p[i], child); }
                node = (Map<String, Object>) child;
            }
            String v = e.getValue() == null ? "" : e.getValue().trim();
            if (v.isEmpty()) node.remove(p[p.length - 1]); else node.put(p[p.length - 1], v);
        }
        Path target = f.toPath();
        if (target.getParent() != null) Files.createDirectories(target.getParent());
        if (Files.isRegularFile(target)) {
            Path bak = target.resolveSibling(target.getFileName() + ".bak");
            Files.copy(target, bak, StandardCopyOption.REPLACE_EXISTING);
            ownerOnly(bak);                                // в копии те же секреты
        }
        Path tmp = target.resolveSibling(target.getFileName() + ".tmp");
        YAML.writeValue(tmp.toFile(), root);
        ownerOnly(tmp);
        Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        loadedMtime = Long.MIN_VALUE;                      // перечитать при следующем get
        log.info("Обновлены значения в {}: {}", target, updates.keySet());
    }

    /** Права 600 (где файловая система их поддерживает). */
    private static void ownerOnly(Path p) throws IOException {
        try { Files.setPosixFilePermissions(p, java.nio.file.attribute.PosixFilePermissions.fromString("rw-------")); }
        catch (UnsupportedOperationException ignored) { }
    }

    /** Плоское имя -> путь в YAML; null — такое имя в файле не хранится. */
    static String[] path(String key) {
        switch (key) {
            case "ADMIN_ENABLED": return new String[]{"admin", "enabled"};
            case "ADMIN_PORT": return new String[]{"admin", "port"};
            case "ADMIN_TOKEN": return new String[]{"admin", "token"};
            case "STATE_DB": return new String[]{"storage", "state-db"};
            default:
        }
        for (String suffix : new String[]{"_API_KEY", "_API_SECRET", "_PASSPHRASE"}) {
            if (key.endsWith(suffix) && key.length() > suffix.length() && key.matches("[A-Z][A-Z0-9]*_[A-Z_]+")) {
                String ex = key.substring(0, key.length() - suffix.length()).toLowerCase(Locale.ROOT);
                if (!ex.matches("[a-z][a-z0-9]*")) return null;
                return new String[]{"keys", ex, suffix.substring(1).toLowerCase(Locale.ROOT).replace('_', '-')};
            }
        }
        return null;
    }

    /** Непустая строка. */
    private static boolean notBlank(String v) { return v != null && !v.isBlank(); }

    /** Перечитать файл, если внешний файл изменился (или появился/исчез). */
    private static synchronized void refresh() {
        File f = file();
        long mtime = f.isFile() ? f.lastModified() : -1;
        if (mtime == loadedMtime) return;
        loadedMtime = mtime;
        try {
            if (mtime >= 0) {
                vars = Collections.unmodifiableMap(flatten(YAML.readValue(f, Map.class)));
                loadedFrom = f.toString();
            } else {
                try (InputStream in = Env.class.getResourceAsStream("/application.yml")) {
                    vars = in == null ? Map.of() : Collections.unmodifiableMap(flatten(YAML.readValue(in, Map.class)));
                    loadedFrom = in == null ? "" : "ресурсы (внутри jar)";
                }
            }
            if (!loadedFrom.isEmpty()) log.info("Прочитан application.yml ({}): {}", loadedFrom, vars.keySet());
        } catch (Exception e) {
            log.error("Не удалось прочитать application.yml: {}", e.getMessage());
        }
    }

    /** Содержимое внешнего файла, иначе файла из ресурсов, иначе пусто — основа для записи. */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> readRoot(File f) throws IOException {
        if (f.isFile()) {
            Map<String, Object> m = YAML.readValue(f, Map.class);
            return m == null ? new LinkedHashMap<>() : m;
        }
        try (InputStream in = Env.class.getResourceAsStream("/application.yml")) {
            Map<String, Object> m = in == null ? null : YAML.readValue(in, Map.class);
            return m == null ? new LinkedHashMap<>() : m;
        }
    }

    /** admin.*, storage.state-db и keys.&lt;биржа&gt;.&lt;поле&gt; -> плоские имена; пустые пропускаются. */
    @SuppressWarnings("unchecked")
    static Map<String, String> flatten(Map<String, Object> root) {
        Map<String, String> out = new LinkedHashMap<>();
        if (root == null) return out;
        if (root.get("admin") instanceof Map<?, ?> admin)
            for (String f : new String[]{"enabled", "port", "token"}) put(out, "ADMIN_" + f.toUpperCase(Locale.ROOT), admin.get(f));
        if (root.get("storage") instanceof Map<?, ?> storage) put(out, "STATE_DB", storage.get("state-db"));
        if (root.get("keys") instanceof Map<?, ?> keys) {
            for (var ex : ((Map<String, Object>) keys).entrySet()) {
                if (!(ex.getValue() instanceof Map<?, ?> fields)) continue;
                String prefix = ex.getKey().trim().toUpperCase(Locale.ROOT);
                for (var fe : ((Map<String, Object>) fields).entrySet())
                    put(out, prefix + "_" + fe.getKey().trim().toUpperCase(Locale.ROOT).replace('-', '_'), fe.getValue());
            }
        }
        return out;
    }

    /** Положить непустое значение. */
    private static void put(Map<String, String> out, String k, Object v) {
        if (v == null) return;
        String s = String.valueOf(v).trim();
        if (!s.isEmpty()) out.put(k, s);
    }
}
