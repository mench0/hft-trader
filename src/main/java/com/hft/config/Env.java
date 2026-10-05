package com.hft.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Переменные окружения с подхватом файла .env (путь — ENV_FILE, по умолчанию .env в рабочей папке).
 * Файл перечитывается, когда меняется его время изменения, поэтому новые ключи применяются без
 * перезапуска процесса — при следующем /control/start (биржи создаются заново при старте).
 * Значение из файла важнее окружения процесса: правка .env всегда побеждает старый export.
 * Формат: KEY=value, пустые строки и # комментарии пропускаются, кавычки вокруг значения снимаются.
 */
public final class Env {

    /** Логгер. */
    private static final Logger log = LoggerFactory.getLogger(Env.class);
    /** Файл с переменными. */
    private static final Path FILE = Path.of(System.getenv().getOrDefault("ENV_FILE", ".env"));
    /** Последнее прочитанное содержимое файла. */
    private static volatile Map<String, String> fileVars = Map.of();
    /** Время изменения файла при последнем чтении; -1 — файла не было. */
    private static long loadedMtime = Long.MIN_VALUE;

    private Env() {}

    /** Значение переменной: файл .env, затем окружение процесса, затем блок keys в application.yml; пустое — null. */
    public static String get(String key) {
        refresh();
        String v = fileVars.get(key);
        if (v == null || v.isBlank()) v = System.getenv(key);
        if (v == null || v.isBlank()) v = YamlKeys.vars().get(key);
        return v == null || v.isBlank() ? null : v.trim();
    }

    /** Откуда берётся значение: ".env", "окружение", "application.yml" или "" — не задано. */
    public static String source(String key) {
        refresh();
        if (notBlank(fileVars.get(key))) return ".env";
        if (notBlank(System.getenv(key))) return "окружение";
        if (notBlank(YamlKeys.vars().get(key))) return "application.yml";
        return "";
    }

    /** Непустая строка. */
    private static boolean notBlank(String v) { return v != null && !v.isBlank(); }

    /** Путь к файлу .env. */
    public static Path file() { return FILE.toAbsolutePath(); }

    /** Есть ли файл .env. */
    public static boolean fileExists() { return Files.isRegularFile(FILE); }

    /** Переменные из файла (после перечитывания). */
    public static Map<String, String> fileVars() { refresh(); return fileVars; }

    /**
     * Записать переменные в .env: существующие строки меняются на месте, новые дописываются в конец,
     * пустое значение удаляет строку. Файл пишется атомарно (временный файл + переименование), права 600.
     */
    public static synchronized void set(Map<String, String> updates) throws IOException {
        for (String k : updates.keySet())
            if (!k.matches("[A-Z][A-Z0-9_]*")) throw new IllegalArgumentException("Недопустимое имя переменной: " + k);
        for (String v : updates.values())
            if (v != null && (v.contains("\n") || v.contains("\r"))) throw new IllegalArgumentException("Значение не может содержать перевод строки");
        java.util.List<String> lines = Files.isRegularFile(FILE)
                ? new java.util.ArrayList<>(Files.readAllLines(FILE, StandardCharsets.UTF_8)) : new java.util.ArrayList<>();
        Map<String, String> left = new LinkedHashMap<>(updates);
        for (var it = lines.listIterator(); it.hasNext(); ) {
            String line = it.next().strip();
            if (line.startsWith("export ")) line = line.substring(7).strip();
            int eq = line.indexOf('=');
            if (line.startsWith("#") || eq <= 0) continue;
            String k = line.substring(0, eq).strip();
            if (!left.containsKey(k)) continue;
            String v = left.remove(k);
            if (v == null || v.isBlank()) it.remove(); else it.set(k + "=" + v.strip());
        }
        left.forEach((k, v) -> { if (v != null && !v.isBlank()) lines.add(k + "=" + v.strip()); });
        Path abs = FILE.toAbsolutePath();
        if (abs.getParent() != null) Files.createDirectories(abs.getParent());
        Path tmp = abs.resolveSibling(abs.getFileName() + ".tmp");
        Files.write(tmp, lines, StandardCharsets.UTF_8);
        try { Files.setPosixFilePermissions(tmp, java.nio.file.attribute.PosixFilePermissions.fromString("rw-------")); }
        catch (UnsupportedOperationException ignored) { }
        Files.move(tmp, abs, java.nio.file.StandardCopyOption.REPLACE_EXISTING, java.nio.file.StandardCopyOption.ATOMIC_MOVE);
        loadedMtime = Long.MIN_VALUE;                      // перечитать при следующем get, даже если mtime совпал
        log.info("Обновлены переменные в {}: {}", abs, updates.keySet());
    }

    /** Перечитать файл, если он изменился (или появился/исчез). */
    private static synchronized void refresh() {
        long mtime;
        try { mtime = Files.isRegularFile(FILE) ? Files.getLastModifiedTime(FILE).toMillis() : -1; }
        catch (IOException e) { mtime = -1; }
        if (mtime == loadedMtime) return;
        loadedMtime = mtime;
        if (mtime < 0) { fileVars = Map.of(); return; }
        try {
            fileVars = Collections.unmodifiableMap(parse(Files.readString(FILE, StandardCharsets.UTF_8)));
            log.info("Прочитан {}: {} переменных", FILE.toAbsolutePath(), fileVars.size());
        } catch (IOException e) {
            log.error("Не удалось прочитать {}: {}", FILE.toAbsolutePath(), e.getMessage());
        }
    }

    /** Разобрать текст .env. */
    static Map<String, String> parse(String text) {
        Map<String, String> out = new LinkedHashMap<>();
        for (String raw : text.split("\\R")) {
            String line = raw.strip();
            if (line.isEmpty() || line.startsWith("#")) continue;
            if (line.startsWith("export ")) line = line.substring(7).strip();
            int eq = line.indexOf('=');
            if (eq <= 0) continue;
            String k = line.substring(0, eq).strip(), v = line.substring(eq + 1).strip();
            if (v.length() >= 2 && (v.startsWith("\"") && v.endsWith("\"") || v.startsWith("'") && v.endsWith("'")))
                v = v.substring(1, v.length() - 1);
            out.put(k, v);
        }
        return out;
    }
}
