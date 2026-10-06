package com.hft.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;

/**
 * Хранилище настроек на SQLite вместо самодельного JSON-файла.
 *
 * <p>Почему SQLite, а не JSON на диске:
 * <ul>
 *   <li>
 *     Атомарность транзакций обеспечивает СУБД, а не ручной механизм
 *     {@code temp file + rename}.
 *   </li>
 *   <li>
 *     WAL-режим позволяет читать состояние, например через обычный
 *     {@code sqlite3} CLI, параллельно с записью без блокировок.
 *   </li>
 *   <li>
 *     Если позже понадобится история изменений настроек или несколько
 *     профилей конфигурации, уже есть полноценная база, а не файл,
 *     который придётся дорабатывать.
 *   </li>
 * </ul>
 *
 * <p>Почему это не влияет на производительность бота:
 * <ul>
 *   <li>
 *     Запись происходит только в ответ на HTTP-запрос к админке:
 *     изменение риск-параметров, выбор биржи и т. п. — считанные разы
 *     за сессию, а не на каждом тике.
 *   </li>
 *   <li>
 *     Поток Disruptor, обрабатывающий рыночные данные, вообще не знает
 *     о существовании этого класса. Обращение к БД происходит исключительно
 *     в потоке HTTP-сервера админки.
 *   </li>
 *   <li>
 *     Используется одна таблица {@code key-value} с одной строкой на ключ,
 *     поэтому объём операций минимален.
 *   </li>
 * </ul>
 *
 * <p>Схема:
 * <pre>{@code
 * app_state(key   TEXT PRIMARY KEY, value TEXT)
 * }</pre>
 *
 * <p>Под ключом {@code "state"} хранится весь {@code PersistedState},
 * сериализованный в JSON одной строкой.
 *
 * <p>Отдельные таблицы для каждой сущности были бы оверинжинирингом
 * для конфигурации всего из нескольких десятков полей.
 */
public final class SqliteStateStore {

    /** Логгер. */
    private static final Logger log = LoggerFactory.getLogger(SqliteStateStore.class);
    /** Ключ строки состояния в таблице app_state. */
    private static final String STATE_KEY = "state";

    /** Файл базы. */
    private final Path dbPath;
    /** JDBC-адрес базы. */
    private final String jdbcUrl;
    private final ObjectMapper mapper = new ObjectMapper()
            .configure(com.fasterxml.jackson.databind.DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    /** Путь к базе — переменная окружения STATE_DB (по умолчанию data/state.db). */
    public SqliteStateStore() {
        String path = java.util.Objects.requireNonNullElse(com.hft.config.Env.get("STATE_DB"), "data/state.db");
        this.dbPath = Path.of(path);
        this.jdbcUrl = "jdbc:sqlite:" + dbPath;
        init();
    }

    /** Создать каталог, таблицу и включить WAL. */
    private void init() {
        try {
            dbPath.toAbsolutePath().getParent().toFile().mkdirs();
            try (Connection conn = connect(); Statement st = conn.createStatement()) {
                // WAL — читатели не блокируют писателя и наоборот, это на будущее
                // (например, дашборд, читающий настройки напрямую из БД)
                st.execute("PRAGMA journal_mode=WAL");
                st.execute("""
                    CREATE TABLE IF NOT EXISTS app_state (
                        key   TEXT PRIMARY KEY,
                        value TEXT NOT NULL,
                        updated_at INTEGER NOT NULL
                    )
                    """);
            }
            log.info("SQLite-хранилище настроек инициализировано: {}", dbPath.toAbsolutePath());
        } catch (SQLException e) {
            throw new IllegalStateException("Не удалось инициализировать SQLite по пути " + dbPath, e);
        }
    }

    /** Новое соединение (SQLite — дёшево). */
    private Connection connect() throws SQLException {
        return DriverManager.getConnection(jdbcUrl);
    }

    /** Прочитать сохранённое состояние; пусто — нет записи или она не читается. */
    public Optional<PersistedState> load() {
        String sql = "SELECT value FROM app_state WHERE key = ?";
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            ps.setString(1, STATE_KEY);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    log.info("В {} ещё нет сохранённого состояния — стартуем с настроек по умолчанию", dbPath);
                    return Optional.empty();
                }
                PersistedState state = mapper.readValue(rs.getString("value"), PersistedState.class);
                log.info("Состояние загружено из {}: биржи={} autoStart={}",
                        dbPath, state.selection().keySet(), state.autoStart());
                return Optional.of(state);
            }
        } catch (Exception e) {
            log.error("Не удалось прочитать состояние из {} — стартуем с настроек по умолчанию: {}",
                    dbPath, e.getMessage());
            return Optional.empty();
        }
    }

    /**
     * UPSERT одной строкой в транзакции. SQLite сам обеспечивает атомарность —
     * никакого ручного temp-file/rename, как было бы при работе с обычным файлом.
     */
    public synchronized void save(PersistedState state) {
        String sql = """
            INSERT INTO app_state (key, value, updated_at) VALUES (?, ?, ?)
            ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
            """;
        try (Connection conn = connect(); PreparedStatement ps = conn.prepareStatement(sql)) {
            String json = mapper.writeValueAsString(state);
            ps.setString(1, STATE_KEY);
            ps.setString(2, json);
            ps.setLong(3, System.currentTimeMillis());
            ps.executeUpdate();
        } catch (Exception e) {
            log.error("Не удалось сохранить состояние в {}", dbPath, e);
        }
    }

    /** Путь к файлу базы. */
    public Path filePath() { return dbPath; }
}
