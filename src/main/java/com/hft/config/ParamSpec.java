package com.hft.config;

import java.lang.reflect.Constructor;
import java.lang.reflect.RecordComponent;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Описание одного настраиваемого параметра: значение по умолчанию, допустимые значения,
 * нужен ли перезапуск и текст справки для админки.
 *
 * Наборы параметров ({@link TradingParams}, {@link GlobalParams}) — неизменяемые record'ы. Они собираются
 * из карты «имя → строка» по именам полей ({@link #build}), поэтому порядок аргументов перепутать нельзя,
 * а у каждого поля обязано быть описание — иначе ошибка при старте.
 *
 * @param name     имя параметра (совпадает с полем record'а и ключом в запросе админки)
 * @param def      значение по умолчанию строкой
 * @param min      нижняя граница для чисел (для строк и флагов не используется)
 * @param max      верхняя граница для чисел
 * @param regex    допустимый формат строки (для чисел и флагов не используется)
 * @param restart  true — новое значение применяется только после /control/stop и /control/start
 * @param help     что это и в каких единицах — показывается в GET /exchange/params/schema
 */
public record ParamSpec(String name, String def, double min, double max, String regex, boolean restart, String help) {

    /** Число в диапазоне [min, max]; применяется на лету. */
    public static ParamSpec num(String name, double def, double min, double max, String help) {
        return new ParamSpec(name, plain(def), min, max, null, false, help);
    }

    /** Флаг true/false; применяется на лету. */
    public static ParamSpec flag(String name, boolean def, String help) {
        return new ParamSpec(name, String.valueOf(def), 0, 0, null, false, help);
    }

    /** Строка в формате regex; применяется на лету. */
    public static ParamSpec text(String name, String def, String regex, String help) {
        return new ParamSpec(name, def, 0, 0, regex, false, help);
    }

    /** Тот же параметр, но применяется только после перезапуска биржи. */
    public ParamSpec needsRestart() { return new ParamSpec(name, def, min, max, regex, true, help); }

    /** Число без хвоста «.0» для целых значений (в описаниях и сообщениях об ошибках). */
    private static String plain(double v) {
        return v == Math.rint(v) && Math.abs(v) < 1e15 ? String.valueOf((long) v) : String.valueOf(v);
    }

    /** Карта описаний по имени (в порядке объявления). */
    static Map<String, ParamSpec> index(List<ParamSpec> specs) {
        Map<String, ParamSpec> m = new LinkedHashMap<>();
        for (ParamSpec s : specs) if (m.put(s.name(), s) != null) throw new IllegalStateException("параметр описан дважды: " + s.name());
        return m;
    }

    /**
     * Собрать record из строковых значений: отсутствующие берутся по умолчанию, каждое проверяется по описанию.
     * Неизвестные ключи игнорируются (в запросе админки рядом лежат exchange и т.п.).
     */
    static <R extends Record> R build(Class<R> type, Map<String, ParamSpec> specs, Map<String, String> values) {
        RecordComponent[] comps = type.getRecordComponents();
        Object[] args = new Object[comps.length];
        Class<?>[] types = new Class<?>[comps.length];
        for (int i = 0; i < comps.length; i++) {
            RecordComponent c = comps[i];
            ParamSpec s = specs.get(c.getName());
            if (s == null) throw new IllegalStateException("у поля " + type.getSimpleName() + "." + c.getName() + " нет описания ParamSpec");
            String raw = values.getOrDefault(c.getName(), s.def());
            args[i] = parse(s, c.getType(), raw == null ? s.def() : raw.trim());
            types[i] = c.getType();
        }
        if (specs.size() != comps.length) throw new IllegalStateException("в описаниях " + type.getSimpleName() + " есть лишние параметры");
        try {
            Constructor<R> ctor = type.getDeclaredConstructor(types);
            return ctor.newInstance(args);
        } catch (java.lang.reflect.InvocationTargetException e) {
            if (e.getCause() instanceof RuntimeException re) throw re;
            throw new IllegalStateException(e.getCause());
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException(e);
        }
    }

    /** Все поля record'а строками (для SQLite и ответа админки). */
    static Map<String, String> toMap(Record r) {
        Map<String, String> m = new LinkedHashMap<>();
        for (RecordComponent c : r.getClass().getRecordComponents()) {
            try { m.put(c.getName(), String.valueOf(c.getAccessor().invoke(r))); }
            catch (ReflectiveOperationException e) { throw new IllegalStateException(e); }
        }
        return m;
    }

    /** Описания для админки: значение по умолчанию, границы, перезапуск, справка. */
    static Map<String, Object> schema(Map<String, ParamSpec> specs) {
        Map<String, Object> out = new LinkedHashMap<>();
        for (ParamSpec s : specs.values()) {
            Map<String, Object> m = new LinkedHashMap<>();
            m.put("default", s.def());
            if (s.regex() != null) m.put("format", s.regex());
            else if (!s.def().equals("true") && !s.def().equals("false")) { m.put("min", s.min()); m.put("max", s.max()); }
            m.put("restart", s.restart());
            m.put("help", s.help());
            out.put(s.name(), m);
        }
        return out;
    }

    /**
     * Разобрать строку в тип поля record'а и проверить по описанию: true/false, формат строки, диапазон и целочисленность.
     */
    private static Object parse(ParamSpec s, Class<?> t, String v) {
        if (t == boolean.class) {
            if (!v.equalsIgnoreCase("true") && !v.equalsIgnoreCase("false"))
                throw new IllegalArgumentException(s.name() + ": ожидалось true/false, получено " + v);
            return Boolean.parseBoolean(v);
        }
        if (t == String.class) {
            if (s.regex() != null && !v.matches(s.regex())) throw new IllegalArgumentException(s.name() + ": недопустимое значение «" + v + "»");
            return v;
        }
        double d;
        try { d = Double.parseDouble(v); }
        catch (NumberFormatException e) { throw new IllegalArgumentException(s.name() + ": не число: " + v); }
        if (!(d >= s.min() && d <= s.max())) throw new IllegalArgumentException(s.name() + ": " + v + " вне диапазона [" + plain(s.min()) + ", " + plain(s.max()) + "]");
        if (t == double.class) return d;
        if (d != Math.rint(d)) throw new IllegalArgumentException(s.name() + ": нужно целое число, получено " + v);
        if (t == int.class) return (int) d;
        if (t == long.class) return (long) d;
        throw new IllegalStateException("тип " + t + " не поддерживается");
    }
}
