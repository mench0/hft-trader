package com.hft.exchange.generic;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;

import java.io.IOException;

/**
 * Помощники потокового разбора JSON (Jackson streaming) для горячего пути WS-фидов:
 * без дерева узлов и без строк на каждое число.
 */
public final class FastJson {

    public static final JsonFactory F = new JsonFactory();

    private FastJson() {}

    private static final double[] POW10 = new double[23];
    static { POW10[0] = 1; for (int i = 1; i < POW10.length; i++) POW10[i] = POW10[i - 1] * 10; }
    private static final long MAX_EXACT = 1L << 53;

    /** Число из текущего токена (строка "123.45" или число 123.45). */
    public static double num(JsonParser p) throws IOException {
        JsonToken t = p.currentToken();
        if (t != JsonToken.VALUE_STRING && t != JsonToken.VALUE_NUMBER_INT && t != JsonToken.VALUE_NUMBER_FLOAT)
            throw new IllegalArgumentException("ожидалось число, получено " + t);
        double d = parse(p.getTextCharacters(), p.getTextOffset(), p.getTextLength());
        if (!Double.isFinite(d) || d < 0) throw new IllegalArgumentException("некорректное число");
        return d;
    }

    public static long longOf(JsonParser p, long def) throws IOException {
        JsonToken t = p.currentToken();
        if (t == JsonToken.VALUE_NUMBER_INT) return p.getLongValue();
        if (t == JsonToken.VALUE_STRING) {
            char[] c = p.getTextCharacters(); int off = p.getTextOffset(), len = p.getTextLength();
            long v = 0;
            if (len == 0 || len > 18) return def;
            for (int i = 0; i < len; i++) { char ch = c[off + i]; if (ch < '0' || ch > '9') return def; v = v * 10 + (ch - '0'); }
            return v;
        }
        return def;
    }

    /**
     * Быстрый разбор десятичной записи: до 18 значащих цифр и до 22 знаков после точки считаются
     * точно (мантисса < 2^53 и точная степень десяти — результат округлён корректно), остальное — Double.parseDouble.
     */
    public static double parse(char[] c, int off, int len) {
        int i = off, end = off + len;
        if (i >= end) throw new NumberFormatException("пусто");
        boolean neg = false;
        if (c[i] == '-') { neg = true; i++; } else if (c[i] == '+') i++;
        long m = 0;
        int scale = 0, sig = 0;
        boolean dot = false, any = false;
        for (; i < end; i++) {
            char ch = c[i];
            if (ch >= '0' && ch <= '9') {
                any = true;
                if (m == 0 && ch == '0') { if (dot) scale++; continue; }
                if (sig >= 18) return slow(c, off, len);
                m = m * 10 + (ch - '0');
                sig++;
                if (dot) scale++;
            } else if (ch == '.' && !dot) {
                dot = true;
            } else {
                return slow(c, off, len);                  // экспонента и прочее — медленный путь
            }
        }
        if (!any) throw new NumberFormatException(new String(c, off, len));
        if (m > MAX_EXACT || scale >= POW10.length) return slow(c, off, len);
        double v = scale == 0 ? m : m / POW10[scale];
        return neg ? -v : v;
    }

    private static double slow(char[] c, int off, int len) { return Double.parseDouble(new String(c, off, len)); }

    public static boolean textIs(JsonParser p, String s) throws IOException {
        if (p.currentToken() != JsonToken.VALUE_STRING) return false;
        int len = p.getTextLength();
        if (len != s.length()) return false;
        char[] c = p.getTextCharacters(); int off = p.getTextOffset();
        for (int i = 0; i < len; i++) if (c[off + i] != s.charAt(i)) return false;
        return true;
    }

    public static boolean equals(char[] c, int len, String s) {
        if (len != s.length()) return false;
        for (int i = 0; i < len; i++) if (c[i] != s.charAt(i)) return false;
        return true;
    }

    /** Уровни: [[p,q,...],...], [{price,size},...] или [{px,sz,...},...]. Текущий токен — START_ARRAY (или null). */
    static void levels(JsonParser p, BookBatch b, boolean bid) throws IOException {
        JsonToken t = p.currentToken();
        if (t == JsonToken.VALUE_NULL) return;
        if (t != JsonToken.START_ARRAY) throw new IllegalArgumentException("ожидался массив уровней, получено " + t);
        while ((t = p.nextToken()) != JsonToken.END_ARRAY) {
            double px = Double.NaN, qty = Double.NaN;
            if (t == JsonToken.START_ARRAY) {
                p.nextToken(); px = num(p);
                p.nextToken(); qty = num(p);
                while (p.nextToken() != JsonToken.END_ARRAY) p.skipChildren();
            } else if (t == JsonToken.START_OBJECT) {
                while (p.nextToken() == JsonToken.FIELD_NAME) {
                    String f = p.currentName();
                    p.nextToken();
                    switch (f) {
                        case "price", "px" -> px = num(p);
                        case "size", "sz" -> qty = num(p);
                        default -> p.skipChildren();
                    }
                }
            } else throw new IllegalArgumentException("неизвестная форма уровня: " + t);
            if (Double.isNaN(px) || Double.isNaN(qty)) throw new IllegalArgumentException("уровень без цены или объёма");
            if (bid) b.bid(px, qty); else b.ask(px, qty);
        }
    }

    /** Строка текущего значения (только для редких сообщений: ошибки, id). */
    public static String text(JsonParser p) throws IOException {
        JsonToken t = p.currentToken();
        if (t == JsonToken.START_OBJECT || t == JsonToken.START_ARRAY) {
            // вложенный объект — вернём его как есть (для сообщений об ошибке)
            StringBuilder sb = new StringBuilder();
            int depth = 0;
            do {
                JsonToken x = p.currentToken();
                if (x == JsonToken.START_OBJECT || x == JsonToken.START_ARRAY) depth++;
                if (x == JsonToken.END_OBJECT || x == JsonToken.END_ARRAY) depth--;
                if (x == JsonToken.FIELD_NAME) sb.append(p.currentName()).append('=');
                else if (x.isScalarValue()) sb.append(p.getText()).append(' ');
                if (depth == 0) break;
            } while (p.nextToken() != null);
            return sb.toString().trim();
        }
        return t == JsonToken.VALUE_NULL ? null : p.getText();
    }
}
