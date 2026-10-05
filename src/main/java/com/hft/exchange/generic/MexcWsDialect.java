package com.hft.exchange.generic;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * MEXC спот, WebSocket v3 (wss://wbs-api.mexc.com/ws): стакан из канала
 * spot@public.limit.depth.v3.api.pb@SYMBOL@LEVEL (снимки 5/10/20 уровней).
 * Данные приходят бинарными кадрами в protobuf, служебные ответы (подписка, PONG) — текстом JSON.
 *
 * Разбор protobuf — свой, без библиотеки: читаются только нужные поля, остальные пропускаются по типу.
 * Схема (mexcdevelop/websocket-proto):
 * <pre>
 * PushDataV3ApiWrapper { string channel = 1; string symbol = 3; int64 createTime = 5; int64 sendTime = 6;
 *                        PublicLimitDepthsV3Api publicLimitDepths = 303; … }
 * PublicLimitDepthsV3Api { repeated Item asks = 1; repeated Item bids = 2; string eventType = 3; string version = 4; }
 * Item { string price = 1; string quantity = 2; }
 * </pre>
 * Ограничения MEXC: не больше 30 подписок на соединение, соединение живёт до 24 ч (переподключение — в WsBookFeed),
 * без подписки сервер закрывает соединение через 30 с.
 *
 * ВНИМАНИЕ: номера полей взяты из опубликованной схемы без доступа к живому API — проверяйте по
 * /exchanges/request-stats (ws.messages, bookUpdates, parseErrors) на первом запуске.
 */
final class MexcWsDialect implements WsDialect {

    /** Логгер. */
    private static final Logger log = LoggerFactory.getLogger(MexcWsDialect.class);
    /** Подписок на одно соединение у MEXC. */
    static final int MAX_SUBSCRIPTIONS = 30;

    /** Буфер для перевода строк-чисел и символа в char[] без новых строк. */
    private char[] scratch = new char[64];
    /** Метка времени текущего сообщения. */
    private long createTime, sendTime;

    @Override public String defaultUrl(boolean testnet) { return "wss://wbs-api.mexc.com/ws"; }

    @Override public String venueSymbol(String symbol) { return symbol.toUpperCase(Locale.ROOT); }

    @Override public List<String> subscribe(List<String> v, int depth) { return op("SUBSCRIPTION", v, depth); }

    @Override public List<String> unsubscribe(List<String> v, int depth) { return op("UNSUBSCRIPTION", v, depth); }

    /** Один запрос на все символы (до 30). */
    private static List<String> op(String method, List<String> v, int depth) {
        String level = depth <= 5 ? "5" : depth <= 10 ? "10" : "20";
        List<String> use = v;
        if (v.size() > MAX_SUBSCRIPTIONS) {
            log.warn("[mexc] WS: не больше {} подписок на соединение — {} символов сверх лимита без WS-стакана", MAX_SUBSCRIPTIONS, v.size() - MAX_SUBSCRIPTIONS);
            use = v.subList(0, MAX_SUBSCRIPTIONS);
        }
        StringBuilder sb = new StringBuilder(64 + use.size() * 48).append("{\"method\":\"").append(method).append("\",\"params\":[");
        for (int i = 0; i < use.size(); i++) {
            if (i > 0) sb.append(',');
            sb.append("\"spot@public.limit.depth.v3.api.pb@").append(use.get(i)).append('@').append(level).append('"');
        }
        List<String> out = new ArrayList<>(1);
        out.add(sb.append("]}").toString());
        return out;
    }

    @Override public String pingMessage() { return "{\"method\":\"PING\"}"; }

    @Override public long pingIntervalMs() { return 20_000; }

    /** Текстовые кадры — только служебные: подтверждение подписки, PONG, ошибки. */
    @Override public String parse(char[] c, int len, BookBatch out) throws Exception {
        out.reset();
        String s = new String(c, 0, len);
        if (s.contains("Not Subscribed") || s.contains("Blocked")) throw new IllegalStateException("MEXC: подписка отклонена: " + s);
        int code = s.indexOf("\"code\":");
        if (code >= 0) {
            int i = code + 7;
            while (i < s.length() && s.charAt(i) == ' ') i++;
            if (i < s.length() && s.charAt(i) != '0') throw new IllegalStateException("MEXC: " + s);
        }
        return null;
    }

    @Override public boolean parsesBinary() { return true; }

    /** PushDataV3ApiWrapper со снимком стакана -> out. Чужие каналы (сделки и т.п.) пропускаются. */
    @Override public String parseBinary(byte[] b, int len, BookBatch out) {
        out.reset();
        createTime = sendTime = 0;
        int pos = 0, depthOff = -1, depthLen = 0, symOff = -1, symLen = 0;
        while (pos < len) {
            long tag = varint(b, pos, len);
            pos = next;
            int field = (int) (tag >>> 3), wire = (int) (tag & 7);
            if (wire == 2) {
                int n = (int) varint(b, pos, len);
                pos = next;
                if (pos + n > len) throw new IllegalStateException("MEXC: обрезанное сообщение");
                if (field == 303) { depthOff = pos; depthLen = n; }
                else if (field == 3) { symOff = pos; symLen = n; }
                pos += n;
            } else if (wire == 0) {
                long v = varint(b, pos, len);
                pos = next;
                if (field == 5) createTime = v;
                else if (field == 6) sendTime = v;
            } else {
                pos = skip(b, pos, len, wire);
            }
        }
        if (depthOff < 0) return null;                          // не стакан
        if (symOff < 0) throw new IllegalStateException("MEXC: нет symbol в сообщении стакана");
        char[] sc = chars(b, symOff, symLen);
        out.venue = out.resolve(sc, 0, symLen);
        out.snapshot = true;
        out.tsMs = sendTime != 0 ? sendTime : createTime != 0 ? createTime : System.currentTimeMillis();
        int end = depthOff + depthLen;
        pos = depthOff;
        while (pos < end) {
            long tag = varint(b, pos, end);
            pos = next;
            int field = (int) (tag >>> 3), wire = (int) (tag & 7);
            if (wire == 2) {
                int n = (int) varint(b, pos, end);
                pos = next;
                if (field == 1 || field == 2) level(b, pos, pos + n, out, field == 2);
                pos += n;
            } else {
                pos = skip(b, pos, end, wire);
            }
        }
        return null;
    }

    /** Item { price = 1; quantity = 2 } -> уровень стакана. */
    private void level(byte[] b, int pos, int end, BookBatch out, boolean bid) {
        double price = Double.NaN, qty = Double.NaN;
        while (pos < end) {
            long tag = varint(b, pos, end);
            pos = next;
            int field = (int) (tag >>> 3), wire = (int) (tag & 7);
            if (wire == 2) {
                int n = (int) varint(b, pos, end);
                pos = next;
                if (field == 1) price = number(b, pos, n);
                else if (field == 2) qty = number(b, pos, n);
                pos += n;
            } else {
                pos = skip(b, pos, end, wire);
            }
        }
        if (Double.isNaN(price) || Double.isNaN(qty)) throw new IllegalStateException("MEXC: уровень без цены или объёма");
        if (bid) out.bid(price, qty); else out.ask(price, qty);
    }

    // ------------------------------------------------------------ protobuf

    /** Позиция после последнего прочитанного varint. */
    private int next;

    /** varint с позиции pos; позиция после него — в next. */
    private long varint(byte[] b, int pos, int end) {
        long v = 0;
        for (int shift = 0; shift < 64; shift += 7) {
            if (pos >= end) throw new IllegalStateException("MEXC: обрезанный varint");
            byte x = b[pos++];
            v |= (long) (x & 0x7F) << shift;
            if ((x & 0x80) == 0) { next = pos; return v; }
        }
        throw new IllegalStateException("MEXC: слишком длинный varint");
    }

    /** Пропустить поле неинтересного типа. */
    private int skip(byte[] b, int pos, int end, int wire) {
        return switch (wire) {
            case 0 -> { varint(b, pos, end); yield next; }
            case 1 -> pos + 8;
            case 5 -> pos + 4;
            default -> throw new IllegalStateException("MEXC: неподдерживаемый тип поля protobuf " + wire);
        };
    }

    /** ASCII-байты в общий буфер символов. */
    private char[] chars(byte[] b, int off, int n) {
        if (n > scratch.length) scratch = new char[Math.max(n, scratch.length * 2)];
        for (int i = 0; i < n; i++) scratch[i] = (char) (b[off + i] & 0xFF);
        return scratch;
    }

    /** Число из строки-поля без новых строк. */
    private double number(byte[] b, int off, int n) {
        return FastJson.parse(chars(b, off, n), 0, n);
    }
}
