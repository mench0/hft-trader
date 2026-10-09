package com.hft.exchange.mexc;

import java.nio.charset.StandardCharsets;

/**
 * Минимальный читатель protobuf без библиотеки и без схемы: проходит по полям сообщения,
 * отдавая номер поля, тип и значение. Нужен для приватного потока MEXC, где сообщений мало
 * и простота важнее скорости (горячий путь стакана разбирается в MexcWsDialect без аллокаций).
 *
 * <pre>
 * MexcProtobuf.Reader r = new MexcProtobuf.Reader(bytes, 0, bytes.length);
 * while (r.next()) switch (r.field()) { case 3 -> sym = r.string(); case 304 -> body = r.message(); default -> r.skip(); }
 * </pre>
 */
public final class MexcProtobuf {

    private MexcProtobuf() {}

    /** Последовательный читатель полей одного сообщения. */
    public static final class Reader {
        /** Байты и границы сообщения. */
        private final byte[] b;
        private final int end;
        /** Текущая позиция. */
        private int pos;
        /** Номер и тип (wire type) текущего поля. */
        private int field, wire;

        /**
         * @param b байты
         * @param off начало сообщения
         * @param len длина сообщения
         */
        public Reader(byte[] b, int off, int len) {
            this.b = b;
            this.pos = off;
            this.end = off + len;
        }

        /** Перейти к следующему полю; false — сообщение кончилось. */
        public boolean next() {
            if (pos >= end) return false;
            long tag = varint();
            field = (int) (tag >>> 3);
            wire = (int) (tag & 7);
            return true;
        }

        /** Номер текущего поля. */
        public int field() { return field; }

        /** Целое (varint): int32/int64/bool/enum. */
        public long varintValue() {
            if (wire != 0) throw new IllegalStateException("protobuf: поле " + field + " не varint");
            return varint();
        }

        /** Строка UTF-8. */
        public String string() {
            int n = length();
            String s = new String(b, pos, n, StandardCharsets.UTF_8);
            pos += n;
            return s;
        }

        /** Вложенное сообщение. */
        public Reader message() {
            int n = length();
            Reader r = new Reader(b, pos, n);
            pos += n;
            return r;
        }

        /** Пропустить значение текущего поля. */
        public void skip() {
            switch (wire) {
                case 0 -> varint();
                case 1 -> pos += 8;
                case 2 -> { int n = length(); pos += n; }   // не «pos += length()»: length() сам сдвигает pos
                case 5 -> pos += 4;
                default -> throw new IllegalStateException("protobuf: неподдерживаемый тип поля " + wire);
            }
            if (pos > end) throw new IllegalStateException("protobuf: обрезанное сообщение");
        }

        /** Длина значения length-delimited поля. */
        private int length() {
            if (wire != 2) throw new IllegalStateException("protobuf: поле " + field + " не length-delimited");
            int n = (int) varint();
            if (n < 0 || pos + n > end) throw new IllegalStateException("protobuf: обрезанное сообщение");
            return n;
        }

        /** varint с текущей позиции. */
        private long varint() {
            long v = 0;
            for (int shift = 0; shift < 64; shift += 7) {
                if (pos >= end) throw new IllegalStateException("protobuf: обрезанный varint");
                byte x = b[pos++];
                v |= (long) (x & 0x7F) << shift;
                if ((x & 0x80) == 0) return v;
            }
            throw new IllegalStateException("protobuf: слишком длинный varint");
        }
    }
}
