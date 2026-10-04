package com.hft.exchange.generic;

import java.net.http.HttpRequest;

/**
 * Всё, что отличает одну биржу от другой при чтении стакана: как строится
 * запрос и как разбирается ответ. Остальное (опрос, лимитер, запись в
 * MarketDataStore) общее и лежит в {@link PollingBookFeed}.
 */
public interface BookDialect {

    /** HTTP-запрос стакана символа на depth уровней. */
    HttpRequest request(String baseUrl, String symbol, int depth);

    /** Разобрать ответ в отсортированный стакан; ошибка биржи — исключение. */
    ParsedBook parse(String body, String symbol) throws Exception;

    /** Стакан, уже отсортированный: bids по убыванию цены, asks по возрастанию. */
    record ParsedBook(double[] bp, double[] bq, double[] ap, double[] aq, long tsMs) {
        /** Хотя бы одна сторона пуста. */
        public boolean isEmpty() { return bp.length == 0 || ap.length == 0; }
    }
}
