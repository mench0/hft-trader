package com.hft.exchange.generic;

import java.net.http.HttpRequest;

/**
 * Всё, что отличает одну биржу от другой при чтении стакана: как строится
 * запрос и как разбирается ответ. Остальное (опрос, лимитер, запись в
 * MarketDataStore) общее и лежит в {@link PollingBookFeed}.
 */
public interface BookDialect {

    HttpRequest request(String baseUrl, String symbol, int depth);

    ParsedBook parse(String body, String symbol) throws Exception;

    /** Стакан, уже отсортированный: bids по убыванию цены, asks по возрастанию. */
    record ParsedBook(double[] bp, double[] bq, double[] ap, double[] aq, long tsMs) {
        public boolean isEmpty() { return bp.length == 0 || ap.length == 0; }
    }
}
