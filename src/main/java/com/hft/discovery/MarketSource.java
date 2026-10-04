package com.hft.discovery;

import java.util.List;

/** Публичные рыночные данные одной биржи для подбора тикеров. Ключи не нужны. */
public interface MarketSource {
    /** Идентификатор биржи. */
    String exchange();

    /** Сводка 24ч по всем тикерам (одним запросом). */
    List<TickerSnapshot> tickers() throws Exception;

    /** Цены закрытия минутных свечей по возрастанию времени (до limit штук). Пусто — биржа свечей не даёт. */
    double[] closes1m(TickerSnapshot t, int limit) throws Exception;
}
