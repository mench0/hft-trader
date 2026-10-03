package com.hft.discovery;

import java.util.List;
import java.util.Map;

/**
 * Тикер, предложенный стратегией.
 *
 * @param exchange биржа (для арбитража — «купить→продать», например "okx→gate")
 * @param suitable прошёл все условия стратегии (и бэктест, если он есть)
 * @param verdict  «подходит», «под вопросом», «не подходит» — коротко для фронта
 * @param reasons  почему так: по каждому условию
 */
public record Candidate(
        String exchange,
        String symbol,
        boolean suitable,
        String verdict,
        double score,
        List<String> reasons,
        Map<String, Object> metrics,
        Map<String, Object> backtest
) {}
