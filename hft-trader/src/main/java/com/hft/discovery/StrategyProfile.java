package com.hft.discovery;

import java.util.List;
import java.util.Map;

/** Правила, по которым стратегия отбирает тикеры. */
public interface StrategyProfile {
    String id();
    String title();
    String description();
    /** true — стратегия реально торгует в боте; false — только сканер (исполнения нет). */
    boolean executable();
    List<String> criteria();

    List<Candidate> screen(Map<String, List<TickerSnapshot>> byExchange, ClosesProvider closes);

    /** Минутные свечи с бюджетом запросов на биржу; null — свечей нет или бюджет исчерпан. */
    interface ClosesProvider {
        double[] closes(TickerSnapshot t, int limit);
    }
}
