package com.hft.discovery;

import java.util.List;
import java.util.Map;

/** Правила, по которым стратегия отбирает тикеры. */
public interface StrategyProfile {
    /** Идентификатор профиля. */
    String id();
    /** Название для админки. */
    String title();
    /** Как работает стратегия и что ей нужно. */
    String description();
    /** true — стратегия реально торгует в боте; false — только сканер (исполнения нет). */
    boolean executable();
    /** Условия отбора текстом. */
    List<String> criteria();

    /** Отобрать кандидатов по сводкам бирж (и свечам через closes). */
    List<Candidate> screen(Map<String, List<TickerSnapshot>> byExchange, ClosesProvider closes);

    /** Минутные свечи с бюджетом запросов на биржу; null — свечей нет или бюджет исчерпан. */
    interface ClosesProvider {
        /** Минутные цены закрытия (не больше limit) или null — нет данных или бюджета. */
        double[] closes(TickerSnapshot t, int limit);
    }
}
