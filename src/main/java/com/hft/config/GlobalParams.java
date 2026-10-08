package com.hft.config;

import java.util.List;
import java.util.Map;

import static com.hft.config.ParamSpec.flag;
import static com.hft.config.ParamSpec.num;
import static com.hft.config.ParamSpec.text;

/**
 * Настройки процесса целиком (не конкретной биржи): фоновые задачи, подбор тикеров, лимиты запросов.
 * Задаются через {@code GET/POST /settings}, хранятся в SQLite. Описание каждого — в {@link #SPECS}.
 */
public record GlobalParams(
        long statusLogSec,
        long timeSyncMin,
        double rateLimitSafety,
        boolean discoveryEnabled,
        String discoveryExchanges,
        String discoveryQuotes,
        long discoveryRefreshMin,
        double discoveryMinVolume,
        int discoveryBacktestPerExchange,
        int discoveryKlineBudget,
        int discoveryBacktestWindow,
        int discoveryBacktestMaxHoldBars,
        double discoveryMrMaxSpreadPercent,
        double discoveryMrMinRangePercent,
        double discoveryMrMaxRangePercent,
        double discoveryMrMaxTrend,
        double discoveryArbMinVolume,
        double discoveryArbMaxGrossPercent,
        double discoverySpreadMinVolume,
        double discoverySpreadMaxVolume,
        double discoverySpreadMinEdgePercent,
        double discoverySpreadMaxPercent,
        // ---- funding-арбитраж (между биржами, только перпы) ----
        boolean fundingArbEnabled,
        long fundingPollSec,
        double fundingArbMinDiffPercent,
        double fundingArbExitDiffPercent,
        double fundingArbPaybackPeriods,
        double fundingArbOrderQuote,
        int fundingArbMaxPositions,
        double fundingArbMaxBasisPercent,
        double fundingArbMaxHoldHours,
        String fundingArbSymbols,
        // ---- ценовой арбитраж перпов между биржами ----
        boolean perpArbEnabled,
        long perpArbCheckMs,
        double perpArbMinProfitPercent,
        double perpArbExitSpreadPercent,
        double perpArbStopLossPercent,
        double perpArbOrderQuote,
        int perpArbMaxPositions,
        double perpArbMaxHoldMinutes,
        long perpArbMaxBookAgeMs,
        double perpArbDepthUsage,
        String perpArbSymbols,
        // ---- cash-and-carry: спот + шорт перпа ----
        boolean carryEnabled,
        double carryMinRatePercent,
        double carryExitRatePercent,
        double carryPaybackPeriods,
        double carryOrderQuote,
        int carryMaxPositions,
        double carryMaxBasisPercent,
        double carryMaxHoldHours,
        String carrySymbols
) {

    /** Описания всех параметров. */
    public static final Map<String, ParamSpec> SPECS = ParamSpec.index(List.of(
            num("statusLogSec", 60, 5, 86_400, "Как часто писать в лог сводку по биржам, с"),
            num("timeSyncMin", 30, 1, 1440, "Как часто сверять часы с биржей (Binance), мин"),
            num("rateLimitSafety", 0.8, 0.1, 1, "Какую долю официального лимита запросов бирж разрешено тратить").needsRestart(),
            flag("discoveryEnabled", true, "Подбор тикеров под стратегии работает (GET /discovery)"),
            text("discoveryExchanges", "", "(|[a-z0-9]+(,[a-z0-9]+)*)", "Биржи для подбора через запятую; пусто — все с источником сводок"),
            text("discoveryQuotes", "USDT,USDC,USD", "[A-Z0-9]+(,[A-Z0-9]+)*", "Котируемые валюты, которые берутся в подбор"),
            num("discoveryRefreshMin", 15, 1, 1440, "Как часто пересчитывать подбор, мин"),
            num("discoveryMinVolume", 1_000_000, 0, 1e15, "Минимальный оборот за 24 ч для возврата к среднему"),
            num("discoveryBacktestPerExchange", 8, 0, 200, "Сколько кандидатов на бирже прогонять бэктестом"),
            num("discoveryKlineBudget", 12, 0, 500, "Сколько запросов свечей на биржу за один пересчёт"),
            num("discoveryBacktestWindow", 30, 5, 1000, "Бэктест возврата к среднему: окно среднего, минутных свечей"),
            num("discoveryBacktestMaxHoldBars", 60, 1, 10_000, "Бэктест возврата к среднему: закрыть позицию через столько свечей"),
            num("discoveryMrMaxSpreadPercent", 0.1, 0, 100, "Возврат к среднему: спред не шире, %"),
            num("discoveryMrMinRangePercent", 1, 0, 1000, "Возврат к среднему: дневной диапазон не меньше, %"),
            num("discoveryMrMaxRangePercent", 25, 0, 1000, "Возврат к среднему: дневной диапазон не больше, %"),
            num("discoveryMrMaxTrend", 0.7, 0, 1, "Возврат к среднему: |изменение за 24 ч| / диапазон не больше (не тренд)"),
            num("discoveryArbMinVolume", 50_000, 0, 1e15, "Межбиржевой арбитраж: оборот за 24 ч не меньше"),
            num("discoveryArbMaxGrossPercent", 5, 0, 1000, "Межбиржевой арбитраж: расхождение больше — считаем ошибкой данных, %"),
            num("discoverySpreadMinVolume", 50_000, 0, 1e15, "Сбор спреда: оборот за 24 ч не меньше"),
            num("discoverySpreadMaxVolume", 5_000_000, 0, 1e15, "Сбор спреда: оборот за 24 ч не больше (ликвидные пары заняты маркет-мейкерами)"),
            num("discoverySpreadMinEdgePercent", 0.1, 0, 100, "Сбор спреда: спред минус две maker-комиссии не меньше, %"),
            num("discoverySpreadMaxPercent", 3, 0, 100, "Сбор спреда: спред не шире, %"),
            // funding-арбитраж
            flag("fundingArbEnabled", false, "Funding-арбитраж между биржами: шорт перпа там, где ставка funding выше, лонг — где ниже. "
                    + "Нужны минимум две биржи с market=perp и общими символами; торгует только на биржах с включённой торговлей"),
            num("fundingPollSec", 30, 5, 3600, "Как часто обновлять ставки funding по REST, с").needsRestart(),
            num("fundingArbMinDiffPercent", 0.03, 0.001, 10, "Вход: разница ставок (за 8 ч) больше этого, %; окупаемость комиссий проверяется отдельно (fundingArbPaybackPeriods)"),
            num("fundingArbExitDiffPercent", 0.005, -10, 10, "Выход: разница ставок (за 8 ч) упала ниже этого, %"),
            num("fundingArbPaybackPeriods", 6, 0.5, 1000, "Вход, только если разница ставок окупает комиссии полного круга (4 сделки тейкера по takerFeePercent обеих бирж) "
                    + "не больше чем за столько периодов по 8 ч (6 = 2 суток)"),
            num("fundingArbOrderQuote", 50, 0, 1e9, "Размер каждой ноги в котируемой валюте (без плеча)"),
            num("fundingArbMaxPositions", 3, 1, 100, "Сколько пар позиций держать одновременно"),
            num("fundingArbMaxBasisPercent", 0.15, 0, 10, "Вход только если цены на двух биржах отличаются не больше, %"),
            num("fundingArbMaxHoldHours", 72, 1, 24 * 90, "Закрыть пару позиций по таймауту, ч"),
            text("fundingArbSymbols", "", "([A-Z0-9]+(,[A-Z0-9]+)*)?", "Символы для арбитража через запятую; пусто — все общие символы бирж с market=perp"),
            // ценовой арбитраж перпов
            flag("perpArbEnabled", false, "Ценовой арбитраж перпов между биржами: продать там, где перп дороже, купить там, где дешевле, закрыть при схождении цен"),
            num("perpArbCheckMs", 200, 50, 60_000, "Как часто сравнивать стаканы, мс").needsRestart(),
            num("perpArbMinProfitPercent", 0.05, 0, 10, "Вход: ожидаемая прибыль после 4 комиссий тейкера обеих бирж не меньше, %"),
            num("perpArbExitSpreadPercent", 0.0, -10, 10, "Выход: стоимость закрытия пары (аск дорогой − бид дешёвой) упала до этого, %"),
            num("perpArbStopLossPercent", 0.5, 0.01, 50, "Стоп: расхождение цен выросло, и убыток пары по ценам больше этого, %"),
            num("perpArbOrderQuote", 50, 0, 1e9, "Размер каждой ноги в котируемой валюте"),
            num("perpArbMaxPositions", 3, 1, 100, "Сколько пар держать одновременно"),
            num("perpArbMaxHoldMinutes", 60, 1, 10_080, "Закрыть пару по таймауту, мин"),
            num("perpArbMaxBookAgeMs", 1000, 10, 60_000, "Оба стакана не старше, мс"),
            num("perpArbDepthUsage", 0.5, 0.01, 1, "Какую долю объёма лучших уровней можно взять"),
            text("perpArbSymbols", "", "([A-Z0-9]+(,[A-Z0-9]+)*)?", "Монеты или символы через запятую; пусто — все общие"),
            // cash-and-carry
            flag("carryEnabled", false, "Cash-and-carry: лонг спота на бирже с market=spot и шорт перпа той же монеты на бирже с market=perp — "
                    + "получать положительный funding без ценового риска"),
            num("carryMinRatePercent", 0.01, 0.0001, 10, "Вход: ставка перпа за 8 ч не меньше, %"),
            num("carryExitRatePercent", 0.0, -10, 10, "Выход: ставка за 8 ч упала ниже, %"),
            num("carryPaybackPeriods", 9, 0.5, 1000, "Вход, только если ставка окупает комиссии круга (2 сделки спота + 2 перпа) за столько периодов по 8 ч"),
            num("carryOrderQuote", 50, 0, 1e9, "Сумма покупки спота (шорт перпа — на тот же объём), в котируемой валюте"),
            num("carryMaxPositions", 3, 1, 100, "Сколько позиций держать одновременно"),
            num("carryMaxBasisPercent", 0.3, 0, 10, "Вход, только если цены спота и перпа отличаются не больше, %"),
            num("carryMaxHoldHours", 168, 1, 24 * 365, "Закрыть позицию по таймауту, ч"),
            text("carrySymbols", "", "([A-Z0-9]+(,[A-Z0-9]+)*)?", "Монеты или символы через запятую; пусто — все общие для спота и перпов")
    ));

    /** Значения по умолчанию. */
    public static final GlobalParams DEFAULTS = ParamSpec.build(GlobalParams.class, SPECS, Map.of());

    /** Описания для админки. */
    public static Map<String, Object> schema() { return ParamSpec.schema(SPECS); }

    /** Параметр применяется только после перезапуска процесса. */
    public static boolean requiresRestart(String key) {
        ParamSpec s = SPECS.get(key);
        return s != null && s.restart();
    }

    /** Новый набор поверх текущего; ошибка в любом значении — не меняется ничего. */
    public GlobalParams with(Map<String, String> updates) {
        Map<String, String> m = toStringMap();
        for (var e : updates.entrySet()) {
            if (!SPECS.containsKey(e.getKey())) continue;
            String v = e.getValue().trim();
            m.put(e.getKey(), java.util.Set.of("discoveryQuotes", "fundingArbSymbols", "perpArbSymbols", "carrySymbols").contains(e.getKey()) ? v.toUpperCase() : e.getKey().equals("discoveryExchanges") ? v.toLowerCase() : v);
        }
        GlobalParams g = ParamSpec.build(GlobalParams.class, SPECS, m);
        if (g.fundingArbExitDiffPercent >= g.fundingArbMinDiffPercent)
            throw new IllegalArgumentException("нужно fundingArbExitDiffPercent < fundingArbMinDiffPercent");
        if (g.carryExitRatePercent >= g.carryMinRatePercent)
            throw new IllegalArgumentException("нужно carryExitRatePercent < carryMinRatePercent");
        return g;
    }

    /** Все параметры строками. */
    public Map<String, String> toStringMap() { return ParamSpec.toMap(this); }
}
