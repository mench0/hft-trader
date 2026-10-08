package com.hft.config;

import java.util.List;
import java.util.Map;

import static com.hft.config.ParamSpec.flag;
import static com.hft.config.ParamSpec.num;
import static com.hft.config.ParamSpec.text;

/**
 * Все настройки одной биржи: подключение, риск, стратегии, работа фидов.
 * <p>
 * Задаются только через админку ({@code POST /exchange/params?exchange=bybit&maxPositionQuote=50}) и только для
 * выбранных бирж; хранятся в SQLite. Описание каждого параметра (по умолчанию, границы, нужен ли перезапуск,
 * справка) — в {@link #SPECS}, его же отдаёт {@code GET /exchange/params/schema}.
 * <p>
 * Неизменяемый: при изменении из админки собирается новый экземпляр и атомарно подменяется в
 * {@link TradingSettings}. Поток стратегии читает одну volatile-ссылку и видит согласованный набор значений.
 */
public record TradingParams(
        // ---- подключение ----
        boolean testnet,
        boolean live,
        String restUrl,
        String wsUrl,
        int recvWindowMs,
        boolean wsTrade,
        double paperStartBalance,
        // ---- рынок ----
        com.hft.exchange.Market market,
        int leverage,
        // ---- работа фидов и клиента ----
        long wsStaleMs,
        long wsReconnectBaseMs,
        long restFallbackGraceMs,
        long pollBackoffMs,
        int feedMaxFailures,
        int wsMaxParseErrors,
        int wsMaxCrossedBooks,
        long balanceSyncMs,
        long marketFillWaitMs,
        double marketPriceBandPercent,
        int orderThreads,
        // ---- риск ----
        boolean tradingEnabled,
        double maxPositionQuote,
        double maxDailyLossQuote,
        double maxSlippagePercent,
        double feeReservePercent,
        double takerFeePercent,
        double tradeCostQuote,
        int maxOrdersPerMinute,
        long maxDataAgeMs,
        // ---- стратегия mean-reversion ----
        boolean meanReversionEnabled,
        double entryZ,
        double exitZ,
        double stopLossPercent,
        double minImbalance,
        int imbalanceLevels,
        double orderQuote,
        long maxBookAgeMs,
        double maxSpreadPercent,
        long positionTimeoutMs,
        // ---- структуры данных ----
        int bookDepth,
        int priceWindow,
        // ---- треугольный арбитраж ----
        boolean triangularEnabled,
        String triHomeAsset,
        double triMinProfitPercent,
        double triOrderQuote,
        double triDepthUsage,
        long triMaxBookAgeMs,
        long triCooldownMs,
        boolean triUnwindOnFail,
        // ---- статистический арбитраж ----
        boolean statArbEnabled,
        String statArbPairs,
        int statArbMaxAutoPairs,
        int statArbWindow,
        long statArbSampleMs,
        double statArbEntryZ,
        double statArbExitZ,
        double statArbStopZ,
        double statArbMinCorrelation,
        double statArbOrderQuote,
        long statArbMaxHoldMs,
        // ---- Uniswap V2 (только для uniswapv2) ----
        String uniRouter,
        String uniTokens,
        String uniPools,
        double uniSlippagePercent
) {

    private static final String URL = "(|https?://\\S+|wss?://\\S+)";
    /** Формат адреса Ethereum (0x + 40 hex) или пусто. */
    private static final String ADDR = "(|0x[0-9a-fA-F]{40})";

    /** Описания всех параметров: по умолчанию, границы, перезапуск, справка. */
    public static final Map<String, ParamSpec> SPECS = ParamSpec.index(List.of(
            // подключение
            flag("testnet", true, "Тестовая сеть биржи (если есть). Безопасное значение по умолчанию; для реальной торговли — false").needsRestart(),
            flag("live", false, "Реальные ордера: нужны ещё API-ключи в окружении (ID_API_KEY/_SECRET). false — бумажная торговля на живых данных").needsRestart(),
            text("restUrl", "", URL, "REST-адрес вместо стандартного (пусто — по каталогу с учётом testnet; для Uniswap — RPC ноды)").needsRestart(),
            text("wsUrl", "", URL, "WebSocket-адрес вместо стандартного (пусто — по каталогу с учётом testnet)").needsRestart(),
            num("recvWindowMs", 5000, 100, 60_000, "Окно годности подписанного запроса, мс").needsRestart(),
            flag("wsTrade", true, "Отправлять ордера по WebSocket, где биржа это умеет (иначе REST)").needsRestart(),
            num("paperStartBalance", 1000, 0, 1e12, "Стартовый бумажный баланс в котируемой валюте каждого символа").needsRestart(),
            // рынок
            text("market", com.hft.exchange.Market.SPOT.id(), "(spot|perp)", "Рынок: perp — бессрочные фьючерсы (USDT-M; лонг и шорт, плечо, funding), spot — спот. "
                    + "Новая биржа с фьючерсами получает perp: Binance, Bybit, OKX, Gate, KuCoin, MEXC, Aster (оба рынка), Hyperliquid (только perp); "
                    + "Uniswap V2 — только spot (у AMM фьючерсов нет)").needsRestart(),
            num("leverage", 2, 1, 50, "Плечо для фьючерсов (выставляется на бирже при старте); на споте не используется").needsRestart(),
            // фиды и клиент
            num("wsStaleMs", 0, 0, 600_000, "Тишина в WebSocket, после которой переподключение, мс (0 — по умолчанию для биржи)").needsRestart(),
            num("wsReconnectBaseMs", 500, 50, 60_000, "Начальная пауза перед переподключением WebSocket, мс (дальше растёт вдвое)").needsRestart(),
            num("restFallbackGraceMs", 2000, 0, 600_000, "Сколько ждать восстановления WebSocket, прежде чем включить REST-опрос стакана, мс").needsRestart(),
            num("pollBackoffMs", 60_000, 1000, 3_600_000, "Пауза REST-опроса после ответа «слишком часто», мс (повторы — вдвое дольше)").needsRestart(),
            num("feedMaxFailures", 15, 1, 10_000, "Неудач подряд (подключений WS или циклов REST-опроса), после которых источник данных сдаётся: заявки снимаются, торговля останавливается").needsRestart(),
            num("wsMaxParseErrors", 5, 1, 10_000, "Ошибок разбора сообщений WS подряд, после которых соединение сбрасывается").needsRestart(),
            num("wsMaxCrossedBooks", 200, 1, 1_000_000, "Перекрещённых стаканов подряд (bid ≥ ask), после которых символ переподписывается").needsRestart(),
            num("balanceSyncMs", 300_000, 5_000, 86_400_000, "Как часто сверять баланс с биржей по REST, мс (если баланс приходит по WebSocket)"),
            num("marketFillWaitMs", 400, 0, 10_000, "Сколько ждать исполнения рыночного/IOC-ордера в WebSocket перед запросом статуса, мс"),
            num("marketPriceBandPercent", 5, 0.1, 50, "Hyperliquid: «рыночный» ордер — это IOC с ценой не дальше этого % от середины"),
            num("orderThreads", 4, 1, 64, "Потоков для отправки ордеров стратегией возврата к среднему").needsRestart(),
            // риск
            flag("tradingEnabled", false, "Торговля на бирже разрешена (также /trading/start и /trading/stop)"),
            num("maxPositionQuote", 100, 0, 1e9, "Максимальный размер ордера в котируемой валюте"),
            num("maxDailyLossQuote", 50, 0, 1e9, "Дневной лимит убытка (реализованный, после комиссий); после него kill switch"),
            num("maxSlippagePercent", 0.3, 0, 100, "Предел ожидаемого проскальзывания рыночного ордера по стакану, %"),
            num("feeReservePercent", 0.2, 0, 50, "Резерв под комиссию при ордере «на весь баланс», %"),
            num("takerFeePercent", 0.1, 0, 5, "Комиссия тейкера для расчёта прибыли, % (по умолчанию — из каталога биржи)"),
            num("tradeCostQuote", 0, 0, 1e6, "Фиксированная стоимость одной сделки в котируемой валюте помимо комиссии (Uniswap — газ свопа): "
                    + "списывается из баланса, вычитается из результата и учитывается в порогах входа стратегий"),
            num("maxOrdersPerMinute", 30, 1, 100_000, "Лимит ордеров в минуту (защита от цикла в стратегии)"),
            num("maxDataAgeMs", 5000, 50, 600_000, "Стакан старше — ордер отклоняется, мс"),
            // mean reversion
            flag("meanReversionEnabled", true, "Стратегия возврата к среднему включена (на фьючерсах — лонг и шорт)"),
            num("entryZ", 2.0, 0.1, 20, "Вход, когда цена ниже (лонг) или, на фьючерсах, выше (шорт) среднего на столько сигм"),
            num("exitZ", 0.3, -20, 20, "Выход, когда отклонение вернулось к этому значению"),
            num("stopLossPercent", 0.5, 0.01, 100, "Стоп-лосс, %"),
            num("minImbalance", 0.15, -1, 1, "Минимальный перевес бидов (для шорта — асков) в стакане для входа"),
            num("imbalanceLevels", 5, 1, 1000, "По скольким уровням считается перевес"),
            num("orderQuote", 20, 0, 1e9, "Размер сделки в котируемой валюте"),
            num("maxBookAgeMs", 2000, 10, 600_000, "Вход только по стакану не старше, мс"),
            num("maxSpreadPercent", 0.1, 0, 100, "Не входить при спреде шире, %"),
            num("positionTimeoutMs", 3_600_000, 1000, 7L * 24 * 3_600_000, "Закрыть позицию по таймауту, мс"),
            num("bookDepth", 20, 1, 1000, "Глубина стакана в памяти, уровней").needsRestart(),
            num("priceWindow", 1000, 4, 1_000_000, "Окно цен для среднего и сигмы, тиков").needsRestart(),
            // треугольный арбитраж
            flag("triangularEnabled", false, "Треугольный арбитраж включён (только market=spot: обмен через три валюты на фьючерсах невозможен)"),
            text("triHomeAsset", "USDT", "[A-Z0-9]{2,10}", "Валюта, с которой начинается и где заканчивается круг").needsRestart(),
            num("triMinProfitPercent", 0.15, 0, 10, "Минимальная чистая прибыль круга после трёх комиссий, %"),
            num("triOrderQuote", 20, 0, 1e9, "Размер круга в triHomeAsset"),
            num("triDepthUsage", 0.5, 0.01, 1, "Какую долю объёма лучших уровней можно взять кругом"),
            num("triMaxBookAgeMs", 1000, 10, 60_000, "Все три стакана не старше, мс"),
            num("triCooldownMs", 3000, 0, 3_600_000, "Пауза перед повтором того же круга, мс"),
            flag("triUnwindOnFail", true, "Нога не исполнилась — продать остаток обратно в triHomeAsset"),
            // статистический арбитраж
            flag("statArbEnabled", false, "Статистический арбитраж (пары) включён; на фьючерсах — лонг дешёвой ноги и шорт дорогой"),
            text("statArbPairs", "", "([A-Z0-9]+/[A-Z0-9]+(,[A-Z0-9]+/[A-Z0-9]+)*)?", "Пары A/B через запятую; пусто — все пары выбранных символов с одной котируемой валютой").needsRestart(),
            num("statArbMaxAutoPairs", 15, 1, 500, "Сколько пар брать автоматически, если statArbPairs пуст").needsRestart(),
            num("statArbWindow", 300, 30, 100_000, "Окно в отсчётах").needsRestart(),
            num("statArbSampleMs", 1000, 100, 3_600_000, "Шаг отсчётов по времени, мс"),
            num("statArbEntryZ", 2.0, 0.5, 20, "Вход по |z| спреда"),
            num("statArbExitZ", 0.5, -5, 20, "Выход, когда |z| вернулся к этому значению"),
            num("statArbStopZ", 4.0, 1, 50, "Стоп: спред разошёлся до этого |z|"),
            num("statArbMinCorrelation", 0.6, -1, 1, "Минимальная корреляция доходностей пары"),
            num("statArbOrderQuote", 20, 0, 1e9, "Размер позиции в котируемой валюте"),
            num("statArbMaxHoldMs", 3_600_000, 1000, 30L * 24 * 3_600_000, "Закрыть позицию по таймауту, мс"),
            // Uniswap
            text("uniRouter", "", ADDR, "Uniswap V2: адрес Router02").needsRestart(),
            text("uniTokens", "", "(|[A-Za-z0-9]+=0x[0-9a-fA-F]{40}:\\d+(;[A-Za-z0-9]+=0x[0-9a-fA-F]{40}:\\d+)*)", "Uniswap V2: токены SYM=0xадрес:десятичные;… ").needsRestart(),
            text("uniPools", "", "(|[A-Za-z0-9]+=0x[0-9a-fA-F]{40}:(true|false):\\d+:\\d+(;[A-Za-z0-9]+=0x[0-9a-fA-F]{40}:(true|false):\\d+:\\d+)*)",
                    "Uniswap V2: пулы ТИКЕР=0xадрес_пары:база_это_token0:десятичные_базы:десятичные_котир;… (например WETHUSDC=0x…:true:18:6)").needsRestart(),
            num("uniSlippagePercent", 0.5, 0, 50, "Uniswap V2: допустимое проскальзывание свопа, %")
    ));

    /** Значения по умолчанию. */
    public static final TradingParams DEFAULTS = ParamSpec.build(TradingParams.class, SPECS, Map.of());

    /** Торговля бессрочными фьючерсами (иначе спот). Не параметр — в JSON админки не выводится. */
    @com.fasterxml.jackson.annotation.JsonIgnore
    public boolean isPerp() { return market == com.hft.exchange.Market.PERP; }

    /** Параметры, которые уходят в верхний регистр (тикеры, валюты). */
    private static final java.util.Set<String> UPPER = java.util.Set.of("triHomeAsset", "statArbPairs");

    /** Параметр применяется только после перезапуска биржи. */
    public static boolean requiresRestart(String key) {
        ParamSpec s = SPECS.get(key);
        return s != null && s.restart();
    }

    /** Описания для админки. */
    public static Map<String, Object> schema() { return ParamSpec.schema(SPECS); }

    /**
     * Новый набор: текущие значения, поверх которых применены переданные.
     * Значение вне допустимого диапазона — IllegalArgumentException, и тогда не применяется ничего.
     * Чужие ключи (exchange и т.п.) игнорируются.
     */
    public TradingParams with(Map<String, String> updates) {
        Map<String, String> m = toStringMap();
        for (var e : updates.entrySet()) {
            if (!SPECS.containsKey(e.getKey())) continue;
            String v = e.getValue().trim();
            m.put(e.getKey(), UPPER.contains(e.getKey()) ? v.toUpperCase() : v);
        }
        TradingParams p = ParamSpec.build(TradingParams.class, SPECS, m);
        if (p.statArbExitZ >= p.statArbEntryZ || p.statArbStopZ <= p.statArbEntryZ)
            throw new IllegalArgumentException("нужно statArbExitZ < statArbEntryZ < statArbStopZ");
        if (p.imbalanceLevels > p.bookDepth)
            throw new IllegalArgumentException("imbalanceLevels (" + p.imbalanceLevels + ") больше bookDepth (" + p.bookDepth + ")");
        return p;
    }

    /** Все параметры строками — для сохранения в SQLite и для ответа админки. */
    public Map<String, String> toStringMap() { return ParamSpec.toMap(this); }
}
