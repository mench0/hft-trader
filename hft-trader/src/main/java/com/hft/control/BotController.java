package com.hft.control;

import com.hft.config.AppConfig;
import com.hft.config.ExchangeConfig;
import com.hft.exchange.ExchangeGateway;
import com.hft.exchange.binance.BinanceExchange;
import com.hft.exchange.bybit.BybitExchange;
import com.hft.persistence.PersistedState;
import com.hft.persistence.SqliteStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Управляет тем, какие биржи и какие символы выбраны, и когда бот
 * реально подключается к рынку. Всё, что здесь меняется — выбор бирж,
 * тикеров, параметры риска и стратегии — сразу сохраняется на диск через
 * {@link SqliteStateStore}, поэтому перезапуск процесса не откатывает настройки
 * к тем, что были в application.yml при первом старте.
 *
 * Состояния:
 *   NOT_STARTED — биржи не подключены, можно менять выбор
 *   RUNNING     — биржи подключены и работают
 *   STOPPED     — было запущено, потом остановлено; можно поменять
 *                 выбор и запустить заново
 */
public final class BotController {

    private static final Logger log = LoggerFactory.getLogger(BotController.class);

    private static final List<String> SUPPORTED_EXCHANGES = com.hft.exchange.catalog.ExchangeCatalog.runnableIds();

    private final AppConfig config;
    private final SqliteStateStore stateStore;

    /** Что выбрано в текущей сессии: биржа -> список символов. Меняется до старта. */
    private final Map<String, List<String>> selection = new ConcurrentHashMap<>();

    /** Живые подключения после start(). Пусто, пока бот не запущен. */
    private final Map<String, ExchangeGateway> active = new LinkedHashMap<>();

    /** Последние применённые параметры стратегии на биржу — переживают рестарт
     *  и переприменяются к новой стратегии при следующем start(). */
    private final Map<String, PersistedState.StrategyParams> strategyParams = new ConcurrentHashMap<>();

    private final AtomicBoolean running = new AtomicBoolean(false);
    private volatile boolean autoStart;
    private volatile boolean autoTrade;

    /** Подбор тикеров под стратегии (сводки бирж + бэктест); запускается из Main до старта бирж. */
    private final com.hft.discovery.DiscoveryService discovery;

    public BotController(AppConfig config, SqliteStateStore stateStore) {
        this.config = config;
        this.stateStore = stateStore;
        restoreFromDiskOrDefaults();
        // параметры mean-reversion для бэктеста — те, что заданы для биржи в админке (или по умолчанию)
        this.discovery = com.hft.discovery.DiscoveryService.createDefault(ex -> {
            PersistedState.StrategyParams sp = strategyParams.get(ex);
            return sp == null ? null : new com.hft.discovery.MeanReversionBacktest.Params(
                    sp.entryZ(), sp.exitZ(), sp.stopLossPercent(), 30, 60);
        });
    }

    public com.hft.discovery.DiscoveryService discovery() { return discovery; }

    private void restoreFromDiskOrDefaults() {
        var saved = stateStore.load();
        if (saved.isPresent()) {
            PersistedState s = saved.get();
            s.selection().forEach((ex, syms) -> selection.put(ex, new CopyOnWriteArrayList<>(syms)));
            strategyParams.putAll(s.strategyParams());
            autoStart = s.autoStart();
            autoTrade = s.autoTrade();

            var risk = s.risk();
            config.setMaxPositionQuote(risk.maxPositionQuote());
            config.setMaxDailyLossQuote(risk.maxDailyLossQuote());
            config.setMaxSlippagePercent(risk.maxSlippagePercent());
            config.setFeeReservePercent(risk.feeReservePercent());
            config.setMaxOrdersPerMinute(risk.maxOrdersPerMinute());
            config.setTradingEnabled(risk.tradingEnabled());
            log.info("Настройки восстановлены из {}", stateStore.filePath());
            return;
        }

        // Файла состояния ещё нет (первый запуск) — берём отправную точку
        // из application.yml, если там что-то прописано
        for (ExchangeConfig ec : config.exchanges()) {
            if (ec.enabled()) {
                selection.put(ec.id(), new CopyOnWriteArrayList<>(ec.symbols()));
            }
        }
        persist(); // сразу создаём файл состояния, чтобы он появился на диске
    }

    /** Собрать текущее состояние и записать на диск. Вызывается после любого изменения. */
    private void persist() {
        Map<String, List<String>> selectionCopy = new LinkedHashMap<>();
        selection.forEach((k, v) -> selectionCopy.put(k, List.copyOf(v)));

        var risk = new PersistedState.RiskSnapshot(
                config.maxPositionQuote(), config.maxDailyLossQuote(), config.maxSlippagePercent(),
                config.feeReservePercent(), config.maxOrdersPerMinute(), config.tradingEnabled());

        stateStore.save(new PersistedState(selectionCopy, autoStart, autoTrade, risk, Map.copyOf(strategyParams)));
    }

    /** Вызывается извне (из AdminServer) после смены риск-параметров, чтобы они тоже сохранились. */
    public void persistNow() { persist(); }

    // ======================= ВЫБОР ДО СТАРТА =======================

    public List<String> supportedExchanges() { return SUPPORTED_EXCHANGES; }

    public Map<String, List<String>> selection() {
        Map<String, List<String>> copy = new LinkedHashMap<>();
        selection.forEach((k, v) -> copy.put(k, List.copyOf(v)));
        return copy;
    }

    public void selectExchange(String exchangeId, List<String> symbols) {
        requireNotRunning();
        if (!SUPPORTED_EXCHANGES.contains(exchangeId)) {
            throw new IllegalArgumentException("Неподдерживаемая биржа: " + exchangeId
                    + ". Доступны: " + SUPPORTED_EXCHANGES);
        }
        if (symbols.isEmpty()) {
            throw new IllegalArgumentException("Нужен хотя бы один символ");
        }
        selection.put(exchangeId, new CopyOnWriteArrayList<>(symbols));
        log.info("Биржа {} выбрана с символами {}", exchangeId, symbols);
        persist();
    }

    public void deselectExchange(String exchangeId) {
        requireNotRunning();
        selection.remove(exchangeId);
        strategyParams.remove(exchangeId);
        log.info("Биржа {} убрана из выбора", exchangeId);
        persist();
    }

    public void addSymbol(String exchangeId, String symbol) {
        requireNotRunning();
        List<String> list = selection.computeIfAbsent(exchangeId, k -> new CopyOnWriteArrayList<>());
        String s = symbol.toUpperCase();
        if (!list.contains(s)) list.add(s);
        persist();
    }

    public void removeSymbol(String exchangeId, String symbol) {
        requireNotRunning();
        List<String> list = selection.get(exchangeId);
        if (list != null) list.remove(symbol.toUpperCase());
        persist();
    }

    private void requireNotRunning() {
        if (running.get()) {
            throw new IllegalStateException(
                    "Бот запущен. Сначала остановите (/control/stop), чтобы поменять выбор бирж/символов.");
        }
    }

    // ======================= ПАРАМЕТРЫ СТРАТЕГИИ =======================

    /**
     * Применить параметры стратегии к конкретной бирже и запомнить их —
     * при следующем /control/start (в том числе после рестарта процесса)
     * они применятся к новой стратегии автоматически.
     */
    public void applyStrategyParams(String exchangeId, PersistedState.StrategyParams params) {
        strategyParams.put(exchangeId, params);
        ExchangeGateway gw = active.get(exchangeId);
        if (gw != null) {
            var strat = gw.strategy();
            strat.setEntryZ(params.entryZ());
            strat.setExitZ(params.exitZ());
            strat.setStopLossPercent(params.stopLossPercent());
            strat.setMinImbalance(params.minImbalance());
            strat.setOrderQuote(params.orderQuote());
        }
        persist();
    }

    public PersistedState.StrategyParams strategyParamsFor(String exchangeId) {
        var saved = strategyParams.get(exchangeId);
        if (saved != null) return saved;
        // Если ничего не сохранено — вернуть значения по умолчанию из живой стратегии
        ExchangeGateway gw = active.get(exchangeId);
        if (gw != null) {
            var s = gw.strategy();
            return new PersistedState.StrategyParams(
                    s.entryZ(), s.exitZ(), s.stopLossPercent(), s.minImbalance(), s.orderQuote());
        }
        return new PersistedState.StrategyParams(2.0, 0.3, 0.5, 0.15, 20.0);
    }

    private void applyStoredStrategyParams(String exchangeId, ExchangeGateway gw) {
        var params = strategyParams.get(exchangeId);
        if (params == null) return;
        var strat = gw.strategy();
        strat.setEntryZ(params.entryZ());
        strat.setExitZ(params.exitZ());
        strat.setStopLossPercent(params.stopLossPercent());
        strat.setMinImbalance(params.minImbalance());
        strat.setOrderQuote(params.orderQuote());
    }

    // ======================= АВТОЗАПУСК =======================

    public boolean autoStart() { return autoStart; }
    public boolean autoTrade() { return autoTrade; }

    /** Управляется через POST /control/autostart. Действует при следующем перезапуске процесса. */
    public void setAutoStart(boolean autoStart, boolean autoTrade) {
        this.autoStart = autoStart;
        this.autoTrade = autoTrade;
        persist();
    }

    // ======================= ЗАПУСК/ОСТАНОВКА =======================

    public boolean isRunning() { return running.get(); }

    public Map<String, ExchangeGateway> active() { return Map.copyOf(active); }

    public synchronized void start() throws Exception {
        if (running.get()) {
            throw new IllegalStateException("Бот уже запущен");
        }
        if (selection.isEmpty()) {
            throw new IllegalStateException(
                    "Не выбрано ни одной биржи. Сначала POST /control/select?exchange=binance&symbols=BTCUSDT");
        }

        active.clear();
        for (var entry : selection.entrySet()) {
            String id = entry.getKey();
            List<String> symbols = entry.getValue();
            ExchangeConfig ec = buildExchangeConfig(id, symbols);
            ExchangeGateway gw = createExchange(id, ec);
            active.put(id, gw);
        }

        for (ExchangeGateway gw : active.values()) {
            try {
                gw.start();
                applyStoredStrategyParams(gw.id(), gw);
            } catch (Exception e) {
                log.error("Не удалось запустить биржу {}", gw.id(), e);
                active.values().forEach(g -> { try { g.stop(); } catch (Exception ignored) {} });
                active.clear();
                throw e;
            }
        }

        running.set(true);
        log.info("Бот запущен. Активные биржи: {}", active.keySet());
    }

    public synchronized void stop() {
        if (!running.get()) return;
        for (ExchangeGateway gw : active.values()) {
            try { gw.stop(); } catch (Exception e) { log.error("Ошибка остановки {}", gw.id(), e); }
        }
        active.clear();
        running.set(false);
        log.info("Бот остановлен");
    }

    /** Включить торговлю на всех активных биржах (или на одной, если id != "all"). */
    public void startTrading(String targetId) {
        config.setTradingEnabled(true);
        for (ExchangeGateway gw : resolveTargets(targetId)) {
            gw.risk().resumeTrading();
            gw.strategy().enable();
        }
        persist();
    }

    public void stopTrading(String targetId) {
        for (ExchangeGateway gw : resolveTargets(targetId)) {
            gw.strategy().disable();
            gw.risk().stopTrading("Остановлено через админку");
        }
    }

    public List<ExchangeGateway> resolveTargets(String id) {
        if (id == null || "all".equalsIgnoreCase(id)) return List.copyOf(active.values());
        ExchangeGateway gw = active.get(id);
        if (gw == null) throw new IllegalArgumentException("Биржа не активна: " + id);
        return List.of(gw);
    }

    private ExchangeConfig buildExchangeConfig(String id, List<String> symbols) {
        ExchangeConfig base = config.exchanges().stream()
                .filter(e -> e.id().equals(id))
                .findFirst()
                .orElseGet(() -> config.defaultExchangeConfig(id));
        return new ExchangeConfig(base.id(), true, base.testnet(), base.restUrl(), base.wsUrl(),
                base.recvWindowMs(), symbols, base.bookDepth(), base.priceWindowSize());
    }

    private ExchangeGateway createExchange(String id, ExchangeConfig ec) {
        return switch (id) {
            case "binance" -> new BinanceExchange(ec, config);
            case "bybit" -> new BybitExchange(ec, config);
            case "okx" -> new com.hft.exchange.okx.OkxExchange(ec, config);
            case "mexc" -> new com.hft.exchange.mexc.MexcExchange(ec, config);
            case "gate" -> new com.hft.exchange.gate.GateExchange(ec, config);
            case "lbank" -> new com.hft.exchange.lbank.LbankExchange(ec, config);
            case "hyperliquid" -> new com.hft.exchange.hyperliquid.HyperliquidExchange(ec, config);
            case "uniswapv2" -> new com.hft.exchange.uniswap.UniswapV2Exchange(ec, config);
            case "bingx" -> new com.hft.exchange.bingx.BingxExchange(ec, config);
            default -> {
                var info = com.hft.exchange.catalog.ExchangeCatalog.find(id)
                        .filter(i -> i.adapter() == com.hft.exchange.catalog.ExchangeInfo.Adapter.PAPER_BLIND)
                        .orElseThrow(() -> new IllegalArgumentException("Неизвестная или не реализованная биржа: " + id));
                yield new com.hft.exchange.generic.PaperExchange(info, ec, config);
            }
        };
    }
}
