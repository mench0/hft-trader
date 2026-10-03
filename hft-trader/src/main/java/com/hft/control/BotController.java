package com.hft.control;

import com.hft.config.AppConfig;
import com.hft.config.ExchangeConfig;
import com.hft.config.TradingParams;
import com.hft.config.TradingSettings;
import com.hft.exchange.ExchangeFactory;
import com.hft.exchange.ExchangeGateway;
import com.hft.exchange.catalog.ExchangeCatalog;
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
 * тикеров, торговые параметры каждой биржи — сразу сохраняется на диск через
 * {@link SqliteStateStore}, поэтому перезапуск процесса настройки не откатывает.
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

    /** Торговые параметры каждой биржи. Объект на биржу живёт всё время процесса: шлюз биржи держит
     *  на него ссылку, поэтому изменение из админки сразу видно работающей стратегии и риску. */
    private final Map<String, TradingSettings> settings = new ConcurrentHashMap<>();

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
            TradingSettings ts = settings.get(ex);
            if (ts == null) return null;
            TradingParams p = ts.get();
            return new com.hft.discovery.MeanReversionBacktest.Params(p.entryZ(), p.exitZ(), p.stopLossPercent(), 30, 60);
        });
    }

    public com.hft.discovery.DiscoveryService discovery() { return discovery; }

    private void restoreFromDiskOrDefaults() {
        var saved = stateStore.load();
        if (saved.isPresent()) {
            PersistedState s = saved.get();
            if (s.selection() != null) s.selection().forEach((ex, syms) -> selection.put(ex, new CopyOnWriteArrayList<>(syms)));
            if (s.trading() != null) s.trading().forEach((ex, values) -> {
                try {
                    settings.put(ex, new TradingSettings(defaultsFor(ex).with(values)));
                } catch (IllegalArgumentException e) {
                    log.error("[{}] сохранённые параметры некорректны ({}) — беру значения по умолчанию", ex, e.getMessage());
                }
            });
            autoStart = s.autoStart();
            autoTrade = s.autoTrade();
            log.info("Настройки восстановлены из {}", stateStore.filePath());
            return;
        }
        persist(); // сразу создаём файл состояния, чтобы он появился на диске
    }

    /** Собрать текущее состояние и записать на диск. Вызывается после любого изменения. */
    private void persist() {
        Map<String, List<String>> selectionCopy = new LinkedHashMap<>();
        selection.forEach((k, v) -> selectionCopy.put(k, List.copyOf(v)));
        Map<String, Map<String, String>> trading = new LinkedHashMap<>();
        settings.forEach((k, v) -> trading.put(k, v.get().toStringMap()));
        stateStore.save(new PersistedState(selectionCopy, autoStart, autoTrade, trading));
    }

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

    // ======================= ТОРГОВЫЕ ПАРАМЕТРЫ =======================

    /** Значения по умолчанию для биржи: комиссия тейкера — из каталога. */
    private static TradingParams defaultsFor(String exchangeId) {
        TradingParams d = TradingParams.DEFAULTS;
        return ExchangeCatalog.find(exchangeId)
                .map(i -> d.with(Map.of("takerFeePercent", String.valueOf(i.takerFeePct()))))
                .orElse(d);
    }

    private TradingSettings settingsFor(String exchangeId) {
        return settings.computeIfAbsent(exchangeId, id -> new TradingSettings(defaultsFor(id)));
    }

    /** Текущие параметры биржи (значения по умолчанию, если их ещё не меняли). */
    public TradingParams params(String exchangeId) {
        TradingSettings ts = settings.get(exchangeId);
        return ts != null ? ts.get() : defaultsFor(exchangeId);
    }

    /**
     * Частичное изменение параметров биржи: переданные ключи меняются, остальные остаются.
     * Работающая биржа подхватывает их на следующем тике; bookDepth/priceWindow — после перезапуска.
     */
    public synchronized TradingParams updateParams(String exchangeId, Map<String, String> updates) {
        if (!SUPPORTED_EXCHANGES.contains(exchangeId)) {
            throw new IllegalArgumentException("Неподдерживаемая биржа: " + exchangeId + ". Доступны: " + SUPPORTED_EXCHANGES);
        }
        TradingSettings ts = settingsFor(exchangeId);
        TradingParams updated = ts.get().with(updates);
        ts.set(updated);
        persist();
        log.info("[{}] торговые параметры изменены: {}", exchangeId, updates);
        return updated;
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
            TradingSettings ts = settingsFor(id);
            ExchangeConfig ec = buildExchangeConfig(id, symbols, ts.get());
            ExchangeGateway gw = ExchangeFactory.create(id, ec, ts);
            active.put(id, gw);
        }

        for (ExchangeGateway gw : active.values()) {
            try {
                gw.start();
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
    public synchronized void startTrading(String targetId) {
        for (ExchangeGateway gw : resolveTargets(targetId)) {
            setTradingEnabled(gw.id(), true);
            gw.risk().resumeTrading();
            gw.strategy().enable();
        }
        persist();
    }

    public synchronized void stopTrading(String targetId, String reason) {
        for (ExchangeGateway gw : resolveTargets(targetId)) {
            setTradingEnabled(gw.id(), false);
            gw.strategy().disable();
            gw.risk().stopTrading(reason);
        }
        persist();
    }

    private void setTradingEnabled(String exchangeId, boolean enabled) {
        TradingSettings ts = settingsFor(exchangeId);
        ts.set(ts.get().with(Map.of("tradingEnabled", String.valueOf(enabled))));
    }

    public List<ExchangeGateway> resolveTargets(String id) {
        if (id == null || "all".equalsIgnoreCase(id)) return List.copyOf(active.values());
        ExchangeGateway gw = active.get(id);
        if (gw == null) throw new IllegalArgumentException("Биржа не активна: " + id);
        return List.of(gw);
    }

    private ExchangeConfig buildExchangeConfig(String id, List<String> symbols, TradingParams p) {
        ExchangeConfig base = config.exchanges().stream()
                .filter(e -> e.id().equals(id))
                .findFirst()
                .orElseGet(() -> config.defaultExchangeConfig(id));
        return new ExchangeConfig(base.id(), base.testnet(), base.restUrl(), base.wsUrl(),
                base.recvWindowMs(), symbols, p.bookDepth(), p.priceWindow());
    }
}
