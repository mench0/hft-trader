package com.hft.control;

import com.hft.config.AppConfig;
import com.hft.config.ExchangeConfig;
import com.hft.config.GlobalParams;
import com.hft.config.TradingParams;
import com.hft.config.TradingSettings;
import com.hft.discovery.DiscoveryService;
import com.hft.discovery.MeanReversionBacktest;
import com.hft.exchange.ExchangeFactory;
import com.hft.exchange.ExchangeGateway;
import com.hft.exchange.catalog.ExchangeCatalog;
import com.hft.exchange.catalog.ExchangeInfo;
import com.hft.persistence.PersistedState;
import com.hft.persistence.SqliteStateStore;
import com.hft.rest.RateBudget;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Управляет тем, какие биржи и какие символы выбраны, их параметрами и тем, когда бот
 * реально подключается к рынку. Всё, что здесь меняется, сразу сохраняется на диск через
 * {@link SqliteStateStore}, поэтому перезапуск процесса настройки не откатывает.
 *
 * Порядок работы: выбрать биржи (сразу можно передать их параметры) → при необходимости поменять
 * параметры → /control/start. Параметры хранятся только для выбранных бирж: при снятии биржи
 * с выбора они удаляются и в память бота не попадают.
 *
 * Состояния:
 *   NOT_STARTED — биржи не подключены, можно менять выбор
 *   RUNNING     — биржи подключены и работают
 *   STOPPED     — было запущено, потом остановлено; можно поменять выбор и запустить заново
 */
public final class BotController {

    /** Логгер. */
    private static final Logger log = LoggerFactory.getLogger(BotController.class);

    /** Биржи с адаптером, которые можно выбрать. */
    private static final List<String> SUPPORTED_EXCHANGES = ExchangeCatalog.runnableIds();

    /** Хранилище состояния. */
    private final SqliteStateStore stateStore;

    /** Выбранные биржи -> символы. Меняется до старта. */
    private final Map<String, List<String>> selection = new ConcurrentHashMap<>();

    /** Живые подключения после start(). Пусто, пока бот не запущен. */
    private final Map<String, ExchangeGateway> active = new LinkedHashMap<>();

    /** Параметры выбранных бирж. Объект на биржу живёт, пока биржа выбрана: шлюз держит на него ссылку,
     *  поэтому изменение из админки сразу видно работающей стратегии и риску. */
    private final Map<String, TradingSettings> settings = new ConcurrentHashMap<>();

    /** Настройки процесса (фоновые задачи, подбор тикеров, лимиты). */
    private volatile GlobalParams global = GlobalParams.DEFAULTS;

    /** Биржи подключены. */
    private final AtomicBoolean running = new AtomicBoolean(false);
    /** Поднимать биржи при старте процесса. */
    private volatile boolean autoStart;
    /** Вместе с автозапуском включать торговлю. */
    private volatile boolean autoTrade;

    /** Подбор тикеров под стратегии; null — выключен в настройках. */
    private volatile DiscoveryService discovery;

    /**
     * @param config     настройки админки (сам контроллер их не использует, оставлены для единообразия создания)
     * @param stateStore где хранится состояние между перезапусками
     */
    public BotController(AppConfig config, SqliteStateStore stateStore) {
        this.stateStore = stateStore;
        restoreFromDiskOrDefaults();
        RateBudget.setSafety(global.rateLimitSafety());     // до создания первого бюджета
        this.discovery = createDiscovery(global);
    }

    // ======================= СОСТОЯНИЕ НА ДИСКЕ =======================

    /** Прочитать выбор, параметры выбранных бирж и настройки процесса; нет файла — создать. */
    private void restoreFromDiskOrDefaults() {
        var saved = stateStore.load();
        if (saved.isEmpty()) {
            persist();                                       // сразу создаём файл состояния
            return;
        }
        PersistedState s = saved.get();
        if (s.selection() != null) s.selection().forEach((ex, syms) -> {
            if (SUPPORTED_EXCHANGES.contains(ex)) selection.put(ex, new CopyOnWriteArrayList<>(syms));
        });
        for (String ex : selection.keySet()) {               // параметры — только выбранных бирж
            Map<String, String> values = s.trading() == null ? null : s.trading().get(ex);
            TradingParams p = defaultsFor(ex);
            if (values != null) {
                try { p = p.with(values); }
                catch (IllegalArgumentException e) { log.error("[{}] сохранённые параметры некорректны ({}) — беру значения по умолчанию", ex, e.getMessage()); }
            }
            settings.put(ex, new TradingSettings(applyEnvMode(ex, p)));
        }
        if (s.global() != null) {
            try { global = GlobalParams.DEFAULTS.with(s.global()); }
            catch (IllegalArgumentException e) { log.error("сохранённые настройки процесса некорректны ({}) — беру значения по умолчанию", e.getMessage()); }
        }
        autoStart = s.autoStart();
        autoTrade = s.autoTrade();
        log.info("Настройки восстановлены из {}: биржи {}", stateStore.filePath(), selection.keySet());
    }

    /** Собрать текущее состояние и записать на диск. Вызывается после любого изменения. */
    private void persist() {
        Map<String, List<String>> selectionCopy = new LinkedHashMap<>();
        selection.forEach((k, v) -> selectionCopy.put(k, List.copyOf(v)));
        Map<String, Map<String, String>> trading = new LinkedHashMap<>();
        settings.forEach((k, v) -> trading.put(k, v.get().toStringMap()));
        stateStore.save(new PersistedState(selectionCopy, autoStart, autoTrade, trading, global.toStringMap()));
    }

    // ======================= ВЫБОР ДО СТАРТА =======================

    /** Биржи, которые можно выбрать (есть адаптер). */
    public List<String> supportedExchanges() { return SUPPORTED_EXCHANGES; }

    /** Копия выбора: биржа -> символы. */
    public Map<String, List<String>> selection() {
        Map<String, List<String>> copy = new LinkedHashMap<>();
        selection.forEach((k, v) -> copy.put(k, List.copyOf(v)));
        return copy;
    }

    /**
     * Выбрать биржу с символами и (необязательно) её параметрами. Повторный выбор меняет символы
     * и применяет переданные параметры поверх уже заданных. Ошибка в параметрах — не меняется ничего.
     */
    public synchronized void selectExchange(String exchangeId, List<String> symbols, Map<String, String> params) {
        requireNotRunning();
        requireSupported(exchangeId);
        if (symbols.isEmpty()) throw new IllegalArgumentException("Нужен хотя бы один символ");
        TradingSettings current = settings.get(exchangeId);
        TradingParams p = (current != null ? current.get() : defaultsFor(exchangeId)).with(params);
        validate(exchangeId, p);
        if (current != null) current.set(p); else settings.put(exchangeId, new TradingSettings(p));
        selection.put(exchangeId, new CopyOnWriteArrayList<>(symbols));
        log.info("Биржа {} выбрана с символами {} (testnet={}, live={})", exchangeId, symbols, p.testnet(), p.live());
        persist();
    }

    /** Выбрать биржу без параметров (значения по умолчанию или уже заданные). */
    public void selectExchange(String exchangeId, List<String> symbols) { selectExchange(exchangeId, symbols, Map.of()); }

    /** Убрать биржу из выбора вместе с её параметрами. */
    public synchronized void deselectExchange(String exchangeId) {
        requireNotRunning();
        selection.remove(exchangeId);
        settings.remove(exchangeId);
        log.info("Биржа {} убрана из выбора, её параметры удалены", exchangeId);
        persist();
    }

    /** Добавить символ к выбранной бирже. */
    public synchronized void addSymbol(String exchangeId, String symbol) {
        requireNotRunning();
        List<String> list = requireSelected(exchangeId);
        String s = symbol.toUpperCase();
        if (!list.contains(s)) list.add(s);
        persist();
    }

    /** Убрать символ у выбранной биржи. */
    public synchronized void removeSymbol(String exchangeId, String symbol) {
        requireNotRunning();
        requireSelected(exchangeId).remove(symbol.toUpperCase());
        persist();
    }

    /** Менять выбор можно только при остановленном боте. */
    private void requireNotRunning() {
        if (running.get()) {
            throw new IllegalStateException("Бот запущен. Сначала остановите (/control/stop), чтобы поменять выбор бирж/символов.");
        }
    }

    /** Биржа есть в списке поддерживаемых, иначе IllegalArgumentException. */
    private static void requireSupported(String exchangeId) {
        if (!SUPPORTED_EXCHANGES.contains(exchangeId))
            throw new IllegalArgumentException("Неподдерживаемая биржа: " + exchangeId + ". Доступны: " + SUPPORTED_EXCHANGES);
    }

    /** Символы выбранной биржи, иначе IllegalArgumentException с подсказкой. */
    private List<String> requireSelected(String exchangeId) {
        List<String> list = selection.get(exchangeId);
        if (list == null) throw new IllegalArgumentException("Биржа не выбрана: " + exchangeId + ". Сначала POST /control/select?exchange=" + exchangeId + "&symbols=…");
        return list;
    }

    // ======================= ПАРАМЕТРЫ БИРЖ =======================

    /**
     * Значения по умолчанию для биржи: комиссия тейкера — из каталога; testnet — если он у биржи есть;
     * testnet/live из окружения (&lt;ID&gt;_TESTNET, &lt;ID&gt;_LIVE), если заданы.
     */
    public static TradingParams defaultsFor(String exchangeId) {
        TradingParams d = TradingParams.DEFAULTS;
        TradingParams p = ExchangeCatalog.find(exchangeId)
                .map(i -> {
                    boolean perp = ExchangeCatalog.supportsPerp(i.id());
                    double taker = perp ? ExchangeCatalog.perp(i.id()).orElseThrow().takerFeePct() : i.takerFeePct();
                    return d.with(Map.of("takerFeePercent", String.valueOf(taker),
                            "testnet", String.valueOf(i.hasTestnet()),
                            "market", perp ? "perp" : "spot"));
                })
                .orElse(d);
        Map<String, String> mode = envMode(exchangeId);
        if (mode.isEmpty()) return p;
        try { TradingParams withMode = p.with(mode); validate(exchangeId, withMode); return withMode; }
        catch (IllegalArgumentException e) { return p; }      // причину пишет applyEnvMode при старте
    }

    /**
     * Режим биржи из окружения (.env или export): &lt;ID&gt;_TESTNET и &lt;ID&gt;_LIVE = true/false.
     * Незаданные или некорректные значения пропускаются.
     */
    static Map<String, String> envMode(String exchangeId) {
        Map<String, String> out = new java.util.LinkedHashMap<>();
        String prefix = exchangeId.toUpperCase(java.util.Locale.ROOT);
        for (String k : new String[]{"testnet", "live"}) {
            String v = com.hft.config.Env.get(prefix + "_" + k.toUpperCase(java.util.Locale.ROOT));
            if (v == null) continue;
            if (v.equalsIgnoreCase("true") || v.equalsIgnoreCase("false")) out.put(k, v.toLowerCase(java.util.Locale.ROOT));
            else log.warn("[{}] {}_{}={} — ожидается true или false, пропускаю", exchangeId, prefix, k.toUpperCase(java.util.Locale.ROOT), v);
        }
        return out;
    }

    /** При старте процесса режим из окружения важнее сохранённого в базе; невозможный (testnet без тестовой сети) — пропускается. */
    private static TradingParams applyEnvMode(String exchangeId, TradingParams p) {
        Map<String, String> mode = envMode(exchangeId);
        if (mode.isEmpty()) return p;
        try {
            TradingParams q = p.with(mode);
            validate(exchangeId, q);
            if (q.testnet() != p.testnet() || q.live() != p.live())
                log.info("[{}] режим из окружения: testnet={} live={} (было testnet={} live={})", exchangeId, q.testnet(), q.live(), p.testnet(), p.live());
            return q;
        } catch (IllegalArgumentException e) {
            log.warn("[{}] режим из окружения {} не применён: {}", exchangeId, mode, e.getMessage());
            return p;
        }
    }

    /**
     * testnet=true возможен, только если у биржи есть тестовая сеть или задан свой restUrl;
     * market=perp — только у бирж с фьючерсами, market=spot — только у бирж со спотом.
     */
    private static void validate(String exchangeId, TradingParams p) {
        ExchangeInfo info = ExchangeCatalog.find(exchangeId).orElseThrow();
        if (p.isPerp() && !ExchangeCatalog.supportsPerp(exchangeId))
            throw new IllegalArgumentException("У биржи " + exchangeId + " в боте нет фьючерсов: задайте market=spot");
        if (!p.isPerp() && !ExchangeCatalog.supportsSpot(exchangeId))
            throw new IllegalArgumentException("Биржа " + exchangeId + " в боте торгует только фьючерсами: задайте market=perp");
        if (p.testnet() && !info.hasTestnet() && p.restUrl().isBlank())
            throw new IllegalArgumentException("У биржи " + exchangeId + " нет тестовой сети: задайте testnet=false"
                    + (exchangeId.equals("uniswapv2") ? " или restUrl тестовой сети (RPC Sepolia и т.п.)" : ""));
    }

    /** Выбранные биржи (у них и только у них есть параметры). */
    public Set<String> configuredExchanges() { return Set.copyOf(settings.keySet()); }

    /** Параметры выбранной биржи; для невыбранной — значения по умолчанию (в память бота не сохраняются). */
    public TradingParams params(String exchangeId) {
        TradingSettings ts = settings.get(exchangeId);
        return ts != null ? ts.get() : defaultsFor(exchangeId);
    }

    /** Биржа выбрана. */
    public boolean isSelected(String exchangeId) { return selection.containsKey(exchangeId); }

    /**
     * Частичное изменение параметров выбранной биржи: переданные ключи меняются, остальные остаются.
     * Работающая биржа подхватывает их на следующем тике; помеченные «restart» — после перезапуска.
     */
    public synchronized TradingParams updateParams(String exchangeId, Map<String, String> updates) {
        requireSupported(exchangeId);
        requireSelected(exchangeId);
        TradingSettings ts = settings.get(exchangeId);
        TradingParams updated = ts.get().with(updates);
        validate(exchangeId, updated);
        ts.set(updated);
        persist();
        log.info("[{}] параметры изменены: {}", exchangeId, updates);
        return updated;
    }

    // ======================= НАСТРОЙКИ ПРОЦЕССА =======================

    /** Текущие настройки процесса. */
    public GlobalParams global() { return global; }

    /** Частичное изменение настроек процесса; подбор тикеров пересоздаётся с новыми значениями. */
    public synchronized GlobalParams updateGlobal(Map<String, String> updates) {
        GlobalParams updated = global.with(updates);
        boolean discoveryChanged = updates.keySet().stream().anyMatch(k -> k.startsWith("discovery"));
        global = updated;
        persist();
        if (discoveryChanged) {
            DiscoveryService old = discovery;
            if (old != null) old.stop();
            discovery = createDiscovery(updated);
            if (discovery != null) discovery.start();
        }
        log.info("Настройки процесса изменены: {}", updates);
        return updated;
    }

    /** Подбор тикеров; null — выключен (discoveryEnabled=false). */
    public DiscoveryService discovery() { return discovery; }

    /** Подбор тикеров по настройкам процесса; null — выключен. */
    private DiscoveryService createDiscovery(GlobalParams g) {
        if (!g.discoveryEnabled()) return null;
        // параметры mean-reversion для бэктеста — те, что заданы для выбранной биржи (или по умолчанию)
        return DiscoveryService.createDefault(ex -> {
            TradingSettings ts = settings.get(ex);
            if (ts == null) return null;
            TradingParams p = ts.get();
            return new MeanReversionBacktest.Params(p.entryZ(), p.exitZ(), p.stopLossPercent(), g.discoveryBacktestWindow(), g.discoveryBacktestMaxHoldBars());
        }, g);
    }

    // ======================= АВТОЗАПУСК =======================

    /** Поднимать биржи сами при старте процесса. */
    public boolean autoStart() { return autoStart; }

    /** Вместе с автозапуском сразу включать торговлю. */
    public boolean autoTrade() { return autoTrade; }

    /** Управляется через POST /control/autostart. Действует при следующем перезапуске процесса. */
    public void setAutoStart(boolean autoStart, boolean autoTrade) {
        this.autoStart = autoStart;
        this.autoTrade = autoTrade;
        persist();
    }

    // ======================= ЗАПУСК/ОСТАНОВКА =======================

    /** Биржи подключены. */
    public boolean isRunning() { return running.get(); }

    /** Работающие шлюзы бирж. */
    public Map<String, ExchangeGateway> active() { return Map.copyOf(active); }

    /** Funding-арбитраж между биржами (работает, пока запущен бот). */
    private volatile com.hft.perp.FundingArbitrage fundingArb;

    /** Funding-арбитраж; null — бот не запущен. */
    public com.hft.perp.FundingArbitrage fundingArb() { return fundingArb; }

    /** Подключить все выбранные биржи с их текущими параметрами. */
    public synchronized void start() throws Exception {
        if (running.get()) throw new IllegalStateException("Бот уже запущен");
        if (selection.isEmpty()) {
            throw new IllegalStateException("Не выбрано ни одной биржи. Сначала POST /control/select?exchange=binance&symbols=BTCUSDT");
        }
        active.clear();
        com.hft.perp.PerpAccount.setPollSec(global.fundingPollSec());
        for (var entry : selection.entrySet()) {
            String id = entry.getKey();
            TradingSettings ts = settings.get(id);
            ExchangeConfig ec = buildExchangeConfig(id, entry.getValue(), ts.get());
            active.put(id, ExchangeFactory.create(id, ec, ts));
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
        fundingArb = new com.hft.perp.FundingArbitrage(() -> global, () -> List.copyOf(active.values()),
                id -> { TradingSettings ts = settings.get(id); return ts != null && ts.get().tradingEnabled(); });
        fundingArb.start();
        log.info("Бот запущен. Активные биржи: {}", active.keySet());
    }

    /** Отключить все биржи; выбор и параметры сохраняются. */
    public synchronized void stop() {
        if (!running.get()) return;
        com.hft.perp.FundingArbitrage fa = fundingArb;
        fundingArb = null;
        if (fa != null) fa.stop();                           // пары закрываются, пока биржи ещё подключены
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

    /** Выключить торговлю (стратегии и риск) на всех активных биржах или на одной. */
    public synchronized void stopTrading(String targetId, String reason) {
        for (ExchangeGateway gw : resolveTargets(targetId)) {
            setTradingEnabled(gw.id(), false);
            gw.strategy().disable();
            gw.risk().stopTrading(reason);
        }
        persist();
    }

    /** Записать tradingEnabled в параметры биржи (без сохранения на диск — его делает вызывающий). */
    private void setTradingEnabled(String exchangeId, boolean enabled) {
        TradingSettings ts = settings.get(exchangeId);
        if (ts != null) ts.set(ts.get().with(Map.of("tradingEnabled", String.valueOf(enabled))));
    }

    /** Активные шлюзы: все ("all"/null) или один по id. */
    public List<ExchangeGateway> resolveTargets(String id) {
        if (id == null || "all".equalsIgnoreCase(id)) return List.copyOf(active.values());
        ExchangeGateway gw = active.get(id);
        if (gw == null) throw new IllegalArgumentException("Биржа не активна: " + id);
        return List.of(gw);
    }

    /** Подключение биржи: адреса из параметров или каталога (с учётом testnet), символы из выбора. */
    static ExchangeConfig buildExchangeConfig(String id, List<String> symbols, TradingParams p) {
        ExchangeInfo info = ExchangeCatalog.find(id).orElseThrow();
        var perp = p.isPerp() ? ExchangeCatalog.perp(id) : java.util.Optional.<ExchangeCatalog.PerpVenue>empty();
        String rest = !p.restUrl().isBlank() ? p.restUrl() : perp.map(v -> v.restUrl(p.testnet())).orElse(info.restUrl(p.testnet()));
        String catalogWs = perp.map(v -> v.wsUrl(p.testnet())).orElse(info.wsUrl(p.testnet()));
        String ws = p.wsUrl().isBlank() ? (catalogWs == null ? "" : catalogWs) : p.wsUrl();
        return new ExchangeConfig(id, p.testnet(), rest, ws, p.recvWindowMs(), List.copyOf(symbols), p.bookDepth(), p.priceWindow(), p);
    }
}
