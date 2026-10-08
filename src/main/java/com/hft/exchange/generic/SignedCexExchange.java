package com.hft.exchange.generic;

import com.hft.config.Credentials;
import com.hft.config.ExchangeConfig;
import com.hft.config.TradingSettings;
import com.hft.engine.MarketDataHandler;
import com.hft.engine.StrategySet;
import com.hft.engine.OrderService;
import com.hft.engine.TickPipeline;
import com.hft.exchange.ExchangeGateway;
import com.hft.exchange.catalog.ExchangeCatalog;
import com.hft.exchange.catalog.ExchangeInfo;
import com.hft.paper.PaperOrderApi;
import com.hft.perp.PerpAccount;
import com.hft.store.PositionStore;
import com.hft.risk.RiskManager;
import com.hft.rest.ExchangeOrderApi;
import com.hft.rest.SignedCexClient;
import com.hft.store.BalanceStore;
import com.hft.store.MarketDataStore;
import com.hft.store.SymbolFilters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Биржа на клиенте {@link SignedCexClient} (OKX, Gate, MEXC, KuCoin, Aster, Hyperliquid, Uniswap V2):
 * свои хранилища, клиент, риск-менеджер, сервис ордеров, конвейер тиков и фид.
 * Биржи отличаются только клиентом — он передаётся фабрикой.
 * LIVE включается только при ID_API_KEY/_SECRET и ID_LIVE=true, иначе — бумажный движок на живых данных.
 */
public final class SignedCexExchange implements ExchangeGateway, RequestStatsSource {

    /** Логгер. */
    private static final Logger log = LoggerFactory.getLogger(SignedCexExchange.class);

    /** Описание биржи из каталога. */
    private final ExchangeInfo info;
    /** Подключение и параметры биржи. */
    private final ExchangeConfig config;
    /** API-ключи из окружения. */
    private final Credentials credentials;

    /** Стаканы, окна цен и статистика символов. */
    private final MarketDataStore market;
    /** Балансы (с биржи или бумажные). */
    private final BalanceStore balances;
    /** Правила торговли символов. */
    private final SymbolFilters filters;
    /** Создаёт клиент биржи для режима LIVE. */
    @FunctionalInterface
    public interface ClientFactory {
        /** Клиент биржи для режима LIVE. */
        SignedCexClient create(ExchangeConfig config, Credentials credentials, SymbolFilters filters);
    }

    private final SignedCexClient rest;               // null в режиме PAPER
    private final PaperOrderApi paper;         // null в режиме LIVE
    /** Проверки риска перед ордером. */
    private final RiskManager risk;
    /** Отправка ордеров с проверками и учётом баланса. */
    private final OrderService orderService;

    /** Фьючерсный счёт; null — спот. */
    private final PerpAccount perp;

    /** Первая стадия конвейера: запись тиков в память. */
    private final MarketDataHandler dataHandler;
    /** Стратегии биржи. */
    private final StrategySet strategy;
    /** Конвейер тиков (Disruptor). */
    private final TickPipeline pipeline;
    /** Фид рыночных данных. */
    private final BookFeed feed;
    /** Когда баланс последний раз сверялся с биржей по REST. */
    private volatile long lastRestSyncMs = System.currentTimeMillis();

    /**
     * @param id биржа из каталога
     * @param config подключение и параметры
     * @param settings параметры биржи
     * @param clientFactory создаёт REST-клиент для LIVE
     */
    public SignedCexExchange(String id, ExchangeConfig config, TradingSettings settings, ClientFactory clientFactory) {
        this.info = ExchangeCatalog.find(id).orElseThrow();
        this.config = config;
        this.credentials = Credentials.fromEnv(info.id());

        this.market = new MarketDataStore(config.bookDepth(), config.priceWindowSize());
        this.balances = new BalanceStore();
        this.filters = new SymbolFilters();
        config.symbols().forEach(market::register);

        boolean live = ExchangeSupport.isLive(info, config, credentials);
        this.rest = live ? clientFactory.create(config, credentials, filters) : null;
        // Hyperliquid в боте — только перпы, даже если в старом конфиге market не задан
        boolean isPerp = config.params().isPerp() || !ExchangeCatalog.supportsSpot(info.id());
        PositionStore positions = new PositionStore();
        var fees = ExchangeSupport.fees(info, config);
        this.paper = live ? null : new PaperOrderApi(market, balances, fees[0], fees[1]);
        if (paper != null && isPerp) paper.perp(positions);
        ExchangeOrderApi api = live ? rest : paper;
        if (api.isPerp() != isPerp)
            throw new IllegalStateException("[" + info.id() + "] клиент биржи не поддерживает market=" + config.params().market());
        if (paper != null) config.symbols().forEach(s -> ExchangeSupport.putDefaultFilter(filters, s));

        this.risk = new RiskManager(settings, market, info.id());
        this.orderService = new OrderService(api, market, balances, filters, risk, settings, positions);
        this.perp = isPerp ? new PerpAccount(info.id(), config, market, balances, positions, api, !live) : null;

        this.dataHandler = new MarketDataHandler(market);
        this.strategy = new StrategySet(market, orderService, info.id(), settings);
        this.pipeline = new TickPipeline(dataHandler, strategy.handlers());
        this.feed = ExchangeSupport.newFeed(info, config, market, pipeline, paper, this::onFeedGaveUp);
        strategy.setRealtimeSource(feed::isRealtime);          // на REST-запасе новых входов нет

        if (!credentials.isPresent()) {
            String env = info.id().toUpperCase();
            log.warn("[{}] API-ключи не заданы ({}_API_KEY/{}_API_SECRET) — только сбор данных и бумажная торговля", info.id(), env, env);
        }
    }

    /** Идентификатор биржи. */
    @Override
    public String id() { return info.id(); }

    /** Загрузить правила и балансы, запустить конвейер и фид. */
    @Override
    public void start() throws Exception {
        if (rest != null) {
            try {                                   // сначала сокеты: стартовые запросы пойдут по ним, REST — запасной
                rest.startStreams(balances);
                rest.awaitStreams(4000);
            } catch (Exception e) {
                log.warn("[{}] WS-каналы не поднялись ({}), работаю через REST", info.id(), e.toString());
            }
            rest.loadFilters(config.symbols());
            for (String s : config.symbols()) {
                if (!filters.has(s)) throw new IllegalStateException("[" + info.id() + "] нет торговых правил для " + s + " — LIVE невозможен");
            }
            rest.loadBalances(balances);
        } else {
            ExchangeSupport.seedPaperBalances(balances, config);
        }
        if (perp != null) perp.start();
        pipeline.start();
        feed.start();
        log.info("[{}] Биржа запущена ({}, {}), символы: {}", info.id(), rest != null ? "LIVE" : "PAPER",
                perp != null ? "фьючерсы, плечо " + config.params().leverage() + "x" : "спот", config.symbols());
    }

    /** Остановить стратегии, фид и конвейер. */
    @Override
    public void stop() {
        strategy.disable();
        if (perp != null) perp.stop();
        if (rest != null) rest.stopStreams();
        feed.stop();
        pipeline.shutdown();
        log.info("[{}] Биржа остановлена", info.id());
    }

    /** Рыночные данные идут. */
    @Override
    public boolean isConnected() { return feed.isConnected(); }

    /** Получено сообщений с биржи. */
    @Override
    public long messageCount() { return feed.messageCount(); }

    /** Символы, по которым идут данные. */
    @Override
    public List<String> symbols() { return feed.activeSymbols(); }

    /** Рыночные данные биржи. */
    @Override
    public MarketDataStore marketData() { return market; }

    /** Балансы биржи. */
    @Override
    public BalanceStore balances() { return balances; }

    /** Сервис ордеров биржи. */
    @Override
    public OrderService orders() { return orderService; }

    /** Риск-менеджер биржи. */
    @Override
    public RiskManager risk() { return risk; }

    /** Стратегии биржи. */
    @Override
    public StrategySet strategy() { return strategy; }

    /** Фид рыночных данных. */
    public BookFeed feed() { return feed; }

    /** Сверить баланс с биржей по REST, не чаще balanceSyncMs (в бумажном режиме — ничего). */
    @Override
    public void syncBalances() {
        if (rest == null) return;
        // баланс приходит по WS или обновляется после ордеров — сверяемся с REST раз в balanceSyncMs
        if (System.currentTimeMillis() - lastRestSyncMs < config.params().balanceSyncMs()) return;
        lastRestSyncMs = System.currentTimeMillis();
        try { rest.loadBalances(balances); }
        catch (Exception e) { log.warn("[{}] Не удалось обновить балансы: {}", info.id(), e.getMessage()); }
        if (perp != null) perp.syncPositions();
    }

    /** Фьючерсный счёт; null — спот. */
    @Override
    public PerpAccount perp() { return perp; }

    /** Источник данных потерян: снять заявки, закрыть позиции, остановить стратегию. */
    private void onFeedGaveUp() {
        ExchangeSupport.feedGaveUp(info.id(), feed, orderService, strategy, risk);
    }

    /** Режим, метрики фида (WS/опрос) и (в LIVE) метрики запросов к бирже — для админки. */
    @Override
    public Map<String, Object> requestStats() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("mode", rest != null ? "LIVE" : "PAPER");
        m.put("market", config.params().market());
        m.put("marketData", feed.stats());
        m.put("realtime", feed.isRealtime());
        m.put("strategies", strategy.stats());
        if (rest != null) m.put("orders", rest.stats());
        return m;
    }

    /** Ордера идут на биржу (LIVE), а не в бумажный движок. */
    @Override
    public boolean isLive() { return rest != null; }
}
