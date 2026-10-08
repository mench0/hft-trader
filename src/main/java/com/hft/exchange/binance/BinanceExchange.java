package com.hft.exchange.binance;

import com.hft.config.TradingSettings;
import com.hft.config.Credentials;
import com.hft.config.ExchangeConfig;
import com.hft.exchange.generic.ExchangeSupport;
import com.hft.exchange.catalog.ExchangeInfo;
import com.hft.exchange.catalog.ExchangeCatalog;
import com.hft.paper.PaperOrderApi;
import com.hft.engine.MarketDataHandler;
import com.hft.strategy.StrategySet;
import com.hft.engine.OrderService;
import com.hft.engine.TickPipeline;
import com.hft.exchange.ExchangeGateway;
import com.hft.rest.BinanceRestClient;
import com.hft.risk.RiskManager;
import com.hft.store.BalanceStore;
import com.hft.store.MarketDataStore;
import com.hft.store.SymbolFilters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Binance целиком: REST-клиент, WebSocket-фид, конвейер и стратегия
 * собраны в одном месте. Main получает готовый {@link ExchangeGateway}
 * и не видит внутренней сборки — так же будет с Bybit и любой
 * следующей биржей.
 */
public final class BinanceExchange implements ExchangeGateway, com.hft.exchange.generic.RequestStatsSource {

    /** Логгер. */
    private static final Logger log = LoggerFactory.getLogger(BinanceExchange.class);

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
    /** REST-клиент биржи. */
    private final BinanceRestClient rest;
    /** Бумажный движок, если live=false или нет ключей; иначе null. */
    private final PaperOrderApi paper;
    /** Когда баланс последний раз сверялся с биржей по REST. */
    private volatile long lastBalanceSyncMs;
    /** Проверки риска перед ордером. */
    private final RiskManager risk;
    /** Отправка ордеров с проверками и учётом баланса. */
    private final OrderService orderService;

    /** Первая стадия конвейера: запись тиков в память. */
    private final MarketDataHandler dataHandler;
    /** Стратегии биржи. */
    private final StrategySet strategy;
    /** Конвейер тиков (Disruptor). */
    private final TickPipeline pipeline;
    /** Фид рыночных данных. */
    private final BinanceMarketDataFeed feed;

    /**
     * @param config подключение и параметры биржи
     * @param settings параметры биржи (общий объект, меняется из админки)
     */
    public BinanceExchange(ExchangeConfig config, TradingSettings settings) {
        this.config = config;
        this.credentials = Credentials.fromEnv(config.id());

        this.market = new MarketDataStore(config.bookDepth(), config.priceWindowSize());
        this.balances = new BalanceStore();
        this.filters = new SymbolFilters();
        config.symbols().forEach(market::register);

        this.rest = new BinanceRestClient(config, credentials, filters);
        this.risk = new RiskManager(settings, market, config.id());
        boolean live = config.params().live() && credentials.isPresent();
        this.paper = live ? null : new PaperOrderApi(market, balances,
                ExchangeCatalog.find(config.id()).map(ExchangeInfo::makerFeePct).orElse(0.1),
                ExchangeCatalog.find(config.id()).map(ExchangeInfo::takerFeePct).orElse(0.1));
        this.orderService = new OrderService(live ? rest : paper, market, balances, filters, risk, settings);
        log.info("[binance] режим {}", live ? "LIVE — ордера пойдут на биржу" : "PAPER (для LIVE нужны ключи и live=true)");

        this.dataHandler = new MarketDataHandler(market);
        this.strategy = new StrategySet(market, orderService, "binance", settings);
        this.pipeline = new TickPipeline(dataHandler, strategy.handlers());
        this.feed = new BinanceMarketDataFeed(config, market, pipeline);

        if (!credentials.isPresent()) {
            log.warn("[binance] API-ключи не заданы (BINANCE_API_KEY/BINANCE_API_SECRET) — " +
                     "только сбор данных, торговля недоступна");
        }
    }

    /** Идентификатор биржи. */
    @Override
    public String id() { return "binance"; }

    /** Загрузить правила и балансы, запустить конвейер и фид. */
    @Override
    public void start() throws Exception {
        rest.syncTime();
        rest.loadFilters(config.symbols());
        if (paper == null) {
            try {                                   // сокет WebSocket API: ордера и события аккаунта, REST — запасной
                rest.startStreams(balances);
                rest.awaitStreams(4000);
            } catch (Exception e) {
                log.warn("[binance] WebSocket API не поднялся ({}), ордера пойдут через REST", e.toString());
            }
            try {
                rest.loadBalances(balances);
            } catch (Exception e) {
                log.error("[binance] Не удалось загрузить балансы: {}", e.getMessage());
            }
        } else {
            ExchangeSupport.seedPaperBalances(balances, config);
        }
        pipeline.start();
        feed.start();
        log.info("[binance] Биржа запущена, символы: {}", config.symbols());
    }

    /** Остановить стратегии, фид и конвейер. */
    @Override
    public void stop() {
        strategy.disable();
        rest.stopStreams();
        feed.stop();
        pipeline.shutdown();
        log.info("[binance] Биржа остановлена");
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
    public BinanceMarketDataFeed feed() { return feed; }
    /** Конвейер тиков. */
    public TickPipeline pipeline() { return pipeline; }

    /** Периодические фоновые задачи для этой биржи — вызывается из Main через scheduler. */
    public void syncTime() {
        try { rest.syncTime(); }
        catch (Exception e) { log.warn("[binance] Не удалось синхронизировать часы: {}", e.getMessage()); }
    }

    /** Сверить баланс с биржей по REST, не чаще balanceSyncMs (в бумажном режиме — ничего). */
    @Override
    public void syncBalances() {
        if (paper != null) return;                          // бумажный баланс ведёт движок
        long now = System.currentTimeMillis();
        // баланс приходит событиями WebSocket API; REST — сверка раз в balanceSyncMs
        if (now - lastBalanceSyncMs < config.params().balanceSyncMs()) return;
        lastBalanceSyncMs = now;
        try { rest.loadBalances(balances); }
        catch (Exception e) { log.warn("[binance] Не удалось обновить балансы: {}", e.getMessage()); }
    }

    /** Режим, фид и (в LIVE) метрики WebSocket API — для админки. */
    @Override
    public Map<String, Object> requestStats() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("mode", paper == null ? "LIVE" : "PAPER");
        m.put("marketData", Map.of("ws", feed.isConnected(), "messages", feed.messageCount()));
        if (paper == null) m.put("orders", rest.stats());
        return m;
    }

    /** Ордера идут на биржу (LIVE), а не в бумажный движок. */
    @Override
    public boolean isLive() { return paper == null; }
}
