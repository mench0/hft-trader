package com.hft.exchange.generic;

import com.hft.config.TradingSettings;
import com.hft.config.ExchangeConfig;
import com.hft.engine.MarketDataHandler;
import com.hft.engine.StrategySet;
import com.hft.engine.OrderService;
import com.hft.engine.TickPipeline;
import com.hft.exchange.ExchangeGateway;
import com.hft.exchange.catalog.ExchangeInfo;
import com.hft.paper.PaperOrderApi;
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
 * PaperExchange целиком, собран по тому же паттерну, что и {@link com.hft.exchange.bybit.BybitExchange}:
 * свои хранилища, бумажный движок, риск-менеджер, сервис ордеров, конвейер тиков и фид.
 * Биржа без собственного торгового клиента (dYdX): живые данные (WebSocket + REST-запас) и бумажные ордера.
 * Всегда бумажный движок на живых данных.
 */
public final class PaperExchange implements ExchangeGateway, RequestStatsSource {

    /** Логгер. */
    private static final Logger log = LoggerFactory.getLogger(PaperExchange.class);

    /** Описание биржи из каталога. */
    private final ExchangeInfo info;
    /** Подключение и параметры биржи. */
    private final ExchangeConfig config;

    /** Стаканы, окна цен и статистика символов. */
    private final MarketDataStore market;
    /** Балансы (с биржи или бумажные). */
    private final BalanceStore balances;
    /** Правила торговли символов. */
    private final SymbolFilters filters;
    /** Бумажный движок исполнения. */
    private final PaperOrderApi paper;         
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
    private final BookFeed feed;

    /**
     * @param info описание биржи из каталога
     * @param config подключение и параметры
     * @param settings параметры биржи
     */
    public PaperExchange(ExchangeInfo info, ExchangeConfig config, TradingSettings settings) {
        this.info = info;
        this.config = config;

        this.market = new MarketDataStore(config.bookDepth(), config.priceWindowSize());
        this.balances = new BalanceStore();
        this.filters = new SymbolFilters();
        config.symbols().forEach(market::register);

        this.paper = new PaperOrderApi(market, balances, info.makerFeePct(), info.takerFeePct());
        config.symbols().forEach(s -> ExchangeSupport.putDefaultFilter(filters, s));

        this.risk = new RiskManager(settings, market, info.id());
        this.orderService = new OrderService(paper, market, balances, filters, risk, settings);

        this.dataHandler = new MarketDataHandler(market);
        this.strategy = new StrategySet(market, orderService, info.id(), settings);
        this.pipeline = new TickPipeline(dataHandler, strategy.handlers());
        this.feed = ExchangeSupport.newFeed(info, config, market, pipeline, paper, this::onFeedGaveUp);
        strategy.setRealtimeSource(feed::isRealtime);          // на REST-запасе новых входов нет
    }

    /** Идентификатор биржи. */
    @Override
    public String id() { return info.id(); }

    /** Загрузить правила и балансы, запустить конвейер и фид. */
    @Override
    public void start() throws Exception {
        ExchangeSupport.seedPaperBalances(balances, config);
        pipeline.start();
        feed.start();
        log.info("[{}] Биржа запущена ({}), символы: {}", info.id(), "PAPER", config.symbols());
    }

    /** Остановить стратегии, фид и конвейер. */
    @Override
    public void stop() {
        strategy.disable();
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

    @Override
    public void syncBalances() { /* бумажный баланс ведёт движок, сверять не с чем */ }

    /** Источник данных потерян: снять заявки, закрыть позиции, остановить стратегию. */
    private void onFeedGaveUp() {
        ExchangeSupport.feedGaveUp(info.id(), feed, orderService, strategy, risk);
    }

    /** Режим, метрики фида (WS/опрос) и (в LIVE) метрики запросов к бирже — для админки. */
    @Override
    public Map<String, Object> requestStats() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("mode", "PAPER");
        m.put("marketData", feed.stats());
        m.put("realtime", feed.isRealtime());
        m.put("strategies", strategy.stats());
        return m;
    }

    /** Ордера идут на биржу (LIVE), а не в бумажный движок. */
    @Override
    public boolean isLive() { return false; }
}
