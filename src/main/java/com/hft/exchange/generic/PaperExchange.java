package com.hft.exchange.generic;

import com.hft.config.TradingSettings;
import com.hft.config.ExchangeConfig;
import com.hft.engine.MarketDataHandler;
import com.hft.engine.MeanReversionStrategy;
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

    private static final Logger log = LoggerFactory.getLogger(PaperExchange.class);

    private final ExchangeInfo info;
    private final ExchangeConfig config;

    private final MarketDataStore market;
    private final BalanceStore balances;
    private final SymbolFilters filters;
    private final PaperOrderApi paper;         
    private final RiskManager risk;
    private final OrderService orderService;

    private final MarketDataHandler dataHandler;
    private final MeanReversionStrategy strategy;
    private final TickPipeline pipeline;
    private final BookFeed feed;

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
        this.strategy = new MeanReversionStrategy(market, orderService, info.id(), settings);
        this.pipeline = new TickPipeline(dataHandler, strategy);
        this.feed = ExchangeSupport.newFeed(info, config, market, pipeline, paper, this::onFeedGaveUp);
        strategy.setRealtimeSource(feed::isRealtime);          // на REST-запасе новых входов нет
    }

    @Override
    public String id() { return info.id(); }

    @Override
    public void start() throws Exception {
        ExchangeSupport.seedPaperBalances(balances, config);
        pipeline.start();
        feed.start();
        log.info("[{}] Биржа запущена ({}), символы: {}", info.id(), "PAPER", config.symbols());
    }

    @Override
    public void stop() {
        strategy.disable();
        feed.stop();
        pipeline.shutdown();
        log.info("[{}] Биржа остановлена", info.id());
    }

    @Override
    public boolean isConnected() { return feed.isConnected(); }

    @Override
    public long messageCount() { return feed.messageCount(); }

    @Override
    public List<String> symbols() { return feed.activeSymbols(); }

    @Override
    public MarketDataStore marketData() { return market; }

    @Override
    public BalanceStore balances() { return balances; }

    @Override
    public OrderService orders() { return orderService; }

    @Override
    public RiskManager risk() { return risk; }

    @Override
    public MeanReversionStrategy strategy() { return strategy; }

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
        m.put("orderExecutor", strategy.executor().stats());
        return m;
    }

    @Override
    public boolean isLive() { return false; }
}
