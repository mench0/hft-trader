package com.hft.exchange.binance;

import com.hft.config.TradingSettings;
import com.hft.config.Credentials;
import com.hft.config.ExchangeConfig;
import com.hft.engine.MarketDataHandler;
import com.hft.engine.MeanReversionStrategy;
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

import java.util.List;

/**
 * Binance целиком: REST-клиент, WebSocket-фид, конвейер и стратегия
 * собраны в одном месте. Main получает готовый {@link ExchangeGateway}
 * и не видит внутренней сборки — так же будет с Bybit и любой
 * следующей биржей.
 */
public final class BinanceExchange implements ExchangeGateway {

    private static final Logger log = LoggerFactory.getLogger(BinanceExchange.class);

    private final ExchangeConfig config;
    private final Credentials credentials;

    private final MarketDataStore market;
    private final BalanceStore balances;
    private final SymbolFilters filters;
    private final BinanceRestClient rest;
    private final RiskManager risk;
    private final OrderService orderService;

    private final MarketDataHandler dataHandler;
    private final MeanReversionStrategy strategy;
    private final TickPipeline pipeline;
    private final BinanceMarketDataFeed feed;

    public BinanceExchange(ExchangeConfig config, TradingSettings settings) {
        this.config = config;
        this.credentials = Credentials.fromEnv(config.id());

        this.market = new MarketDataStore(config.bookDepth(), config.priceWindowSize());
        this.balances = new BalanceStore();
        this.filters = new SymbolFilters();
        config.symbols().forEach(market::register);

        this.rest = new BinanceRestClient(config, credentials, filters);
        this.risk = new RiskManager(settings, market, config.id());
        this.orderService = new OrderService(rest, market, balances, filters, risk, settings);

        this.dataHandler = new MarketDataHandler(market);
        this.strategy = new MeanReversionStrategy(market, orderService, "binance", settings);
        this.pipeline = new TickPipeline(dataHandler, strategy);
        this.feed = new BinanceMarketDataFeed(config, market, pipeline);

        if (!credentials.isPresent()) {
            log.warn("[binance] API-ключи не заданы (BINANCE_API_KEY/BINANCE_API_SECRET) — " +
                     "только сбор данных, торговля недоступна");
        }
    }

    @Override
    public String id() { return "binance"; }

    @Override
    public void start() throws Exception {
        rest.syncTime();
        rest.loadFilters(config.symbols());
        if (credentials.isPresent()) {
            try {
                rest.loadBalances(balances);
            } catch (Exception e) {
                log.error("[binance] Не удалось загрузить балансы: {}", e.getMessage());
            }
        }
        pipeline.start();
        feed.start();
        log.info("[binance] Биржа запущена, символы: {}", config.symbols());
    }

    @Override
    public void stop() {
        strategy.disable();
        feed.stop();
        pipeline.shutdown();
        log.info("[binance] Биржа остановлена");
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

    public BinanceMarketDataFeed feed() { return feed; }
    public TickPipeline pipeline() { return pipeline; }

    /** Периодические фоновые задачи для этой биржи — вызывается из Main через scheduler. */
    public void syncTime() {
        try { rest.syncTime(); }
        catch (Exception e) { log.warn("[binance] Не удалось синхронизировать часы: {}", e.getMessage()); }
    }

    @Override
    public void syncBalances() {
        if (!credentials.isPresent()) return;
        try { rest.loadBalances(balances); }
        catch (Exception e) { log.warn("[binance] Не удалось обновить балансы: {}", e.getMessage()); }
    }
}
