package com.hft.exchange.bybit;

import com.hft.config.TradingSettings;
import com.hft.config.Credentials;
import com.hft.config.ExchangeConfig;
import com.hft.engine.MarketDataHandler;
import com.hft.engine.MeanReversionStrategy;
import com.hft.engine.OrderService;
import com.hft.engine.TickPipeline;
import com.hft.exchange.ExchangeGateway;
import com.hft.risk.RiskManager;
import com.hft.store.BalanceStore;
import com.hft.store.MarketDataStore;
import com.hft.store.SymbolFilters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Bybit целиком, собран по тому же паттерну, что и {@link com.hft.exchange.binance.BinanceExchange}.
 * Это и есть подтверждение того, что архитектура расширяется: чтобы
 * добавить биржу, нужно реализовать REST-клиент и WS-фид под её API,
 * а сборка (эта обёртка) почти дословно повторяется.
 */
public final class BybitExchange implements ExchangeGateway {

    private static final Logger log = LoggerFactory.getLogger(BybitExchange.class);

    private final ExchangeConfig config;
    private final Credentials credentials;

    private final MarketDataStore market;
    private final BalanceStore balances;
    private final SymbolFilters filters;
    private final BybitRestClient rest;
    private final RiskManager risk;
    private final OrderService orderService;

    private final MarketDataHandler dataHandler;
    private final MeanReversionStrategy strategy;
    private final TickPipeline pipeline;
    private final BybitMarketDataFeed feed;

    public BybitExchange(ExchangeConfig config, TradingSettings settings) {
        this.config = config;
        this.credentials = Credentials.fromEnv(config.id());

        this.market = new MarketDataStore(config.bookDepth(), config.priceWindowSize());
        this.balances = new BalanceStore();
        this.filters = new SymbolFilters();
        config.symbols().forEach(market::register);

        this.rest = new BybitRestClient(config, credentials, filters);
        this.risk = new RiskManager(settings, market, config.id());
        this.orderService = new OrderService(rest, market, balances, filters, risk, settings);

        this.dataHandler = new MarketDataHandler(market);
        this.strategy = new MeanReversionStrategy(market, orderService, "bybit", settings);
        this.pipeline = new TickPipeline(dataHandler, strategy);
        this.feed = new BybitMarketDataFeed(config, market, pipeline);

        if (!credentials.isPresent()) {
            log.warn("[bybit] API-ключи не заданы (BYBIT_API_KEY/BYBIT_API_SECRET) — " +
                     "только сбор данных, торговля недоступна");
        }
    }

    @Override
    public String id() { return "bybit"; }

    @Override
    public void start() throws Exception {
        rest.loadFilters(config.symbols());
        if (credentials.isPresent()) {
            try {
                rest.loadBalances(balances);
            } catch (Exception e) {
                log.error("[bybit] Не удалось загрузить балансы: {}", e.getMessage());
            }
        }
        pipeline.start();
        feed.start();
        log.info("[bybit] Биржа запущена, символы: {}", config.symbols());
    }

    @Override
    public void stop() {
        strategy.disable();
        feed.stop();
        pipeline.shutdown();
        log.info("[bybit] Биржа остановлена");
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

    public BybitMarketDataFeed feed() { return feed; }

    @Override
    public void syncBalances() {
        if (!credentials.isPresent()) return;
        try { rest.loadBalances(balances); }
        catch (Exception e) { log.warn("[bybit] Не удалось обновить балансы: {}", e.getMessage()); }
    }
}
