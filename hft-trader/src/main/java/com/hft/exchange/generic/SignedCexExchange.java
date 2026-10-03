package com.hft.exchange.generic;

import com.hft.config.Credentials;
import com.hft.config.ExchangeConfig;
import com.hft.config.TradingSettings;
import com.hft.engine.MarketDataHandler;
import com.hft.engine.MeanReversionStrategy;
import com.hft.engine.OrderService;
import com.hft.engine.TickPipeline;
import com.hft.exchange.ExchangeGateway;
import com.hft.exchange.catalog.ExchangeCatalog;
import com.hft.exchange.catalog.ExchangeInfo;
import com.hft.paper.PaperOrderApi;
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
 * Биржа на клиенте {@link SignedCexClient} (OKX, Gate, MEXC, BingX, LBank, Hyperliquid, Uniswap V2):
 * свои хранилища, клиент, риск-менеджер, сервис ордеров, конвейер тиков и фид.
 * Биржи отличаются только клиентом — он передаётся фабрикой.
 * LIVE включается только при ID_API_KEY/_SECRET и ID_LIVE=true, иначе — бумажный движок на живых данных.
 */
public final class SignedCexExchange implements ExchangeGateway, RequestStatsSource {

    private static final Logger log = LoggerFactory.getLogger(SignedCexExchange.class);

    private final ExchangeInfo info;
    private final ExchangeConfig config;
    private final Credentials credentials;

    private final MarketDataStore market;
    private final BalanceStore balances;
    private final SymbolFilters filters;
    /** Создаёт клиент биржи для режима LIVE. */
    @FunctionalInterface
    public interface ClientFactory {
        SignedCexClient create(ExchangeConfig config, Credentials credentials, SymbolFilters filters);
    }

    private final SignedCexClient rest;               // null в режиме PAPER
    private final PaperOrderApi paper;         // null в режиме LIVE
    private final RiskManager risk;
    private final OrderService orderService;

    private final MarketDataHandler dataHandler;
    private final MeanReversionStrategy strategy;
    private final TickPipeline pipeline;
    private final BookFeed feed;
    private volatile long lastRestSyncMs = System.currentTimeMillis();

    public SignedCexExchange(String id, ExchangeConfig config, TradingSettings settings, ClientFactory clientFactory) {
        this.info = ExchangeCatalog.find(id).orElseThrow();
        this.config = config;
        this.credentials = Credentials.fromEnv(info.id());

        this.market = new MarketDataStore(config.bookDepth(), config.priceWindowSize());
        this.balances = new BalanceStore();
        this.filters = new SymbolFilters();
        config.symbols().forEach(market::register);

        boolean live = ExchangeSupport.isLive(info, credentials);
        this.rest = live ? clientFactory.create(config, credentials, filters) : null;
        this.paper = live ? null : new PaperOrderApi(market, balances, info.makerFeePct(), info.takerFeePct());
        ExchangeOrderApi api = live ? rest : paper;
        if (paper != null) config.symbols().forEach(s -> ExchangeSupport.putDefaultFilter(filters, s));

        this.risk = new RiskManager(settings, market, info.id());
        this.orderService = new OrderService(api, market, balances, filters, risk, settings);

        this.dataHandler = new MarketDataHandler(market);
        this.strategy = new MeanReversionStrategy(market, orderService, info.id(), settings);
        this.pipeline = new TickPipeline(dataHandler, strategy);
        this.feed = ExchangeSupport.newFeed(info, config, market, pipeline, paper, this::onFeedGaveUp);
        strategy.setRealtimeSource(feed::isRealtime);          // на REST-запасе новых входов нет

        if (!credentials.isPresent()) {
            String env = info.id().toUpperCase();
            log.warn("[{}] API-ключи не заданы ({}_API_KEY/{}_API_SECRET) — только сбор данных и бумажная торговля", info.id(), env, env);
        }
    }

    @Override
    public String id() { return info.id(); }

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
        pipeline.start();
        feed.start();
        log.info("[{}] Биржа запущена ({}), символы: {}", info.id(), rest != null ? "LIVE" : "PAPER", config.symbols());
    }

    @Override
    public void stop() {
        strategy.disable();
        if (rest != null) rest.stopStreams();
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
    public void addSymbol(String symbol) {
        String s = symbol.toUpperCase();
        if (rest != null) {
            try { rest.loadFilters(List.of(s)); }
            catch (Exception e) { throw new IllegalStateException("Не удалось загрузить правила для " + s + ": " + e.getMessage()); }
        } else {
            ExchangeSupport.putDefaultFilter(filters, s);
        }
        market.register(s);
        feed.addSymbol(s);
    }

    @Override
    public void removeSymbol(String symbol) {
        feed.removeSymbol(symbol);
    }

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
    public void syncBalances() {
        if (rest == null) return;
        // баланс приходит по WS — сверяемся с REST лишь раз в 5 минут
        if (rest.balancesStreamed() && System.currentTimeMillis() - lastRestSyncMs < 300_000) return;
        lastRestSyncMs = System.currentTimeMillis();
        try { rest.loadBalances(balances); }
        catch (Exception e) { log.warn("[{}] Не удалось обновить балансы: {}", info.id(), e.getMessage()); }
    }

    /** Источник данных потерян: снять заявки, закрыть позиции, остановить стратегию. */
    private void onFeedGaveUp() {
        ExchangeSupport.feedGaveUp(info.id(), feed, orderService, strategy, risk);
    }

    /** Режим, метрики фида (WS/опрос) и (в LIVE) метрики запросов к бирже — для админки. */
    @Override
    public Map<String, Object> requestStats() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("mode", rest != null ? "LIVE" : "PAPER");
        m.put("marketData", feed.stats());
        m.put("realtime", feed.isRealtime());
        m.put("orderExecutor", strategy.executor().stats());
        if (rest != null) m.put("orders", rest.stats());
        return m;
    }

    @Override
    public boolean isLive() { return rest != null; }
}
