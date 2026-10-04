package com.hft.exchange.generic;

import com.hft.config.Credentials;
import com.hft.config.ExchangeConfig;
import com.hft.engine.StrategySet;
import com.hft.engine.OrderService;
import com.hft.engine.TickPipeline;
import com.hft.exchange.catalog.ExchangeInfo;
import com.hft.paper.PaperOrderApi;
import com.hft.risk.RiskManager;
import com.hft.store.BalanceStore;
import com.hft.store.MarketDataStore;
import com.hft.store.SymbolFilters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Мелкие общие функции для классов бирж (SignedCexExchange, PaperExchange). Сами классы устроены так же,
 * как BybitExchange: свои поля, свой конвейер тиков, явные start()/stop(); здесь — только то, что иначе
 * пришлось бы копировать построчно (режим LIVE/PAPER, правила и баланс для бумажной торговли, сборка фида).
 */
public final class ExchangeSupport {

    private static final Logger log = LoggerFactory.getLogger(ExchangeSupport.class);

    private ExchangeSupport() {}

    /** LIVE только при ключах и явном ID_LIVE=true, иначе бумажный движок. */
    public static boolean isLive(ExchangeInfo info, Credentials credentials) {
        String id = info.id().toUpperCase();
        boolean flag = "true".equalsIgnoreCase(System.getenv(id + "_LIVE"));
        if (credentials.isPresent() && flag) {
            log.warn("[{}] режим LIVE — ордера пойдут на биржу", info.id());
            return true;
        }
        log.info("[{}] режим PAPER (для LIVE нужны {}_API_KEY/_SECRET и {}_LIVE=true)", info.id(), id, id);
        return false;
    }

    /** Правила торговли для бумажного режима (в LIVE правила только настоящие). */
    public static void putDefaultFilter(SymbolFilters filters, String symbol) {
        filters.put(symbol, new SymbolFilters.Filter(0, 1e12, 1e-6, 0, 1e12, 1e-8, 5.0));
    }

    /** Стартовый бумажный баланс: PAPER_START_BALANCE или 1000 в котируемой валюте каждого символа. */
    public static void seedPaperBalances(BalanceStore balances, ExchangeConfig config) {
        double start = 1000;
        String env = System.getenv("PAPER_START_BALANCE");
        if (env != null && !env.isBlank()) start = Double.parseDouble(env);
        for (String s : config.symbols()) {
            String quote = BalanceStore.quoteAsset(s);
            if (balances.total(quote) == 0) balances.set(quote, start, 0);
        }
        balances.markSynced();
    }

    /** WebSocket — основной канал, REST — запасной; для бирж без WS-диалекта — только REST-опрос. Тики идут в конвейер. */
    public static BookFeed newFeed(ExchangeInfo info, ExchangeConfig config, MarketDataStore market,
                                   TickPipeline pipeline, PaperOrderApi paper, Runnable onGiveUp) {
        TickSink onTick = pipeline::publish;           // тик сразу в кольцо Disruptor, без промежуточного объекта
        java.util.function.Consumer<String> onBook = s -> { if (paper != null) paper.settle(s); };
        var ws = WsDialects.forExchange(info.id());
        return ws.isPresent()
                ? new HybridBookFeed(info, config, ws.get(), Dialects.forExchange(info.id()), market, onTick, onBook, onGiveUp)
                : new PollingBookFeed(info, config, Dialects.forExchange(info.id()), market, onTick, onBook, onGiveUp);
    }

    /** Источник данных потерян: снять заявки, закрыть позиции, остановить стратегию и торговлю. */
    public static void feedGaveUp(String id, BookFeed feed, OrderService orders, StrategySet strategy, RiskManager risk) {
        log.error("[{}] данные потеряны — отменяю заявки, закрываю позиции, останавливаю торговлю", id);
        for (String s : feed.activeSymbols()) {
            try { orders.cancelAll(s); } catch (Exception e) { log.warn("[{}] cancelAll {}: {}", id, s, e.toString()); }
        }
        try { strategy.closeAll(); } catch (Exception e) { log.warn("[{}] closeAll: {}", id, e.toString()); }
        strategy.disable();
        risk.stopTrading("потеряно соединение с биржей " + id);
    }
}
