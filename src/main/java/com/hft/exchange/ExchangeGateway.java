package com.hft.exchange;

import com.hft.strategy.StrategySet;
import com.hft.engine.OrderService;
import com.hft.risk.RiskManager;
import com.hft.store.BalanceStore;
import com.hft.store.MarketDataStore;

import java.util.List;

/**
 * Общий контракт биржи. Main и стратегии работают через этот интерфейс
 * и не знают, Binance это, Bybit или кто-то ещё.
 *
 * Каждая биржа — это связка из четырёх вещей, которые у неё свои:
 *   - REST-клиент со своей подписью запросов и форматом ответов
 *   - WebSocket-фид со своим форматом сообщений и именами полей
 *   - собственное хранилище рыночных данных (стаканы разных бирж
 *     по одному символу — это разные данные, смешивать их нельзя)
 *   - собственные балансы (деньги на Binance и Bybit разделены физически)
 *
 * Единственное общее — это OrderService и MarketDataStore, потому что
 * их код не зависит от конкретной биржи. Каждая биржа просто передаёт
 * туда свою реализацию BinanceRestClient / BybitRestClient и т.д.
 */
public interface ExchangeGateway {

    /** Короткое имя для логов и админки: "binance", "bybit" (строковый id из {@link Exchange}). */
    String id();

    /** Биржа как enum. */
    default Exchange exchange() { return Exchange.of(id()); }

    /** Запустить REST-инициализацию (синхронизация времени, фильтры, балансы) и WebSocket. */
    void start() throws Exception;

    /** Остановить фид, конвейер и приватные каналы. */
    void stop();

    /** Рыночные данные идут (WS или REST-запас). */
    boolean isConnected();

    /** Сколько сообщений получено с момента старта — для мониторинга. */
    long messageCount();

    /** Символы, которые эта биржа сейчас слушает. */
    List<String> symbols();



    /** Рыночные данные именно этой биржи. */
    MarketDataStore marketData();

    /** Балансы именно на этой бирже. */
    BalanceStore balances();

    /** Сервис ордеров, уже привязанный к REST-клиенту и риск-менеджеру этой биржи. */
    OrderService orders();

    /** Риск-менеджер этой биржи — у каждой биржи свой kill switch и счётчики. */
    RiskManager risk();

    /** Стратегия, работающая на этой бирже. Одна и та же логика может крутиться на нескольких биржах параллельно. */
    /** Все стратегии биржи (возврат к среднему, треугольный и статистический арбитраж). */
    StrategySet strategy();

    /** Периодическая пересинхронизация балансов с биржей — вызывается планировщиком из Main. */
    void syncBalances();

    /** Фьючерсный счёт (позиции, funding); null — биржа торгует спотом. */
    default com.hft.perp.PerpAccount perp() { return null; }
}
