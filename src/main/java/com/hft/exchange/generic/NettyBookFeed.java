package com.hft.exchange.generic;

import com.hft.net.AbstractWsFeed;

import java.util.List;
import java.util.Map;

/**
 * Свой Netty-фид биржи (Binance, Bybit — {@link AbstractWsFeed}) в виде {@link BookFeed} для {@link SignedCexExchange}.
 * REST-запаса у такого фида нет: он сам переподключается с нарастающей паузой и не сдаётся;
 * пока данных нет или они старше wsStaleMs, {@link #isRealtime()} = false и стратегии не открывают позиции.
 */
public final class NettyBookFeed implements BookFeed {

    /** Фид биржи. */
    private final AbstractWsFeed ws;

    /** @param ws фид биржи */
    public NettyBookFeed(AbstractWsFeed ws) { this.ws = ws; }

    /** Подключиться (с переподключением при обрыве). */
    @Override
    public void start() {
        try { ws.start(); }
        catch (RuntimeException e) { throw e; }
        catch (Exception e) { throw new IllegalStateException("фид не запустился: " + e.getMessage(), e); }
    }

    /** Отключиться. */
    @Override public void stop() { ws.stop(); }
    /** Символы, по которым идут данные. */
    @Override public List<String> activeSymbols() { return ws.activeSymbols(); }
    /** Соединение живо. */
    @Override public boolean isConnected() { return ws.isConnected(); }
    /** Данные свежие — можно открывать позиции. */
    @Override public boolean isRealtime() { return ws.isRealtime(); }
    /** Не сдаётся: переподключается сам. */
    @Override public boolean hasGivenUp() { return false; }
    /** Получено сообщений. */
    @Override public long messageCount() { return ws.messageCount(); }

    /** Метрики для админки. */
    @Override
    public Map<String, Object> stats() {
        return ws.connectionStats();
    }

    /** Исходный фид (для тестов). */
    public AbstractWsFeed ws() { return ws; }
}
