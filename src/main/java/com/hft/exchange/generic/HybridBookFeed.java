package com.hft.exchange.generic;

import com.hft.config.ExchangeConfig;
import com.hft.exchange.catalog.ExchangeInfo;
import com.hft.store.MarketDataStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

/**
 * WebSocket — основной канал, REST-опрос — запасной. Пока WS жив, опрос стоит на паузе и не
 * тратит лимит запросов; если WS оборвался и не вернулся за {@code graceMs} (2 с) или сдался — опрос включается
 * (переключение по событию от WS, без ожидания таймера); пока данные идут с опроса, {@link #isRealtime()} = false
 * и стратегия новых позиций не открывает,
 * при возвращении WS снова выключается. Торговля останавливается (onGiveUp) только когда
 * сдался и запасной канал: пока хоть один даёт данные, стратегия работает.
 */
public final class HybridBookFeed implements BookFeed {

    /** Логгер. */
    private static final Logger log = LoggerFactory.getLogger(HybridBookFeed.class);

    /** Описание биржи из каталога. */
    private final ExchangeInfo info;
    /** WebSocket — основной источник. */
    private final WsBookFeed ws;
    /** REST-опрос — запасной. */
    private final PollingBookFeed poll;
    /** Сколько ждать восстановления WS перед включением опроса, мс. */
    private volatile long graceMs = 2_000;
    /** Фид запущен. */
    private volatile boolean running;
    /** С какого момента WS не работает (0 — работает). */
    private volatile long wsDownSince;
    /** Поток, переключающий WS и опрос. */
    private Thread supervisor;

    public HybridBookFeed(ExchangeInfo info, ExchangeConfig cfg, WsDialect wsDialect, BookDialect restDialect,
                          MarketDataStore market, TickSink onTick, Consumer<String> onBook, Runnable onGiveUp) {
        this(info, new WsBookFeed(info, cfg, wsDialect, market, onTick, onBook,
                        () -> log.warn("[{}] WS сдался, остаётся REST-опрос", info.id())),
                new PollingBookFeed(info, cfg, restDialect, market, onTick, onBook, onGiveUp));
    }

    /** Собрать из готовых WS- и REST-фидов (тесты). */
    HybridBookFeed(ExchangeInfo info, WsBookFeed ws, PollingBookFeed poll) {
        this.info = info;
        this.ws = ws;
        this.poll = poll;
        ws.onStateChange(this::wake);            // обрыв/подключение WS — переключаемся сразу, а не по таймеру
    }

    /** Монитор для пробуждения супервизора. */
    private final Object signal = new Object();

    /** Разбудить супервизор. */
    private void wake() { synchronized (signal) { signal.notifyAll(); } }

    /** Сколько ждать восстановления WS перед включением опроса, мс. */
    public HybridBookFeed grace(long ms) { this.graceMs = ms; return this; }

    /** WebSocket-половина (для настройки таймаутов). */
    WsBookFeed ws() { return ws; }

    /** REST-половина (для настройки паузы после 429). */
    PollingBookFeed poll() { return poll; }

    @Override public synchronized void start() {
        if (running) return;
        running = true;
        wsDownSince = System.currentTimeMillis();
        poll.pause();
        ws.start();
        poll.start();
        supervisor = new Thread(this::supervise, "feed-sup-" + info.id());
        supervisor.setDaemon(true);
        supervisor.start();
    }

    @Override public synchronized void stop() {
        running = false;
        ws.stop();
        poll.stop();
        if (supervisor != null) supervisor.interrupt();
        wake();
    }

    /** Включать опрос, пока WS не работает дольше grace или сдался; выключать, когда WS вернулся. */
    private void supervise() {
        boolean pollingOn = false;
        while (running) {
            try {
                long now = System.currentTimeMillis();
                if (ws.isConnected()) wsDownSince = 0;
                else if (wsDownSince == 0) wsDownSince = now;
                boolean needPoll = ws.hasGivenUp() || (wsDownSince != 0 && now - wsDownSince >= graceMs);
                if (needPoll && !pollingOn) {
                    log.warn("[{}] WS недоступен — включаю REST-опрос", info.id());
                    poll.resume();
                    pollingOn = true;
                } else if (!needPoll && pollingOn) {
                    log.info("[{}] WS восстановлен — REST-опрос на паузе", info.id());
                    poll.pause();
                    pollingOn = false;
                }
                // ждём события от WS; таймаут нужен, чтобы отсчитать grace и заметить «тишину»
                synchronized (signal) { signal.wait(needPoll || wsDownSince == 0 ? 1_000 : Math.max(10, graceMs - (now - wsDownSince))); }
            } catch (InterruptedException e) {
                return;
            }
        }
    }

    @Override public List<String> activeSymbols() { return poll.activeSymbols(); }
    @Override public boolean isConnected() { return ws.isConnected() || poll.isConnected(); }
    @Override public boolean isRealtime() { return ws.isRealtime(); }
    @Override public boolean hasGivenUp() { return poll.hasGivenUp(); }
    @Override public long messageCount() { return ws.messageCount() + poll.messageCount(); }

    @Override public Map<String, Object> stats() {
        var m = new LinkedHashMap<String, Object>();
        m.put("transport", "hybrid");
        m.put("ws", ws.stats());
        m.put("polling", poll.stats());
        m.put("pollingPaused", poll.isPaused());
        return m;
    }
}
