package com.hft.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import com.hft.net.WsClient;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Приватный WebSocket биржи (или JSON-RPC-сокет ноды): запросы с ответом по id,
 * логин, подписки на приватные потоки и push-события.
 *
 * <p>Безопасность ордеров:
 * <ul>
 *   <li>
 *     {@link WsNotReadyException} — запрос НЕ отправлялся (нет соединения или
 *     логина): можно идти в REST.
 *   </li>
 *   <li>
 *     {@link WsUnknownOutcomeException} — запрос ушёл (или мог уйти), но ответа
 *     нет: исход неизвестен, вслепую повторять нельзя — вызывающий сначала
 *     выясняет судьбу ордера.
 *   </li>
 * </ul>
 *
 * <p>
 * Переподключение с паузой до 30 с. Пять неудачных логинов подряд — канал
 * отключается (остаётся REST), чтобы не долбить биржу неверными ключами.
 */
public final class WsRpcChannel {

    /** Логгер. */
    private static final Logger log = LoggerFactory.getLogger(WsRpcChannel.class);
    /** Отказов логина подряд, после которых канал выключается (ключи, скорее всего, неверны). */
    private static final int MAX_LOGIN_FAILURES = 5;
    /** Через сколько отключённый канал пробует снова (ключи могли исправить, сбой биржи — пройти), мс. */
    private volatile long disabledRetryMs = 10 * 60_000;

    /** Что за сообщение: подтверждение логина, ответ на запрос, событие, служебное. */
    public enum Kind { LOGIN_OK, REPLY, EVENT, IGNORE }

    /** @param id для REPLY; @param text тело (REPLY/EVENT); @param reply немедленный ответ (pong) или null */
    public record Msg(Kind kind, String id, String text, String reply) {
        /** Служебное сообщение — пропустить. */
        public static Msg ignore() { return new Msg(Kind.IGNORE, null, null, null); }
        /** Логин подтверждён. */
        public static Msg loginOk() { return new Msg(Kind.LOGIN_OK, null, null, null); }
        /** Ответ на запрос с id. */
        public static Msg reply(String id, String text) { return new Msg(Kind.REPLY, id, text, null); }
        /** Событие (ордера, исполнения, баланс). */
        public static Msg event(String text) { return new Msg(Kind.EVENT, null, text, null); }
        /** Служебное сообщение, на которое надо сразу ответить (pong, подпись приветствия). */
        public static Msg answer(String reply) { return new Msg(Kind.IGNORE, null, null, reply); }
    }

    /** Протокол биржи: адрес, логин, подписки, пинг, разбор. */
    public interface Protocol {
        /** Адрес приватного WebSocket. */
        String url();
        /** Сообщения логина сразу после подключения; пусто — логина нет. */
        List<String> login() throws Exception;
        /**
         * Логин начинает сервер (KuCoin: приветствие, на которое клиент отвечает подписью через Msg.reply).
         * Тогда при пустом {@link #login()} канал ждёт LOGIN_OK от parse, а не считает себя залогиненным сразу.
         */
        default boolean serverInitiatedLogin() { return false; }
        /** Подписки на приватные потоки, после логина. */
        List<String> subscriptions() throws Exception;
        /** Разбор входящего. Исключение до логина = отказ в логине; после — просто ошибка в статистике. */
        Msg parse(String text) throws Exception;
        /** Прикладной пинг; null — не нужен. */
        default String ping() { return null; }
        /** Интервал пинга, мс. */
        default long pingIntervalMs() { return 20_000; }
        /** Бинарный кадр в текст (gzip у части бирж). */
        default String decodeBinary(byte[] d) throws Exception { return new String(d, StandardCharsets.UTF_8); }
    }

    /** WS не готов — запрос надо отправить по REST. */
    public static final class WsNotReadyException extends RuntimeException {
        /** Без стека: исключение ожидаемое и частое. */
        public WsNotReadyException() { super("WS-канал не готов", null, false, false); }
    }

    /** Запрос ушёл, но ответа нет (обрыв) — исход неизвестен, надо проверить по REST. */
    public static final class WsUnknownOutcomeException extends Exception {
        /** @param m описание */
        public WsUnknownOutcomeException(String m) { super(m, null, false, false); }
    }

    /** Параметры соединения биржи (предел сообщения, таймауты, плановое переподключение). */
    private final com.hft.net.WsSettings ws;
    /** Биржа (для логов и бюджета лимитов). */
    private final String name;
    /** Протокол биржи. */
    private final Protocol protocol;
    /** Ожидающие ответа запросы по id. */
    private final Map<String, CompletableFuture<String>> pending = new ConcurrentHashMap<>();

    /** Куда отдаются события. */
    private volatile EventSink eventSink = e -> {};
    /** Состояние: работает, соединение есть, логин подтверждён, выключен после отказов логина. */
    private volatile boolean running, connected, loggedIn, disabled;
    /** Текущий сокет. */
    private volatile WsClient.Connection socket;
    /** Очередь отправки. */
    private volatile WsSender sender;
    /** Время последнего кадра. */
    private volatile long lastFrameMs;
    /** Отказов логина подряд. */
    private volatile int loginFailures;
    /** Последняя ошибка (для метрик). */
    private volatile String lastError = "";
    /** Начальная пауза переподключения, мс. */
    private volatile long baseBackoffMs = 500;
    /** Тишина, после которой переподключение, мс. */
    private volatile long staleMs;
    /** Поток канала. */
    private Thread thread;

    private final AtomicLong calls = new AtomicLong(), unknown = new AtomicLong(), reconnects = new AtomicLong(),
            events = new AtomicLong(), parseErrors = new AtomicLong();

    /**
     * @param name биржа
     * @param protocol протокол биржи
     */
    public WsRpcChannel(String name, Protocol protocol) {
        this.name = name;
        this.protocol = protocol;
        this.ws = com.hft.net.WsSettings.forExchange(name);
        this.staleMs = protocol.ping() != null ? Math.max(30_000, protocol.pingIntervalMs() * 3) : 90_000;
    }

    /** Получатель событий канала. */
    public interface EventSink { void accept(String text) throws Exception; }

    /** Задать получателя событий. */
    public WsRpcChannel onEvent(EventSink sink) { this.eventSink = sink; return this; }

    /** Для тестов. */
    public WsRpcChannel tune(long staleMs, long baseBackoffMs) { this.staleMs = staleMs; this.baseBackoffMs = baseBackoffMs; return this; }

    /** Запустить поток канала. */
    public synchronized void start() {
        if (running) return;
        running = true;
        disabled = false;
        loginFailures = 0;
        thread = new Thread(this::loop, "wsrpc-" + name);
        thread.setDaemon(true);
        thread.start();
    }

    /** Остановить канал; ожидающие запросы завершаются ошибкой. */
    public synchronized void stop() {
        running = false;
        WsClient.Connection w = socket;
        if (w != null) w.abort();
        failPending("канал остановлен");
        if (thread != null) thread.interrupt();
    }

    /** Можно отправлять запросы: соединение есть, логин подтверждён, канал не молчит. */
    public boolean isReady() { return running && !disabled && connected && loggedIn && System.currentTimeMillis() - lastFrameMs < staleMs; }
    /** Выключен после отказов логина. */
    public boolean isDisabled() { return disabled; }

    /** Метрики канала для админки. */
    public Map<String, Object> stats() {
        var m = new LinkedHashMap<String, Object>();
        m.put("ready", isReady());
        m.put("disabled", disabled);
        m.put("calls", calls.get());
        m.put("unknownOutcomes", unknown.get());
        m.put("events", events.get());
        m.put("reconnects", reconnects.get());
        m.put("parseErrors", parseErrors.get());
        m.put("lastError", lastError);
        return m;
    }

    /**
     * Отправить запрос и дождаться ответа с тем же id.
     * @throws WsNotReadyException запрос не отправлялся
     * @throws WsUnknownOutcomeException отправка/ожидание сорвались — исход неизвестен
     */
    public String call(String id, String payload, long timeoutMs) throws WsUnknownOutcomeException {
        WsClient.Connection w = socket;
        if (!isReady() || w == null) throw new WsNotReadyException();
        WsSender snd = sender;
        if (snd == null) throw new WsNotReadyException();
        CompletableFuture<String> f = new CompletableFuture<>();
        pending.put(id, f);
        calls.incrementAndGet();
        // отправка не блокирует; если она сорвётся — ответ не придёт, будим ожидающего сразу
        snd.send(payload).whenComplete((v, e) -> {
            if (e != null) {
                lastError = "send: " + e;
                CompletableFuture<String> p = pending.remove(id);
                if (p != null) p.completeExceptionally(new WsUnknownOutcomeException("не удалось отправить: " + e.getMessage()));
            }
        });
        try {
            return f.get(timeoutMs, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            pending.remove(id);
            unknown.incrementAndGet();
            throw new WsUnknownOutcomeException("нет ответа за " + timeoutMs + " мс");
        } catch (ExecutionException e) {
            unknown.incrementAndGet();
            throw new WsUnknownOutcomeException(String.valueOf(e.getCause().getMessage()));
        } catch (InterruptedException e) {
            pending.remove(id);
            Thread.currentThread().interrupt();
            unknown.incrementAndGet();
            throw new WsUnknownOutcomeException("прервано");
        }
    }

    // ---------------------------------------------------------------- цикл

    /** Как часто отключённый канал пробует снова, мс (тесты). */
    public WsRpcChannel disabledRetry(long ms) { this.disabledRetryMs = ms; return this; }

    /**
     * Подключаться и переподключаться с нарастающей паузой. После {@value #MAX_LOGIN_FAILURES} отказов логина
     * канал отключается (ордера — по REST), но через {@code disabledRetryMs} пробует снова.
     */
    private void loop() {
        int failures = 0;
        while (running) {
            if (disabled) {
                try { Thread.sleep(disabledRetryMs); } catch (InterruptedException e) { return; }
                if (!running) return;
                log.info("[{}] WS-канал: новая попытка после отключения", name);
                disabled = false;
                loginFailures = 0;
                failures = 0;
            }
            long start = System.currentTimeMillis();
            try {
                serve();
            } catch (InterruptedException e) {
                return;
            } catch (Exception e) {
                lastError = e.getClass().getSimpleName() + ": " + e.getMessage();
                log.warn("[{}] WS-канал: {}", name, lastError);
            }
            connected = false;
            loggedIn = false;
            failPending("соединение потеряно");
            if (!running) return;
            if (disabled) continue;
            failures = System.currentTimeMillis() - start > 60_000 ? 1 : failures + 1;   // долгая сессия — сбрасываем счётчик
            reconnects.incrementAndGet();
            long pause = (long) (Math.min(30_000, baseBackoffMs << Math.min(failures, 10))
                    * (0.8 + 0.4 * java.util.concurrent.ThreadLocalRandom.current().nextDouble()));   // ±20 %: сокеты не ломятся разом
            try { Thread.sleep(pause); } catch (InterruptedException e) { return; }
        }
    }

    /** Одно соединение: логин, подписки, пинг, контроль тишины. */
    private void serve() throws Exception {
        CompletableFuture<Void> closed = new CompletableFuture<>();
        Listener l = new Listener(closed);
        RateBudget.of(name).acquire(RateBudget.Kind.WS_CONNECT, 1, 60_000);   // лимит подключений на IP
        WsClient.Connection w = WsClient.connect(URI.create(protocol.url()), l, ws);
        long connectedAt = System.currentTimeMillis();
        socket = w;                                   // очередь отправки создана в onOpen — до первого входящего сообщения
        lastFrameMs = System.currentTimeMillis();
        connected = true;
        try {
            List<String> login = protocol.login();
            if (login.isEmpty()) { if (!protocol.serverInitiatedLogin()) afterLogin(w); }
            else for (String m : login) { RateBudget.of(name).acquire(RateBudget.Kind.WS_MESSAGE, 1, 60_000); sender.send(m); }

            long lastPing = System.currentTimeMillis();
            long loginDeadline = lastPing + 10_000;
            while (running && !closed.isDone() && !l.abort) {
                try { closed.get(300, TimeUnit.MILLISECONDS); } catch (TimeoutException ignored) {}
                long now = System.currentTimeMillis();
                if (!loggedIn && now > loginDeadline) { noteLoginFailure(); throw new IllegalStateException("нет подтверждения логина"); }
                if (now - lastFrameMs > staleMs) throw new IllegalStateException("тишина " + (now - lastFrameMs) + " мс");
                // биржа сама рвёт соединение через сутки — переподключаемся заранее, когда нет запросов в полёте
                if (ws.maxLifetimeMs() > 0 && now - connectedAt > ws.maxLifetimeMs() && pending.isEmpty())
                    throw new IllegalStateException("плановое переподключение");
                String ping = protocol.ping();
                if (ping != null && now - lastPing >= protocol.pingIntervalMs()) { lastPing = now; sender.send(ping); }
            }
            if (closed.isCompletedExceptionally()) closed.get();
            if (running && !disabled) throw new IllegalStateException(l.abort ? "сброс: " + lastError : "сервер закрыл соединение");
        } finally {
            connected = false;
            loggedIn = false;
            w.abort();
        }
    }

    /** Логин подтверждён — отправить подписки. */
    private void afterLogin(WsClient.Connection w) throws Exception {
        loggedIn = true;
        loginFailures = 0;
        for (String m : protocol.subscriptions()) {
            // после логина вызывается из потока чтения — ждать нельзя; сообщений здесь единицы
            if (!RateBudget.of(name).tryAcquire(RateBudget.Kind.WS_MESSAGE, 1))
                log.warn("[{}] WS: бюджет сообщений исчерпан, подписка отправляется сверх него", name);
            sender.send(m);
        }
    }

    /** Учесть отказ логина; после MAX_LOGIN_FAILURES выключить канал. */
    private void noteLoginFailure() {
        if (++loginFailures >= MAX_LOGIN_FAILURES) {
            disabled = true;
            log.error("[{}] {} неудачных логинов подряд — WS-канал отключён, остаётся REST ({})", name, loginFailures, lastError);
        }
    }

    /** Завершить все ожидающие запросы ошибкой «исход неизвестен». */
    private void failPending(String why) {
        for (var e : pending.entrySet()) {
            CompletableFuture<String> f = pending.remove(e.getKey());
            if (f != null) f.completeExceptionally(new WsUnknownOutcomeException(why));
        }
    }

    /** Разобрать сообщение и раздать: логин, ответ, событие, pong. */
    private void handle(WsClient.Connection w, String text, Listener l) {
        try {
            Msg m = protocol.parse(text);
            if (m.reply() != null) sender.send(m.reply());
            switch (m.kind()) {
                case LOGIN_OK -> { if (!loggedIn) afterLogin(w); }
                case REPLY -> {
                    CompletableFuture<String> f = pending.remove(m.id());
                    if (f != null) f.complete(m.text());
                }
                case EVENT -> {
                    try { eventSink.accept(m.text()); }
                    catch (Exception e) { lastError = "event: " + e; parseErrors.incrementAndGet(); }
                    finally { events.incrementAndGet(); }   // после обработки: метрика = обработанные события
                }
                case IGNORE -> {}
            }
        } catch (Exception e) {
            lastError = (loggedIn ? "parse: " : "login: ") + e.getMessage();
            parseErrors.incrementAndGet();
            log.warn("[{}] WS-канал: {}", name, lastError);
            if (!loggedIn) { noteLoginFailure(); l.abort = true; w.abort(); }
        }
    }

    /** Обработчик соединения (поток Netty): сообщения, закрытие. */
    private final class Listener implements WsClient.Listener {
        /** Завершается при закрытии сокета. */
        private final CompletableFuture<Void> closed;
        /** Запрошен сброс соединения из обработчика. */
        volatile boolean abort;

        Listener(CompletableFuture<Void> closed) { this.closed = closed; }

        /** Очередь отправки — сразу при открытии: сервер может написать первым (KuCoin: приветствие), и ответ нужен немедленно. */
        @Override public void onOpen(WsClient.Connection w) { sender = new WsSender(w, 1000); }

        @Override public void onText(WsClient.Connection w, char[] buf, int len) {
            lastFrameMs = System.currentTimeMillis();
            handle(w, new String(buf, 0, len), this);
        }

        @Override public void onBinary(WsClient.Connection w, byte[] data) {
            lastFrameMs = System.currentTimeMillis();
            try { handle(w, protocol.decodeBinary(data), this); }
            catch (Exception e) { lastError = "binary: " + e; parseErrors.incrementAndGet(); }
        }

        @Override public void onPing(WsClient.Connection w) { lastFrameMs = System.currentTimeMillis(); }
        @Override public void onClose(WsClient.Connection w, int code, String reason) { lastError = "close " + code + " " + reason; closed.complete(null); }
        @Override public void onError(WsClient.Connection w, Throwable e) { lastError = "ws error: " + e; closed.completeExceptionally(e); }
    }
}
