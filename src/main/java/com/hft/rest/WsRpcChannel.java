package com.hft.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Приватный WebSocket биржи (или JSON-RPC-сокет ноды): запросы с ответом по id, логин,
 * подписки на приватные потоки и push-события.
 *
 * Безопасность ордеров:
 *  - {@link WsNotReadyException} — запрос НЕ отправлялся (нет соединения или логина): можно идти в REST;
 *  - {@link WsUnknownOutcomeException} — запрос ушёл (или мог уйти), но ответа нет: исход неизвестен,
 *    вслепую повторять нельзя — вызывающий сначала выясняет судьбу ордера.
 *
 * Переподключение с паузой до 30 с. Пять неудачных логинов подряд — канал отключается
 * (остаётся REST), чтобы не долбить биржу неверными ключами.
 */
public final class WsRpcChannel {

    private static final Logger log = LoggerFactory.getLogger(WsRpcChannel.class);
    private static final int MAX_LOGIN_FAILURES = 5;

    public enum Kind { LOGIN_OK, REPLY, EVENT, IGNORE }

    /** @param id для REPLY; @param text тело (REPLY/EVENT); @param reply немедленный ответ (pong) или null */
    public record Msg(Kind kind, String id, String text, String reply) {
        public static Msg ignore() { return new Msg(Kind.IGNORE, null, null, null); }
        public static Msg loginOk() { return new Msg(Kind.LOGIN_OK, null, null, null); }
        public static Msg reply(String id, String text) { return new Msg(Kind.REPLY, id, text, null); }
        public static Msg event(String text) { return new Msg(Kind.EVENT, null, text, null); }
    }

    public interface Protocol {
        String url();
        /** Сообщения логина сразу после подключения; пусто — логина нет. */
        List<String> login() throws Exception;
        /** Подписки на приватные потоки, после логина. */
        List<String> subscriptions() throws Exception;
        /** Разбор входящего. Исключение до логина = отказ в логине; после — просто ошибка в статистике. */
        Msg parse(String text) throws Exception;
        default String ping() { return null; }
        default long pingIntervalMs() { return 20_000; }
        default String decodeBinary(byte[] d) throws Exception { return new String(d, StandardCharsets.UTF_8); }
    }

    public static final class WsNotReadyException extends RuntimeException {
        public WsNotReadyException() { super("WS-канал не готов", null, false, false); }
    }

    public static final class WsUnknownOutcomeException extends Exception {
        public WsUnknownOutcomeException(String m) { super(m, null, false, false); }
    }

    private final String name;
    private final Protocol protocol;
    private final HttpClient http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(5)).build();
    private final Map<String, CompletableFuture<String>> pending = new ConcurrentHashMap<>();

    private volatile EventSink eventSink = e -> {};
    private volatile boolean running, connected, loggedIn, disabled;
    private volatile WebSocket socket;
    private volatile WsSender sender;
    private volatile long lastFrameMs;
    private volatile int loginFailures;
    private volatile String lastError = "";
    private volatile long baseBackoffMs = 500;
    private volatile long staleMs;
    private Thread thread;

    private final AtomicLong calls = new AtomicLong(), unknown = new AtomicLong(), reconnects = new AtomicLong(),
            events = new AtomicLong(), parseErrors = new AtomicLong();

    public WsRpcChannel(String name, Protocol protocol) {
        this.name = name;
        this.protocol = protocol;
        this.staleMs = protocol.ping() != null ? Math.max(30_000, protocol.pingIntervalMs() * 3) : 90_000;
    }

    public interface EventSink { void accept(String text) throws Exception; }

    public WsRpcChannel onEvent(EventSink sink) { this.eventSink = sink; return this; }

    /** Для тестов. */
    public WsRpcChannel tune(long staleMs, long baseBackoffMs) { this.staleMs = staleMs; this.baseBackoffMs = baseBackoffMs; return this; }

    public synchronized void start() {
        if (running) return;
        running = true;
        disabled = false;
        loginFailures = 0;
        thread = new Thread(this::loop, "wsrpc-" + name);
        thread.setDaemon(true);
        thread.start();
    }

    public synchronized void stop() {
        running = false;
        WebSocket w = socket;
        if (w != null) w.abort();
        failPending("канал остановлен");
        if (thread != null) thread.interrupt();
    }

    public boolean isReady() { return running && !disabled && connected && loggedIn && System.currentTimeMillis() - lastFrameMs < staleMs; }
    public boolean isDisabled() { return disabled; }

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
        WebSocket w = socket;
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

    private void loop() {
        int failures = 0;
        while (running && !disabled) {
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
            if (!running || disabled) return;
            failures = System.currentTimeMillis() - start > 60_000 ? 1 : failures + 1;   // долгая сессия — сбрасываем счётчик
            reconnects.incrementAndGet();
            try { Thread.sleep(Math.min(30_000, baseBackoffMs << Math.min(failures, 10))); } catch (InterruptedException e) { return; }
        }
    }

    private void serve() throws Exception {
        CompletableFuture<Void> closed = new CompletableFuture<>();
        Listener l = new Listener(closed);
        RateBudget.of(name).acquire(RateBudget.Kind.WS_CONNECT, 1, 60_000);   // лимит подключений на IP
        WebSocket w = http.newWebSocketBuilder().connectTimeout(Duration.ofSeconds(5))
                .buildAsync(URI.create(protocol.url()), l).get(10, TimeUnit.SECONDS);
        socket = w;
        sender = new WsSender(w, 1000);
        lastFrameMs = System.currentTimeMillis();
        connected = true;
        try {
            List<String> login = protocol.login();
            if (login.isEmpty()) afterLogin(w);
            else for (String m : login) { RateBudget.of(name).acquire(RateBudget.Kind.WS_MESSAGE, 1, 60_000); sender.send(m); }

            long lastPing = System.currentTimeMillis();
            long loginDeadline = lastPing + 10_000;
            while (running && !closed.isDone() && !l.abort) {
                try { closed.get(300, TimeUnit.MILLISECONDS); } catch (TimeoutException ignored) {}
                long now = System.currentTimeMillis();
                if (!loggedIn && now > loginDeadline) { noteLoginFailure(); throw new IllegalStateException("нет подтверждения логина"); }
                if (now - lastFrameMs > staleMs) throw new IllegalStateException("тишина " + (now - lastFrameMs) + " мс");
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

    private void afterLogin(WebSocket w) throws Exception {
        loggedIn = true;
        loginFailures = 0;
        for (String m : protocol.subscriptions()) {
            // после логина вызывается из потока чтения — ждать нельзя; сообщений здесь единицы
            if (!RateBudget.of(name).tryAcquire(RateBudget.Kind.WS_MESSAGE, 1))
                log.warn("[{}] WS: бюджет сообщений исчерпан, подписка отправляется сверх него", name);
            sender.send(m);
        }
    }

    private void noteLoginFailure() {
        if (++loginFailures >= MAX_LOGIN_FAILURES) {
            disabled = true;
            log.error("[{}] {} неудачных логинов подряд — WS-канал отключён, остаётся REST ({})", name, loginFailures, lastError);
        }
    }

    private void failPending(String why) {
        for (var e : pending.entrySet()) {
            CompletableFuture<String> f = pending.remove(e.getKey());
            if (f != null) f.completeExceptionally(new WsUnknownOutcomeException(why));
        }
    }

    private void handle(WebSocket w, String text, Listener l) {
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
                    events.incrementAndGet();
                    try { eventSink.accept(m.text()); }
                    catch (Exception e) { lastError = "event: " + e; parseErrors.incrementAndGet(); }
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

    private final class Listener implements WebSocket.Listener {
        private final CompletableFuture<Void> closed;
        private final StringBuilder text = new StringBuilder();
        private ByteBuffer bin = ByteBuffer.allocate(0);
        volatile boolean abort;

        Listener(CompletableFuture<Void> closed) { this.closed = closed; }

        @Override public void onOpen(WebSocket w) { w.request(1); }

        @Override public CompletionStage<?> onText(WebSocket w, CharSequence data, boolean last) {
            lastFrameMs = System.currentTimeMillis();
            text.append(data);
            if (last) { String s = text.toString(); text.setLength(0); handle(w, s, this); }
            w.request(1);
            return null;
        }

        @Override public CompletionStage<?> onBinary(WebSocket w, ByteBuffer data, boolean last) {
            lastFrameMs = System.currentTimeMillis();
            ByteBuffer merged = ByteBuffer.allocate(bin.remaining() + data.remaining());
            merged.put(bin).put(data).flip();
            bin = merged;
            if (last) {
                byte[] b = new byte[bin.remaining()];
                bin.get(b);
                bin = ByteBuffer.allocate(0);
                try { handle(w, protocol.decodeBinary(b), this); }
                catch (Exception e) { lastError = "binary: " + e; parseErrors.incrementAndGet(); }
            }
            w.request(1);
            return null;
        }

        @Override public CompletionStage<?> onPing(WebSocket w, ByteBuffer m) { lastFrameMs = System.currentTimeMillis(); w.request(1); return null; }
        @Override public CompletionStage<?> onPong(WebSocket w, ByteBuffer m) { lastFrameMs = System.currentTimeMillis(); w.request(1); return null; }
        @Override public CompletionStage<?> onClose(WebSocket w, int code, String reason) { lastError = "close " + code + " " + reason; closed.complete(null); return null; }
        @Override public void onError(WebSocket w, Throwable e) { lastError = "ws error: " + e; closed.completeExceptionally(e); }
    }
}
