package com.hft.net;

import com.hft.rest.RateBudget;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.handler.codec.http.websocketx.extensions.compression.WebSocketClientCompressionHandler;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Общая часть WebSocket-клиента для любой биржи: TLS-хендшейк, Netty
 * pipeline, переподключение, контроль тишины, пинги.
 *
 * <p>Надёжность соединения:
 * <ul>
 *   <li>одна цепочка переподключения: обрыв, ошибка и неудачный коннект ведут в {@link #scheduleReconnect()},
 *       повторный вызов, пока попытка уже запланирована, ничего не делает;</li>
 *   <li>пауза растёт вдвое от {@code baseBackoffMs} до 30 с, со случайным разбросом ±20 % — чтобы после сбоя
 *       сети сокеты не ломились на биржу одновременно; счётчик сбрасывается только после первого кадра данных,
 *       а не после рукопожатия — соединение, которое сразу рвётся, не крутится в быстром цикле;</li>
 *   <li>подключение и рукопожатие ограничены по времени (5 с и 10 с) и идут не в потоке Netty;</li>
 *   <li>тишина дольше {@code staleMs} — соединение закрывается и переподключается;</li>
 *   <li>на ping сервера — pong с тем же содержимым; свой пинг уровня приложения — {@link #heartbeat()}.</li>
 * </ul>
 *
 * У каждой биржи свой формат URL для подписки и свой JSON сообщений —
 * это остаётся в наследнике через {@link #buildUri()} и {@link #onText(String, long)}.
 */
public abstract class AbstractWsFeed {

    /** Логгер наследника. */
    protected final Logger log = LoggerFactory.getLogger(getClass());

    /** Таймаут TCP-подключения, мс. */
    private static final int CONNECT_TIMEOUT_MS = 5_000;
    /** Таймаут WebSocket-рукопожатия, мс. */
    private static final long HANDSHAKE_TIMEOUT_MS = 10_000;
    /** Верхняя граница паузы перед переподключением, мс. */
    private static final long MAX_BACKOFF_MS = 30_000;

    /** Фид запущен. */
    private final AtomicBoolean running = new AtomicBoolean(false);
    /** Попытка переподключения уже запланирована — вторую цепочку не заводим. */
    private final AtomicBoolean reconnectPending = new AtomicBoolean(false);
    /** Попыток переподключения подряд (для нарастающей паузы). */
    private final AtomicInteger reconnectAttempts = new AtomicInteger();
    /** Получено сообщений. */
    private final AtomicInteger messageCount = new AtomicInteger();
    /** Переподключений за всё время. */
    private final AtomicLong reconnects = new AtomicLong();

    /** Потоки Netty. */
    private volatile EventLoopGroup group;
    /** Текущее соединение. */
    private volatile Channel channel;
    /** Когда пришёл последний кадр, мс. */
    private volatile long lastFrameMs;
    /** Последняя ошибка соединения — для админки. */
    private volatile String lastError = "";
    /** Тишина, после которой соединение пересоздаётся, мс. */
    private volatile long staleMs = 15_000;
    /** Начальная пауза перед переподключением, мс. */
    private volatile long baseBackoffMs = 1_000;

    /** Вызывается после обновления стакана символа (бумажный движок сводит заявки); по умолчанию — ничего. */
    protected volatile java.util.function.Consumer<String> onBook = s -> {};

    /** Задать обработчик обновления стакана. */
    public void onBook(java.util.function.Consumer<String> handler) { this.onBook = handler; }

    /**
     * Настроить контроль тишины и паузу переподключения (до {@link #start()}).
     * @param staleMs тишина, после которой переподключение, мс; 0 — оставить по умолчанию (15 с)
     * @param baseBackoffMs начальная пауза перед переподключением, мс
     */
    public AbstractWsFeed tune(long staleMs, long baseBackoffMs) {
        if (staleMs > 0) this.staleMs = staleMs;
        if (baseBackoffMs > 0) this.baseBackoffMs = baseBackoffMs;
        return this;
    }

    /** Полный URL для подключения, включая параметры подписки. Вызывается при каждом (пере)подключении. */
    protected abstract URI buildUri() throws Exception;

    /** Символы, по которым идут данные. */
    public abstract java.util.List<String> activeSymbols();

    /** Разбор одного текстового сообщения. receivedNanos — момент получения, до парсинга. */
    protected abstract void onText(String json, long receivedNanos);

    /** Что сделать сразу после успешного хендшейка — например, сбросить локальные стаканы и отправить подписку. */
    protected void onHandshakeComplete(Channel channel) { }

    /** Пинг уровня приложения (например, {"op":"ping"} у Bybit); null — не нужен. */
    protected String heartbeat() { return null; }

    /** Как часто слать {@link #heartbeat()}, мс. */
    protected long heartbeatIntervalMs() { return 20_000; }

    /** Подключиться; при обрыве переподключаться с нарастающей паузой. Неудачный первый коннект старт не роняет. */
    public final synchronized void start() throws Exception {
        if (running.get()) return;
        running.set(true);
        reconnectPending.set(false);
        reconnectAttempts.set(0);
        group = new NioEventLoopGroup(1, r -> {
            Thread t = new Thread(r, name() + "-ws");
            t.setDaemon(true);
            return t;
        });
        try {
            connect();
        } catch (Exception e) {                      // биржа недоступна на старте — не валим старт, переподключаемся
            fail("подключение не удалось", e);
        }
    }

    /** Установить соединение и WebSocket-рукопожатие (с учётом лимита подключений). Вызывается не из потока Netty. */
    private void connect() throws Exception {
        RateBudget.of(name()).acquire(RateBudget.Kind.WS_CONNECT, 1, 60_000);   // лимит подключений на IP
        if (!running.get()) return;
        URI uri = buildUri();
        boolean tls = !"ws".equalsIgnoreCase(uri.getScheme());          // wss — TLS, ws — без (локальные стенды, тесты)
        int port = uri.getPort() > 0 ? uri.getPort() : tls ? 443 : 80;
        SslContext ssl = tls ? SslContextBuilder.forClient().build() : null;

        WebSocketClientHandshaker handshaker = WebSocketClientHandshakerFactory.newHandshaker(
                uri, WebSocketVersion.V13, null, true, new DefaultHttpHeaders(), 1 << 20);

        Handler handler = new Handler(handshaker);
        long staleSec = Math.max(1, staleMs / 1000);

        Bootstrap bootstrap = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECT_TIMEOUT_MS)
                .handler(new ChannelInitializer<Channel>() {
                    /** Цепочка Netty: TLS, HTTP-кодек, агрегатор, контроль тишины, обработчик WebSocket. */
                    @Override
                    protected void initChannel(Channel ch) {
                        ChannelPipeline p = ch.pipeline();
                        if (ssl != null) p.addLast(ssl.newHandler(ch.alloc(), uri.getHost(), port));
                        p.addLast(new HttpClientCodec());
                        p.addLast(new HttpObjectAggregator(1 << 20));
                        p.addLast(WebSocketClientCompressionHandler.INSTANCE);
                        p.addLast(new IdleStateHandler(staleSec, 0, 0, TimeUnit.SECONDS));
                        p.addLast(handler);
                    }
                });

        log.info("[{}] Подключаюсь: {}", name(), uri.getHost());
        ChannelFuture cf = bootstrap.connect(uri.getHost(), port);
        if (!cf.await(CONNECT_TIMEOUT_MS + 1_000L) || !cf.isSuccess()) {
            cf.channel().close();
            throw new IllegalStateException("TCP: " + (cf.cause() != null ? cf.cause().toString() : "таймаут"));
        }
        Channel ch = cf.channel();
        if (!handler.handshakeFuture.await(HANDSHAKE_TIMEOUT_MS) || !handler.handshakeFuture.isSuccess()) {
            ch.close();
            Throwable c = handler.handshakeFuture.cause();
            throw new IllegalStateException("рукопожатие: " + (c != null ? c.toString() : "таймаут " + HANDSHAKE_TIMEOUT_MS + " мс"));
        }
        if (!running.get()) { ch.close(); return; }        // остановили, пока подключались
        channel = ch;
        lastFrameMs = System.currentTimeMillis();
        handler.live = true;                               // с этого момента обрыв этого канала ведёт к переподключению
        onHandshakeComplete(ch);
        startHeartbeat(ch);
        if (!ch.isActive()) { handler.live = false; throw new IllegalStateException("соединение закрылось сразу после рукопожатия"); }
        log.info("[{}] Поток данных запущен", name());
    }

    /** Пинг уровня приложения по таймеру — пока жив канал. */
    private void startHeartbeat(Channel ch) {
        String hb = heartbeat();
        if (hb == null) return;
        long every = heartbeatIntervalMs();
        ch.eventLoop().scheduleAtFixedRate(() -> {
            if (ch.isActive()) ch.writeAndFlush(new TextWebSocketFrame(hb));
        }, every, every, TimeUnit.MILLISECONDS);
    }

    /** Записать ошибку и запланировать переподключение. */
    private void fail(String what, Throwable e) {
        lastError = what + ": " + e;
        log.warn("[{}] {}", name(), lastError);
        scheduleReconnect();
    }

    /** Переподключиться после паузы (2^n × baseBackoffMs, не больше 30 с, ±20 %); одна цепочка на фид. */
    private void scheduleReconnect() {
        if (!running.get() || !reconnectPending.compareAndSet(false, true)) return;
        int attempt = reconnectAttempts.incrementAndGet();
        long base = Math.min(baseBackoffMs << Math.min(attempt - 1, 10), MAX_BACKOFF_MS);
        long delay = Math.max(50, (long) (base * (0.8 + 0.4 * java.util.concurrent.ThreadLocalRandom.current().nextDouble())));
        log.warn("[{}] Переподключение через {} мс (попытка {})", name(), delay, attempt);
        reconnects.incrementAndGet();
        EventLoopGroup g = group;
        try {
            // connect() ждёт соединения — в потоке Netty это запрещено, поэтому подключаемся в отдельном потоке
            g.schedule(() -> Thread.ofVirtual().name(name() + "-reconnect").start(() -> {
                reconnectPending.set(false);
                if (!running.get()) return;
                try {
                    connect();
                } catch (Exception e) {
                    fail("переподключение не удалось", e);
                }
            }), delay, TimeUnit.MILLISECONDS);
        } catch (java.util.concurrent.RejectedExecutionException e) {
            reconnectPending.set(false);                  // группа остановлена — фид тоже
        }
    }

    /** Остановить фид и потоки Netty. */
    public final synchronized void stop() {
        running.set(false);
        Channel ch = channel;
        if (ch != null) ch.close();
        EventLoopGroup g = group;
        if (g != null) g.shutdownGracefully(0, 2, TimeUnit.SECONDS);
        log.info("[{}] Поток данных остановлен", name());
    }

    /** Соединение открыто. */
    public final boolean isConnected() {
        Channel ch = channel;
        return running.get() && ch != null && ch.isActive();
    }

    /** Соединение открыто и данные свежие (не старше staleMs). */
    public final boolean isRealtime() {
        return isConnected() && System.currentTimeMillis() - lastFrameMs < staleMs;
    }

    /** Получено сообщений. */
    public final int messageCount() { return messageCount.get(); }

    /** Метрики соединения — для админки. */
    public final java.util.Map<String, Object> connectionStats() {
        var m = new java.util.LinkedHashMap<String, Object>();
        m.put("ws", isConnected());
        m.put("messages", messageCount.get());
        m.put("reconnects", reconnects.get());
        m.put("lastFrameAgeMs", lastFrameMs == 0 ? -1 : System.currentTimeMillis() - lastFrameMs);
        m.put("lastError", lastError);
        return m;
    }

    /** Отправить текстовый фрейм в уже установленное соединение — для подписки/отписки на лету. */
    protected final void send(String text) {
        Channel ch = channel;
        if (ch != null && ch.isActive()) {
            // поток Netty ждать не должен; сообщения здесь — редкие подписки
            if (!RateBudget.of(name()).tryAcquire(RateBudget.Kind.WS_MESSAGE, 1))
                log.warn("[{}] WS: бюджет сообщений исчерпан, сообщение отправляется сверх него", name());
            ch.writeAndFlush(new TextWebSocketFrame(text));
        }
    }

    /** Короткое имя для логов — переопределяется наследником. */
    protected String name() { return getClass().getSimpleName(); }

    /** Обработчик Netty: рукопожатие, текстовые кадры, пинги, ошибки. */
    private final class Handler extends SimpleChannelInboundHandler<Object> {
        /** Рукопожатие WebSocket. */
        private final WebSocketClientHandshaker handshaker;
        /** Завершается, когда рукопожатие прошло. */
        private volatile ChannelPromise handshakeFuture;
        /** Канал стал текущим: его обрыв ведёт к переподключению (обрыв на рукопожатии обрабатывает connect()). */
        private volatile boolean live;

        /** @param handshaker рукопожатие для этого соединения */
        Handler(WebSocketClientHandshaker handshaker) {
            this.handshaker = handshaker;
        }

        /** Создать признак завершения рукопожатия. */
        @Override
        public void handlerAdded(ChannelHandlerContext ctx) {
            handshakeFuture = ctx.newPromise();
        }

        /** Соединение установлено — начать WebSocket-рукопожатие. */
        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            handshaker.handshake(ctx.channel());
        }

        /** Соединение закрыто — переподключиться, если фид не остановлен. */
        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            if (handshakeFuture != null && !handshakeFuture.isDone())
                handshakeFuture.tryFailure(new IllegalStateException("соединение закрыто до рукопожатия"));
            if (!live) return;                            // не текущий канал — connect() сам решит, что делать
            live = false;
            if (!running.get()) return;
            lastError = "соединение закрыто";
            log.warn("[{}] Соединение закрыто", name());
            scheduleReconnect();
        }

        /** Тишина дольше staleMs — закрыть соединение (дальше переподключение). */
        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
            if (evt instanceof IdleStateEvent) {
                lastError = "тишина дольше " + staleMs + " мс";
                log.warn("[{}] Нет данных дольше {} мс, переподключаюсь", name(), staleMs);
                ctx.close();
            }
        }

        /** Кадр: завершение рукопожатия, текст — в onText, ping/pong/close. */
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
            Channel ch = ctx.channel();
            if (!handshaker.isHandshakeComplete()) {
                try {
                    handshaker.finishHandshake(ch, (FullHttpResponse) msg);
                    handshakeFuture.trySuccess();
                } catch (Exception e) {                   // не 101 (403, 429, 5xx) — ошибка рукопожатия, не канала
                    handshakeFuture.tryFailure(e);
                    ch.close();
                }
                return;
            }

            long received = System.nanoTime();
            lastFrameMs = System.currentTimeMillis();
            if (msg instanceof TextWebSocketFrame frame) {
                messageCount.incrementAndGet();
                if (reconnectAttempts.get() != 0) reconnectAttempts.set(0);   // данные пошли — соединение рабочее
                try {
                    onText(frame.text(), received);
                } catch (Exception e) {
                    log.error("[{}] Ошибка разбора сообщения", name(), e);
                }
            } else if (msg instanceof PingWebSocketFrame ping) {
                ch.writeAndFlush(new PongWebSocketFrame(ping.content().retain()));   // pong с тем же содержимым
            } else if (msg instanceof CloseWebSocketFrame) {
                ch.close();
            }
        }

        /** Ошибка канала — закрыть (переподключение по channelInactive). */
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            lastError = "ошибка канала: " + cause;
            log.error("[{}] Ошибка в канале: {}", name(), cause.toString());
            if (handshakeFuture != null) handshakeFuture.tryFailure(cause);
            ctx.close();
        }
    }
}
