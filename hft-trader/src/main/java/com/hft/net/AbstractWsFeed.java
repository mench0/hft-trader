package com.hft.net;

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

/**
 * Общая часть WebSocket-клиента для любой биржи: TLS-хендшейк, Netty
 * pipeline, переподключение с экспоненциальной задержкой, ответ на ping.
 *
 * У каждой биржи свой формат URL для подписки и свой JSON сообщений —
 * это остаётся в наследнике через {@link #buildUri()} и {@link #onText(String, long)}.
 * Всё остальное (что раньше было продублировано бы в каждом клиенте)
 * написано один раз здесь.
 */
public abstract class AbstractWsFeed {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private final AtomicBoolean running = new AtomicBoolean(false);
    private final AtomicInteger reconnectAttempts = new AtomicInteger();
    private final AtomicInteger messageCount = new AtomicInteger();

    private EventLoopGroup group;
    private volatile Channel channel;

    /** Полный URL для подключения, включая параметры подписки. Вызывается при каждом (пере)подключении. */
    protected abstract URI buildUri() throws Exception;

    /** Разбор одного текстового сообщения. receivedNanos — момент получения, до парсинга. */
    protected abstract void onText(String json, long receivedNanos);

    /** Что сделать сразу после успешного хендшейка — например, отправить подписку отдельным фреймом. */
    protected void onHandshakeComplete(Channel channel) { }

    public final void start() throws Exception {
        running.set(true);
        group = new NioEventLoopGroup(1, r -> {
            Thread t = new Thread(r, name() + "-ws");
            t.setDaemon(true);
            return t;
        });
        connect();
    }

    private void connect() throws Exception {
        URI uri = buildUri();
        int port = uri.getPort() > 0 ? uri.getPort() : 443;
        SslContext ssl = SslContextBuilder.forClient().build();

        WebSocketClientHandshaker handshaker = WebSocketClientHandshakerFactory.newHandshaker(
                uri, WebSocketVersion.V13, null, true, new DefaultHttpHeaders(), 1 << 20);

        Handler handler = new Handler(handshaker);

        Bootstrap bootstrap = new Bootstrap()
                .group(group)
                .channel(NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
                .handler(new ChannelInitializer<Channel>() {
                    @Override
                    protected void initChannel(Channel ch) {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(ssl.newHandler(ch.alloc(), uri.getHost(), port));
                        p.addLast(new HttpClientCodec());
                        p.addLast(new HttpObjectAggregator(1 << 20));
                        p.addLast(WebSocketClientCompressionHandler.INSTANCE);
                        p.addLast(new IdleStateHandler(60, 0, 0, TimeUnit.SECONDS));
                        p.addLast(handler);
                    }
                });

        log.info("[{}] Подключаюсь: {}", name(), uri.getHost());
        channel = bootstrap.connect(uri.getHost(), port).sync().channel();
        handler.handshakeFuture.sync();
        reconnectAttempts.set(0);
        onHandshakeComplete(channel);
        log.info("[{}] Поток данных запущен", name());
    }

    private void scheduleReconnect() {
        if (!running.get()) return;
        int attempt = reconnectAttempts.incrementAndGet();
        long delay = Math.min(1000L * (1L << Math.min(attempt, 5)), 30_000L);
        log.warn("[{}] Переподключение через {} мс (попытка {})", name(), delay, attempt);
        group.schedule(() -> {
            try {
                connect();
            } catch (Exception e) {
                log.error("[{}] Переподключение не удалось: {}", name(), e.getMessage());
                scheduleReconnect();
            }
        }, delay, TimeUnit.MILLISECONDS);
    }

    public final void stop() {
        running.set(false);
        if (channel != null) channel.close();
        if (group != null) group.shutdownGracefully(0, 2, TimeUnit.SECONDS);
        log.info("[{}] Поток данных остановлен", name());
    }

    public final boolean isConnected() {
        Channel ch = channel;
        return ch != null && ch.isActive();
    }

    public final int messageCount() { return messageCount.get(); }

    /** Отправить текстовый фрейм в уже установленное соединение — для подписки/отписки на лету. */
    protected final void send(String text) {
        Channel ch = channel;
        if (ch != null && ch.isActive()) {
            ch.writeAndFlush(new TextWebSocketFrame(text));
        }
    }

    /** Короткое имя для логов — переопределяется наследником. */
    protected String name() { return getClass().getSimpleName(); }

    private final class Handler extends SimpleChannelInboundHandler<Object> {
        private final WebSocketClientHandshaker handshaker;
        private ChannelPromise handshakeFuture;

        Handler(WebSocketClientHandshaker handshaker) {
            this.handshaker = handshaker;
        }

        @Override
        public void handlerAdded(ChannelHandlerContext ctx) {
            handshakeFuture = ctx.newPromise();
        }

        @Override
        public void channelActive(ChannelHandlerContext ctx) {
            handshaker.handshake(ctx.channel());
        }

        @Override
        public void channelInactive(ChannelHandlerContext ctx) {
            log.warn("[{}] Соединение закрыто", name());
            scheduleReconnect();
        }

        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
            if (evt instanceof IdleStateEvent) {
                log.warn("[{}] Нет данных 60 секунд, переподключаюсь", name());
                ctx.close();
            }
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
            Channel ch = ctx.channel();
            if (!handshaker.isHandshakeComplete()) {
                handshaker.finishHandshake(ch, (FullHttpResponse) msg);
                handshakeFuture.setSuccess();
                return;
            }

            long received = System.nanoTime();
            if (msg instanceof TextWebSocketFrame frame) {
                messageCount.incrementAndGet();
                try {
                    onText(frame.text(), received);
                } catch (Exception e) {
                    log.error("[{}] Ошибка разбора сообщения", name(), e);
                }
            } else if (msg instanceof PingWebSocketFrame) {
                ch.writeAndFlush(new PongWebSocketFrame());
            } else if (msg instanceof CloseWebSocketFrame) {
                ch.close();
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            log.error("[{}] Ошибка в канале: {}", name(), cause.getMessage());
            if (handshakeFuture != null && !handshakeFuture.isDone()) {
                handshakeFuture.setFailure(cause);
            }
            ctx.close();
        }
    }
}
