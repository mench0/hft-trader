package com.hft.net;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.*;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.FullHttpResponse;
import io.netty.handler.codec.http.HttpClientCodec;
import io.netty.handler.codec.http.HttpObjectAggregator;
import io.netty.handler.codec.http.websocketx.*;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * WebSocket-клиент на Netty для всех бирж: стакан ({@code WsBookFeed}), ордера и приватные потоки ({@code WsRpcChannel}).
 * <ul>
 *   <li>одна общая группа потоков на процесс (нативный epoll в Linux, иначе NIO), TCP_NODELAY;</li>
 *   <li>текст сообщения декодируется из буфера Netty в переиспользуемый {@code char[]} соединения — без строки
 *       на каждое сообщение; фрагменты склеиваются {@link WebSocketFrameAggregator};</li>
 *   <li>предел размера сообщения ({@link #MAX_MESSAGE} байт): больше — соединение рвётся, память не съедается;
 *       сжатие (permessage-deflate) не запрашивается — нет «бомб» распаковки;</li>
 *   <li>таймауты подключения (5 с) и рукопожатия (10 с); на ping сервера — pong с тем же содержимым.</li>
 * </ul>
 * Обработчики {@link Listener} вызываются в потоке Netty: блокировать в них нельзя.
 */
public final class WsClient {

    /** Логгер. */
    private static final Logger log = LoggerFactory.getLogger(WsClient.class);

    /** Предел размера одного сообщения, байт. */
    public static final int MAX_MESSAGE = 8 << 20;
    /** Таймаут TCP-подключения, мс. */
    private static final int CONNECT_TIMEOUT_MS = 5_000;
    /** Таймаут рукопожатия WebSocket, мс. */
    private static final long HANDSHAKE_TIMEOUT_MS = 10_000;

    /** Утилитный класс — экземпляры не создаются. */
    private WsClient() {}

    /** Обработчик событий соединения (вызывается в потоке Netty). */
    public interface Listener {
        /** Рукопожатие прошло — до первого сообщения. */
        default void onOpen(Connection c) {}
        /** Текстовое сообщение целиком: buf[0..len) — переиспользуемый буфер, копировать при необходимости. */
        void onText(Connection c, char[] buf, int len);
        /** Бинарное сообщение целиком. */
        default void onBinary(Connection c, byte[] data) {}
        /** Ping или pong от сервера (pong на ping клиент отправляет сам). */
        default void onPing(Connection c) {}
        /** Сервер закрыл соединение или оно оборвалось (кроме {@link Connection#abort()}, вызванного нами). */
        void onClose(Connection c, int code, String reason);
        /** Ошибка канала. */
        void onError(Connection c, Throwable e);
    }

    /** Открытое соединение. */
    public interface Connection {
        /** Отправить текст; future завершается, когда кадр записан в сокет. Потокобезопасно, порядок сохраняется. */
        CompletableFuture<Void> sendText(String text);
        /** Закрыть немедленно (onClose не вызывается). */
        void abort();
        /** Соединение открыто. */
        boolean isOpen();
    }

    // ───────────────────────── общая группа потоков ─────────────────────────

    /** Транспорт: нативный epoll, если доступен, иначе NIO. */
    private static final boolean EPOLL;
    /** Общая группа потоков Netty. */
    private static final EventLoopGroup GROUP;

    static {
        boolean epoll;
        try { epoll = Epoll.isAvailable(); } catch (Throwable t) { epoll = false; }
        EPOLL = epoll;
        AtomicInteger n = new AtomicInteger();
        java.util.concurrent.ThreadFactory tf = r -> {
            Thread t = new Thread(r, "ws-netty-" + n.incrementAndGet());
            t.setDaemon(true);
            return t;
        };
        int threads = Math.max(2, Math.min(4, Runtime.getRuntime().availableProcessors()));
        GROUP = EPOLL ? new EpollEventLoopGroup(threads, tf) : new NioEventLoopGroup(threads, tf);
        log.info("WebSocket: Netty, транспорт {}, потоков {}", EPOLL ? "epoll" : "nio", threads);
    }

    /** Транспорт для метрик: epoll или nio. */
    public static String transport() { return EPOLL ? "netty-epoll" : "netty-nio"; }

    /**
     * Подключиться и пройти рукопожатие. Вызывать не из потока Netty (ждёт результата).
     * @throws Exception не подключилось, рукопожатие отклонено (403, 429…) или не уложилось во время
     */
    public static Connection connect(URI uri, Listener listener) throws Exception {
        String scheme = uri.getScheme() == null ? "wss" : uri.getScheme().toLowerCase();
        boolean tls = !scheme.equals("ws");
        String host = uri.getHost();
        int port = uri.getPort() > 0 ? uri.getPort() : tls ? 443 : 80;
        SslContext ssl = tls ? SslContextBuilder.forClient().build() : null;
        WebSocketClientHandshaker hs = WebSocketClientHandshakerFactory.newHandshaker(
                uri, WebSocketVersion.V13, null, false, new DefaultHttpHeaders(), MAX_MESSAGE);
        Handler h = new Handler(hs, listener);

        Bootstrap b = new Bootstrap().group(GROUP)
                .channel(EPOLL ? EpollSocketChannel.class : NioSocketChannel.class)
                .option(ChannelOption.TCP_NODELAY, true)
                .option(ChannelOption.SO_KEEPALIVE, true)
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, CONNECT_TIMEOUT_MS)
                .handler(new ChannelInitializer<Channel>() {
                    @Override protected void initChannel(Channel ch) {
                        ChannelPipeline p = ch.pipeline();
                        if (ssl != null) p.addLast(ssl.newHandler(ch.alloc(), host, port));
                        p.addLast(new HttpClientCodec());
                        p.addLast(new HttpObjectAggregator(65_536));
                        p.addLast(new WebSocketFrameAggregator(MAX_MESSAGE));
                        p.addLast(h);
                    }
                });
        ChannelFuture cf = b.connect(host, port);
        if (!cf.await(CONNECT_TIMEOUT_MS + 1_000L) || !cf.isSuccess()) {
            cf.channel().close();
            throw new IllegalStateException("TCP " + host + ":" + port + ": " + (cf.cause() != null ? cf.cause() : "таймаут"));
        }
        try {
            h.handshake.get(HANDSHAKE_TIMEOUT_MS, TimeUnit.MILLISECONDS);
        } catch (TimeoutException e) {
            cf.channel().close();
            throw new IllegalStateException("рукопожатие WebSocket: нет ответа за " + HANDSHAKE_TIMEOUT_MS + " мс");
        } catch (ExecutionException e) {
            cf.channel().close();
            throw new IllegalStateException("рукопожатие WebSocket: " + e.getCause().getMessage(), e.getCause());
        }
        return h;
    }

    // ───────────────────────── обработчик канала ─────────────────────────

    /** Рукопожатие, кадры, закрытие; он же — {@link Connection}. */
    private static final class Handler extends SimpleChannelInboundHandler<Object> implements Connection {
        /** Рукопожатие этого соединения. */
        private final WebSocketClientHandshaker hs;
        /** Обработчик событий. */
        private final Listener listener;
        /** Завершается после рукопожатия (успехом или ошибкой). */
        final CompletableFuture<Void> handshake = new CompletableFuture<>();
        /** Канал. */
        private volatile Channel channel;
        /** Закрыли мы сами — onClose не вызывать. */
        private volatile boolean aborted;
        /** onClose уже вызван. */
        private boolean closedReported;
        /** Декодер UTF-8 и переиспользуемые буферы текста. */
        private final CharsetDecoder utf8 = StandardCharsets.UTF_8.newDecoder();
        private CharBuffer chars = CharBuffer.allocate(16_384);

        Handler(WebSocketClientHandshaker hs, Listener listener) { this.hs = hs; this.listener = listener; }

        @Override public void channelActive(ChannelHandlerContext ctx) {
            channel = ctx.channel();
            hs.handshake(ctx.channel());
        }

        @Override public void channelInactive(ChannelHandlerContext ctx) {
            if (!handshake.isDone()) handshake.completeExceptionally(new IllegalStateException("соединение закрыто до рукопожатия"));
            else reportClose(1006, "соединение оборвалось");
        }

        @Override protected void channelRead0(ChannelHandlerContext ctx, Object msg) {
            if (!hs.isHandshakeComplete()) {
                try {
                    hs.finishHandshake(ctx.channel(), (FullHttpResponse) msg);
                } catch (Exception e) {                     // не 101: 403, 429, 5xx
                    handshake.completeExceptionally(e);
                    ctx.close();
                    return;
                }
                try { listener.onOpen(this); } catch (Exception e) { log.warn("onOpen: {}", e.toString()); }
                handshake.complete(null);
                return;
            }
            if (msg instanceof TextWebSocketFrame t) {
                int n = decode(t.content());
                listener.onText(this, chars.array(), n);
            } else if (msg instanceof BinaryWebSocketFrame bf) {
                ByteBuf c = bf.content();
                byte[] data = new byte[c.readableBytes()];
                c.getBytes(c.readerIndex(), data);
                listener.onBinary(this, data);
            } else if (msg instanceof PingWebSocketFrame ping) {
                ctx.writeAndFlush(new PongWebSocketFrame(ping.content().retain()));
                listener.onPing(this);
            } else if (msg instanceof PongWebSocketFrame) {
                listener.onPing(this);
            } else if (msg instanceof CloseWebSocketFrame cl) {
                int code = cl.statusCode();
                String reason = cl.reasonText();
                ctx.writeAndFlush(new CloseWebSocketFrame(code < 0 ? 1000 : code, "")).addListener(ChannelFutureListener.CLOSE);
                reportClose(code, reason);
            }
        }

        /** UTF-8 из буфера Netty в переиспользуемый char[]; возвращает число символов. */
        private int decode(ByteBuf buf) {
            ByteBuffer in = buf.nioBufferCount() == 1 ? buf.nioBuffer() : ByteBuffer.wrap(io.netty.buffer.ByteBufUtil.getBytes(buf));
            int need = (int) (in.remaining() * 1.1) + 16;
            if (chars.capacity() < need) chars = CharBuffer.allocate(Math.min(Math.max(need, chars.capacity() * 2), MAX_MESSAGE + 16));
            chars.clear();
            utf8.reset();
            CoderResult r = utf8.decode(in, chars, true);
            if (r.isOverflow()) {                              // редкий случай: буфер мал — вырастить и повторить
                chars = CharBuffer.allocate(in.capacity() * 2 + 16);
                in = buf.nioBufferCount() == 1 ? buf.nioBuffer() : ByteBuffer.wrap(io.netty.buffer.ByteBufUtil.getBytes(buf));
                utf8.reset();
                utf8.decode(in, chars, true);
            }
            utf8.flush(chars);
            return chars.position();
        }

        @Override public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            if (!handshake.isDone()) handshake.completeExceptionally(cause);
            else if (!aborted) {
                try { listener.onError(this, cause); } catch (Exception e) { log.warn("onError: {}", e.toString()); }
            }
            ctx.close();
        }

        /** Сообщить о закрытии один раз и только если закрыли не мы. */
        private void reportClose(int code, String reason) {
            if (aborted || closedReported) return;
            closedReported = true;
            try { listener.onClose(this, code, reason); } catch (Exception e) { log.warn("onClose: {}", e.toString()); }
        }

        @Override public CompletableFuture<Void> sendText(String text) {
            Channel ch = channel;
            CompletableFuture<Void> f = new CompletableFuture<>();
            if (ch == null || !ch.isActive()) { f.completeExceptionally(new IllegalStateException("сокет закрыт")); return f; }
            ch.writeAndFlush(new TextWebSocketFrame(text)).addListener(r -> {
                if (r.isSuccess()) f.complete(null); else f.completeExceptionally(r.cause());
            });
            return f;
        }

        @Override public void abort() {
            aborted = true;
            Channel ch = channel;
            if (ch != null) ch.close();
        }

        @Override public boolean isOpen() { Channel ch = channel; return ch != null && ch.isActive() && !aborted; }
    }
}
