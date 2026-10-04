package com.hft.rest;

import java.net.http.WebSocket;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Неблокирующая отправка в WebSocket из JDK.
 *
 * JDK требует, чтобы следующий sendText начинался только после завершения предыдущего.
 * Раньше это решалось блокировкой и join() — и медленная отправка (или ответ pong из потока
 * чтения) останавливала приём сообщений. Здесь отправки выстраиваются в цепочку будущих:
 * вызывающий поток не ждёт; кто хочет знать результат — смотрит на возвращённый future.
 * Если в очереди больше maxQueued сообщений — сокет считается зависшим и сбрасывается.
 */
public final class WsSender {

    /** Сокет JDK: send* нельзя вызывать, пока предыдущая отправка не завершилась. */
    private final WebSocket ws;
    /** Предел очереди неотправленных сообщений. */
    private final int maxQueued;
    /** Сколько сообщений ждёт отправки. */
    private final AtomicInteger queued = new AtomicInteger();
    /** Последняя отправка в цепочке — следующая ставится за ней. */
    private CompletableFuture<?> tail = CompletableFuture.completedFuture(null);

    /**
     * @param ws открытый сокет
     * @param maxQueued предел очереди
     */
    public WsSender(WebSocket ws, int maxQueued) {
        this.ws = ws;
        this.maxQueued = maxQueued;
    }

    /** Поставить сообщение в очередь отправки. Возвращает future завершения именно этой отправки. */
    public CompletableFuture<WebSocket> send(String text) {
        if (queued.incrementAndGet() > maxQueued) {
            queued.decrementAndGet();
            ws.abort();
            return CompletableFuture.failedFuture(new IllegalStateException("очередь отправки переполнена — сокет сброшен"));
        }
        CompletableFuture<WebSocket> f;
        synchronized (this) {                                   // только перестановка хвоста — без ожидания
            f = tail.handle((v, e) -> null).thenCompose(v -> ws.sendText(text, true));
            tail = f;
        }
        f.whenComplete((v, e) -> queued.decrementAndGet());
        return f.orTimeout(5, TimeUnit.SECONDS);
    }

    /** Сколько сообщений ждёт отправки. */
    public int queued() { return queued.get(); }
}
