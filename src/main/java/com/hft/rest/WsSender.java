package com.hft.rest;

import com.hft.net.WsClient;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Неблокирующая отправка в WebSocket ({@link WsClient}): вызывающий поток не ждёт, порядок сообщений сохраняет Netty;
 * кто хочет знать результат — смотрит на возвращённый future. Если в очереди больше maxQueued неотправленных
 * сообщений — сокет считается зависшим и сбрасывается.
 */
public final class WsSender {

    /** Соединение. */
    private final WsClient.Connection ws;
    /** Предел очереди неотправленных сообщений. */
    private final int maxQueued;
    /** Сколько сообщений ждёт отправки. */
    private final AtomicInteger queued = new AtomicInteger();

    /**
     * @param ws открытое соединение
     * @param maxQueued предел очереди
     */
    public WsSender(WsClient.Connection ws, int maxQueued) {
        this.ws = ws;
        this.maxQueued = maxQueued;
    }

    /** Поставить сообщение в очередь отправки. Возвращает future завершения именно этой отправки. */
    public CompletableFuture<Void> send(String text) {
        if (queued.incrementAndGet() > maxQueued) {
            queued.decrementAndGet();
            ws.abort();
            return CompletableFuture.failedFuture(new IllegalStateException("очередь отправки переполнена — сокет сброшен"));
        }
        CompletableFuture<Void> f = ws.sendText(text);
        f.whenComplete((v, e) -> queued.decrementAndGet());
        return f.orTimeout(5, TimeUnit.SECONDS);
    }

    /** Сколько сообщений ждёт отправки. */
    public int queued() { return queued.get(); }
}
