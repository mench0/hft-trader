import com.hft.net.AbstractWsFeed;
import io.netty.channel.Channel;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BooleanSupplier;

/** Netty-фид (Binance, Bybit): переподключение после обрыва, отказа рукопожатия, тишины; одна цепочка попыток; пинги. */
public class ReconnectCheck {
  static int pass, fail;
  static void ck(String n, boolean ok){ if(ok) pass++; else {fail++; System.out.println("FAIL "+n);} }
  static boolean await(BooleanSupplier c, long ms) throws Exception { long t=System.currentTimeMillis()+ms; while(System.currentTimeMillis()<t){ if(c.getAsBoolean()) return true; Thread.sleep(20);} return c.getAsBoolean(); }

  /** Простой фид: URL сервера, считает подключения и кадры. */
  static class TestFeed extends AbstractWsFeed {
    final String url; final List<String> texts = new CopyOnWriteArrayList<>();
    volatile int handshakes; volatile String heartbeat; volatile long hbMs = 200;
    TestFeed(String url) { this.url = url; }
    @Override protected URI buildUri() throws Exception { return new URI(url); }
    @Override public List<String> activeSymbols() { return List.of("BTCUSDT"); }
    @Override protected void onText(String json, long ns) { texts.add(json); }
    @Override protected void onHandshakeComplete(Channel ch) { handshakes++; }
    @Override protected String heartbeat() { return heartbeat; }
    @Override protected long heartbeatIntervalMs() { return hbMs; }
    @Override protected String name() { return "reconnect-test"; }
    long reconnects() { return ((Number) connectionStats().get("reconnects")).longValue(); }
  }

  public static void main(String[] a) throws Exception {
    // 1. обрыв со стороны сервера — переподключение, данные снова идут
    try (var ws = new MiniWsServer()) {
      ws.onOpen = c -> c.text("{\"n\":1}");
      var f = new TestFeed(ws.url()); f.tune(5_000, 100); f.start();
      ck("connects", await(() -> f.isConnected() && f.texts.size() == 1, 3000));
      ck("realtime after data", f.isRealtime());
      for (var c : ws.conns) c.closeSocket();
      ck("reconnects after drop", await(() -> f.handshakes == 2 && f.texts.size() == 2 && f.isConnected(), 5000));
      ck("stats: reconnects counted, error recorded", f.reconnects() == 1 && !String.valueOf(f.connectionStats().get("lastError")).isEmpty());
      // pong на ping сервера
      ws.conns.get(ws.conns.size() - 1).ping();
      ck("pong on server ping", await(() -> ws.pongs.get() == 1, 2000));
      f.stop();
      Thread.sleep(300);
      int conns = ws.conns.size();
      Thread.sleep(800);
      ck("no reconnect after stop", ws.conns.size() == conns && !f.isConnected());
    }
    // 2. сервер отклоняет рукопожатие — одна цепочка попыток с нарастающей паузой, без лавины
    try (var ws = new MiniWsServer()) {
      ws.accepting = false;
      var f = new TestFeed(ws.url()); f.tune(5_000, 100); f.start();     // старт не падает
      ck("start survives refused handshake", !f.isConnected());
      Thread.sleep(2_000);
      // паузы ≈100, 200, 400, 800 мс (±20 %) — за 2 с не больше ~5 попыток; две цепочки дали бы вдвое больше
      long r = f.reconnects();
      ck("single backoff chain (attempts=" + r + ")", r >= 3 && r <= 6);
      ws.accepting = true;
      ws.onOpen = c -> c.text("{\"ok\":1}");
      ck("recovers when server accepts again", await(() -> f.isConnected() && !f.texts.isEmpty(), 5000));
      f.stop();
    }
    // 3. сервер недоступен (порт закрыт) — старт не падает, подключается, когда сервер появится
    int port; try (var s = new ServerSocket(0)) { port = s.getLocalPort(); }
    var f3 = new TestFeed("ws://127.0.0.1:" + port + "/ws"); f3.tune(5_000, 100); f3.start();
    ck("start survives closed port", !f3.isConnected());
    Thread.sleep(500);
    try (var ws = new MiniWsServer(port)) {
      ws.onOpen = c -> c.text("{\"up\":1}");
      ck("connects once server is up", await(() -> f3.isConnected() && !f3.texts.isEmpty(), 8000));
      f3.stop();
    }
    // 4. тишина дольше staleMs — соединение пересоздаётся
    try (var ws = new MiniWsServer()) {
      var f = new TestFeed(ws.url()); f.tune(1_000, 100); f.start();
      ck("connected (silent server)", await(f::isConnected, 3000));
      ck("silence -> reconnect", await(() -> f.handshakes >= 2, 5000));
      f.stop();
    }
    // 5. пинг уровня приложения по таймеру
    try (var ws = new MiniWsServer()) {
      var f = new TestFeed(ws.url()); f.heartbeat = "{\"op\":\"ping\"}"; f.tune(5_000, 100); f.start();
      ck("app heartbeat sent", await(() -> ws.received.stream().filter(t -> t.contains("\"ping\"")).count() >= 2, 3000));
      f.stop();
    }
    // 6. канал ордеров: 5 отказов логина — отключён (ордера по REST), но через disabledRetry пробует снова
    try (var ws = new MiniWsServer()) {
      var good = new java.util.concurrent.atomic.AtomicBoolean(false);
      ws.onText = (c, t) -> { if (t.equals("login")) c.text(good.get() ? "ok" : "bad"); };
      var proto = new com.hft.rest.WsRpcChannel.Protocol() {
        public String url() { return ws.url(); }
        public List<String> login() { return List.of("login"); }
        public List<String> subscriptions() { return List.of(); }
        public com.hft.rest.WsRpcChannel.Msg parse(String t) {
          if (t.equals("ok")) return com.hft.rest.WsRpcChannel.Msg.loginOk();
          throw new IllegalStateException("логин отклонён");
        }
      };
      var ch = new com.hft.rest.WsRpcChannel("rpc-reconnect-test", proto).tune(5000, 20).disabledRetry(1500);
      ch.start();
      ck("rpc disabled after login rejects", await(ch::isDisabled, 10000) && !ch.isReady());
      good.set(true);
      ck("rpc re-enabled after cooldown", await(() -> !ch.isDisabled() && ch.isReady(), 10000));
      ch.stop();
    }
    System.out.println("pass="+pass+" fail="+fail); System.exit(fail==0?0:1);
  }
}
