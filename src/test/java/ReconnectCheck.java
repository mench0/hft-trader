import java.util.*;
import java.util.function.BooleanSupplier;

/** Netty-клиент WebSocket (ping/pong, предел размера, закрытие) и канал ордеров (отключение после отказов логина, повтор). */
public class ReconnectCheck {
  static int pass, fail;
  static void ck(String n, boolean ok){ if(ok) pass++; else {fail++; System.out.println("FAIL "+n);} }
  static boolean await(BooleanSupplier c, long ms) throws Exception { long t=System.currentTimeMillis()+ms; while(System.currentTimeMillis()<t){ if(c.getAsBoolean()) return true; Thread.sleep(20);} return c.getAsBoolean(); }

  public static void main(String[] a) throws Exception {
    // ───── WsClient: сообщения, pong на ping, закрытие сервером, abort без onClose, предел размера
    try (var srv = new MiniWsServer()) {
      var texts = new java.util.concurrent.CopyOnWriteArrayList<String>();
      var closes = new java.util.concurrent.atomic.AtomicInteger();
      var listener = new com.hft.net.WsClient.Listener() {
        public void onText(com.hft.net.WsClient.Connection c, char[] b, int n) { texts.add(new String(b, 0, n)); }
        public void onClose(com.hft.net.WsClient.Connection c, int code, String reason) { closes.incrementAndGet(); }
        public void onError(com.hft.net.WsClient.Connection c, Throwable e) { closes.incrementAndGet(); }
      };
      srv.onText = (c, t) -> c.text("echo:" + t);
      var conn = com.hft.net.WsClient.connect(java.net.URI.create(srv.url()), listener);
      conn.sendText("привет, мир").get(2, java.util.concurrent.TimeUnit.SECONDS);
      ck("netty: send and receive utf-8", await(() -> texts.contains("echo:привет, мир"), 3000));
      srv.conns.get(0).textFragmented("{\"part\":\"склейка фрагментов\"}");
      ck("netty: fragments joined", await(() -> texts.contains("{\"part\":\"склейка фрагментов\"}"), 3000));
      srv.conns.get(0).ping();
      ck("netty: pong on server ping", await(() -> srv.pongs.get() == 1, 3000));
      srv.conns.get(0).text("x".repeat(com.hft.net.WsClient.MAX_MESSAGE + 10));
      ck("netty: oversized message drops connection", await(() -> !conn.isOpen() && closes.get() >= 1, 5000));
      int before = closes.get();
      var conn2 = com.hft.net.WsClient.connect(java.net.URI.create(srv.url()), listener);
      conn2.abort();
      Thread.sleep(300);
      ck("netty: abort does not report close", closes.get() == before && !conn2.isOpen());
      srv.accepting = false;
      boolean refused = false;
      try { com.hft.net.WsClient.connect(java.net.URI.create(srv.url()), listener); } catch (Exception e) { refused = true; }
      ck("netty: refused handshake -> exception", refused);
    }

    // канал ордеров: 5 отказов логина — отключён (ордера по REST), но через disabledRetry пробует снова
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
