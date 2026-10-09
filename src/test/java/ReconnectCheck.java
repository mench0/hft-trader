import java.util.*;
import java.util.function.BooleanSupplier;

/** Канал ордеров по WebSocket: отключение после отказов логина и повторная попытка. Переподключения фида стакана — WsFeedCheck. */
public class ReconnectCheck {
  static int pass, fail;
  static void ck(String n, boolean ok){ if(ok) pass++; else {fail++; System.out.println("FAIL "+n);} }
  static boolean await(BooleanSupplier c, long ms) throws Exception { long t=System.currentTimeMillis()+ms; while(System.currentTimeMillis()<t){ if(c.getAsBoolean()) return true; Thread.sleep(20);} return c.getAsBoolean(); }

  public static void main(String[] a) throws Exception {
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
