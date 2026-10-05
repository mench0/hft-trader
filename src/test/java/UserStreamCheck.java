import com.fasterxml.jackson.databind.*;
import com.hft.config.*;
import com.hft.exchange.aster.AsterRestClient;
import com.hft.exchange.bingx.BingxRestClient;
import com.hft.model.OrderEnums.*;
import com.hft.store.*;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.BooleanSupplier;
import java.util.zip.GZIPOutputStream;

/** Приватные потоки через listenKey: Aster (исполнения + баланс), BingX (исполнения, gzip, Ping/Pong). */
public class UserStreamCheck {
  static int pass, fail;
  static void ck(String n, boolean ok){ if(ok) pass++; else {fail++; System.out.println("FAIL "+n);} }
  static boolean await(BooleanSupplier c, long ms) throws Exception { long t=System.currentTimeMillis()+ms; while(System.currentTimeMillis()<t){ if(c.getAsBoolean()) return true; Thread.sleep(20);} return c.getAsBoolean(); }
  static Map<String,AtomicInteger> hits = new ConcurrentHashMap<>();
  static Map<String,String> routes = new ConcurrentHashMap<>();
  static List<String> heads = new CopyOnWriteArrayList<>();
  static int h(String k){ var a=hits.get(k); return a==null?0:a.get(); }
  static byte[] gzip(String s) throws Exception { var bo=new ByteArrayOutputStream(); try(var g=new GZIPOutputStream(bo)){ g.write(s.getBytes()); } return bo.toByteArray(); }

  public static void main(String[] a) throws Exception {
    HttpServer s = HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
    s.createContext("/", ex -> {
      String key = ex.getRequestMethod()+" "+ex.getRequestURI().getPath();
      hits.computeIfAbsent(key, k->new AtomicInteger()).incrementAndGet();
      heads.add(key+" key="+ex.getRequestHeaders().getFirst("X-BX-APIKEY"));
      byte[] b = routes.getOrDefault(key, "{}").getBytes(); ex.sendResponseHeaders(200, b.length); ex.getResponseBody().write(b); ex.close();
    });
    s.start();
    String rest = "http://127.0.0.1:"+s.getAddress().getPort();
    var f = new SymbolFilters(); f.put("BTCUSDT", new SymbolFilters.Filter(0.0001,1e9,0.0001,0,1e9,0.01,1));
    var cr = new Credentials("0x00000000000000000000000000000000000000bb","SECRET");

    // ───── Aster
    routes.put("POST /api/v3/listenKey", "{\"listenKey\":\"LK1\"}");
    routes.put("POST /api/v3/order", "{\"orderId\":555,\"clientOrderId\":\"c\",\"status\":\"NEW\",\"origQty\":\"0.5\",\"executedQty\":\"0\",\"cummulativeQuoteQty\":\"0\",\"side\":\"BUY\"}");
    try (var ws = new MiniWsServer()) {
      String[] path = {null};
      ws.onOpen = c -> c.text("{\"e\":\"outboundAccountPosition\",\"B\":[{\"a\":\"USDT\",\"f\":\"123.5\",\"l\":\"1.5\"}]}");
      var cfg = new ExchangeConfig("aster", false, rest, "", 5000, List.of("BTCUSDT"), 20, 100);
      var c = new AsterRestClient(cfg, cr, f, new TC());
      c.setUserStreamBase(ws.url().replace("/ws", "/ws/"));
      var bal = new BalanceStore();
      c.startStreams(bal);
      c.awaitStreams(3000);
      ck("aster stream ready", c.wsReady() && h("POST /api/v3/listenKey")==1);
      ck("aster balance from stream", await(() -> bal.free("USDT")==123.5, 3000) && c.balancesStreamed());
      ws.conns.get(0).text("{\"e\":\"executionReport\",\"s\":\"BTCUSDT\",\"c\":\"c\",\"S\":\"BUY\",\"X\":\"FILLED\",\"i\":555,\"q\":\"0.5\",\"z\":\"0.5\",\"Z\":\"50.5\"}");
      ck("aster event received", await(() -> String.valueOf(c.stats()).contains("events=2"), 3000));
      var ast = c.orderStatus("BTCUSDT",555);
      ck("aster status from stream", ast.status().equals("FILLED") && Math.abs(ast.avgPrice()-101)<1e-9 && h("GET /api/v3/order")==0);
      c.stopStreams();
      ck("aster stopped", !c.wsReady());
    }

    // ───── BingX
    routes.put("POST /openApi/user/auth/userDataStream", "{\"listenKey\":\"LK2\"}");
    routes.put("POST /openApi/spot/v1/trade/order", "{\"code\":0,\"data\":{\"orderId\":777}}");
    try (var ws = new MiniWsServer()) {
      var cfg = new ExchangeConfig("bingx", false, rest, "", 5000, List.of("BTCUSDT"), 20, 100);
      var c = new BingxRestClient(cfg, new Credentials("KEY","SECRET"), f);
      c.setUserStreamBase(ws.url()+"?listenKey=");
      MiniWsServer.Conn[] conn = {null};
      ws.onText = (cn, t) -> {
        conn[0] = cn;
        if (t.contains("spot.executionReport")) {
          try {
            cn.binary(gzip("{\"code\":0,\"id\":\"x\",\"msg\":\"SUCCESS\",\"dataType\":\"\",\"data\":null}"));
            cn.binary(gzip("Ping"));
          } catch (Exception e) { throw new RuntimeException(e); }
        }
      };
      c.startStreams(new BalanceStore());
      c.awaitStreams(3000);
      ck("bingx stream ready, listenKey with api key header", c.wsReady() && heads.stream().anyMatch(x -> x.equals("POST /openApi/user/auth/userDataStream key=KEY")));
      ck("bingx subscribed", await(() -> ws.received.stream().anyMatch(x -> x.contains("\"reqType\":\"sub\"") && x.contains("spot.executionReport")), 3000));
      ck("bingx Ping -> Pong", await(() -> ws.received.contains("Pong"), 3000));
      var r = c.buyLimit("BTCUSDT", 0.5, 100, TimeInForce.GTC);
      ck("bingx order via REST", h("POST /openApi/spot/v1/trade/order")==1);
      conn[0].binary(gzip("{\"dataType\":\"spot.executionReport\",\"data\":{\"e\":\"executionReport\",\"s\":\"BTC-USDT\",\"S\":\"BUY\",\"X\":\"FILLED\",\"i\":777,\"c\":\"c\",\"q\":\"0.5\",\"z\":\"0.5\",\"Z\":\"51\"}}"));
      ck("bingx event received", await(() -> String.valueOf(c.stats()).contains("events=1"), 3000));
      var bst = c.orderStatus("BTCUSDT", r.orderId());
      ck("bingx status from stream", bst.status().equals("FILLED") && Math.abs(bst.avgPrice()-102)<1e-9 && h("GET /openApi/spot/v1/trade/query")==0);
      c.stopStreams();
      ck("bingx stopped", !c.wsReady());
    }
    s.stop(0);
    System.out.println("pass="+pass+" fail="+fail);
    System.exit(fail==0?0:1);
  }
}
