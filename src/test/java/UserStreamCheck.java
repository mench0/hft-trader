import com.fasterxml.jackson.databind.*;
import com.hft.config.*;
import com.hft.exchange.aster.AsterRestClient;
import com.hft.model.OrderEnums.*;
import com.hft.store.*;
import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.BooleanSupplier;

/** Приватные потоки через listenKey: Aster (исполнения + баланс). */
public class UserStreamCheck {
  static int pass, fail;
  static void ck(String n, boolean ok){ if(ok) pass++; else {fail++; System.out.println("FAIL "+n);} }
  static boolean await(BooleanSupplier c, long ms) throws Exception { long t=System.currentTimeMillis()+ms; while(System.currentTimeMillis()<t){ if(c.getAsBoolean()) return true; Thread.sleep(20);} return c.getAsBoolean(); }
  static Map<String,AtomicInteger> hits = new ConcurrentHashMap<>();
  static Map<String,String> routes = new ConcurrentHashMap<>();
  static List<String> heads = new CopyOnWriteArrayList<>();
  static int h(String k){ var a=hits.get(k); return a==null?0:a.get(); }

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

    s.stop(0);
    System.out.println("pass="+pass+" fail="+fail);
    System.exit(fail==0?0:1);
  }
}
