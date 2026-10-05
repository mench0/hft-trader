import com.fasterxml.jackson.databind.*;
import com.hft.config.*;
import com.hft.exchange.bybit.BybitRestClient;
import com.hft.model.OrderEnums.*;
import com.hft.store.*;
import com.hft.util.Signer;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.BooleanSupplier;

/** Bybit: ордера и отмены по торговому WS (/v5/trade), исполнения и баланс по приватному (/v5/private). */
public class BybitWsCheck {
  static int pass, fail;
  static void ck(String n, boolean ok){ if(ok) pass++; else {fail++; System.out.println("FAIL "+n);} }
  static boolean await(BooleanSupplier c, long ms) throws Exception { long t=System.currentTimeMillis()+ms; while(System.currentTimeMillis()<t){ if(c.getAsBoolean()) return true; Thread.sleep(20);} return c.getAsBoolean(); }
  static ObjectMapper m = new ObjectMapper();
  static JsonNode j(String s){ try { return m.readTree(s); } catch(Exception e){ throw new RuntimeException(e); } }
  static Map<String,AtomicInteger> hits = new ConcurrentHashMap<>();
  static Map<String,String> routes = new ConcurrentHashMap<>();
  static List<String> queries = new CopyOnWriteArrayList<>();
  static int h(String k){ var a=hits.get(k); return a==null?0:a.get(); }

  public static void main(String[] a) throws Exception {
    HttpServer s = HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
    s.createContext("/", ex -> {
      String key = ex.getRequestMethod()+" "+ex.getRequestURI().getPath();
      hits.computeIfAbsent(key, k->new AtomicInteger()).incrementAndGet();
      queries.add(key+" "+ex.getRequestURI().getRawQuery()+" "+new String(ex.getRequestBody().readAllBytes()));
      byte[] b = routes.getOrDefault(key, "{\"retCode\":0}").getBytes(); ex.sendResponseHeaders(200, b.length); ex.getResponseBody().write(b); ex.close();
    });
    s.start();
    String rest = "http://127.0.0.1:"+s.getAddress().getPort();
    routes.put("POST /v5/order/create", "{\"retCode\":0,\"result\":{\"orderId\":\"42\",\"orderLinkId\":\"x\"}}");
    routes.put("GET /v5/order/realtime", "{\"retCode\":0,\"result\":{\"list\":[{\"orderId\":\"77\",\"orderLinkId\":\"x\",\"side\":\"Buy\",\"orderStatus\":\"Filled\",\"qty\":\"0.5\",\"cumExecQty\":\"0.5\",\"avgPrice\":\"100\"}]}}");

    var cfg = new ExchangeConfig("bybit", false, rest, "", 5000, List.of("BTCUSDT"), 20, 100);
    var f = new SymbolFilters(); f.put("BTCUSDT", new SymbolFilters.Filter(0.0001,1e9,0.0001,0,1e9,0.01,1));
    var cr = new Credentials("KEY","SECRET");

    try (var tr = new MiniWsServer(); var pv = new MiniWsServer()) {
      String[] mode = {"ok"};
      List<JsonNode> treqs = new CopyOnWriteArrayList<>();
      MiniWsServer.Conn[] pconn = {null};
      pv.onText = (c, t) -> {
        JsonNode n = j(t); pconn[0] = c;
        switch (n.path("op").asText()) {
          case "auth" -> {
            long exp = n.path("args").get(1).asLong();
            boolean ok = n.path("args").get(2).asText().equals(new Signer("SECRET").sign("GET/realtime"+exp));
            c.text("{\"success\":"+ok+",\"ret_msg\":\"\",\"op\":\"auth\",\"conn_id\":\"p1\"}");
          }
          case "subscribe" -> {
            c.text("{\"success\":true,\"ret_msg\":\"\",\"op\":\"subscribe\",\"conn_id\":\"p1\"}");
            c.text("{\"topic\":\"wallet\",\"data\":[{\"coin\":[{\"coin\":\"USDT\",\"walletBalance\":\"125\",\"locked\":\"1.5\"}]}]}");
          }
          case "ping" -> c.text("{\"success\":true,\"ret_msg\":\"pong\",\"op\":\"ping\"}");
        }
      };
      tr.onText = (c, t) -> {
        JsonNode n = j(t); treqs.add(n);
        String op = n.path("op").asText(), id = n.path("reqId").asText();
        switch (op) {
          case "auth" -> c.text("{\"retCode\":0,\"retMsg\":\"OK\",\"op\":\"auth\",\"connId\":\"t1\"}");
          case "order.create" -> {
            if (mode[0].equals("close")) { c.closeSocket(); return; }
            if (mode[0].equals("error")) { c.text("{\"reqId\":\""+id+"\",\"retCode\":170131,\"retMsg\":\"Insufficient balance.\",\"op\":\"order.create\",\"data\":{}}"); return; }
            JsonNode b = n.path("args").get(0);
            c.text("{\"reqId\":\""+id+"\",\"retCode\":0,\"retMsg\":\"OK\",\"op\":\"order.create\",\"data\":{\"orderId\":\"555\",\"orderLinkId\":\""+b.path("orderLinkId").asText()+"\"}}");
            if (b.path("orderType").asText().equals("Market") && pconn[0]!=null)
              pconn[0].text("{\"topic\":\"order\",\"data\":[{\"category\":\"spot\",\"orderId\":\"555\",\"orderLinkId\":\"l\",\"symbol\":\"BTCUSDT\",\"side\":\"Buy\",\"orderStatus\":\"Filled\",\"qty\":\"0.5\",\"cumExecQty\":\"0.5\",\"avgPrice\":\"101\"}]}");
          }
          case "order.cancel" -> c.text("{\"reqId\":\""+id+"\",\"retCode\":0,\"retMsg\":\"OK\",\"op\":\"order.cancel\",\"data\":{\"orderId\":\"555\"}}");
          case "ping" -> c.text("{\"op\":\"pong\"}");
        }
      };
      var c = new BybitRestClient(cfg, cr, f);
      c.setWsUrls(tr.url(), pv.url());
      var bal = new BalanceStore();
      c.startStreams(bal);
      c.awaitStreams(3000);
      ck("trade ws ready", c.wsReady());
      ck("wallet event: free = total - locked", await(() -> bal.free("USDT") == 123.5, 3000));
      ck("balancesStreamed", c.balancesStreamed());

      var r = c.buyMarket("BTCUSDT", 0.5);
      ck("market via ws, fill from private stream", r.orderId()==555 && r.status().equals("FILLED") && Math.abs(r.avgPrice()-101)<1e-9);
      ck("no REST create", h("POST /v5/order/create")==0);
      JsonNode cr1 = treqs.stream().filter(n -> n.path("op").asText().equals("order.create")).findFirst().get();
      ck("request shape", cr1.path("header").has("X-BAPI-TIMESTAMP") && cr1.path("args").get(0).path("category").asText().equals("spot")
          && cr1.path("args").get(0).path("marketUnit").asText().equals("baseCoin") && cr1.path("args").get(0).path("qty").asText().equals("0.5"));
      ck("status from stream", c.orderStatus("BTCUSDT", 555).status().equals("FILLED") && h("GET /v5/order/realtime")==0);

      var lim = c.buyLimit("BTCUSDT", 0.5, 100, TimeInForce.GTC);
      ck("limit via ws NEW", lim.orderId()==555 && lim.status().equals("NEW"));
      c.cancelOrder("BTCUSDT", 555);
      ck("cancel via ws", h("POST /v5/order/cancel")==0);

      mode[0] = "error";
      boolean threw = false;
      try { c.buyLimit("BTCUSDT", 0.5, 100, TimeInForce.GTC); } catch (BybitRestClient.ExchangeException e) { threw = e.getMessage().contains("170131"); }
      ck("ws business error -> exception, no REST retry", threw && h("POST /v5/order/create")==0);

      mode[0] = "close";
      var u = c.buyLimit("BTCUSDT", 0.5, 100, TimeInForce.GTC);
      ck("unknown outcome -> REST lookup by orderLinkId", u.orderId()==77 && u.status().equals("FILLED") && h("POST /v5/order/create")==0
          && queries.stream().anyMatch(x -> x.startsWith("GET /v5/order/realtime") && x.contains("orderLinkId=hft")));
      c.stopStreams();
      ck("stopped", !c.wsReady());
    }
    var c2 = new BybitRestClient(cfg, cr, f);
    var r2 = c2.buyMarketForQuote("BTCUSDT", 50);
    ck("REST fallback when no ws, quote unit", r2.orderId()==42 && h("POST /v5/order/create")==1
        && queries.stream().anyMatch(x -> x.startsWith("POST /v5/order/create") && x.contains("\"marketUnit\":\"quoteCoin\"")));
    s.stop(0);
    System.out.println("pass="+pass+" fail="+fail);
    System.exit(fail==0?0:1);
  }
}
