import com.fasterxml.jackson.databind.*;
import com.hft.config.*;
import com.hft.model.OrderEnums.*;
import com.hft.rest.*;
import com.hft.store.*;
import com.hft.util.Signer;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.BooleanSupplier;

/** Binance: ордера, отмены, статусы и события аккаунта по WebSocket API; REST — запасной. */
public class BinanceWsCheck {
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
      byte[] b = routes.getOrDefault(key, "{}").getBytes(); ex.sendResponseHeaders(200, b.length); ex.getResponseBody().write(b); ex.close();
    });
    s.start();
    String rest = "http://127.0.0.1:"+s.getAddress().getPort();
    routes.put("POST /api/v3/order", "{\"orderId\":42,\"clientOrderId\":\"r\",\"status\":\"FILLED\",\"origQty\":\"0.5\",\"executedQty\":\"0.5\",\"cummulativeQuoteQty\":\"50\"}");
    routes.put("GET /api/v3/order", "{\"orderId\":77,\"clientOrderId\":\"x\",\"side\":\"BUY\",\"status\":\"FILLED\",\"origQty\":\"0.5\",\"executedQty\":\"0.5\",\"cummulativeQuoteQty\":\"55\"}");
    routes.put("GET /api/v3/account", "{\"balances\":[{\"asset\":\"USDT\",\"free\":\"10\",\"locked\":\"0\"}]}");

    var cfg = new ExchangeConfig("binance", false, rest, "", 5000, List.of("BTCUSDT"), 20, 100);
    var f = new SymbolFilters(); f.put("BTCUSDT", new SymbolFilters.Filter(0.0001,1e9,0.0001,0,1e9,0.01,1));
    var cr = new Credentials("KEY","SECRET");

    try (var ws = new MiniWsServer()) {
      String[] mode = {"ok"};
      List<JsonNode> reqs = new CopyOnWriteArrayList<>();
      ws.onText = (c, t) -> {
        JsonNode n = j(t); reqs.add(n);
        String id = n.path("id").asText(), method = n.path("method").asText();
        switch (method) {
          case "userDataStream.subscribe.signature" -> {
            c.text("{\"id\":\""+id+"\",\"status\":200,\"result\":{\"subscriptionId\":0}}");
            c.text("{\"subscriptionId\":0,\"event\":{\"e\":\"outboundAccountPosition\",\"B\":[{\"a\":\"USDT\",\"f\":\"123.5\",\"l\":\"1.5\"}]}}");
          }
          case "order.place" -> {
            if (mode[0].equals("close")) { c.closeSocket(); return; }
            if (mode[0].equals("error")) { c.text("{\"id\":\""+id+"\",\"status\":400,\"error\":{\"code\":-2010,\"msg\":\"Account has insufficient balance\"}}"); return; }
            String cl = n.path("params").path("newClientOrderId").asText();
            c.text("{\"id\":\""+id+"\",\"status\":200,\"result\":{\"orderId\":555,\"clientOrderId\":\""+cl+"\",\"status\":\"FILLED\",\"origQty\":\"0.5\",\"executedQty\":\"0.5\",\"cummulativeQuoteQty\":\"50.5\"}}");
            c.text("{\"subscriptionId\":0,\"event\":{\"e\":\"executionReport\",\"s\":\"BTCUSDT\",\"c\":\""+cl+"\",\"S\":\"BUY\",\"X\":\"FILLED\",\"i\":555,\"q\":\"0.5\",\"z\":\"0.5\",\"Z\":\"50.5\"}}");
          }
          case "order.cancel" -> c.text("{\"id\":\""+id+"\",\"status\":200,\"result\":{\"orderId\":555,\"status\":\"CANCELED\"}}");
          case "openOrders.cancelAll" -> c.text("{\"id\":\""+id+"\",\"status\":200,\"result\":[{\"orderId\":1},{\"orderId\":2}]}");
          case "order.status" -> c.text("{\"id\":\""+id+"\",\"status\":200,\"result\":{\"orderId\":9,\"side\":\"SELL\",\"status\":\"NEW\",\"origQty\":\"1\",\"executedQty\":\"0\",\"cummulativeQuoteQty\":\"0\"}}");
          default -> {}
        }
      };
      var c = new BinanceRestClient(cfg, cr, f);
      c.setWsApiUrl(ws.url());
      var bal = new BalanceStore();
      c.startStreams(bal);
      c.awaitStreams(3000);
      ck("ws ready", c.wsReady());
      ck("balance from event", await(() -> bal.free("USDT") == 123.5, 3000));
      ck("balancesStreamed", c.balancesStreamed());

      // подпись: параметры по алфавиту, HMAC-SHA256 hex
      JsonNode sub = reqs.get(0).path("params");
      ck("subscribe signed", sub.path("signature").asText().equals(new Signer("SECRET").sign("apiKey=KEY&timestamp="+sub.path("timestamp").asText())));

      var r = c.buyMarket("BTCUSDT", 0.5);
      ck("ws order FILLED", r.status().equals("FILLED") && r.orderId()==555 && Math.abs(r.avgPrice()-101)<1e-9);
      ck("no REST order", h("POST /api/v3/order")==0);
      JsonNode op = reqs.stream().filter(n -> n.path("method").asText().equals("order.place")).findFirst().get().path("params");
      var sorted = new TreeMap<String,String>(); op.fields().forEachRemaining(e -> { if(!e.getKey().equals("signature")) sorted.put(e.getKey(), e.getValue().asText()); });
      StringBuilder q = new StringBuilder(); sorted.forEach((k,v) -> { if(q.length()>0) q.append('&'); q.append(k).append('=').append(v); });
      ck("order signature", op.path("signature").asText().equals(new Signer("SECRET").sign(q.toString())));
      ck("order params", op.path("quantity").asText().equals("0.5") && op.path("type").asText().equals("MARKET") && op.path("apiKey").asText().equals("KEY"));
      ck("status from event", c.orderStatus("BTCUSDT", 555).status().equals("FILLED") && h("GET /api/v3/order")==0);
      ck("status via ws", c.orderStatus("BTCUSDT", 9).status().equals("NEW") && h("GET /api/v3/order")==0);

      c.cancelOrder("BTCUSDT", 555);
      ck("cancel via ws", h("DELETE /api/v3/order")==0);
      ck("cancelAll via ws", c.cancelAll("BTCUSDT")==2 && h("DELETE /api/v3/openOrders")==0);

      mode[0] = "error";
      boolean threw = false;
      try { c.buyLimit("BTCUSDT", 0.5, 100, TimeInForce.GTC); } catch (BinanceRestClient.ExchangeException e) { threw = e.getMessage().contains("-2010"); }
      ck("ws business error -> exception, no REST retry", threw && h("POST /api/v3/order")==0);

      mode[0] = "close";
      var u = c.buyLimit("BTCUSDT", 0.5, 100, TimeInForce.GTC);
      ck("unknown outcome -> REST lookup by origClientOrderId", u.orderId()==77 && h("POST /api/v3/order")==0
          && queries.stream().anyMatch(x -> x.startsWith("GET /api/v3/order") && x.contains("origClientOrderId=hft")));
      c.stopStreams();
      ck("stopped", !c.wsReady());
    }

    // без WS-сокета — REST
    var c2 = new BinanceRestClient(cfg, cr, f);
    var r2 = c2.buyMarket("BTCUSDT", 0.5);
    ck("REST fallback when no ws", r2.orderId()==42 && h("POST /api/v3/order")==1);
    s.stop(0);
    System.out.println("pass="+pass+" fail="+fail);
    System.exit(fail==0?0:1);
  }
}
