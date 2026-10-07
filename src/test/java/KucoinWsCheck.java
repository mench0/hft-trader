import com.fasterxml.jackson.databind.*;
import com.hft.config.*;
import com.hft.exchange.kucoin.KucoinRestClient;
import com.hft.model.OrderEnums.*;
import com.hft.rest.ApiException;
import com.hft.store.*;
import com.hft.util.Hmac;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.BooleanSupplier;

/** KuCoin: ордера и отмены по Pro WS API (welcome + подпись), исполнения и баланс по приватному потоку. Запуск: KUCOIN_PASSPHRASE=pp */
public class KucoinWsCheck {
  static int pass, fail;
  static void ck(String n, boolean ok){ if(ok) pass++; else {fail++; System.out.println("FAIL "+n);} }
  static boolean await(BooleanSupplier c, long ms) throws Exception { long t=System.currentTimeMillis()+ms; while(System.currentTimeMillis()<t){ if(c.getAsBoolean()) return true; Thread.sleep(20);} return c.getAsBoolean(); }
  static ObjectMapper m = new ObjectMapper();
  static JsonNode j(String s){ try { return m.readTree(s); } catch(Exception e){ throw new RuntimeException(e); } }
  static Map<String,AtomicInteger> hits = new ConcurrentHashMap<>();
  static Map<String,String> routes = new ConcurrentHashMap<>();
  static List<String> paths = new CopyOnWriteArrayList<>();
  static int h(String k){ var a=hits.get(k); return a==null?0:a.get(); }

  public static void main(String[] a) throws Exception {
    HttpServer s = HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
    s.createContext("/", ex -> {
      String key = ex.getRequestMethod()+" "+ex.getRequestURI().getPath();
      String route = key.startsWith("GET /api/v1/order/client-order/") ? "GET /api/v1/order/client-order" : key;
      hits.computeIfAbsent(route, k->new AtomicInteger()).incrementAndGet(); paths.add(key);
      byte[] b = routes.getOrDefault(route, "{\"code\":\"200000\",\"data\":{}}").getBytes(); ex.sendResponseHeaders(200, b.length); ex.getResponseBody().write(b); ex.close();
    });
    s.start();
    String rest = "http://127.0.0.1:"+s.getAddress().getPort();
    routes.put("POST /api/v1/orders", "{\"code\":\"200000\",\"data\":{\"orderId\":\"rest-1\"}}");
    routes.put("GET /api/v1/order/client-order", "{\"code\":\"200000\",\"data\":{\"id\":\"lost-1\",\"clientOid\":\"x\",\"side\":\"buy\",\"isActive\":false,\"cancelExist\":false,\"size\":\"0.5\",\"dealSize\":\"0.5\",\"dealFunds\":\"50\"}}");

    var cfg = new ExchangeConfig("kucoin", false, rest, "", 5000, List.of("BTCUSDT"), 20, 100);
    var f = new SymbolFilters(); f.put("BTCUSDT", new SymbolFilters.Filter(0.0001,1e9,0.0001,0,1e9,0.01,1));
    var cr = new Credentials("KEY","SECRET");

    try (var tr = new MiniWsServer(); var pv = new MiniWsServer()) {
      String[] mode = {"ok"};
      String welcome = "{\"sessionId\":\"s-1\",\"data\":\"welcome\",\"timestamp\":1700000000000}";
      AtomicReference<String> authReply = new AtomicReference<>();
      tr.onOpen = c -> c.text(welcome);
      MiniWsServer.Conn[] pconn = {null};
      tr.onText = (c, t) -> {
        if (!t.startsWith("{")) {                        // ответ на приветствие: подпись
          authReply.set(t);
          if (t.equals(Hmac.sha256Base64("SECRET", welcome))) c.text("{\"sessionId\":\"s-1\",\"data\":\"session ok\",\"timestamp\":1700000000001,\"pingInterval\":18000}");
          else c.closeSocket();
          return;
        }
        JsonNode n = j(t); String op = n.path("op").asText(), id = n.path("id").asText();
        switch (op) {
          case "ping" -> c.text("{\"id\":\""+id+"\",\"op\":\"pong\",\"timestamp\":1}");
          case "spot.order" -> {
            if (mode[0].equals("close")) { c.closeSocket(); return; }
            if (mode[0].equals("error")) { c.text("{\"id\":\""+id+"\",\"op\":\"spot.order\",\"code\":\"200004\",\"msg\":\"Balance insufficient!\"}"); return; }
            JsonNode b = n.path("args");
            c.text("{\"id\":\""+id+"\",\"op\":\"spot.order\",\"code\":\"200000\",\"data\":{\"orderId\":\"ws-555\",\"clientOid\":\""+b.path("clientOid").asText()+"\"},\"inTime\":1,\"outTime\":2}");
            if (b.path("type").asText().equals("market") && pconn[0]!=null) {
              pconn[0].text("{\"type\":\"message\",\"topic\":\"/spotMarket/tradeOrdersV2\",\"subject\":\"orderChange\",\"data\":{\"orderId\":\"ws-555\",\"clientOid\":\"c\",\"symbol\":\"BTC-USDT\",\"side\":\"buy\",\"type\":\"match\",\"status\":\"match\",\"size\":\"0.5\",\"filledSize\":\"0.5\",\"matchSize\":\"0.5\",\"matchPrice\":\"101\"}}");
              pconn[0].text("{\"type\":\"message\",\"topic\":\"/spotMarket/tradeOrdersV2\",\"subject\":\"orderChange\",\"data\":{\"orderId\":\"ws-555\",\"clientOid\":\"c\",\"symbol\":\"BTC-USDT\",\"side\":\"buy\",\"type\":\"filled\",\"status\":\"done\",\"size\":\"0.5\",\"filledSize\":\"0.5\"}}");
            }
          }
          case "spot.cancel" -> c.text("{\"id\":\""+id+"\",\"op\":\"spot.cancel\",\"code\":\"200000\",\"data\":{\"orderId\":\"ws-555\"}}");
        }
      };
      pv.onOpen = c -> { pconn[0] = c; c.text("{\"id\":\"w1\",\"type\":\"welcome\"}"); };
      pv.onText = (c, t) -> {
        JsonNode n = j(t);
        switch (n.path("type").asText()) {
          case "subscribe" -> {
            c.text("{\"id\":\""+n.path("id").asText()+"\",\"type\":\"ack\"}");
            if (n.path("topic").asText().equals("/account/balance"))
              c.text("{\"type\":\"message\",\"topic\":\"/account/balance\",\"subject\":\"account.balance\",\"data\":{\"currency\":\"USDT\",\"available\":\"123.5\",\"hold\":\"1.5\",\"relationEvent\":\"trade.hold\"}}");
          }
          case "ping" -> c.text("{\"id\":\""+n.path("id").asText()+"\",\"type\":\"pong\"}");
        }
      };
      var c = new KucoinRestClient(cfg, cr, f);
      c.setWsUrls(tr.url(), pv.url());
      var bal = new BalanceStore();
      c.startStreams(bal);
      c.awaitStreams(3000);
      ck("trade ws ready after welcome signature", c.wsReady() && authReply.get()!=null);
      ck("balance from private stream", await(() -> bal.free("USDT") == 123.5, 3000));
      ck("balancesStreamed", await(c::balancesStreamed, 2000));

      var r = c.buyMarket("BTCUSDT", 0.5);
      ck("market via ws, fill from private stream", r.status().equals("FILLED") && Math.abs(r.avgPrice()-101)<1e-9 && Math.abs(r.executedQty()-0.5)<1e-12);
      ck("no REST order", h("POST /api/v1/orders")==0);
      JsonNode req = tr.received.stream().filter(x -> x.contains("spot.order")).map(KucoinWsCheck::j).findFirst().get();
      ck("order args", req.path("args").path("symbol").asText().equals("BTC-USDT") && req.path("args").path("size").asText().equals("0.5")
          && req.path("args").path("clientOid").asText().startsWith("hft"));

      var lim = c.buyLimit("BTCUSDT", 0.5, 100, TimeInForce.GTC);
      ck("limit via ws NEW", lim.status().equals("NEW"));
      c.cancelOrder("BTCUSDT", lim.orderId());
      ck("cancel via ws", h("DELETE /api/v1/orders/ws-555")==0 && tr.received.stream().anyMatch(x -> x.contains("spot.cancel") && x.contains("ws-555")));

      mode[0] = "error";
      boolean threw = false;
      try { c.buyLimit("BTCUSDT", 0.5, 100, TimeInForce.GTC); } catch (ApiException e) { threw = e.getMessage().contains("Balance insufficient"); }
      ck("ws business error -> exception, no REST retry", threw && h("POST /api/v1/orders")==0);

      mode[0] = "close";
      var u = c.buyLimit("BTCUSDT", 0.5, 100, TimeInForce.GTC);
      ck("unknown outcome -> REST lookup by clientOid", u.status().equals("FILLED") && h("POST /api/v1/orders")==0 && h("GET /api/v1/order/client-order")==1);
      c.stopStreams();
      ck("stopped", !c.wsReady());
    }
    // ───── UTA: uta.order / uta.cancel с tradeType=SPOT (KUCOIN_UTA=true в .env рабочей папки)
    java.nio.file.Path envFile = java.nio.file.Path.of(".env");
    java.nio.file.Files.writeString(envFile, "KUCOIN_UTA=true\n");
    try (var tr = new MiniWsServer(); var pv = new MiniWsServer()) {
      String welcome = "{\"sessionId\":\"s-2\",\"data\":\"welcome\",\"timestamp\":1}";
      tr.onOpen = cn -> cn.text(welcome);
      tr.onText = (cn, t) -> {
        if (!t.startsWith("{")) { cn.text("{\"sessionId\":\"s-2\",\"data\":\"ok\",\"timestamp\":2}"); return; }
        JsonNode n = j(t); String op = n.path("op").asText(), id = n.path("id").asText();
        if (op.equals("uta.order")) cn.text("{\"id\":\""+id+"\",\"op\":\"uta.order\",\"code\":\"200000\",\"data\":{\"orderId\":\"u-1\",\"clientOid\":\"c\",\"tradeType\":\"SPOT\"}}");
        if (op.equals("uta.cancel")) cn.text("{\"id\":\""+id+"\",\"op\":\"uta.cancel\",\"code\":\"200000\",\"data\":{\"orderId\":\"u-1\"}}");
      };
      pv.onOpen = cn -> cn.text("{\"id\":\"w\",\"type\":\"welcome\"}");
      var cu = new KucoinRestClient(cfg, cr, f);
      cu.setWsUrls(tr.url(), pv.url());
      cu.startStreams(new BalanceStore());
      cu.awaitStreams(3000);
      int restBefore = h("POST /api/v1/orders");
      var ur = cu.buyLimit("BTCUSDT", 0.5, 100, TimeInForce.GTC);
      cu.cancelOrder("BTCUSDT", ur.orderId());
      JsonNode uo = tr.received.stream().filter(x -> x.contains("uta.order")).map(KucoinWsCheck::j).findFirst().orElse(null);
      JsonNode uc = tr.received.stream().filter(x -> x.contains("uta.cancel")).map(KucoinWsCheck::j).findFirst().orElse(null);
      ck("uta.order with tradeType=SPOT", uo != null && uo.path("args").path("tradeType").asText().equals("SPOT") && uo.path("args").path("symbol").asText().equals("BTC-USDT"));
      ck("uta.cancel with tradeType=SPOT", uc != null && uc.path("args").path("tradeType").asText().equals("SPOT") && uc.path("args").path("orderId").asText().equals("u-1"));
      ck("uta: no spot.* ops, no REST", tr.received.stream().noneMatch(x -> x.contains("spot.order") || x.contains("spot.cancel")) && h("POST /api/v1/orders")==restBefore);
      cu.stopStreams();
    } finally {
      java.nio.file.Files.deleteIfExists(envFile);
    }
    Thread.sleep(20);

    var c2 = new KucoinRestClient(cfg, cr, f);
    var r2 = c2.buyLimit("BTCUSDT", 0.5, 100, TimeInForce.GTC);
    ck("REST fallback when no ws", r2.status().equals("NEW") && h("POST /api/v1/orders")==1);
    s.stop(0);
    System.out.println("pass="+pass+" fail="+fail);
    System.exit(fail==0?0:1);
  }
}
