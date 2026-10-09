import com.fasterxml.jackson.databind.*;
import com.hft.config.*;
import com.hft.exchange.catalog.*;
import com.hft.exchange.generic.*;
import com.hft.exchange.gate.GateRestClient;
import com.hft.exchange.hyperliquid.HyperliquidRestClient;
import com.hft.exchange.okx.OkxRestClient;
import com.hft.exchange.uniswapv2.UniswapV2Client;
import com.hft.model.OrderEnums.*;
import com.hft.model.OrderResult;
import com.hft.rest.*;
import com.hft.store.*;
import com.hft.util.Hmac;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.BooleanSupplier;

/** Торговля и события по WebSocket: OKX, Gate, Hyperliquid, Uniswap (JSON-RPC по сокету). Запуск: OKX_PASSPHRASE=pp */
public class WsTradeCheck {
  static int pass, fail;
  static void ck(String n, boolean ok){ if(ok) pass++; else {fail++; System.out.println("FAIL "+n);} }
  static boolean await(BooleanSupplier c, long ms) throws Exception { long t=System.currentTimeMillis()+ms; while(System.currentTimeMillis()<t){ if(c.getAsBoolean()) return true; Thread.sleep(20);} return c.getAsBoolean(); }
  static boolean near(double a,double b){ return Math.abs(a-b)<1e-9*Math.max(1,Math.abs(b)); }
  static ObjectMapper m = new ObjectMapper();
  static JsonNode j(String s){ try { return m.readTree(s); } catch(Exception e){ throw new RuntimeException(e); } }
  static ExchangeConfig cfg(String id, String url, String sym){ return new ExchangeConfig(id, false,url,"",5000,List.of(sym),20,100,
      id.equals("hyperliquid") ? TradingParams.DEFAULTS.with(Map.of("market","perp")) : TradingParams.DEFAULTS); }   // Hyperliquid — перп, остальные — спот
  static SymbolFilters filt(String sym){ var f=new SymbolFilters(); f.put(sym, new SymbolFilters.Filter(0.0001,1e9,0.0001,0,1e9,0.01,1)); return f; }
  static Credentials cr = new Credentials("KEY","SECRET");

  static Map<String,AtomicInteger> hits = new ConcurrentHashMap<>();
  static Map<String,String> routes = new ConcurrentHashMap<>();
  static List<String> restBodies = new CopyOnWriteArrayList<>();
  static HttpServer http() throws Exception {
    HttpServer s = HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
    s.createContext("/", ex -> {
      String body = new String(ex.getRequestBody().readAllBytes());
      String key = ex.getRequestMethod()+" "+ex.getRequestURI().getPath();
      hits.computeIfAbsent(key, k->new AtomicInteger()).incrementAndGet(); restBodies.add(key+" "+body);
      String resp = routes.getOrDefault(key, "{}");
      byte[] b = resp.getBytes(); ex.sendResponseHeaders(200, b.length); ex.getResponseBody().write(b); ex.close();
    });
    s.start(); return s;
  }
  static int h(String k){ var a=hits.get(k); return a==null?0:a.get(); }

  public static void main(String[] a) throws Exception {
    var srv = http(); String rest = "http://127.0.0.1:"+srv.getAddress().getPort();
    okx(rest); gate(rest); hl(rest); uni(rest); channel();
    System.out.println("pass="+pass+" fail="+fail);
    System.exit(fail==0?0:1);
  }

  // ───────────────────────────── OKX
  static void okx(String rest) throws Exception {
    routes.put("GET /api/v5/trade/orders-pending", "{\"code\":\"0\",\"data\":[{\"ordId\":\"900\",\"instId\":\"BTC-USDT\"}]}");
    routes.put("GET /api/v5/trade/order", "{\"code\":\"0\",\"data\":[{\"ordId\":\"777\",\"state\":\"filled\",\"accFillSz\":\"0.5\",\"avgPx\":\"100\",\"sz\":\"0.5\",\"side\":\"buy\",\"clOrdId\":\"x\"}]}");
    routes.put("POST /api/v5/trade/order", "{\"code\":\"0\",\"data\":[{\"ordId\":\"888\",\"sCode\":\"0\",\"sMsg\":\"\"}]}");
    try (var ws = new MiniWsServer()) {
      String[] mode = {"ok"};
      ws.onText = (c, t) -> {
        if (t.equals("ping")) { c.text("pong"); return; }
        JsonNode n = j(t); String op = n.path("op").asText(), id = n.path("id").asText();
        switch (op) {
          case "login" -> c.text("{\"event\":\"login\",\"code\":\"0\",\"msg\":\"\"}");
          case "subscribe" -> {
            c.text("{\"event\":\"subscribe\",\"arg\":{\"channel\":\"orders\",\"instType\":\"SPOT\"}}");
            c.text("{\"arg\":{\"channel\":\"account\"},\"data\":[{\"details\":[{\"ccy\":\"USDT\",\"availBal\":\"123.5\",\"frozenBal\":\"1.5\"}]}]}");
          }
          case "order" -> {
            if (mode[0].equals("close")) { c.closeSocket(); return; }
            if (mode[0].equals("error")) { c.text("{\"id\":\""+id+"\",\"op\":\"order\",\"code\":\"1\",\"msg\":\"All operations failed\",\"data\":[{\"ordId\":\"\",\"sCode\":\"51008\",\"sMsg\":\"insufficient balance\"}]}"); return; }
            String cl = n.path("args").get(0).path("clOrdId").asText();
            c.text("{\"id\":\""+id+"\",\"op\":\"order\",\"code\":\"0\",\"msg\":\"\",\"data\":[{\"clOrdId\":\""+cl+"\",\"ordId\":\"555\",\"sCode\":\"0\",\"sMsg\":\"\"}]}");
            boolean market = n.path("args").get(0).path("ordType").asText().equals("market");
            c.text("{\"arg\":{\"channel\":\"orders\",\"instType\":\"SPOT\"},\"data\":[{\"ordId\":\"555\",\"clOrdId\":\""+cl+"\",\"instId\":\"BTC-USDT\",\"side\":\"buy\",\"sz\":\"0.5\",\"accFillSz\":\""+(market?"0.5":"0")+"\",\"avgPx\":\""+(market?"100":"0")+"\",\"state\":\""+(market?"filled":"live")+"\"}]}");
          }
          case "cancel-order", "batch-cancel-orders" -> c.text("{\"id\":\""+id+"\",\"op\":\""+op+"\",\"code\":\"0\",\"msg\":\"\",\"data\":[{\"ordId\":\"x\",\"sCode\":\"0\",\"sMsg\":\"\"}]}");
          default -> {}
        }
      };
      var store = new BalanceStore();
      var c = new OkxRestClient(cfg("okx", rest, "BTCUSDT"), cr, filt("BTCUSDT"));
      c.setPrivateWsUrl(ws.url());
      c.startStreams(store); c.awaitStreams(5000);
      ck("okx ws ready", c.wsReady());
      ck("okx login sent", ws.received.stream().anyMatch(t -> t.contains("\"op\":\"login\"") && t.contains("\"apiKey\":\"KEY\"") && t.contains("\"passphrase\":\"pp\"")));
      String loginMsg = ws.received.stream().filter(t -> t.contains("\"op\":\"login\"")).findFirst().get();
      String ts = j(loginMsg).path("args").get(0).path("timestamp").asText();
      ck("okx login sign", j(loginMsg).path("args").get(0).path("sign").asText().equals(Hmac.sha256Base64("SECRET", ts+"GET/users/self/verify")));
      ck("okx subscribed orders+account", ws.received.stream().anyMatch(t -> t.contains("\"channel\":\"orders\"") && t.contains("\"channel\":\"account\"")));
      ck("okx account stream -> balances", await(() -> near(store.free("USDT"),123.5) && near(store.locked("USDT"),1.5) && c.balancesStreamed(), 3000));

      OrderResult r = c.buyMarket("BTCUSDT", 0.5);
      ck("okx ws market filled", r.isFilled() && near(r.executedQty(),0.5) && near(r.avgPrice(),100));
      ck("okx order via ws, no REST", h("POST /api/v5/trade/order")==0 && h("GET /api/v5/trade/order")==0);
      ck("okx order payload", ws.received.stream().anyMatch(t -> t.contains("\"op\":\"order\"") && t.contains("BTC-USDT") && t.contains("\"tdMode\":\"cash\"") && t.contains("\"clOrdId\":\"hft")));

      OrderResult lim = c.buyLimit("BTCUSDT", 0.5, 99, TimeInForce.GTC);
      ck("okx limit resting NEW", "NEW".equals(lim.status()));
      c.cancelOrder("BTCUSDT", lim.orderId());
      ck("okx cancel via ws", ws.received.stream().anyMatch(t -> t.contains("\"op\":\"cancel-order\"") && t.contains("\"ordId\":\"555\"")) && h("POST /api/v5/trade/cancel-order")==0);
      int n = c.cancelAll("BTCUSDT");
      ck("okx cancelAll via ws batch (seeded 900)", n==1 && ws.received.stream().anyMatch(t -> t.contains("batch-cancel-orders") && t.contains("\"ordId\":\"900\"")) && h("POST /api/v5/trade/cancel-batch-orders")==0);

      mode[0] = "error";
      int restBefore = h("POST /api/v5/trade/order");
      boolean threw = false;
      try { c.buyLimit("BTCUSDT", 0.5, 99, TimeInForce.GTC); } catch (ApiException e) { threw = e.getMessage().contains("insufficient") || String.valueOf(e).contains("51008"); }
      ck("okx business error not retried via REST", threw && h("POST /api/v5/trade/order")==restBefore);

      mode[0] = "close";
      r = c.buyMarket("BTCUSDT", 0.5);       // сервер закрыл сокет после получения ордера
      ck("okx unknown outcome resolved via clOrdId", r.isFilled() && h("GET /api/v5/trade/order")>=1 && restBodies.stream().anyMatch(b -> b.startsWith("GET /api/v5/trade/order") ));
      ck("okx unknown not re-sent via POST", h("POST /api/v5/trade/order")==restBefore);
      ck("okx ws down now", !c.wsReady());
      mode[0] = "ok";
      r = c.buyLimit("BTCUSDT", 0.5, 99, TimeInForce.GTC);       // WS не готов -> REST
      ck("okx REST fallback when ws not ready", h("POST /api/v5/trade/order")==restBefore+1);
      ck("okx stats expose ws", c.stats().containsKey("ws"));
      c.stopStreams();
    }
  }

  // ───────────────────────────── Gate
  static void gate(String rest) throws Exception {
    routes.put("GET /api/v4/spot/orders/t-x", "{}");
    try (var ws = new MiniWsServer()) {
      String[] mode = {"ok"};
      ws.onText = (c, t) -> {
        JsonNode n = j(t); String ch = n.path("channel").asText(), ev = n.path("event").asText();
        if (ch.equals("spot.ping")) { c.text("{\"time\":1,\"channel\":\"spot.pong\",\"event\":\"\",\"result\":null}"); return; }
        String rid = n.path("payload").path("req_id").asText();
        if (ch.equals("spot.login")) { c.text("{\"request_id\":\"login\",\"header\":{\"status\":\"200\",\"channel\":\"spot.login\",\"event\":\"api\"},\"data\":{\"result\":{\"uid\":\"1\"}}}"); return; }
        if (ev.equals("subscribe")) {
          c.text("{\"time\":1,\"channel\":\""+ch+"\",\"event\":\"subscribe\",\"result\":{\"status\":\"success\"}}");
          if (ch.equals("spot.balances")) c.text("{\"time\":2,\"channel\":\"spot.balances\",\"event\":\"update\",\"result\":[{\"currency\":\"USDT\",\"total\":\"100\",\"available\":\"90\",\"freeze\":\"10\"}]}");
          return;
        }
        String hdr = "\"header\":{\"status\":\"200\",\"channel\":\""+ch+"\",\"event\":\"api\"}";
        switch (ch) {
          case "spot.order_place" -> {
            if (mode[0].equals("close")) { c.closeSocket(); return; }
            if (mode[0].equals("error")) { c.text("{\"request_id\":\""+rid+"\",\"header\":{\"status\":\"400\",\"channel\":\""+ch+"\",\"event\":\"api\"},\"data\":{\"errs\":{\"label\":\"BALANCE_NOT_ENOUGH\",\"message\":\"not enough\"}}}"); return; }
            String text = n.path("payload").path("req_param").path("text").asText();
            c.text("{\"request_id\":\""+rid+"\","+hdr+",\"data\":{\"result\":{\"id\":\"42\",\"text\":\""+text+"\",\"currency_pair\":\"BTC_USDT\",\"side\":\"buy\",\"type\":\"limit\",\"amount\":\"0.5\",\"left\":\"0.5\",\"filled_total\":\"0\",\"status\":\"open\"}}}");
            c.text("{\"time\":3,\"channel\":\"spot.orders\",\"event\":\"update\",\"result\":[{\"id\":\"42\",\"text\":\""+text+"\",\"currency_pair\":\"BTC_USDT\",\"side\":\"buy\",\"type\":\"limit\",\"amount\":\"0.5\",\"left\":\"0\",\"filled_total\":\"50\",\"avg_deal_price\":\"100\",\"finish_as\":\"filled\"}]}");
          }
          case "spot.order_cancel" -> c.text("{\"request_id\":\""+rid+"\","+hdr+",\"data\":{\"result\":{\"id\":\"42\",\"status\":\"cancelled\"}}}");
          case "spot.order_cancel_cp" -> c.text("{\"request_id\":\""+rid+"\","+hdr+",\"data\":{\"result\":[{\"id\":\"1\"},{\"id\":\"2\"}]}}");
          default -> {}
        }
      };
      var store = new BalanceStore();
      var c = new GateRestClient(cfg("gate", rest, "BTCUSDT"), cr, filt("BTCUSDT"));
      c.setPrivateWsUrl(ws.url());
      c.startStreams(store); c.awaitStreams(5000);
      ck("gate ws ready", c.wsReady());
      String login = ws.received.stream().filter(t -> t.contains("spot.login")).findFirst().get();
      String ts = j(login).path("payload").path("timestamp").asText();
      ck("gate login sign", j(login).path("payload").path("signature").asText().equals(Hmac.sha512Hex("SECRET", "api\nspot.login\n\n"+ts)) && j(login).path("payload").path("api_key").asText().equals("KEY"));
      // подписка уходит сразу после подтверждения логина — ждём её, а не смотрим мгновенно
      ck("gate subscribe auth", await(() -> ws.received.stream().anyMatch(t -> t.contains("spot.orders") && t.contains("\"event\":\"subscribe\"") && t.contains("BTC_USDT") && t.contains("\"KEY\":\"KEY\"")), 2000));
      ck("gate balance stream", await(() -> near(store.free("USDT"),90) && near(store.locked("USDT"),10) && c.balancesStreamed(), 3000));

      OrderResult r = c.buyLimit("BTCUSDT", 0.5, 100, TimeInForce.GTC);
      ck("gate ws order NEW", "NEW".equals(r.status()) && r.requestedQty()==0.5);
      ck("gate order via ws, no REST", h("POST /api/v4/spot/orders")==0);
      ck("gate order payload", ws.received.stream().anyMatch(t -> t.contains("spot.order_place") && t.contains("\"currency_pair\":\"BTC_USDT\"") && t.contains("\"text\":\"t-hft")));
      Thread.sleep(400);
      ck("gate stream gives terminal status without REST", await(() -> { try { return c.orderStatus("BTCUSDT", r.orderId()).isFilled(); } catch(Exception e){ return false; } }, 3000)
          && h("GET /api/v4/spot/orders/42")==0);
      OrderResult st = c.orderStatus("BTCUSDT", r.orderId());
      ck("gate streamed fill values", near(st.executedQty(),0.5) && near(st.avgPrice(),100));
      c.cancelOrder("BTCUSDT", r.orderId());
      ck("gate cancel via ws", ws.received.stream().anyMatch(t -> t.contains("spot.order_cancel\"") && t.contains("\"order_id\":\"42\"")) && h("DELETE /api/v4/spot/orders/42")==0);
      ck("gate cancelAll via ws", c.cancelAll("BTCUSDT")==2 && h("DELETE /api/v4/spot/orders")==0);

      mode[0] = "error";
      boolean threw = false;
      try { c.buyLimit("BTCUSDT", 0.5, 100, TimeInForce.GTC); } catch (ApiException e) { threw = String.valueOf(e).contains("BALANCE_NOT_ENOUGH") || e.getMessage().contains("not enough"); }
      ck("gate business error", threw && h("POST /api/v4/spot/orders")==0);

      mode[0] = "close";
      routes.put("GET /api/v4/spot/orders/t-x", "{}");
      // REST-разрешение: сервер вернёт заказ по любому t-... (маршрут по префиксу не поддерживается — проверяем попытку и результат ошибки)
      boolean unknownThrown = false;
      try { c.buyLimit("BTCUSDT", 0.5, 100, TimeInForce.GTC); } catch (Exception e) { unknownThrown = true; }
      ck("gate unknown outcome checks REST by text, never re-posts", h("POST /api/v4/spot/orders")==0 && restBodies.stream().anyMatch(b -> b.startsWith("GET /api/v4/spot/orders/t-hft")));
      c.stopStreams();
    }
  }

  // ───────────────────────────── Hyperliquid
  static void hl(String rest) throws Exception {
    routes.put("POST /exchange", "{\"status\":\"ok\",\"response\":{\"type\":\"order\",\"data\":{\"statuses\":[{\"filled\":{\"totalSz\":\"0.001\",\"avgPx\":\"50010\",\"oid\":99}}]}}}");
    routes.put("POST /info", "{\"marginSummary\":{\"accountValue\":\"1000\"},\"withdrawable\":\"800\"}");
    try (var ws = new MiniWsServer()) {
      String[] mode = {"ok"};
      ws.onText = (c, t) -> {
        JsonNode n = j(t); String method = n.path("method").asText();
        if (method.equals("ping")) { c.text("{\"channel\":\"pong\"}"); return; }
        if (method.equals("subscribe")) { c.text("{\"channel\":\"subscriptionResponse\",\"data\":{\"method\":\"subscribe\",\"subscription\":"+n.path("subscription")+"}}"); return; }
        if (!method.equals("post")) return;
        long id = n.path("id").asLong(); JsonNode rq = n.path("request"); String type = rq.path("type").asText();
        if (type.equals("action")) {
          if (mode[0].equals("close")) { c.closeSocket(); return; }
          String at = rq.path("payload").path("action").path("type").asText();
          String payload = at.equals("order")
              ? (rq.path("payload").path("action").path("orders").get(0).path("t").path("limit").path("tif").asText().equals("Gtc")
                  ? "{\"status\":\"ok\",\"response\":{\"type\":\"order\",\"data\":{\"statuses\":[{\"resting\":{\"oid\":78}}]}}}"
                  : "{\"status\":\"ok\",\"response\":{\"type\":\"order\",\"data\":{\"statuses\":[{\"filled\":{\"totalSz\":\"0.001\",\"avgPx\":\"50010\",\"oid\":77}}]}}}")
              : "{\"status\":\"ok\",\"response\":{\"type\":\"cancel\",\"data\":{\"statuses\":[\"success\"]}}}";
          c.text("{\"channel\":\"post\",\"data\":{\"id\":"+id+",\"response\":{\"type\":\"action\",\"payload\":"+payload+"}}}");
        } else {
          String it = rq.path("payload").path("type").asText();
          String data = switch (it) {
            case "meta" -> "{\"type\":\"meta\",\"data\":{\"universe\":[{\"name\":\"BTC\",\"szDecimals\":5}]}}";
            case "allMids" -> "{\"type\":\"allMids\",\"data\":{\"mids\":{\"BTC\":\"50000.0\"}}}";
            case "clearinghouseState" -> "{\"type\":\"clearinghouseState\",\"data\":{}}";   // неожиданная форма -> REST
            case "openOrders" -> "{\"type\":\"openOrders\",\"data\":[{\"coin\":\"BTC\",\"oid\":78}]}";
            default -> "{}";
          };
          c.text("{\"channel\":\"post\",\"data\":{\"id\":"+id+",\"response\":{\"type\":\"info\",\"payload\":"+data+"}}}");
        }
      };
      var f = new SymbolFilters();
      var c = new HyperliquidRestClient(cfg("hyperliquid", rest, "BTCUSDC"), new Credentials("0xAbC0000000000000000000000000000000000001","key"), f, new TC());
      c.setWsUrl(ws.url());
      c.startStreams(new BalanceStore()); c.awaitStreams(5000);
      ck("hl ws ready (no login)", c.wsReady());
      ck("hl subscribed to user streams", ws.received.stream().anyMatch(t -> t.contains("orderUpdates") && t.contains("0xabc")) && ws.received.stream().anyMatch(t -> t.contains("userFills")));
      int infoBefore = h("POST /info");
      c.loadFilters(List.of("BTCUSDC"));
      ck("hl meta via ws (wrapped shape)", f.has("BTCUSDC") && h("POST /info")==infoBefore);
      OrderResult r = c.buyMarket("BTCUSDC", 0.001);
      ck("hl market via ws", r.isFilled() && r.orderId()==77 && near(r.avgPrice(),50010) && h("POST /exchange")==0);
      JsonNode post = ws.received.stream().map(WsTradeCheck::j).filter(x -> x.path("request").path("type").asText().equals("action")).findFirst().get();
      ck("hl post payload signed", post.path("request").path("payload").path("signature").path("v").asInt()==27 && post.path("request").path("payload").has("nonce"));
      ck("hl allMids unwrapped", ws.received.stream().anyMatch(t -> t.contains("allMids")));

      OrderResult lim = c.buyLimit("BTCUSDC", 0.001, 49000, TimeInForce.GTC);
      ck("hl resting NEW", "NEW".equals(lim.status()) && lim.orderId()==78);
      ws.conns.get(0).text("{\"channel\":\"orderUpdates\",\"data\":[{\"order\":{\"coin\":\"BTC\",\"side\":\"B\",\"limitPx\":\"49000\",\"sz\":\"0.0\",\"oid\":78,\"origSz\":\"0.001\"},\"status\":\"filled\",\"statusTimestamp\":1}]}");
      ws.conns.get(0).text("{\"channel\":\"userFills\",\"data\":{\"user\":\"0xabc\",\"fills\":[{\"coin\":\"BTC\",\"px\":\"48990\",\"sz\":\"0.0004\",\"oid\":78},{\"coin\":\"BTC\",\"px\":\"49000\",\"sz\":\"0.0006\",\"oid\":78}]}}");
      Thread.sleep(400);
      int infoMid = h("POST /info");
      ck("hl stream terminal state, no REST/WS-info", await(() -> { try { var s = c.orderStatus("BTCUSDC", lim.orderId()); return s.isFilled() && near(s.executedQty(),0.001) && Math.abs(s.avgPrice()-48996)<1e-6; } catch(Exception e){ return false; } }, 3000)
          && h("POST /info")==infoMid);
      ck("hl cancelAll via ws", c.cancelAll("BTCUSDC")==1 && h("POST /exchange")==0);

      // сломанная форма info -> REST
      var store = new BalanceStore();
      int infoNow = h("POST /info");
      c.loadBalances(store);
      ck("hl bad ws info shape falls back to REST", near(store.free("USDC"),800) && h("POST /info")==infoNow+1);

      // неизвестный исход: сокет закрыт после получения -> тот же подписанный nonce уходит по REST
      mode[0] = "close";
      restBodies.clear();
      OrderResult u = c.buyMarket("BTCUSDC", 0.001);
      JsonNode wsBody = ws.received.stream().map(WsTradeCheck::j).filter(x -> x.path("request").path("type").asText().equals("action")).reduce((x, y) -> y).get().path("request").path("payload");
      JsonNode restBody = j(restBodies.stream().filter(b -> b.startsWith("POST /exchange")).findFirst().get().substring("POST /exchange ".length()));
      ck("hl unknown outcome replays SAME nonce+signature via REST", u.isFilled() && wsBody.path("nonce").asLong()==restBody.path("nonce").asLong() && wsBody.path("nonce").asLong()>0
          && wsBody.path("signature").equals(restBody.path("signature")));
      c.stopStreams();
    }
  }

  // ───────────────────────────── Uniswap
  static String word(java.math.BigInteger n){ String s=n.toString(16); return "0".repeat(64-s.length())+s; }

  static void uni(String rest) throws Exception {
    ck("sync topic = keccak(Sync(uint112,uint112))", ("0x"+K.hex(K.keccak256("Sync(uint112,uint112)".getBytes()))).equals("0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1"));
    String pair = "0x00000000000000000000000000000000000000d1";
    java.math.BigInteger e18 = java.math.BigInteger.TEN.pow(18), e6 = java.math.BigInteger.TEN.pow(6);
    // 1000 WETH / 2 000 000 USDC => 2000
    String reserves = "0x" + word(e18.multiply(java.math.BigInteger.valueOf(1000))) + word(e6.multiply(java.math.BigInteger.valueOf(2_000_000))) + word(java.math.BigInteger.ONE);
    try (var ws = new MiniWsServer()) {
      ws.onText = (c, t) -> {
        JsonNode n = j(t); String method = n.path("method").asText(), id = n.path("id").asText();
        switch (method) {
          case "eth_subscribe" -> c.text("{\"jsonrpc\":\"2.0\",\"id\":\""+id+"\",\"result\":\"0xsub1\"}");
          case "eth_call" -> c.text("{\"jsonrpc\":\"2.0\",\"id\":\""+id+"\",\"result\":\""+reserves+"\"}");
          case "eth_blockNumber" -> c.text("{\"jsonrpc\":\"2.0\",\"id\":\""+id+"\",\"result\":\"0x10\"}");
          default -> {}
        }
      };
      ExchangeInfo info = ExchangeCatalog.find("uniswapv2").get();
      var cf = new ExchangeConfig("uniswapv2", false, "", ws.url(), 5000, List.of("WETHUSDC"), 20, 100);
      var mk = new MarketDataStore(20, 100); mk.register("WETHUSDC");
      var feed = new WsBookFeed(info, cf, WsDialects.uniswap("WETHUSDC="+pair+":true:18:6"), mk, (sy,px,q,bm,ts,rn) -> {}, s -> {}, () -> {});
      feed.start();
      var b = mk.book("WETHUSDC");
      ck("uni initial reserves via eth_call over ws", await(() -> b.bestBid() > 1990 && b.bestAsk() < 2020 && b.bestBid() < b.bestAsk(), 5000));
      ck("uni subscribe msg", ws.received.stream().anyMatch(t -> t.contains("eth_subscribe") && t.contains("\"logs\"") && t.contains(pair) && t.contains("0x1c411e9a96e071241c2f21f7726b17ae89e3cab4c78be50e062b03a9fffbbad1")));
      // Sync: цена 2100
      String r2 = "0x" + word(e18.multiply(java.math.BigInteger.valueOf(1000))) + word(e6.multiply(java.math.BigInteger.valueOf(2_100_000)));
      ws.conns.get(0).text("{\"jsonrpc\":\"2.0\",\"method\":\"eth_subscription\",\"params\":{\"subscription\":\"0xsub1\",\"result\":{\"address\":\""+pair+"\",\"data\":\""+r2+"\",\"removed\":false}}}");
      ck("uni Sync event updates book", await(() -> b.bestBid() > 2080 && b.bestAsk() > b.bestBid() && b.bestAsk() < 2120, 5000));
      double before = b.bestBid();
      ws.conns.get(0).text("{\"jsonrpc\":\"2.0\",\"method\":\"eth_subscription\",\"params\":{\"subscription\":\"0xsub1\",\"result\":{\"address\":\""+pair+"\",\"data\":\""+reserves+"\",\"removed\":true}}}");
      Thread.sleep(300);
      ck("uni removed (reorg) log ignored", near(b.bestBid(), before));
      ck("uni no REST polling needed", feed.stats().get("parseErrors").equals(0L));
      feed.stop();
    }
    // клиент: JSON-RPC по сокету, HTTP только запасной
    try (var ws = new MiniWsServer()) {
      ws.onText = (c, t) -> {
        JsonNode n = j(t); String method = n.path("method").asText(), id = n.path("id").asText();
        String res = switch (method) { case "eth_getBalance" -> "\"0xde0b6b3a7640000\""; case "eth_call" -> "\"0x"+word(java.math.BigInteger.valueOf(5_000_000))+"\""; default -> "null"; };
        if (!method.equals("eth_blockNumber")) c.text("{\"jsonrpc\":\"2.0\",\"id\":\""+id+"\",\"result\":"+res+"}");
      };
      routes.put("POST /", "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"0xde0b6b3a7640000\"}");
      String W="0x00000000000000000000000000000000000000e1", U="0x00000000000000000000000000000000000000c1";
      var c = new UniswapV2Client(cfg("uniswapv2", rest, "WETHUSDC"), new Credentials("0xaa","key"), new SymbolFilters(), new TC(),
          "0x0000000000000000000000000000000000000f01", "WETH="+W+":18;USDC="+U+":6", "0.5");
      c.setWsUrl(ws.url());
      c.startStreams(new BalanceStore()); c.awaitStreams(5000);
      ck("uni rpc ws ready", c.wsReady());
      int httpBefore = h("POST /");
      var store = new BalanceStore();
      c.loadBalances(store);
      ck("uni balances via JSON-RPC over ws, no HTTP", near(store.free("ETH"),1.0) && h("POST /")==httpBefore);
      ck("uni rpc methods seen on ws", ws.received.stream().anyMatch(t -> t.contains("eth_getBalance")) && ws.received.stream().anyMatch(t -> t.contains("eth_call")));
      for (var cn : ws.conns) cn.closeSocket();
      Thread.sleep(150);
      var store2 = new BalanceStore();
      c.loadBalances(store2);
      ck("uni falls back to HTTP when ws down", near(store2.free("ETH"),1.0) && h("POST /")>httpBefore);
      c.stopStreams();
    }
  }

  // ───────────────────────────── сам канал: отключение после неверного логина
  static void channel() throws Exception {
    try (var ws = new MiniWsServer()) {
      ws.onText = (c, t) -> c.text("{\"login\":\"denied\"}");
      var ch = new WsRpcChannel("t", new WsRpcChannel.Protocol() {
        public String url() { return ws.url(); }
        public List<String> login() { return List.of("{\"login\":1}"); }
        public List<String> subscriptions() { return List.of(); }
        public WsRpcChannel.Msg parse(String t) { throw new IllegalStateException("denied"); }
      }).tune(30000, 5);
      ch.start();
      ck("channel disables itself after repeated login failures", await(() -> ch.isDisabled(), 15000));
      ck("channel not ready, call -> NotReady", !ch.isReady());
      boolean nr = false; try { ch.call("x", "{}", 100); } catch (WsRpcChannel.WsNotReadyException e) { nr = true; }
      ck("NotReady thrown (never sent)", nr && ws.received.stream().noneMatch(t -> t.equals("{}")));
      ch.stop();
    }
  }
}
