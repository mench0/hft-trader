import com.hft.config.*;
import com.hft.exchange.mexc.MexcRestClient;
import com.hft.model.OrderEnums.*;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import com.hft.exchange.catalog.*;
import com.hft.exchange.generic.*;
import com.hft.store.*;
import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.function.BooleanSupplier;

/** MEXC: стакан по WebSocket в protobuf (spot@public.limit.depth.v3.api.pb), служебные ответы — JSON. */
public class MexcWsCheck {
  static int pass, fail;
  static void ck(String n, boolean ok){ if(ok) pass++; else {fail++; System.out.println("FAIL "+n);} }
  static boolean await(BooleanSupplier c, long ms) throws Exception { long t=System.currentTimeMillis()+ms; while(System.currentTimeMillis()<t){ if(c.getAsBoolean()) return true; Thread.sleep(20);} return c.getAsBoolean(); }
  static boolean near(double a,double b){ return Math.abs(a-b)<1e-9; }

  // ---- мини-кодировщик protobuf
  static void varint(ByteArrayOutputStream o, long v){ while ((v & ~0x7FL) != 0) { o.write((int)((v & 0x7F) | 0x80)); v >>>= 7; } o.write((int) v); }
  static void tag(ByteArrayOutputStream o, int f, int w){ varint(o, ((long) f << 3) | w); }
  static void bytes(ByteArrayOutputStream o, int f, byte[] b){ tag(o,f,2); varint(o,b.length); o.writeBytes(b); }
  static void str(ByteArrayOutputStream o, int f, String s){ bytes(o,f,s.getBytes(StandardCharsets.UTF_8)); }
  static byte[] item(String p, String q){ var o=new ByteArrayOutputStream(); str(o,1,p); str(o,2,q); return o.toByteArray(); }
  static byte[] depth(String sym, String[][] asks, String[][] bids, long ts){
    var d=new ByteArrayOutputStream();
    for (String[] x: asks) bytes(d,1,item(x[0],x[1]));
    for (String[] x: bids) bytes(d,2,item(x[0],x[1]));
    str(d,3,"spot@public.limit.depth.v3.api.pb"); str(d,4,"123");
    var o=new ByteArrayOutputStream();
    str(o,1,"spot@public.limit.depth.v3.api.pb@"+sym+"@20");
    str(o,3,sym); str(o,4,"id-1"); tag(o,5,0); varint(o,ts-5); tag(o,6,0); varint(o,ts);
    tag(o,7,1); o.writeBytes(new byte[8]);               // незнакомое поле фиксированной длины — пропускается
    bytes(o,303,d.toByteArray());
    return o.toByteArray();
  }
  static byte[] deals(String sym){ var o=new ByteArrayOutputStream(); str(o,1,"spot@public.deals.v3.api.pb@"+sym); str(o,3,sym); bytes(o,301,item("1","2")); return o.toByteArray(); }

  static byte[] order(String sym, String id, int side, String qty, String cumQty, String cumAmt, int status){
    var o=new ByteArrayOutputStream(); str(o,1,id); str(o,2,"cl-1"); str(o,3,"100"); str(o,4,qty); str(o,6,"0");
    tag(o,7,0); varint(o,1); tag(o,8,0); varint(o,side); tag(o,9,0); varint(o,0);
    str(o,13,cumQty); str(o,14,cumAmt); tag(o,15,0); varint(o,status); tag(o,16,0); varint(o,1_700_000_000_000L);
    var w=new ByteArrayOutputStream(); str(w,1,"spot@private.orders.v3.api.pb"); str(w,3,sym); bytes(w,304,o.toByteArray()); tag(w,6,0); varint(w,1_700_000_000_001L);
    return w.toByteArray();
  }
  static byte[] account(String coin, String free, String frozen){
    var o=new ByteArrayOutputStream(); str(o,1,coin); str(o,2,"1"); str(o,3,free); str(o,4,"0"); str(o,5,frozen); str(o,6,"0"); str(o,7,"ENTRUST"); tag(o,8,0); varint(o,1);
    var w=new ByteArrayOutputStream(); str(w,1,"spot@private.account.v3.api.pb"); bytes(w,307,o.toByteArray());
    return w.toByteArray();
  }

  /** Приватный поток: listenKey по REST, подписка, события ордеров и баланса в protobuf. */
  static void privateStream() throws Exception {
    var hits = new java.util.concurrent.ConcurrentHashMap<String, AtomicInteger>();
    HttpServer s = HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
    s.createContext("/", ex -> {
      String key = ex.getRequestMethod()+" "+ex.getRequestURI().getPath();
      hits.computeIfAbsent(key, k->new AtomicInteger()).incrementAndGet();
      String body = key.equals("POST /api/v3/userDataStream") ? "{\"listenKey\":\"LK9\"}"
          : key.equals("POST /api/v3/order") ? "{\"orderId\":\"C02__42\",\"symbol\":\"BTCUSDT\"}" : "{}";
      byte[] b = body.getBytes(); ex.sendResponseHeaders(200, b.length); ex.getResponseBody().write(b); ex.close();
    });
    s.start();
    try (var ws = new MiniWsServer()) {
      String rest = "http://127.0.0.1:"+s.getAddress().getPort();
      var cfg = new ExchangeConfig("mexc", false, rest, "", 5000, List.of("BTCUSDT"), 20, 100);
      var f = new SymbolFilters(); f.put("BTCUSDT", new SymbolFilters.Filter(0.0001,1e9,0.0001,0,1e9,0.01,1));
      var c = new MexcRestClient(cfg, new Credentials("KEY","SECRET"), f);
      c.setUserStreamBase(ws.url()+"?listenKey=");
      MiniWsServer.Conn[] conn = {null};
      ws.onText = (cn, t) -> {
        conn[0] = cn;
        if (t.contains("SUBSCRIPTION")) { cn.text("{\"id\":0,\"code\":0,\"msg\":\"spot@private.orders.v3.api.pb,spot@private.account.v3.api.pb\"}"); cn.binary(account("USDT","123.5","1.5")); }
        else if (t.contains("PING")) cn.text("{\"id\":0,\"code\":0,\"msg\":\"PONG\"}");
      };
      var bal = new BalanceStore();
      c.startStreams(bal);
      c.awaitStreams(3000);
      ck("mexc private stream ready, listenKey via REST", c.wsReady() && hits.get("POST /api/v3/userDataStream").get()==1);
      ck("mexc subscribed to private channels", await(() -> ws.received.stream().anyMatch(x -> x.contains("spot@private.orders.v3.api.pb") && x.contains("spot@private.account.v3.api.pb")), 3000));
      ck("mexc balance from protobuf", await(() -> bal.free("USDT")==123.5 && bal.locked("USDT")==1.5, 3000) && c.balancesStreamed());
      var r = c.buyLimit("BTCUSDT", 0.5, 100, TimeInForce.GTC);
      conn[0].binary(order("BTCUSDT", "C02__42", 1, "0.5", "0.5", "50.5", 2));
      ck("mexc order event", await(() -> String.valueOf(c.stats()).contains("events=2"), 3000));
      var st = c.orderStatus("BTCUSDT", r.orderId());
      ck("mexc status from protobuf stream", st.status().equals("FILLED") && Math.abs(st.avgPrice()-101)<1e-9 && st.side()==Side.BUY
          && !hits.containsKey("GET /api/v3/order"));
      conn[0].binary(order("BTCUSDT", "C02__43", 2, "1", "0.4", "40", 5));
      ck("partially canceled -> CANCELED with fill", await(() -> { try { var x=c.orderStatus("BTCUSDT", 9_000_000_000_000_000_002L); return x.status().equals("CANCELED") && x.side()==Side.SELL && Math.abs(x.executedQty()-0.4)<1e-12; } catch(Exception e){ return false; } }, 3000));
      ck("no parse errors", String.valueOf(c.stats()).contains("parseErrors=0"));
      c.stopStreams();
      ck("mexc stream stopped", !c.wsReady());
    }
    s.stop(0);
  }

  public static void main(String[] a) throws Exception {
    privateStream();
    var dialect = WsDialects.forExchange("mexc").get();
    ck("parsesBinary", dialect.parsesBinary());
    ck("subscribe", dialect.subscribe(List.of("BTCUSDT","ETHUSDT"), 20).get(0).equals(
        "{\"method\":\"SUBSCRIPTION\",\"params\":[\"spot@public.limit.depth.v3.api.pb@BTCUSDT@20\",\"spot@public.limit.depth.v3.api.pb@ETHUSDT@20\"]}"));
    ck("depth level 5", dialect.subscribe(List.of("BTCUSDT"), 5).get(0).contains("@BTCUSDT@5\""));
    var many = new ArrayList<String>(); for (int i=0;i<40;i++) many.add("S"+i+"USDT");
    ck("max 30 subscriptions", dialect.subscribe(many, 20).get(0).split("spot@").length - 1 == 30);
    ck("ping", dialect.pingMessage().equals("{\"method\":\"PING\"}"));
    char[] err = "{\"id\":0,\"code\":0,\"msg\":\"Not Subscribed successfully! [spot@x]. Reason: Blocked!\"}".toCharArray();
    boolean threw=false; try { dialect.parse(err, err.length, null); } catch (Exception e) { threw=true; }
    ck("rejected subscription -> error", threw);

    ExchangeInfo info = ExchangeCatalog.find("mexc").get();
    ck("catalog: no testnet", !info.hasTestnet());
    try (var srv = new MiniWsServer()) {
      var cfg = new ExchangeConfig("mexc", false, "", srv.url(), 5000, List.of("BTCUSDT"), 20, 100);
      var m = new MarketDataStore(20, 100); m.register("BTCUSDT");
      var books = new AtomicInteger();
      var f = new WsBookFeed(info, cfg, WsDialects.forExchange("mexc").get(), m, (sy,px,q,bm,ts,rn) -> {}, s -> books.incrementAndGet(), () -> {}).tune(30000, 50);
      srv.onText = (c, t) -> {
        if (t.contains("SUBSCRIPTION")) {
          c.text("{\"id\":0,\"code\":0,\"msg\":\"spot@public.limit.depth.v3.api.pb@BTCUSDT@20\"}");
          c.binary(deals("BTCUSDT"));
          c.binary(depth("BTCUSDT", new String[][]{{"101.5","2"},{"102","1"}}, new String[][]{{"99.25","1.5"},{"98","4"}}, 1_700_000_000_000L));
        } else if (t.contains("PING")) c.text("{\"id\":0,\"code\":0,\"msg\":\"PONG\"}");
      };
      f.start();
      var b = m.book("BTCUSDT");
      ck("book from protobuf", await(() -> near(b.bestBid(), 99.25) && near(b.bestAsk(), 101.5), 5000));
      ck("subscribed", srv.received.stream().anyMatch(x -> x.contains("spot@public.limit.depth.v3.api.pb@BTCUSDT@20")));
      // второй снимок заменяет первый целиком
      srv.conns.get(0).binary(depth("BTCUSDT", new String[][]{{"105","1"}}, new String[][]{{"104","3"}}, 1_700_000_000_100L));
      ck("snapshot replaces book", await(() -> near(b.bestBid(), 104) && near(b.bestAsk(), 105), 3000));
      ck("no parse errors", String.valueOf(f.stats()).contains("parseErrors=0"));
      f.stop();
    }
    System.out.println("pass="+pass+" fail="+fail);
    System.exit(fail==0?0:1);
  }
}
