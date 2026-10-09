import com.hft.config.*;
import com.hft.exchange.okx.*; import com.hft.exchange.mexc.*; import com.hft.exchange.gate.*;
import com.hft.model.OrderResult; import com.hft.model.OrderEnums.*;
import com.hft.rest.*; import com.hft.store.*; import com.hft.util.Hmac;
import com.sun.net.httpserver.*; import java.net.InetSocketAddress; import java.util.*;

public class ExchangeClientsCheck {
  static int pass, fail;
  static void ck(String n, boolean ok){ if(ok) pass++; else {fail++; System.out.println("FAIL "+n);} }
  static boolean near(double a,double b){ return Math.abs(a-b)<1e-9*Math.max(1,Math.abs(b)); }
  record Seen(String method, String uri, String body, Headers h) {}
  static List<Seen> seen = new ArrayList<>();
  static Map<String,String> routes = new HashMap<>(); static int[] status = {200};

  static HttpServer server() throws Exception {
    HttpServer s = HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
    s.createContext("/", ex -> {
      String body = new String(ex.getRequestBody().readAllBytes());
      seen.add(new Seen(ex.getRequestMethod(), ex.getRequestURI().toString(), body, ex.getRequestHeaders()));
      String key = ex.getRequestMethod()+" "+ex.getRequestURI().getPath();
      String resp = routes.getOrDefault(key, "{}");
      byte[] b = resp.getBytes(); ex.sendResponseHeaders(status[0], b.length); ex.getResponseBody().write(b); ex.close();
    });
    s.start(); return s;
  }
  static ExchangeConfig cfg(String id, String url){ return new ExchangeConfig(id, false,url,"",5000,List.of("BTCUSDT"),20,100); }
  static SymbolFilters filt(){ var f=new SymbolFilters(); f.put("BTCUSDT", new SymbolFilters.Filter(0.0001,1e9,0.0001,0,1e9,0.01,1)); return f; }
  static Credentials cr = new Credentials("KEY","SECRET");

  public static void main(String[] a) throws Exception {
    ck("hmac256", Hmac.sha256Hex("key","The quick brown fox jumps over the lazy dog").equals("f7bc83f430538424b13298e6aa6fb143ef4d59a14946175997479dbc2d1a3cd8"));
    ck("hmac512", Hmac.sha512Hex("key","The quick brown fox jumps over the lazy dog").startsWith("b42af09057bac1e2d41708e48a902e09"));
    ck("sha512 empty", Hmac.sha512HexOf("").startsWith("cf83e1357eefb8bdf1542850d66d8007"));

    var srv = server(); String url = "http://127.0.0.1:"+srv.getAddress().getPort();

    // ---------- OKX
    routes.put("POST /api/v5/trade/order", "{\"code\":\"0\",\"msg\":\"\",\"data\":[{\"ordId\":\"555\",\"sCode\":\"0\",\"sMsg\":\"\"}]}");
    routes.put("GET /api/v5/trade/order", "{\"code\":\"0\",\"data\":[{\"state\":\"filled\",\"accFillSz\":\"0.5\",\"avgPx\":\"100\",\"sz\":\"0.5\",\"side\":\"buy\",\"clOrdId\":\"x\"}]}");
    var okx = new OkxRestClient(cfg("okx",url), cr, filt());
    OrderResult r = okx.buyMarket("BTCUSDT", 0.5);
    ck("okx filled", r.isFilled() && near(r.executedQty(),0.5) && near(r.avgPrice(),100) && r.orderId()==555);
    Seen s0 = seen.get(0);
    ck("okx body", s0.body().contains("\"instId\":\"BTC-USDT\"") && s0.body().contains("\"tgtCcy\":\"base_ccy\"") && s0.body().contains("\"sz\":\"0.5\"") && s0.body().contains("\"ordType\":\"market\""));
    String ts = s0.h().getFirst("OK-ACCESS-TIMESTAMP");
    ck("okx ts fmt", ts.matches("\\d{4}-\\d\\d-\\d\\dT\\d\\d:\\d\\d:\\d\\d\\.\\d{3}Z"));
    ck("okx sign", s0.h().getFirst("OK-ACCESS-SIGN").equals(Hmac.sha256Base64("SECRET", ts+"POST/api/v5/trade/order"+s0.body())));
    ck("okx passphrase", "pp".equals(s0.h().getFirst("OK-ACCESS-PASSPHRASE")));
    Seen s1 = seen.get(1); String ts1 = s1.h().getFirst("OK-ACCESS-TIMESTAMP");
    ck("okx get sign includes query", s1.h().getFirst("OK-ACCESS-SIGN").equals(Hmac.sha256Base64("SECRET", ts1+"GET"+s1.uri())));
    OrderResult q = okx.buyMarketForQuote("BTCUSDT", 50);
    ck("okx quote tgt", seen.get(seen.size()-2).body().contains("quote_ccy") && seen.get(seen.size()-2).body().contains("\"sz\":\"50\""));
    okx.buyLimit("BTCUSDT", 0.1, 100.123, TimeInForce.IOC);
    String lb = seen.stream().filter(x->x.body().contains("\"px\"")).findFirst().get().body();
    ck("okx ioc limit", lb.contains("\"ordType\":\"ioc\"") && lb.contains("\"px\":\"100.12\""));
    routes.put("POST /api/v5/trade/order", "{\"code\":\"1\",\"msg\":\"All operations failed\",\"data\":[{\"sCode\":\"51008\",\"sMsg\":\"Insufficient balance\"}]}");
    boolean thrown=false; try{ okx.buyMarket("BTCUSDT",0.5);}catch(ApiException e){thrown=!e.isRateLimit()&&e.getMessage().contains("Insufficient");}
    ck("okx error mapped", thrown);
    status[0]=429; routes.put("POST /api/v5/trade/order","{\"code\":\"50011\",\"msg\":\"Too many\"}");
    boolean rl=false; try{ okx.buyMarket("BTCUSDT",0.5);}catch(ApiException e){rl=e.isRateLimit();}
    ck("okx rate limit", rl);
    boolean blocked=false; try{ okx.buyMarket("BTCUSDT",0.5);}catch(com.hft.rest.LocalThrottleException e){blocked=e.getMessage().contains("приостановлены") && e.reason()==com.hft.rest.LocalThrottleException.Reason.PAUSED;}
    ck("okx local block after 429", blocked);
    status[0]=200; seen.clear();

    // ---------- MEXC
    routes.put("POST /api/v3/order", "{\"symbol\":\"BTCUSDT\",\"orderId\":\"C02__8888\",\"price\":\"0\",\"origQty\":\"0.5\"}");
    routes.put("GET /api/v3/order", "{\"status\":\"FILLED\",\"executedQty\":\"0.5\",\"cummulativeQuoteQty\":\"50\",\"origQty\":\"0.5\",\"side\":\"BUY\"}");
    var mexc = new MexcRestClient(cfg("mexc",url), cr, filt());
    r = mexc.buyLimit("BTCUSDT", 0.5, 100, TimeInForce.IOC);
    ck("mexc filled avg", r.isFilled() && near(r.avgPrice(),100) && r.orderId() >= 9_000_000_000_000_000_000L);
    Seen m0 = seen.get(0);
    ck("mexc ioc type", m0.uri().contains("type=IMMEDIATE_OR_CANCEL") && m0.uri().contains("quantity=0.5") && m0.uri().contains("price=100"));
    String full = m0.uri().substring(m0.uri().indexOf('?')+1); int ix = full.indexOf("&signature=");
    ck("mexc sign", full.substring(ix+11).equals(Hmac.sha256Hex("SECRET", full.substring(0,ix))));
    ck("mexc header", "KEY".equals(m0.h().getFirst("X-MEXC-APIKEY")));
    ck("mexc status uses venue id", seen.get(1).uri().contains("orderId=C02__8888"));
    mexc.buyMarketForQuote("BTCUSDT", 25);
    ck("mexc quoteOrderQty", seen.stream().anyMatch(x->x.uri().contains("quoteOrderQty=25")));
    status[0]=400; routes.put("POST /api/v3/order","{\"code\":30004,\"msg\":\"Insufficient position\"}");
    thrown=false; try{ mexc.buyMarket("BTCUSDT",0.5);}catch(ApiException e){thrown=e.getMessage().contains("Insufficient");}
    ck("mexc error", thrown); status[0]=200; seen.clear();

    // ---------- Gate
    routes.put("POST /api/v4/spot/orders", "{\"id\":\"1234\",\"status\":\"closed\",\"type\":\"market\",\"side\":\"buy\",\"amount\":\"50\",\"left\":\"0\",\"filled_total\":\"50\",\"avg_deal_price\":\"100\",\"finish_as\":\"filled\",\"text\":\"t-x\"}");
    var gate = new GateRestClient(cfg("gate",url), cr, filt());
    r = gate.buyMarketForQuote("BTCUSDT", 50);
    ck("gate market buy exec", r.isFilled() && near(r.executedQty(), 0.5) && near(r.avgPrice(),100));
    Seen g0 = seen.get(0);
    ck("gate body", g0.body().contains("\"currency_pair\":\"BTC_USDT\"") && g0.body().contains("\"time_in_force\":\"ioc\"") && g0.body().contains("\"amount\":\"50\"") && g0.body().contains("\"text\":\"t-hft"));
    String gts = g0.h().getFirst("Timestamp");
    ck("gate ts seconds", gts.length()==10);
    ck("gate sign", g0.h().getFirst("SIGN").equals(Hmac.sha512Hex("SECRET","POST\n/api/v4/spot/orders\n\n"+Hmac.sha512HexOf(g0.body())+"\n"+gts)));
    routes.put("GET /api/v4/spot/orders/77", "{\"id\":\"77\",\"status\":\"open\",\"type\":\"limit\",\"side\":\"sell\",\"amount\":\"1\",\"left\":\"0.4\"}");
    r = gate.orderStatus("BTCUSDT", 77);
    ck("gate partial", r.isPartial() && near(r.executedQty(),0.6) && r.side()==Side.SELL);
    Seen g1 = seen.get(seen.size()-1); String gts1=g1.h().getFirst("Timestamp");
    ck("gate get sign with query", g1.h().getFirst("SIGN").equals(Hmac.sha512Hex("SECRET","GET\n/api/v4/spot/orders/77\ncurrency_pair=BTC_USDT\n"+Hmac.sha512HexOf("")+"\n"+gts1)));
    status[0]=400; routes.put("POST /api/v4/spot/orders","{\"label\":\"BALANCE_NOT_ENOUGH\",\"message\":\"Not enough balance\"}");
    thrown=false; try{ gate.buyMarket("BTCUSDT",0.5);}catch(ApiException e){thrown=e.getMessage().contains("Not enough");}
    ck("gate error", thrown); status[0]=200; seen.clear();

    // ---------- валидация до отправки
    int before = seen.size();
    thrown=false; try{ okx.buyLimit("BTCUSDT", 0.00001, 100, TimeInForce.GTC);}catch(Exception e){thrown=true;}
    ck("validation blocks tiny order", thrown && seen.size()==before);
    srv.stop(0);
    System.out.println("passed="+pass+" failed="+fail); System.exit(fail==0?0:1);
  }
}
