import com.hft.config.*; import com.hft.exchange.lbank.*; import com.hft.model.OrderResult; import com.hft.model.OrderEnums.*;
import com.hft.rest.*; import com.hft.store.*; import com.hft.util.Hmac;
import com.sun.net.httpserver.*; import java.net.InetSocketAddress; import java.util.*; import java.security.MessageDigest; import java.net.URLDecoder;
public class LbankClientCheck {
  static int pass, fail; static void ck(String n, boolean ok){ if(ok) pass++; else {fail++; System.out.println("FAIL "+n);} }
  public static void main(String[] a) throws Exception {
    List<String> bodies = new ArrayList<>(); List<Headers> hs = new ArrayList<>(); Map<String,String> routes = new HashMap<>();
    HttpServer s = HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
    s.createContext("/", ex -> { bodies.add(new String(ex.getRequestBody().readAllBytes())); hs.add(ex.getRequestHeaders());
      byte[] b = routes.getOrDefault(ex.getRequestURI().getPath(),"{}").getBytes(); ex.sendResponseHeaders(200,b.length); ex.getResponseBody().write(b); ex.close(); });
    s.start(); String url="http://127.0.0.1:"+s.getAddress().getPort();
    var f = new SymbolFilters(); f.put("BTCUSDT", new SymbolFilters.Filter(0.0001,1e9,0.0001,0,1e9,0.01,1));
    var c = new LbankRestClient(new ExchangeConfig("lbank",true,false,url,"",5000,List.of("BTCUSDT"),20,100), new Credentials("KEY","SECRET"), f);
    routes.put("/v2/supplement/create_order.do","{\"result\":\"true\",\"data\":{\"order_id\":\"uuid-1\"},\"error_code\":0}");
    routes.put("/v2/supplement/orders_info.do","{\"result\":\"true\",\"data\":{\"orders\":[{\"status\":2,\"deal_amount\":\"0.5\",\"amount\":\"0.5\",\"avg_price\":\"100\",\"type\":\"sell_market\"}]},\"error_code\":0}");
    OrderResult r = c.sellMarket("BTCUSDT", 0.5);
    ck("lbank filled", r.isFilled() && r.executedQty()==0.5 && r.avgPrice()==100 && r.side()==Side.SELL);
    Map<String,String> p = new TreeMap<>(); for (String kv : bodies.get(0).split("&")) { String[] x = kv.split("=",2); p.put(x[0], URLDecoder.decode(x[1],"UTF-8")); }
    ck("params", p.get("symbol").equals("btc_usdt") && p.get("type").equals("sell_market") && p.get("amount").equals("0.5") && p.get("signature_method").equals("HmacSHA256") && p.get("echostr").length()>=30);
    String sign = p.remove("sign"); StringBuilder sb = new StringBuilder(); for (var e : p.entrySet()) { if (sb.length()>0) sb.append('&'); sb.append(e.getKey()).append('=').append(e.getValue()); }
    String md5 = HexFormat.of().withUpperCase().formatHex(MessageDigest.getInstance("MD5").digest(sb.toString().getBytes()));
    ck("sign", sign.equals(Hmac.sha256Hex("SECRET", md5)));
    c.buyMarketForQuote("BTCUSDT", 25);
    ck("market buy uses price", bodies.stream().anyMatch(b->b.contains("type=buy_market") && b.contains("price=25") && !b.contains("amount=")));
    c.buyLimit("BTCUSDT", 0.1, 100.123, TimeInForce.FOK);
    ck("fok type", bodies.stream().anyMatch(b->b.contains("type=buy_fok") && b.contains("price=100.12")));
    boolean t=false; try { c.buyMarket("BTCUSDT", 0.5); } catch (IllegalArgumentException e) { t=true; } ck("market buy by base rejected", t);
    routes.put("/v2/supplement/create_order.do","{\"result\":\"false\",\"error_code\":10016,\"msg\":\"balance not enough\"}");
    t=false; try { c.sellMarket("BTCUSDT", 0.5); } catch (ApiException e) { t = e.getMessage().contains("balance"); } ck("error", t);
    s.stop(0); System.out.println("passed="+pass+" failed="+fail);
  }
}
