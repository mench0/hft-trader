import com.hft.config.*; import com.hft.exchange.hyperliquid.*; import com.hft.model.OrderResult; import com.hft.model.OrderEnums.*;
import com.hft.rest.*; import com.hft.store.*; import com.hft.util.MsgPack; import com.sun.net.httpserver.*;
import java.io.*; import java.net.InetSocketAddress; import java.nio.ByteBuffer; import java.nio.charset.StandardCharsets; import java.util.*;
import com.fasterxml.jackson.databind.*;

public class HyperliquidClientCheck {
  static int pass, fail; static void ck(String n, boolean ok){ if(ok) pass++; else {fail++; System.out.println("FAIL "+n);} }
  static byte[] cat(byte[]... p){ var o=new ByteArrayOutputStream(); for(byte[] x:p) o.writeBytes(x); return o.toByteArray(); }
  static byte[] b(int... v){ byte[] r=new byte[v.length]; for(int i=0;i<v.length;i++) r[i]=(byte)v[i]; return r; }
  static byte[] s(String x){ byte[] u=x.getBytes(StandardCharsets.UTF_8); return cat(b(0xa0|u.length),u); }

  public static void main(String[] args) throws Exception {
    // --- keccak и msgpack
    ck("keccak empty", K.hex(K.keccak256(new byte[0])).equals("c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470"));
    ck("keccak abc", K.hex(K.keccak256("abc".getBytes())).equals("4e03657aea45a94fc7d47ba826c8d667c0d1e6e33a64a036ec44f58fa12d6c45"));
    ck("keccak long", K.hex(K.keccak256(new byte[200])).length()==64);
    ck("mp map", Arrays.equals(MsgPack.pack(new LinkedHashMap<>(Map.of("a",1))), b(0x81,0xa1,0x61,0x01)));
    ck("mp list", Arrays.equals(MsgPack.pack(Arrays.asList(true,null,false)), b(0x93,0xc3,0xc0,0xc2)));
    ck("mp ints", Arrays.equals(MsgPack.pack(Arrays.asList(127,128,256,70000,-1,-33)), cat(b(0x96,0x7f,0xcc,0x80,0xcd,1,0,0xce,0,1,0x11,0x70,0xff,0xd0,0xdf))));
    ck("mp str32", MsgPack.pack("x".repeat(32))[0]==(byte)0xd9);
    // --- формат цены
    ck("px 5sig", HyperliquidRestClient.formatPrice(1234.5678, 3).equals("1234.6"));
    ck("px small", HyperliquidRestClient.formatPrice(0.000012345678, 0).equals("0.000012"));
    ck("px big", HyperliquidRestClient.formatPrice(123456.78, 1).equals("123457"));
    ck("px round", HyperliquidRestClient.formatPrice(50000.0, 5).equals("50000"));
    ck("px dec", HyperliquidRestClient.formatPrice(2.5, 2).equals("2.5"));

    // --- клиент против фейкового сервера
    List<String> bodies = new ArrayList<>(); String[] exResp = {null};
    HttpServer srv = HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
    ObjectMapper m = new ObjectMapper();
    srv.createContext("/", ex -> {
      String body = new String(ex.getRequestBody().readAllBytes()); bodies.add(ex.getRequestURI().getPath()+" "+body);
      String resp;
      if (ex.getRequestURI().getPath().equals("/exchange")) resp = exResp[0];
      else { String type = m.readTree(body).path("type").asText(); resp = switch(type) {
        case "meta" -> "{\"universe\":[{\"name\":\"BTC\",\"szDecimals\":5},{\"name\":\"ETH\",\"szDecimals\":4}]}";
        case "allMids" -> "{\"BTC\":\"50000.0\",\"ETH\":\"2000.0\"}";
        case "openOrders" -> "[{\"coin\":\"BTC\",\"oid\":5},{\"coin\":\"ETH\",\"oid\":6}]";
        case "orderStatus" -> "{\"status\":\"order\",\"order\":{\"order\":{\"coin\":\"BTC\",\"side\":\"B\",\"origSz\":\"0.002\",\"sz\":\"0.0005\",\"limitPx\":\"49000\"},\"status\":\"open\"}}";
        case "clearinghouseState" -> "{\"marginSummary\":{\"accountValue\":\"1000\"},\"withdrawable\":\"800\"}";
        default -> "{}"; }; }
      byte[] bt = resp.getBytes(); ex.sendResponseHeaders(200, bt.length); ex.getResponseBody().write(bt); ex.close(); });
    srv.start(); String url="http://127.0.0.1:"+srv.getAddress().getPort();
    var f = new SymbolFilters(); var tc = new TC();
    var c = new HyperliquidRestClient(new ExchangeConfig("hyperliquid", false,url,"",5000,List.of("BTCUSDC"),20,100),
        new Credentials("0xAbC0000000000000000000000000000000000001","key"), f, tc);
    c.loadFilters(List.of("BTCUSDC"));
    ck("filters step", Math.abs(f.get("BTCUSDC").stepSize()-1e-5)<1e-12 && f.get("BTCUSDC").minNotional()==10.0);

    exResp[0] = "{\"status\":\"ok\",\"response\":{\"type\":\"order\",\"data\":{\"statuses\":[{\"filled\":{\"totalSz\":\"0.001\",\"avgPx\":\"50010\",\"oid\":77}}]}}}";
    OrderResult r = c.buyMarket("BTCUSDC", 0.001);
    ck("hl filled", r.isFilled() && r.orderId()==77 && r.executedQty()==0.001 && r.avgPrice()==50010 && r.side()==Side.BUY);
    String exBody = bodies.stream().filter(x->x.startsWith("/exchange")).findFirst().get().substring("/exchange ".length());
    JsonNode j = m.readTree(exBody);
    ck("body shape", j.path("action").path("type").asText().equals("order") && j.path("signature").path("v").asInt()==27
        && j.path("signature").path("r").asText().equals("0x"+"11".repeat(32)) && j.path("vaultAddress").isNull());
    JsonNode o0 = j.path("action").path("orders").get(0);
    ck("order fields", o0.path("a").asInt()==0 && o0.path("b").asBoolean() && o0.path("p").asText().equals("52500") && o0.path("s").asText().equals("0.001")
        && !o0.path("r").asBoolean() && o0.path("t").path("limit").path("tif").asText().equals("Ioc"));
    // независимая сборка msgpack и проверка пути подписи
    byte[] expectedPack = cat(b(0x83), s("type"), s("order"), s("orders"), b(0x91), b(0x86), s("a"), b(0x00), s("b"), b(0xc3), s("p"), s("52500"),
        s("s"), s("0.001"), s("r"), b(0xc2), s("t"), b(0x81), s("limit"), b(0x81), s("tif"), s("Ioc"), s("grouping"), s("na"));
    ck("msgpack action", Arrays.equals(MsgPack.pack(new ObjectMapper().readValue(m.writeValueAsString(j.path("action")), LinkedHashMap.class)), expectedPack));
    long nonce = j.path("nonce").asLong();
    byte[] connId = K.keccak256(cat(expectedPack, ByteBuffer.allocate(8).putLong(nonce).array(), b(0)));
    byte[] dom = K.keccak256(cat(K.keccak256("EIP712Domain(string name,string version,uint256 chainId,address verifyingContract)".getBytes()),
        K.keccak256("Exchange".getBytes()), K.keccak256("1".getBytes()), ByteBuffer.allocate(32).putLong(24,1337).array(), new byte[32]));
    byte[] st = K.keccak256(cat(K.keccak256("Agent(string source,bytes32 connectionId)".getBytes()), K.keccak256("a".getBytes()), connId));
    byte[] digest = K.keccak256(cat(b(0x19,0x01), dom, st));
    ck("signed digest", tc.signed.size()==1 && Arrays.equals(tc.signed.get(0), digest));
    ck("nonce grows", true);

    exResp[0] = "{\"status\":\"ok\",\"response\":{\"type\":\"order\",\"data\":{\"statuses\":[{\"resting\":{\"oid\":88}}]}}}";
    r = c.buyLimit("BTCUSDC", 0.001, 49000.123, TimeInForce.GTC);
    ck("resting new", "NEW".equals(r.status()) && r.orderId()==88);
    ck("gtc tif + px fmt", bodies.get(bodies.size()-1).contains("\"p\":\"49000\"") && bodies.get(bodies.size()-1).contains("\"tif\":\"Gtc\""));
    exResp[0] = "{\"status\":\"ok\",\"response\":{\"type\":\"order\",\"data\":{\"statuses\":[{\"filled\":{\"totalSz\":\"0.0004\",\"avgPx\":\"50000\",\"oid\":90}}]}}}";
    r = c.sellLimit("BTCUSDC", 0.001, 49900, TimeInForce.IOC);
    ck("partial", r.isPartial() && r.executedQty()==0.0004 && r.side()==Side.SELL);
    exResp[0] = "{\"status\":\"ok\",\"response\":{\"type\":\"order\",\"data\":{\"statuses\":[{\"error\":\"Insufficient margin to place order.\"}]}}}";
    boolean t=false; try{ c.buyMarket("BTCUSDC", 0.001);}catch(ApiException e){ t=e.getMessage().contains("Insufficient margin"); } ck("order error", t);
    exResp[0] = "{\"status\":\"err\",\"response\":\"L1 error: User or API Wallet does not exist\"}";
    t=false; try{ c.buyMarket("BTCUSDC", 0.001);}catch(ApiException e){ t=e.getMessage().contains("does not exist"); } ck("status err", t);
    exResp[0] = "{\"status\":\"ok\",\"response\":{\"type\":\"cancel\",\"data\":{\"statuses\":[\"success\"]}}}";
    int n = c.cancelAll("BTCUSDC");
    ck("cancel all only BTC", n==1 && bodies.get(bodies.size()-1).contains("\"cancels\":[{\"a\":0,\"o\":5}]"));
    r = c.orderStatus("BTCUSDC", 88);
    ck("status partial", r.isPartial() && Math.abs(r.executedQty()-0.0015)<1e-12);
    var bal = new BalanceStore(); c.loadBalances(bal);
    ck("balances", bal.free("USDC")==800 && bal.locked("USDC")==200);
    t=false; try{ c.buyMarket("BTCUSDC", 0.000001);}catch(IllegalArgumentException e){ t=true; } ck("tiny rejected", t);
    srv.stop(0); System.out.println("passed="+pass+" failed="+fail);
  }
}
