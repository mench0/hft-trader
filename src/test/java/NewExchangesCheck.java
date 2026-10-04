import com.fasterxml.jackson.databind.*;
import com.hft.config.*;
import com.hft.crypto.Web3jCrypto;
import com.hft.exchange.ExchangeFactory;
import com.hft.exchange.aster.AsterRestClient;
import com.hft.exchange.catalog.*;
import com.hft.exchange.generic.*;
import com.hft.exchange.kucoin.KucoinRestClient;
import com.hft.model.OrderResult;
import com.hft.store.*;
import com.hft.util.Hmac;
import com.sun.net.httpserver.*;
import java.math.BigInteger;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.BooleanSupplier;
import org.web3j.crypto.Keys;
import org.web3j.crypto.Sign;
import org.web3j.crypto.StructuredDataEncoder;
import org.web3j.utils.Numeric;

/** KuCoin и Aster: подпись, ордера, статусы, правила, балансы, стакан по WebSocket и REST, сборка шлюза. */
public class NewExchangesCheck {
  static int pass, fail;
  static void ck(String n, boolean ok){ if(ok) pass++; else {fail++; System.out.println("FAIL "+n);} }
  static boolean await(BooleanSupplier c, long ms) throws Exception { long t=System.currentTimeMillis()+ms; while(System.currentTimeMillis()<t){ if(c.getAsBoolean()) return true; Thread.sleep(20);} return c.getAsBoolean(); }
  static final ObjectMapper M = new ObjectMapper();

  record Req(String method, String pathQuery, Headers h, String body) {}

  static HttpServer server(Map<String, String> routes, List<Req> seen) throws Exception {
    HttpServer s = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    s.createContext("/", ex -> {
      String pq = ex.getRequestURI().getRawPath() + (ex.getRequestURI().getRawQuery() == null ? "" : "?" + ex.getRequestURI().getRawQuery());
      seen.add(new Req(ex.getRequestMethod(), pq, ex.getRequestHeaders(), new String(ex.getRequestBody().readAllBytes())));
      String key = ex.getRequestMethod() + " " + ex.getRequestURI().getPath();
      byte[] b = routes.getOrDefault(key, "{}").getBytes();
      ex.sendResponseHeaders(200, b.length); ex.getResponseBody().write(b); ex.close();
    });
    s.start();
    return s;
  }

  public static void main(String[] a) throws Exception {
    kucoinRest();
    asterRest();
    kucoinWs();
    asterWs();
    gateways();
    System.out.println("pass="+pass+" fail="+fail); System.exit(fail==0?0:1);
  }

  static void kucoinRest() throws Exception {
    Map<String, String> r = new HashMap<>(); List<Req> seen = new CopyOnWriteArrayList<>();
    r.put("GET /api/v2/symbols", "{\"code\":\"200000\",\"data\":[{\"symbol\":\"BTC-USDT\",\"baseMinSize\":\"0.00001\",\"baseMaxSize\":\"10000\",\"baseIncrement\":\"0.00000001\",\"priceIncrement\":\"0.1\",\"minFunds\":\"0.1\"},{\"symbol\":\"ETH-USDT\",\"baseMinSize\":\"0.0001\",\"baseIncrement\":\"0.0000001\",\"priceIncrement\":\"0.01\",\"minFunds\":\"0.1\"}]}");
    r.put("POST /api/v1/orders", "{\"code\":\"200000\",\"data\":{\"orderId\":\"5bd6e9286d99522a52e458de\"}}");
    r.put("GET /api/v1/orders/5bd6e9286d99522a52e458de", "{\"code\":\"200000\",\"data\":{\"id\":\"5bd6e9286d99522a52e458de\",\"symbol\":\"BTC-USDT\",\"side\":\"buy\",\"size\":\"0.01\",\"dealSize\":\"0.01\",\"dealFunds\":\"650\",\"isActive\":false,\"cancelExist\":false,\"clientOid\":\"x\"}}");
    r.put("GET /api/v1/accounts", "{\"code\":\"200000\",\"data\":[{\"currency\":\"USDT\",\"type\":\"trade\",\"balance\":\"110\",\"available\":\"100\",\"holds\":\"10\"},{\"currency\":\"BTC\",\"available\":\"0\",\"holds\":\"0\"}]}");
    r.put("DELETE /api/v1/orders", "{\"code\":\"200000\",\"data\":{\"cancelledOrderIds\":[\"a\",\"b\"]}}");
    HttpServer s = server(r, seen);
    String url = "http://127.0.0.1:" + s.getAddress().getPort();
    // KUCOIN_PASSPHRASE задаёт скрипт запуска проверок (pp)
    var f = new SymbolFilters();
    var c = new KucoinRestClient(new ExchangeConfig("kucoin", false, url, "", 5000, List.of("BTCUSDT"), 20, 100), new Credentials("KEY", "SECRET"), f);
    c.loadFilters(List.of("BTCUSDT", "ETHUSDT"));
    ck("kucoin filters loaded under internal symbol", f.has("BTCUSDT") && f.has("ETHUSDT") && Math.abs(f.get("BTCUSDT").tickSize() - 0.1) < 1e-12);
    OrderResult res = c.buyMarket("BTCUSDT", 0.01);
    Req place = seen.stream().filter(q -> q.method().equals("POST")).findFirst().get();
    JsonNode body = M.readTree(place.body());
    ck("kucoin order body", body.path("symbol").asText().equals("BTC-USDT") && body.path("side").asText().equals("buy")
        && body.path("type").asText().equals("market") && body.path("size").asText().equals("0.01") && !body.path("clientOid").asText().isEmpty());
    String ts = place.h().getFirst("KC-API-TIMESTAMP");
    ck("kucoin signature = base64 HMAC(ts+method+path+body)", Hmac.sha256Base64("SECRET", ts + "POST" + "/api/v1/orders" + place.body()).equals(place.h().getFirst("KC-API-SIGN")));
    ck("kucoin passphrase signed (key v2)", Hmac.sha256Base64("SECRET", "pp").equals(place.h().getFirst("KC-API-PASSPHRASE")) && "2".equals(place.h().getFirst("KC-API-KEY-VERSION")));
    ck("kucoin market order filled via status", res.isFilled() && Math.abs(res.executedQty() - 0.01) < 1e-12 && Math.abs(res.avgPrice() - 65000) < 1e-6);
    var bal = new BalanceStore(); c.loadBalances(bal);
    ck("kucoin balances (available/holds)", bal.free("USDT") == 100 && bal.total("USDT") == 110);
    ck("kucoin cancelAll by symbol", c.cancelAll("BTCUSDT") == 2 && seen.stream().anyMatch(q -> q.method().equals("DELETE") && q.pathQuery().equals("/api/v1/orders?symbol=BTC-USDT")));
    s.stop(0);
  }

  static void asterRest() throws Exception {
    Map<String, String> r = new HashMap<>(); List<Req> seen = new CopyOnWriteArrayList<>();
    r.put("GET /api/v3/exchangeInfo", "{\"symbols\":[{\"symbol\":\"BTCUSDT\",\"filters\":[{\"filterType\":\"PRICE_FILTER\",\"minPrice\":\"0.1\",\"maxPrice\":\"1000000\",\"tickSize\":\"0.1\"},{\"filterType\":\"LOT_SIZE\",\"minQty\":\"0.0001\",\"maxQty\":\"100\",\"stepSize\":\"0.0001\"},{\"filterType\":\"MIN_NOTIONAL\",\"minNotional\":\"5\"}]}]}");
    r.put("POST /api/v3/order", "{\"orderId\":123,\"symbol\":\"BTCUSDT\",\"status\":\"FILLED\",\"side\":\"BUY\",\"origQty\":\"0.01\",\"executedQty\":\"0.01\",\"cummulativeQuoteQty\":\"650\"}");
    r.put("GET /api/v3/account", "{\"balances\":[{\"asset\":\"USDT\",\"free\":\"50\",\"locked\":\"1\"}]}");
    HttpServer s = server(r, seen);
    String url = "http://127.0.0.1:" + s.getAddress().getPort();
    String key = "0x4c0883a69102937d6231471b5dbb6204fe5129617082792ae468d01a3f362318";
    var crypto = new Web3jCrypto(key);
    var f = new SymbolFilters();
    var c = new AsterRestClient(new ExchangeConfig("aster", false, url, "", 5000, List.of("BTCUSDT"), 20, 100), new Credentials("0x00000000000000000000000000000000000000aa", key), f, crypto);
    c.loadFilters(List.of("BTCUSDT"));
    ck("aster filters (Binance format)", f.has("BTCUSDT") && Math.abs(f.get("BTCUSDT").stepSize() - 0.0001) < 1e-12);
    OrderResult res = c.buyMarket("BTCUSDT", 0.01);
    ck("aster immediate fill from order response", res.isFilled() && Math.abs(res.avgPrice() - 65000) < 1e-6);
    Req place = seen.stream().filter(q -> q.method().equals("POST")).findFirst().get();
    String pq = place.pathQuery();
    int sig = pq.indexOf("&signature=");
    String msg = pq.substring(pq.indexOf('?') + 1, sig), signature = pq.substring(sig + 11);
    Map<String, String> params = new HashMap<>();
    for (String kv : msg.split("&")) { int e = kv.indexOf('='); params.put(kv.substring(0, e), URLDecoder.decode(kv.substring(e + 1), StandardCharsets.UTF_8)); }
    ck("aster v3 auth params", params.get("user").equals("0x00000000000000000000000000000000000000aa") && params.get("signer").equals(crypto.address())
        && params.get("nonce").length() >= 16 && params.get("symbol").equals("BTCUSDT") && params.get("type").equals("MARKET"));
    // эталон: EIP-712 из web3j по описанию Aster, затем восстановление адреса из подписи
    String typed = "{\"types\":{\"EIP712Domain\":[{\"name\":\"name\",\"type\":\"string\"},{\"name\":\"version\",\"type\":\"string\"},{\"name\":\"chainId\",\"type\":\"uint256\"},{\"name\":\"verifyingContract\",\"type\":\"address\"}],"
        + "\"Message\":[{\"name\":\"msg\",\"type\":\"string\"}]},\"primaryType\":\"Message\",\"domain\":{\"name\":\"AsterSignTransaction\",\"version\":\"1\",\"chainId\":1666,\"verifyingContract\":\"0x0000000000000000000000000000000000000000\"},"
        + "\"message\":{\"msg\":" + M.writeValueAsString(msg) + "}}";
    byte[] digest = new StructuredDataEncoder(typed).hashStructuredData();
    byte[] sb = Numeric.hexStringToByteArray(signature);
    var sd = new Sign.SignatureData(sb[64], Arrays.copyOfRange(sb, 0, 32), Arrays.copyOfRange(sb, 32, 64));
    String recovered = "0x" + Keys.getAddress(Sign.signedMessageHashToKey(digest, sd));
    ck("aster EIP-712 signature recovers to signer (matches web3j encoder)", recovered.equalsIgnoreCase(crypto.address()) && sb.length == 65);
    long n1 = Long.parseLong(params.get("nonce"));
    c.buyMarket("BTCUSDT", 0.01);
    String pq2 = seen.stream().filter(q -> q.method().equals("POST")).reduce((x, y) -> y).get().pathQuery();
    long n2 = Long.parseLong(pq2.replaceAll(".*[?&]nonce=(\\d+).*", "$1"));
    ck("aster nonce strictly increases", n2 > n1);
    var bal = new BalanceStore(); c.loadBalances(bal);
    ck("aster balances", bal.free("USDT") == 50 && bal.total("USDT") == 51);
    s.stop(0);
  }

  record Rig(WsBookFeed feed, MarketDataStore market, AtomicInteger books) {}
  static Rig rig(String id, String restUrl, String wsUrl, List<String> syms) {
    var cfg = new ExchangeConfig(id, false, restUrl, wsUrl, 5000, syms, 20, 100);
    var m = new MarketDataStore(20, 100); syms.forEach(m::register);
    var books = new AtomicInteger();
    var f = new WsBookFeed(ExchangeCatalog.find(id).get(), cfg, WsDialects.forExchange(id).get(), m, (sy,px,q,bm,ts,rn) -> {}, s -> books.incrementAndGet(), () -> {});
    return new Rig(f, m, books);
  }

  static void kucoinWs() throws Exception {
    try (var ws = new MiniWsServer()) {
      Map<String, String> r = new HashMap<>(); List<Req> seen = new CopyOnWriteArrayList<>();
      r.put("POST /api/v1/bullet-public", "{\"code\":\"200000\",\"data\":{\"token\":\"TOK\",\"instanceServers\":[{\"endpoint\":\"" + ws.url() + "\",\"protocol\":\"websocket\",\"encrypt\":false,\"pingInterval\":18000,\"pingTimeout\":10000}]}}");
      HttpServer s = server(r, seen);
      ws.onOpen = c -> c.text("{\"id\":\"w1\",\"type\":\"welcome\"}");
      ws.onText = (c, t) -> {
        if (t.contains("\"subscribe\"")) {
          c.text("{\"id\":\"1\",\"type\":\"ack\"}");
          c.text("{\"type\":\"message\",\"topic\":\"/spotMarket/level2Depth50:BTC-USDT\",\"subject\":\"level2\",\"data\":{\"asks\":[[\"101\",\"2\"],[\"102\",\"1\"]],\"bids\":[[\"100\",\"3\"]],\"timestamp\":1700000000000}}");
        }
      };
      var rg = rig("kucoin", "http://127.0.0.1:" + s.getAddress().getPort(), "", List.of("BTCUSDT"));
      rg.feed().start();
      var b = rg.market().book("BTCUSDT");
      ck("kucoin WS: token from bullet-public, subscribe, book", await(() -> b.isReady() && b.bestBid() == 100 && b.bestAsk() == 101, 5000));
      ck("kucoin WS: connected with token and topic", ws.received.stream().anyMatch(t -> t.contains("/spotMarket/level2Depth50:BTC-USDT"))
          && seen.stream().anyMatch(q -> q.pathQuery().equals("/api/v1/bullet-public")));
      rg.feed().stop(); s.stop(0);
    }
  }

  static void asterWs() throws Exception {
    try (var ws = new MiniWsServer()) {
      ws.onText = (c, t) -> {
        if (t.contains("SUBSCRIBE")) {
          c.text("{\"result\":null,\"id\":1}");
          c.text("{\"stream\":\"btcusdt@depth20@100ms\",\"data\":{\"lastUpdateId\":5,\"bids\":[[\"64999.9\",\"0.5\"]],\"asks\":[[\"65000.1\",\"0.7\"]]}}");
        }
      };
      var rg = rig("aster", "http://127.0.0.1:9", ws.url(), List.of("BTCUSDT"));
      rg.feed().start();
      var b = rg.market().book("BTCUSDT");
      ck("aster WS: SUBSCRIBE depth20 and snapshot book", await(() -> b.isReady() && b.bestBid() == 64999.9 && b.bestAsk() == 65000.1, 5000)
          && ws.received.stream().anyMatch(t -> t.contains("btcusdt@depth20@100ms")));
      rg.feed().stop();
    }
    // REST-запас обеих бирж
    ck("REST book dialects", Dialects.forExchange("kucoin").parse("{\"code\":\"200000\",\"data\":{\"time\":1,\"bids\":[[\"10\",\"1\"]],\"asks\":[[\"11\",\"2\"]]}}", "BTCUSDT").bp()[0] == 10
        && Dialects.forExchange("aster").parse("{\"lastUpdateId\":1,\"bids\":[[\"20\",\"1\"]],\"asks\":[[\"21\",\"2\"]]}", "BTCUSDT").ap()[0] == 21
        && Dialects.forExchange("kucoin").request("https://api.kucoin.com", "BTCUSDT", 20).uri().toString().endsWith("/api/v1/market/orderbook/level2_20?symbol=BTC-USDT"));
  }

  static void gateways() throws Exception {
    var settings = new TradingSettings(TradingParams.DEFAULTS);
    for (String id : List.of("kucoin", "aster")) {
      var gw = ExchangeFactory.create(id, new ExchangeConfig(id, false, "http://127.0.0.1:9", "ws://127.0.0.1:9/ws", 5000, List.of("BTCUSDT"), 20, 100), settings);
      ck(id + " gateway in paper mode", gw.id().equals(id) && gw instanceof RequestStatsSource rs && !rs.isLive());
    }
    ck("catalog lists new exchanges as selectable", ExchangeCatalog.runnableIds().containsAll(List.of("kucoin", "aster")));
  }
}
