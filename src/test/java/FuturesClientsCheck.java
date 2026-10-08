import com.hft.config.*;
import com.hft.exchange.binance.BinanceFuturesClient;
import com.hft.exchange.bybit.BybitRestClient;
import com.hft.exchange.okx.OkxRestClient;
import com.hft.model.OrderEnums.Side;
import com.hft.model.OrderResult;
import com.hft.store.*;
import com.sun.net.httpserver.*;

import java.net.InetSocketAddress;
import java.util.*;

/** Фьючерсные клиенты на поддельном сервере: что уходит на биржу и как разбирается ответ. */
public class FuturesClientsCheck {
  static int pass, fail;
  static void ck(String n, boolean ok){ if(ok) pass++; else {fail++; System.out.println("FAIL "+n);} }
  static boolean near(double a,double b){ return Math.abs(a-b)<1e-9*Math.max(1,Math.abs(b)); }
  record Seen(String method, String uri, String body) {}
  static final List<Seen> seen = Collections.synchronizedList(new ArrayList<>());
  static final Map<String,String> routes = new HashMap<>();

  static HttpServer server() throws Exception {
    HttpServer s = HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
    s.createContext("/", ex -> {
      String body = new String(ex.getRequestBody().readAllBytes());
      seen.add(new Seen(ex.getRequestMethod(), ex.getRequestURI().toString(), body));
      String resp = routes.getOrDefault(ex.getRequestMethod()+" "+ex.getRequestURI().getPath(), "{}");
      byte[] b = resp.getBytes(); ex.sendResponseHeaders(200, b.length); ex.getResponseBody().write(b); ex.close();
    });
    s.start(); return s;
  }
  static final TradingParams PERP = TradingParams.DEFAULTS.with(Map.of("market", "perp", "wsTrade", "false", "marketFillWaitMs", "0"));
  static ExchangeConfig cfg(String id, String url){ return new ExchangeConfig(id, false, url, "", 5000, List.of("BTCUSDT"), 20, 100, PERP); }
  static Seen last(String method, String pathPart){
    synchronized (seen) { for (int i = seen.size()-1; i >= 0; i--) { Seen s = seen.get(i); if (s.method().equals(method) && s.uri().contains(pathPart)) return s; } }
    return null;
  }
  static final Credentials CR = new Credentials("KEY","SECRET");

  public static void main(String[] a) throws Exception {
    var srv = server(); String url = "http://127.0.0.1:"+srv.getAddress().getPort();

    // ---------- Binance USDⓈ-M
    routes.put("GET /fapi/v1/time", "{\"serverTime\":" + System.currentTimeMillis() + "}");
    routes.put("GET /fapi/v1/exchangeInfo", "{\"symbols\":[{\"symbol\":\"BTCUSDT\",\"contractType\":\"PERPETUAL\",\"filters\":["
        + "{\"filterType\":\"LOT_SIZE\",\"minQty\":\"0.001\",\"maxQty\":\"1000\",\"stepSize\":\"0.001\"},"
        + "{\"filterType\":\"PRICE_FILTER\",\"tickSize\":\"0.10\"},{\"filterType\":\"MIN_NOTIONAL\",\"notional\":\"100\"}]}]}");
    routes.put("POST /fapi/v1/order", "{\"orderId\":42,\"clientOrderId\":\"c\",\"symbol\":\"BTCUSDT\",\"side\":\"SELL\",\"status\":\"FILLED\",\"origQty\":\"0.010\",\"executedQty\":\"0.010\",\"avgPrice\":\"50000\"}");
    routes.put("GET /fapi/v2/balance", "[{\"asset\":\"USDT\",\"balance\":\"1000\",\"availableBalance\":\"800\"}]");
    routes.put("GET /fapi/v2/positionRisk", "[{\"symbol\":\"BTCUSDT\",\"positionAmt\":\"-0.010\",\"entryPrice\":\"50000\"},{\"symbol\":\"ETHUSDT\",\"positionAmt\":\"0\",\"entryPrice\":\"0\"}]");
    routes.put("POST /fapi/v1/leverage", "{\"leverage\":3,\"symbol\":\"BTCUSDT\"}");
    SymbolFilters bf = new SymbolFilters();
    var bin = new BinanceFuturesClient(cfg("binance", url), CR, bf);
    ck("binance perp", bin.isPerp());
    bin.loadFilters(List.of("BTCUSDT"));
    ck("binance filters", bf.has("BTCUSDT") && near(bf.roundQuantity("BTCUSDT", 0.0129), 0.012));
    OrderResult r = bin.sellMarket("BTCUSDT", 0.01);
    Seen o = last("POST", "/fapi/v1/order");
    ck("binance order filled", r.isFilled() && r.orderId() == 42 && near(r.avgPrice(), 50000));
    ck("binance order params", o != null && o.uri().contains("side=SELL") && o.uri().contains("type=MARKET") && o.uri().contains("quantity=0.01&") && o.uri().contains("signature="));
    ck("binance no reduceOnly on open", !o.uri().contains("reduceOnly"));
    bin.reduceMarket("BTCUSDT", Side.BUY, 0.01);
    ck("binance reduceOnly", last("POST", "/fapi/v1/order").uri().contains("reduceOnly=true"));
    BalanceStore bb = new BalanceStore(); bin.loadBalances(bb);
    ck("binance margin balance", near(bb.free("USDT"), 800) && near(bb.locked("USDT"), 200));
    PositionStore bp = new PositionStore(); bp.set("ETHUSDT", 1, 1);
    bin.loadPositions(bp);
    ck("binance positions", near(bp.qty("BTCUSDT"), -0.01) && bp.get("ETHUSDT").isFlat());
    bin.setLeverage("BTCUSDT", 3);
    ck("binance leverage", last("POST", "/fapi/v1/leverage").uri().contains("leverage=3"));
    // поток аккаунта: ORDER_TRADE_UPDATE и ACCOUNT_UPDATE
    var onUser = BinanceFuturesClient.class.getDeclaredMethod("onUserEvent", String.class); onUser.setAccessible(true);
    onUser.invoke(bin, "{\"e\":\"ACCOUNT_UPDATE\",\"a\":{\"B\":[],\"P\":[{\"s\":\"BTCUSDT\",\"pa\":\"0.02\",\"ep\":\"51000\",\"ps\":\"BOTH\"}]}}");
    ck("binance stream position", near(bp.qty("BTCUSDT"), 0.02) && near(bp.get("BTCUSDT").entryPrice(), 51000));

    // ---------- Bybit linear
    seen.clear();
    routes.put("GET /v5/market/instruments-info", "{\"retCode\":0,\"result\":{\"list\":[{\"symbol\":\"BTCUSDT\",\"lotSizeFilter\":{\"minOrderQty\":\"0.001\",\"maxOrderQty\":\"100\",\"qtyStep\":\"0.001\",\"minNotionalValue\":\"5\"},\"priceFilter\":{\"tickSize\":\"0.1\"}}]}}");
    routes.put("POST /v5/order/create", "{\"retCode\":0,\"result\":{\"orderId\":\"777\"}}");
    routes.put("POST /v5/position/set-leverage", "{\"retCode\":110043,\"retMsg\":\"leverage not modified\"}");
    routes.put("GET /v5/position/list", "{\"retCode\":0,\"result\":{\"list\":[{\"symbol\":\"BTCUSDT\",\"side\":\"Sell\",\"size\":\"0.5\",\"avgPrice\":\"60000\"}]}}");
    routes.put("GET /v5/account/wallet-balance", "{\"retCode\":0,\"result\":{\"list\":[{\"coin\":[{\"coin\":\"USDT\",\"walletBalance\":\"1000\",\"totalPositionIM\":\"150\",\"totalOrderIM\":\"50\",\"locked\":\"0\"}]}]}}");
    SymbolFilters yf = new SymbolFilters();
    var by = new BybitRestClient(cfg("bybit", url), CR, yf);
    ck("bybit perp", by.isPerp());
    by.loadFilters(List.of("BTCUSDT"));
    ck("bybit linear instruments", last("GET", "/v5/market/instruments-info").uri().contains("category=linear") && near(yf.roundQuantity("BTCUSDT", 0.0129), 0.012));
    by.reduceMarket("BTCUSDT", Side.BUY, 0.5);
    String body = last("POST", "/v5/order/create").body();
    ck("bybit linear order", body.contains("\"category\":\"linear\"") && body.contains("\"reduceOnly\":true") && !body.contains("marketUnit"));
    by.setLeverage("BTCUSDT", 2);
    ck("bybit leverage not modified ok", last("POST", "/v5/position/set-leverage").body().contains("\"buyLeverage\":\"2\""));
    PositionStore yp = new PositionStore(); by.loadPositions(yp);
    ck("bybit positions", near(yp.qty("BTCUSDT"), -0.5));
    BalanceStore yb = new BalanceStore(); by.loadBalances(yb);
    ck("bybit margin balance", near(yb.free("USDT"), 800) && near(yb.locked("USDT"), 200));

    // ---------- OKX SWAP: объём в контрактах
    seen.clear();
    routes.put("GET /api/v5/public/instruments", "{\"code\":\"0\",\"data\":[{\"instId\":\"BTC-USDT-SWAP\",\"settleCcy\":\"USDT\",\"ctVal\":\"0.01\",\"lotSz\":\"0.1\",\"minSz\":\"0.1\",\"tickSz\":\"0.1\",\"maxLmtSz\":\"10000\"}]}");
    routes.put("POST /api/v5/trade/order", "{\"code\":\"0\",\"data\":[{\"ordId\":\"9\",\"sCode\":\"0\"}]}");
    routes.put("GET /api/v5/trade/order", "{\"code\":\"0\",\"data\":[{\"instId\":\"BTC-USDT-SWAP\",\"state\":\"filled\",\"accFillSz\":\"25\",\"sz\":\"25\",\"avgPx\":\"60000\",\"side\":\"sell\",\"clOrdId\":\"x\"}]}");
    routes.put("GET /api/v5/account/positions", "{\"code\":\"0\",\"data\":[{\"instId\":\"BTC-USDT-SWAP\",\"pos\":\"-25\",\"avgPx\":\"60000\"}]}");
    routes.put("POST /api/v5/account/set-leverage", "{\"code\":\"0\",\"data\":[{}]}");
    SymbolFilters of = new SymbolFilters();
    var okx = new OkxRestClient(cfg("okx", url), CR, of);
    okx.loadFilters(List.of("BTCUSDT"));
    ck("okx swap step in coins", near(of.roundQuantity("BTCUSDT", 0.2549), 0.254));
    OrderResult or = okx.sellMarket("BTCUSDT", 0.25);
    String ob = last("POST", "/api/v5/trade/order").body();
    ck("okx swap order", ob.contains("\"instId\":\"BTC-USDT-SWAP\"") && ob.contains("\"tdMode\":\"cross\"") && ob.contains("\"sz\":\"25\"") && !ob.contains("tgtCcy"));
    ck("okx swap fill in coins", or.isFilled() && near(or.executedQty(), 0.25));
    okx.reduceMarket("BTCUSDT", Side.BUY, 0.25);
    ck("okx reduceOnly", last("POST", "/api/v5/trade/order").body().contains("\"reduceOnly\":true"));
    PositionStore op = new PositionStore(); okx.loadPositions(op);
    ck("okx positions in coins", near(op.qty("BTCUSDT"), -0.25));
    okx.setLeverage("BTCUSDT", 3);
    ck("okx leverage", last("POST", "/api/v5/account/set-leverage").body().contains("\"lever\":\"3\""));

    srv.stop(0);
    System.out.println("FuturesClientsCheck: pass=" + pass + " fail=" + fail);
    if (fail > 0) System.exit(1);
  }
}
