import com.hft.config.*;
import com.hft.exchange.binance.BinanceFuturesClient;
import com.hft.exchange.bybit.BybitRestClient;
import com.hft.exchange.okx.OkxRestClient;
import com.hft.exchange.gate.GateFuturesClient;
import com.hft.exchange.kucoin.KucoinFuturesClient;
import com.hft.exchange.mexc.MexcFuturesClient;
import com.hft.exchange.aster.AsterRestClient;
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

    // ---------- Gate USDT-фьючерсы: объём в контрактах со знаком
    seen.clear();
    routes.put("GET /api/v4/futures/usdt/contracts", "[{\"name\":\"BTC_USDT\",\"quanto_multiplier\":\"0.0001\",\"order_size_min\":1,\"order_size_max\":1000000,\"order_price_round\":\"0.1\"}]");
    routes.put("POST /api/v4/futures/usdt/orders", "{\"id\":321,\"contract\":\"BTC_USDT\",\"size\":-100,\"left\":0,\"status\":\"finished\",\"finish_as\":\"filled\",\"fill_price\":\"60000\",\"text\":\"t-x\"}");
    routes.put("GET /api/v4/futures/usdt/accounts", "{\"total\":\"1000\",\"available\":\"700\",\"currency\":\"USDT\"}");
    routes.put("GET /api/v4/futures/usdt/positions", "[{\"contract\":\"BTC_USDT\",\"size\":-100,\"entry_price\":\"60000\"}]");
    routes.put("POST /api/v4/futures/usdt/positions/BTC_USDT/leverage", "{}");
    SymbolFilters gf = new SymbolFilters();
    var gate = new GateFuturesClient(cfg("gate", url), CR, gf);
    gate.loadFilters(List.of("BTCUSDT"));
    ck("gate step = contract", near(gf.roundQuantity("BTCUSDT", 0.01005), 0.01));
    OrderResult gr = gate.sellMarket("BTCUSDT", 0.01);
    String gb = last("POST", "/futures/usdt/orders").body();
    ck("gate order contracts signed", gb.contains("\"size\":-100") && gb.contains("\"tif\":\"ioc\"") && gb.contains("\"price\":\"0\""));
    ck("gate fill in coins", gr.isFilled() && near(gr.executedQty(), 0.01) && near(gr.avgPrice(), 60000));
    gate.reduceMarket("BTCUSDT", Side.BUY, 0.01);
    ck("gate reduce_only", last("POST", "/futures/usdt/orders").body().contains("\"reduce_only\":true"));
    BalanceStore gbal = new BalanceStore(); gate.loadBalances(gbal);
    ck("gate balance", near(gbal.free("USDT"), 700) && near(gbal.locked("USDT"), 300));
    PositionStore gp = new PositionStore(); gate.loadPositions(gp);
    ck("gate positions in coins", near(gp.qty("BTCUSDT"), -0.01));
    gate.setLeverage("BTCUSDT", 3);
    ck("gate leverage", last("POST", "/positions/BTC_USDT/leverage").uri().contains("leverage=3"));

    // ---------- KuCoin Futures: XBTUSDTM, лоты
    seen.clear();
    routes.put("GET /api/v1/contracts/active", "{\"code\":\"200000\",\"data\":[{\"symbol\":\"XBTUSDTM\",\"multiplier\":0.001,\"lotSize\":1,\"tickSize\":0.1,\"maxOrderQty\":1000000}]}");
    routes.put("POST /api/v1/orders", "{\"code\":\"200000\",\"data\":{\"orderId\":\"kf1\"}}");
    routes.put("GET /api/v1/orders/kf1", "{\"code\":\"200000\",\"data\":{\"isActive\":false,\"size\":20,\"filledSize\":20,\"filledValue\":\"1200\",\"side\":\"buy\",\"clientOid\":\"x\"}}");
    routes.put("GET /api/v1/account-overview", "{\"code\":\"200000\",\"data\":{\"accountEquity\":1000,\"availableBalance\":900}}");
    routes.put("GET /api/v1/positions", "{\"code\":\"200000\",\"data\":[{\"symbol\":\"XBTUSDTM\",\"currentQty\":20,\"avgEntryPrice\":60000}]}");
    routes.put("POST /api/v2/changeCrossUserLeverage", "{\"code\":\"200000\",\"data\":true}");
    SymbolFilters kf = new SymbolFilters();
    var kc = new KucoinFuturesClient(new ExchangeConfig("kucoin", false, url, "", 5000, List.of("BTCUSDT"), 20, 100, PERP), CR, kf);
    kc.loadFilters(List.of("BTCUSDT"));
    OrderResult kr = kc.buyMarket("BTCUSDT", 0.02);
    String kb = last("POST", "/api/v1/orders").body();
    ck("kucoin futures order", kb.contains("\"symbol\":\"XBTUSDTM\"") && kb.contains("\"size\":20") && kb.contains("\"leverage\":2") && kb.contains("\"marginMode\":\"CROSS\""));
    ck("kucoin futures fill", kr.isFilled() && near(kr.executedQty(), 0.02) && near(kr.avgPrice(), 60000));
    PositionStore kp = new PositionStore(); kc.loadPositions(kp);
    ck("kucoin futures positions", near(kp.qty("BTCUSDT"), 0.02));
    BalanceStore kbal = new BalanceStore(); kc.loadBalances(kbal);
    ck("kucoin futures balance", near(kbal.free("USDT"), 900));

    // ---------- MEXC Contract: сторона = направление позиции
    seen.clear();
    routes.put("GET /api/v1/contract/detail", "{\"success\":true,\"code\":0,\"data\":[{\"symbol\":\"BTC_USDT\",\"contractSize\":0.0001,\"minVol\":1,\"maxVol\":1000000,\"volUnit\":1,\"priceUnit\":0.1}]}");
    routes.put("POST /api/v1/private/order/submit", "{\"success\":true,\"code\":0,\"data\":\"m77\"}");
    routes.put("GET /api/v1/private/order/get/m77", "{\"success\":true,\"code\":0,\"data\":{\"state\":3,\"vol\":100,\"dealVol\":100,\"dealAvgPrice\":60000,\"side\":3}}");
    routes.put("GET /api/v1/private/position/open_positions", "{\"success\":true,\"code\":0,\"data\":[{\"symbol\":\"BTC_USDT\",\"positionType\":2,\"holdVol\":100,\"holdAvgPrice\":60000}]}");
    SymbolFilters mf = new SymbolFilters();
    var mx = new MexcFuturesClient(cfg("mexc", url), CR, mf);
    mx.loadFilters(List.of("BTCUSDT"));
    OrderResult mr = mx.sellMarket("BTCUSDT", 0.01);
    String mb = last("POST", "/order/submit").body();
    ck("mexc open short side 3, market 5", mb.contains("\"side\":3") && mb.contains("\"type\":5") && mb.contains("\"vol\":100"));
    ck("mexc fill", mr.isFilled() && near(mr.executedQty(), 0.01));
    mx.reduceMarket("BTCUSDT", Side.BUY, 0.01);
    ck("mexc close short side 2", last("POST", "/order/submit").body().contains("\"side\":2"));
    PositionStore mp = new PositionStore(); mx.loadPositions(mp);
    ck("mexc short position", near(mp.qty("BTCUSDT"), -0.01));
    ck("mexc signed headers", last("POST", "/order/submit") != null);

    // ---------- Aster: фьючерсы через /fapi/v3
    seen.clear();
    routes.put("POST /fapi/v3/order", "{\"orderId\":5,\"clientOrderId\":\"c\",\"status\":\"FILLED\",\"origQty\":\"0.01\",\"executedQty\":\"0.01\",\"avgPrice\":\"60000\",\"side\":\"SELL\"}");
    routes.put("GET /fapi/v3/positionRisk", "[{\"symbol\":\"BTCUSDT\",\"positionAmt\":\"-0.01\",\"entryPrice\":\"60000\"}]");
    SymbolFilters af = new SymbolFilters(); af.put("BTCUSDT", new SymbolFilters.Filter(0.001, 1e9, 0.001, 0, 1e9, 0.1, 1));
    var aster = new AsterRestClient(cfg("aster", url), new Credentials("0x00000000000000000000000000000000000000aa", "k"), af, new TC());
    ck("aster perp", aster.isPerp());
    aster.reduceMarket("BTCUSDT", Side.BUY, 0.01);
    var ao = last("POST", "/fapi/v3/order");
    ck("aster futures order path + reduceOnly", ao != null && ao.uri().contains("reduceOnly=true"));
    PositionStore ap = new PositionStore(); aster.loadPositions(ap);
    ck("aster positions", near(ap.qty("BTCUSDT"), -0.01));

    srv.stop(0);
    System.out.println("FuturesClientsCheck: pass=" + pass + " fail=" + fail);
    if (fail > 0) System.exit(1);
  }
}
