import com.hft.config.*;
import com.hft.engine.OrderService;
import com.hft.exchange.binance.BinanceFuturesClient;
import com.hft.exchange.bybit.BybitRestClient;
import com.hft.exchange.gate.GateFuturesClient;
import com.hft.exchange.kucoin.KucoinFuturesClient;
import com.hft.exchange.okx.OkxRestClient;
import com.hft.model.OrderEnums.Side;
import com.hft.paper.PaperOrderApi;
import com.hft.perp.PositionGuard;
import com.hft.risk.RiskManager;
import com.hft.store.*;
import com.sun.net.httpserver.HttpServer;

import java.net.InetSocketAddress;
import java.util.*;

/** Защита позиций: стоп на бирже (бумага и формат запросов бирж), «ничьи» позиции, владельцы позиций. */
public class PositionProtectionCheck {
  static int pass, fail;
  static void ck(String n, boolean ok){ if(ok) pass++; else {fail++; System.out.println("FAIL "+n);} }
  static boolean near(double a,double b){ return Math.abs(a-b)<1e-6*Math.max(1,Math.abs(b)); }
  record Seen(String method, String uri, String body) {}
  static final List<Seen> seen = Collections.synchronizedList(new ArrayList<>());
  static final Map<String,String> routes = new HashMap<>();
  static Seen last(String method, String part){
    synchronized (seen) { for (int i = seen.size()-1; i >= 0; i--) { Seen s = seen.get(i); if (s.method().equals(method) && s.uri().contains(part)) return s; } }
    return null;
  }
  static void book(MarketDataStore m, double bid, double ask) {
    long now = System.currentTimeMillis();
    m.book("BTCUSDT").applySnapshot(new double[]{bid}, new double[]{100}, 1, new double[]{ask}, new double[]{100}, 1, now, now);
  }

  public static void main(String[] a) throws Exception {
    // ───── бумага: стоп на бирже ставится после входа, переставляется, снимается и срабатывает
    var params = TradingParams.DEFAULTS.with(Map.of("market", "perp", "tradingEnabled", "true", "exchangeStopLossPercent", "1",
        "maxPositionQuote", "1000000", "maxSlippagePercent", "100"));
    var settings = new TradingSettings(params);
    var market = new MarketDataStore(20, 100); market.register("BTCUSDT");
    var balances = new BalanceStore(); balances.set("USDT", 100_000, 0);
    var positions = new PositionStore();
    var paper = new PaperOrderApi(market, balances, 0, 0).perp(positions);
    var filters = new SymbolFilters(); filters.put("BTCUSDT", new SymbolFilters.Filter(0, 1e12, 1e-6, 0, 1e12, 0.01, 0));
    var orders = new OrderService(paper, market, balances, filters, new RiskManager(settings, market, "paper"), settings, positions);
    book(market, 99.99, 100);
    var r = orders.buyMarket("BTCUSDT", 1);
    ck("long opened", r.executedQty() > 0 && near(positions.qty("BTCUSDT"), 1));
    ck("stop placed 1% below entry", paper.openStops() == 1 && near(orders.stopPrice("BTCUSDT"), 99.0));
    orders.buyMarket("BTCUSDT", 1);
    ck("stop moved to new size, old removed", paper.openStops() == 1 && near(orders.stopPrice("BTCUSDT"), 99.0));
    orders.closePosition("BTCUSDT");
    ck("stop removed when flat", paper.openStops() == 0 && Double.isNaN(orders.stopPrice("BTCUSDT")));
    book(market, 100, 100.01);
    orders.sellMarket("BTCUSDT", 1);
    ck("short: stop 1% above", near(positions.qty("BTCUSDT"), -1) && near(orders.stopPrice("BTCUSDT"), 101.0));
    book(market, 101.0, 101.02);           // аск дошёл до стопа
    paper.settle("BTCUSDT");
    ck("paper stop triggered, position flat", paper.stopsTriggered() == 1 && positions.get("BTCUSDT").isFlat());
    var guard = new PositionGuard("paper", positions, orders, market, settings);
    guard.check();
    ck("guard forgets triggered stop", orders.stopSymbols().isEmpty());

    // ───── «ничья» позиция: под защиту, стоп-лосс orphanStopLossPercent, владелец — не трогать
    settings.set(params.with(Map.of("exchangeStopLossPercent", "0", "orphanStopLossPercent", "2")));
    positions.set("BTCUSDT", 1, 100);       // осталась после перезапуска
    book(market, 99, 99.02);
    guard.check();
    ck("orphan adopted, -1% kept", near(positions.qty("BTCUSDT"), 1) && guard.stats().get("orphans").toString().contains("BTCUSDT"));
    ck("owned position is not orphan", orders.claim("BTCUSDT", "mean-reversion") && !orders.claim("BTCUSDT", "stat-arb"));
    book(market, 97, 97.02);
    guard.check();
    ck("owned position untouched by guard", near(positions.qty("BTCUSDT"), 1));
    orders.release("BTCUSDT", "mean-reversion");
    guard.check();
    ck("orphan closed on stop-loss", positions.get("BTCUSDT").isFlat() && ((Number) guard.stats().get("orphansClosed")).longValue() == 1);
    // таймаут
    settings.set(params.with(Map.of("exchangeStopLossPercent", "0", "orphanStopLossPercent", "50", "orphanMaxHoldMinutes", "0")));
    positions.set("BTCUSDT", -1, 97);
    guard.check();
    ck("guardOrphans: no timeout by default", near(positions.qty("BTCUSDT"), -1));
    settings.set(params.with(Map.of("exchangeStopLossPercent", "0", "guardOrphans", "false")));
    book(market, 200, 200.1);
    guard.check();
    ck("guardOrphans=false: not touched", near(positions.qty("BTCUSDT"), -1));
    // сторож ставит стоп на бирже и для «ничьей» позиции
    settings.set(params.with(Map.of("exchangeStopLossPercent", "1", "orphanStopLossPercent", "90")));
    book(market, 97, 97.02);
    guard.check();
    ck("guard places exchange stop for orphan", near(orders.stopPrice("BTCUSDT"), 97.97));

    // ───── формат запросов стопа у бирж (поддельный сервер)
    HttpServer s = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    s.createContext("/", ex -> {
      String body = new String(ex.getRequestBody().readAllBytes());
      seen.add(new Seen(ex.getRequestMethod(), ex.getRequestURI().toString(), body));
      String resp = routes.getOrDefault(ex.getRequestMethod() + " " + ex.getRequestURI().getPath(), "{}");
      byte[] b = resp.getBytes(); ex.sendResponseHeaders(200, b.length); ex.getResponseBody().write(b); ex.close();
    });
    s.start();
    String url = "http://127.0.0.1:" + s.getAddress().getPort();
    var perp = TradingParams.DEFAULTS.with(Map.of("market", "perp", "wsTrade", "false"));
    var cr = new Credentials("KEY", "SECRET");
    java.util.function.Function<String, ExchangeConfig> cfg = id -> new ExchangeConfig(id, false, url, "", 5000, List.of("BTCUSDT"), 20, 100, perp);

    routes.put("GET /fapi/v1/time", "{\"serverTime\":" + System.currentTimeMillis() + "}");
    routes.put("POST /fapi/v1/algoOrder", "{\"algoId\":777}");
    var bin = new BinanceFuturesClient(cfg.apply("binance"), cr, new SymbolFilters());
    String id = bin.placeStopLoss("BTCUSDT", Side.SELL, 0.01, 49000.5);
    Seen q = last("POST", "/fapi/v1/algoOrder");
    ck("binance stop via Algo Order API", "a:777".equals(id) && q.uri().contains("algoType=CONDITIONAL") && q.uri().contains("type=STOP_MARKET")
        && q.uri().contains("triggerPrice=49000.5") && q.uri().contains("closePosition=true") && q.uri().contains("workingType=MARK_PRICE") && q.uri().contains("side=SELL"));
    bin.cancelStopLoss("BTCUSDT", id);
    ck("binance stop cancel", last("DELETE", "/fapi/v1/algoOrder").uri().contains("algoId=777"));

    routes.put("POST /v5/position/trading-stop", "{\"retCode\":0,\"retMsg\":\"OK\",\"result\":{}}");
    var by = new BybitRestClient(cfg.apply("bybit"), cr, new SymbolFilters());
    id = by.placeStopLoss("BTCUSDT", Side.BUY, 1, 51000);
    q = last("POST", "/v5/position/trading-stop");
    ck("bybit trading-stop", "position".equals(id) && q.body().contains("\"category\":\"linear\"") && q.body().contains("\"stopLoss\":\"51000\"")
        && q.body().contains("\"tpslMode\":\"Full\"") && q.body().contains("\"slTriggerBy\":\"MarkPrice\""));
    by.cancelStopLoss("BTCUSDT", id);
    ck("bybit cancel = stopLoss 0", last("POST", "/v5/position/trading-stop").body().contains("\"stopLoss\":\"0\""));

    routes.put("POST /api/v5/trade/order-algo", "{\"code\":\"0\",\"data\":[{\"algoId\":\"A1\",\"sCode\":\"0\"}]}");
    routes.put("POST /api/v5/trade/cancel-algos", "{\"code\":\"0\",\"data\":[{\"algoId\":\"A1\",\"sCode\":\"0\"}]}");
    var okx = new OkxRestClient(cfg.apply("okx"), cr, new SymbolFilters());
    id = okx.placeStopLoss("BTCUSDT", Side.SELL, 0.01, 49000);
    q = last("POST", "/api/v5/trade/order-algo");
    ck("okx conditional algo", "A1".equals(id) && q.body().contains("\"ordType\":\"conditional\"") && q.body().contains("\"instId\":\"BTC-USDT-SWAP\"")
        && q.body().contains("\"slTriggerPx\":\"49000\"") && q.body().contains("\"slOrdPx\":\"-1\"") && q.body().contains("\"closeFraction\":\"1\""));
    okx.cancelStopLoss("BTCUSDT", id);
    ck("okx cancel-algos", last("POST", "/api/v5/trade/cancel-algos").body().contains("\"algoId\":\"A1\""));

    routes.put("POST /api/v4/futures/usdt/price_orders", "{\"id\":555}");
    var gate = new GateFuturesClient(cfg.apply("gate"), cr, new SymbolFilters());
    id = gate.placeStopLoss("BTCUSDT", Side.SELL, 0.01, 49000);
    q = last("POST", "/futures/usdt/price_orders");
    ck("gate price trigger", "555".equals(id) && q.body().contains("\"contract\":\"BTC_USDT\"") && q.body().contains("\"close\":true")
        && q.body().contains("\"rule\":2") && q.body().contains("\"price_type\":1") && q.body().contains("\"price\":\"49000\""));
    gate.cancelStopLoss("BTCUSDT", id);
    ck("gate cancel", last("DELETE", "/futures/usdt/price_orders/555") != null);

    routes.put("POST /api/v1/orders", "{\"code\":\"200000\",\"data\":{\"orderId\":\"K9\"}}");
    var ku = new KucoinFuturesClient(cfg.apply("kucoin"), cr, new SymbolFilters());
    id = ku.placeStopLoss("BTCUSDT", Side.BUY, 0.01, 51000);
    q = last("POST", "/api/v1/orders");
    ck("kucoin stop order", "K9".equals(id) && q.body().contains("\"stop\":\"up\"") && q.body().contains("\"stopPriceType\":\"MP\"")
        && q.body().contains("\"closeOrder\":true") && q.body().contains("\"symbol\":\"XBTUSDTM\"") && q.body().contains("\"side\":\"buy\""));
    ku.cancelStopLoss("BTCUSDT", id);
    ck("kucoin cancel", last("DELETE", "/api/v1/orders/K9") != null);
    s.stop(0);

    System.out.println("pass=" + pass + " fail=" + fail);
    System.exit(fail == 0 ? 0 : 1);
  }
}
