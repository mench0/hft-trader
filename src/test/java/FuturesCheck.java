import com.hft.config.*;
import com.hft.engine.OrderService;
import com.hft.engine.StrategySet;
import com.hft.exchange.ExchangeGateway;
import com.hft.exchange.generic.*;
import com.hft.model.OrderEnums.Side;
import com.hft.model.OrderResult;
import com.hft.paper.PaperOrderApi;
import com.hft.perp.*;
import com.hft.risk.RiskManager;
import com.hft.store.*;
import com.hft.store.FundingStore.Funding;
import com.sun.net.httpserver.HttpServer;

import java.net.InetSocketAddress;
import java.util.*;

/** Фьючерсы: позиции, бумажный движок, риск, ставки funding, диалекты, funding-арбитраж, параметр market. */
public class FuturesCheck {
  static int pass, fail;
  static void ck(String n, boolean ok){ if(ok) pass++; else {fail++; System.out.println("FAIL "+n);} }
  static boolean near(double a,double b){ return Math.abs(a-b)<1e-6*Math.max(1,Math.abs(b)); }

  /** Биржа для теста: бумажный перп-движок на заданном стакане. */
  static final class Gw implements ExchangeGateway {
    final String id; final MarketDataStore market = new MarketDataStore(20, 100); final BalanceStore bal = new BalanceStore();
    final PositionStore pos = new PositionStore(); final TradingSettings ts; final RiskManager risk; final OrderService orders; final PerpAccount perp;
    Gw(String id, String symbol, double bid, double ask, Map<String,String> params) {
      this.id = id;
      Map<String,String> all = new HashMap<>(Map.of("maxSlippagePercent", "5", "market", "perp"));   // стакан теста широкий
      all.putAll(params);
      ts = new TradingSettings(TradingParams.DEFAULTS.with(all));
      market.register(symbol);
      market.book(symbol).applySnapshot(new double[]{bid}, new double[]{100}, 1, new double[]{ask}, new double[]{100}, 1, 1, System.currentTimeMillis());
      bal.set(BalanceStore.quoteAsset(symbol), 1000, 0);
      var paper = new PaperOrderApi(market, bal, 0, 0).perp(pos);
      SymbolFilters f = new SymbolFilters(); ExchangeSupport.putDefaultFilter(f, symbol);
      risk = new RiskManager(ts, market, id);
      orders = new OrderService(paper, market, bal, f, risk, ts, pos);
      perp = new PerpAccount(id, new ExchangeConfig(id, false, "http://x", "", 5000, List.of(symbol), 20, 100, ts.get()), market, bal, pos, paper, true);
    }
    public String id(){ return id; } public void start(){} public void stop(){} public boolean isConnected(){ return true; }
    public long messageCount(){ return 0; } public List<String> symbols(){ return List.copyOf(market.symbols()); }
    public MarketDataStore marketData(){ return market; } public BalanceStore balances(){ return bal; }
    public OrderService orders(){ return orders; } public RiskManager risk(){ return risk; }
    public StrategySet strategy(){ return null; } public void syncBalances(){} public PerpAccount perp(){ return perp; }
  }

  public static void main(String[] a) throws Exception {
    // --- PositionStore: усреднение, частичное закрытие, переворот
    PositionStore ps = new PositionStore();
    ck("open long pnl 0", ps.apply("BTCUSDT", true, 1, 100) == 0);
    ps.apply("BTCUSDT", true, 1, 110);
    ck("avg entry", near(ps.get("BTCUSDT").entryPrice(), 105) && near(ps.qty("BTCUSDT"), 2));
    ck("partial close pnl", near(ps.apply("BTCUSDT", false, 1, 120), 15));
    ck("flip to short pnl", near(ps.apply("BTCUSDT", false, 3, 100), -5) && near(ps.qty("BTCUSDT"), -2) && near(ps.get("BTCUSDT").entryPrice(), 100));
    ck("short closes in profit", near(ps.apply("BTCUSDT", true, 2, 90), 20) && ps.get("BTCUSDT").isFlat());
    ps.set("ETHUSDT", -2, 50);
    ck("funding short receives", near(ps.accrueFunding("ETHUSDT", 0.001, 50), 0.1));

    // --- бумажный перп + OrderService: шорт, закрытие, маржа, reduceOnly при kill switch
    Gw g = new Gw("bybit", "BTCUSDT", 99, 100, Map.of("tradingEnabled", "true", "leverage", "2", "maxPositionQuote", "1000"));
    ck("perp mode", g.orders.isPerp());
    OrderResult sh = g.orders.sellMarket("BTCUSDT", 2);
    ck("short filled", sh.isFilled() && near(g.pos.qty("BTCUSDT"), -2));
    ck("balance only realized", near(g.bal.free("USDT"), 1000));
    g.market.book("BTCUSDT").applySnapshot(new double[]{89}, new double[]{100}, 1, new double[]{90}, new double[]{100}, 1, 2, System.currentTimeMillis());
    g.risk.stopTrading("тест");
    OrderResult cl = g.orders.closePosition("BTCUSDT");
    ck("close passes kill switch", cl != null && cl.isFilled() && g.pos.get("BTCUSDT").isFlat());
    ck("short profit to balance", near(g.bal.free("USDT"), 1000 + (99 - 90) * 2));
    ck("new entry blocked by kill switch", g.orders.buyMarket("BTCUSDT", 1).executedQty() == 0);
    g.risk.resumeTrading();
    g.bal.set("USDT", 10, 0);
    ck("margin check rejects", g.orders.buyMarket("BTCUSDT", 1).executedQty() == 0);   // 90/2 = 45 > 10
    ck("margin ok", g.orders.buyMarket("BTCUSDT", 0.2).isFilled());                    // 18/2 = 9 ≤ 10
    ck("position limit", g.orders.buyMarket("BTCUSDT", 20).executedQty() == 0);
    g.bal.set("USDT", 100, 0);
    OrderResult all = g.orders.sellMarketAll("BTCUSDT");                               // свободные 100 × плечо 2 / цена
    ck("full balance uses leverage", all.isFilled() && near(all.executedQty(), Math.floor(100 * 0.998 * 2 / 89 * 1e6) / 1e6));
    ck("reduceOnly capped by position", g.orders.reduceMarket("BTCUSDT", Side.SELL, 5).executedQty() == 0);   // позиция — шорт

    // --- funding в бумажном режиме: начисление в момент смены времени списания
    Gw h = new Gw("binance", "ETHUSDT", 99, 100, Map.of("tradingEnabled", "true"));
    h.pos.set("ETHUSDT", -1, 100);
    long now = System.currentTimeMillis();
    h.perp.onFunding("ETHUSDT", new Funding(0.001, 8, now - 1000, 100, now));
    ck("no payment on first rate", near(h.bal.free("USDT"), 1000));
    h.perp.onFunding("ETHUSDT", new Funding(0.0005, 8, now + 8 * 3_600_000L, 100, now));
    ck("short receives funding", near(h.bal.free("USDT"), 1000 + 0.1));
    ck("funding 8h norm", near(new Funding(0.0001, 1, 0, 1, 0).ratePer8h(), 0.0008));

    // --- источники ставок на поддельном сервере
    HttpServer srv = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    Map<String, String> bodies = Map.of(
        "/fapi/v1/premiumIndex", "[{\"symbol\":\"BTCUSDT\",\"markPrice\":\"100\",\"lastFundingRate\":\"0.0003\",\"nextFundingTime\":1000},{\"symbol\":\"XUSDT\",\"lastFundingRate\":\"1\"}]",
        "/fapi/v1/fundingInfo", "[{\"symbol\":\"BTCUSDT\",\"fundingIntervalHours\":4}]",
        "/v5/market/tickers", "{\"retCode\":0,\"result\":{\"list\":[{\"symbol\":\"BTCUSDT\",\"markPrice\":\"101\",\"fundingRate\":\"0.0001\",\"nextFundingTime\":\"2000\"}]}}",
        "/api/v5/public/funding-rate", "{\"code\":\"0\",\"data\":[{\"fundingRate\":\"-0.0002\",\"fundingTime\":\"3600000\",\"nextFundingTime\":\"32400000\"}]}",
        "/info", "[{\"universe\":[{\"name\":\"ETH\"},{\"name\":\"BTC\"}]},[{\"funding\":\"0.00001\",\"markPx\":\"5\"},{\"funding\":\"0.00002\",\"markPx\":\"100\"}]]");
    srv.createContext("/", ex -> { byte[] b = bodies.getOrDefault(ex.getRequestURI().getPath(), "{}").getBytes(); ex.sendResponseHeaders(200, b.length); ex.getResponseBody().write(b); ex.close(); });
    srv.start();
    String base = "http://127.0.0.1:" + srv.getAddress().getPort();
    var bn = FundingSource.forExchange("binance", base).fetch(List.of("BTCUSDT"));
    ck("binance rate", bn.size() == 1 && near(bn.get("BTCUSDT").rate(), 0.0003) && bn.get("BTCUSDT").intervalHours() == 4 && bn.get("BTCUSDT").nextFundingMs() == 1000);
    var by = FundingSource.forExchange("bybit", base).fetch(List.of("BTCUSDT"));
    ck("bybit rate", near(by.get("BTCUSDT").rate(), 0.0001) && by.get("BTCUSDT").intervalHours() == 8 && near(by.get("BTCUSDT").markPrice(), 101));
    var ok = FundingSource.forExchange("okx", base).fetch(List.of("BTCUSDT"));
    ck("okx rate", near(ok.get("BTCUSDT").rate(), -0.0002) && near(ok.get("BTCUSDT").intervalHours(), 8));
    var hl = FundingSource.forExchange("hyperliquid", base).fetch(List.of("BTCUSDC"));
    ck("hyperliquid rate", near(hl.get("BTCUSDC").rate(), 0.00002) && hl.get("BTCUSDC").intervalHours() == 1);
    ck("no source for gate", FundingSource.forExchange("gate", base) == null);
    srv.stop(0);

    // --- диалекты фьючерсов
    TradingParams PERP = TradingParams.DEFAULTS.with(Map.of("market", "perp"));
    ExchangeConfig perpCfg = new ExchangeConfig("okx", false, "http://127.0.0.1:1", "", 5000, List.of("BTCUSDT"), 20, 100, PERP);
    var okxWs = WsDialects.forExchange("okx", perpCfg).orElseThrow();
    ck("okx swap venue", okxWs.venueSymbol("BTCUSDT").equals("BTC-USDT-SWAP"));
    com.hft.exchange.okx.OkxContracts.put("BTC-USDT-SWAP", 0.01);
    var okxRest = Dialects.forExchange("okx", perpCfg).parse("{\"code\":\"0\",\"data\":[{\"asks\":[[\"101\",\"200\"]],\"bids\":[[\"100\",\"300\"]],\"ts\":\"1\"}]}", "BTCUSDT");
    ck("okx swap contracts -> coins", near(okxRest.aq()[0], 2) && near(okxRest.bq()[0], 3));
    var binWs = WsDialects.forExchange("binance", new ExchangeConfig("binance", false, "", "", 5000, List.of("BTCUSDT"), 20, 100, PERP)).orElseThrow();
    ck("binance futures ws url", binWs.defaultUrl(false).contains("fstream.binance.com"));
    var spotCfg = new ExchangeConfig("okx", false, "", "", 5000, List.of("BTCUSDT"), 20, 100, TradingParams.DEFAULTS.with(Map.of("market", "spot")));
    ck("okx spot venue", WsDialects.forExchange("okx", spotCfg).orElseThrow().venueSymbol("BTCUSDT").equals("BTC-USDT"));
    ck("binance futures rest path", Dialects.forExchange("binance", new ExchangeConfig("binance", false, "", "", 5000, List.of("BTCUSDT"), 20, 100, PERP))
        .request("https://fapi.binance.com", "BTCUSDT", 20).uri().getPath().equals("/fapi/v1/depth"));

    // --- параметр market: значения по умолчанию и проверки
    ck("binance default perp", com.hft.control.BotController.defaultsFor("binance").isPerp());
    ck("gate default spot", !com.hft.control.BotController.defaultsFor("gate").isPerp());
    ck("hyperliquid default perp", com.hft.control.BotController.defaultsFor("hyperliquid").isPerp());
    ck("perp taker fee from catalog", near(com.hft.control.BotController.defaultsFor("bybit").takerFeePercent(), 0.055));
    var validate = com.hft.control.BotController.class.getDeclaredMethod("validate", String.class, TradingParams.class);
    validate.setAccessible(true);
    ck("gate perp rejected", throwsIae(() -> validate.invoke(null, "gate", TradingParams.DEFAULTS.with(Map.of("market", "perp", "testnet", "false")))));
    ck("hyperliquid spot rejected", throwsIae(() -> validate.invoke(null, "hyperliquid", TradingParams.DEFAULTS)));
    ck("bybit spot ok", !throwsIae(() -> validate.invoke(null, "bybit", TradingParams.DEFAULTS.with(Map.of("market", "spot")))));
    ck("market lowercased", TradingParams.DEFAULTS.with(Map.of("market", "PERP")).market().equals("perp"));
    ck("schema default spot", !TradingParams.DEFAULTS.isPerp());
    ck("bad market rejected", throwsIae(() -> TradingParams.DEFAULTS.with(Map.of("market", "margin"))));

    // --- funding-арбитраж: шорт там, где ставка выше; выход при схождении ставок
    Gw x = new Gw("binance", "SOLUSDT", 99.9, 100, Map.of("tradingEnabled", "true", "leverage", "2", "maxPositionQuote", "1000"));
    Gw y = new Gw("hyperliquid", "SOLUSDC", 99.95, 100.05, Map.of("tradingEnabled", "true", "leverage", "2", "maxPositionQuote", "1000"));
    long t = System.currentTimeMillis();
    x.perp.onFunding("SOLUSDT", new Funding(0.0010, 8, t + 3_600_000, 100, t));      // 0.10% за 8 ч
    y.perp.onFunding("SOLUSDC", new Funding(0.00001, 1, t + 3_600_000, 100, t));     // 0.008% за 8 ч
    GlobalParams[] gp = { GlobalParams.DEFAULTS.with(Map.of("fundingArbEnabled", "true", "fundingArbOrderQuote", "100")) };
    var arb = new FundingArbitrage(() -> gp[0], () -> List.of(x, y), id -> true, id -> 0.05);
    arb.tick(t);
    ck("arb opened pair", arb.openPairs() == 1);
    ck("short on high rate", x.pos.qty("SOLUSDT") < 0 && y.pos.qty("SOLUSDC") > 0 && near(-x.pos.qty("SOLUSDT"), y.pos.qty("SOLUSDC")));
    ck("leg size", Math.abs(y.pos.qty("SOLUSDC") - 100 / 99.975) < 1e-4);
    arb.tick(t);
    ck("no duplicate pair", arb.openPairs() == 1);
    x.perp.onFunding("SOLUSDT", new Funding(0.00002, 8, t + 3_600_000, 100, t));       // ставки сошлись
    arb.tick(t);
    ck("arb closed pair", arb.openPairs() == 0 && x.pos.get("SOLUSDT").isFlat() && y.pos.get("SOLUSDC").isFlat());
    gp[0] = GlobalParams.DEFAULTS.with(Map.of("fundingArbEnabled", "true", "fundingArbMaxBasisPercent", "0.01"));
    x.perp.onFunding("SOLUSDT", new Funding(0.0010, 8, t + 3_600_000, 100, t));
    arb.tick(t);
    ck("basis too wide - no entry", arb.openPairs() == 0);
    var noTrade = new FundingArbitrage(() -> GlobalParams.DEFAULTS.with(Map.of("fundingArbEnabled", "true")), () -> List.of(x, y), id -> false, id -> 0.05);
    noTrade.tick(t);
    ck("trading disabled - no entry", noTrade.openPairs() == 0);
    // комиссии: разница 0.092%/8ч, круг 4 × 1% = 4% не окупается за 6 периодов — входа нет
    var costly = new FundingArbitrage(() -> GlobalParams.DEFAULTS.with(Map.of("fundingArbEnabled", "true", "fundingArbOrderQuote", "100")),
        () -> List.of(x, y), id -> true, id -> 1.0);
    costly.tick(t);
    ck("fees not covered - no entry", costly.openPairs() == 0);
    @SuppressWarnings("unchecked") var opp = ((List<Map<String,Object>>) costly.stats().get("opportunities")).get(0);
    ck("round trip fee in stats", near((Double) opp.get("roundTripFeePercent"), 4.0) && Boolean.FALSE.equals(opp.get("enough")));
    // та же разница при комиссиях 0.05%: круг 0.2% окупается за ~2.2 периода — вход есть
    var cheap = new FundingArbitrage(() -> GlobalParams.DEFAULTS.with(Map.of("fundingArbEnabled", "true", "fundingArbOrderQuote", "100")),
        () -> List.of(x, y), id -> true, id -> 0.05);
    cheap.tick(t);
    ck("fees covered - entry", cheap.openPairs() == 1);
    cheap.stop();
    ck("exit diff < entry diff enforced", throwsIae(() -> GlobalParams.DEFAULTS.with(Map.of("fundingArbExitDiffPercent", "0.05"))));

    System.out.println("FuturesCheck: pass=" + pass + " fail=" + fail);
    if (fail > 0) System.exit(1);
  }

  interface Body { void run() throws Exception; }
  static boolean throwsIae(Body b) {
    try { b.run(); return false; }
    catch (IllegalArgumentException e) { return true; }
    catch (java.lang.reflect.InvocationTargetException e) { return e.getCause() instanceof IllegalArgumentException; }
    catch (Exception e) { return false; }
  }
}
