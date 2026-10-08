import com.hft.strategy.*;
import com.hft.config.*;
import com.hft.engine.OrderService;
import com.hft.strategy.StrategySet;
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
      boolean isPerp = ts.get().isPerp();
      var paper = new PaperOrderApi(market, bal, 0, 0);
      if (isPerp) paper.perp(pos);
      SymbolFilters f = new SymbolFilters(); ExchangeSupport.putDefaultFilter(f, symbol);
      risk = new RiskManager(ts, market, id);
      orders = new OrderService(paper, market, bal, f, risk, ts, pos);
      perp = isPerp ? new PerpAccount(id, new ExchangeConfig(id, false, "http://x", "", 5000, List.of(symbol), 20, 100, ts.get()), market, bal, pos, paper, true) : null;
    }
    public String id(){ return id; } public void start(){} public void stop(){} public boolean isConnected(){ return true; }
    public long messageCount(){ return 0; } public List<String> symbols(){ return List.copyOf(market.symbols()); }
    public MarketDataStore marketData(){ return market; } public BalanceStore balances(){ return bal; }
    public OrderService orders(){ return orders; } public RiskManager risk(){ return risk; }
    void book(String s, double bid, double ask) { market.book(s).applySnapshot(new double[]{bid}, new double[]{100}, 1, new double[]{ask}, new double[]{100}, 1, 2, System.currentTimeMillis()); }
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
    ck("paper: fees already in price", g.orders.feesInPrice());
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
    ck("no source for uniswap", FundingSource.forExchange("uniswapv2", base) == null);
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

    // --- фьючерсы Gate, KuCoin, MEXC: стакан в контрактах -> монеты
    com.hft.exchange.generic.ContractSizes.put(com.hft.exchange.Exchange.GATE, "BTC_USDT", 0.0001);
    com.hft.exchange.generic.ContractSizes.put(com.hft.exchange.Exchange.KUCOIN, "XBTUSDTM", 0.001);
    com.hft.exchange.generic.ContractSizes.put(com.hft.exchange.Exchange.MEXC, "BTC_USDT", 0.0001);
    var gws = WsDialects.forExchange("gate", new ExchangeConfig("gate", false, "", "", 5000, List.of("BTCUSDT"), 20, 100, PERP)).orElseThrow();
    ck("gate futures venue", gws.venueSymbol("BTCUSDT").equals("BTC_USDT"));
    ck("kucoin futures venue XBT", WsDialects.forExchange("kucoin", new ExchangeConfig("kucoin", false, "", "", 5000, List.of("BTCUSDT"), 20, 100, PERP)).orElseThrow().venueSymbol("BTCUSDT").equals("XBTUSDTM"));
    var mws = WsDialects.forExchange("mexc", new ExchangeConfig("mexc", false, "", "", 5000, List.of("BTCUSDT"), 20, 100, PERP)).orElseThrow();
    ck("mexc futures ping json", mws.pingMessage().contains("ping") && !mws.parsesBinary());
    var grest = Dialects.forExchange("gate", new ExchangeConfig("gate", false, "", "", 5000, List.of("BTCUSDT"), 20, 100, PERP))
        .parse("{\"current\":1700000000.5,\"asks\":[{\"p\":\"60010\",\"s\":200}],\"bids\":[{\"p\":\"60000\",\"s\":100}]}", "BTCUSDT");
    ck("gate futures rest contracts -> coins", near(grest.aq()[0], 0.02) && near(grest.bq()[0], 0.01));
    var krest = Dialects.forExchange("kucoin", new ExchangeConfig("kucoin", false, "", "", 5000, List.of("BTCUSDT"), 20, 100, PERP))
        .parse("{\"code\":\"200000\",\"data\":{\"bids\":[[\"60000\",10]],\"asks\":[[\"60010\",30]],\"ts\":1700000000000000000}}", "BTCUSDT");
    ck("kucoin futures rest lots -> coins", near(krest.bq()[0], 0.01) && near(krest.aq()[0], 0.03));
    var mrest = Dialects.forExchange("mexc", new ExchangeConfig("mexc", false, "", "", 5000, List.of("BTCUSDT"), 20, 100, PERP))
        .parse("{\"success\":true,\"data\":{\"bids\":[[60000,100,1]],\"asks\":[[60010,300,2]],\"timestamp\":1}}", "BTCUSDT");
    ck("mexc futures rest contracts -> coins", near(mrest.bq()[0], 0.01) && near(mrest.aq()[0], 0.03));
    HttpServer fsrv = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    long nx = System.currentTimeMillis() + 3_600_000;
    Map<String, String> fb = Map.of(
        "/api/v4/futures/usdt/contracts", "[{\"name\":\"BTC_USDT\",\"funding_rate\":\"0.0002\",\"funding_interval\":28800,\"funding_next_apply\":" + nx / 1000 + ",\"mark_price\":\"60000\"}]",
        "/api/v1/contracts/active", "{\"code\":\"200000\",\"data\":[{\"symbol\":\"XBTUSDTM\",\"fundingFeeRate\":0.0001,\"fundingRateGranularity\":28800000,\"nextFundingRateDateTime\":" + nx + ",\"markPrice\":60000}]}",
        "/api/v1/contract/funding_rate/BTC_USDT", "{\"success\":true,\"code\":0,\"data\":{\"fundingRate\":0.0003,\"nextSettleTime\":" + nx + ",\"collectCycle\":8}}",
        "/fapi/v1/premiumIndex", "[{\"symbol\":\"BTCUSDT\",\"markPrice\":\"60000\",\"lastFundingRate\":\"0.0004\",\"nextFundingTime\":" + nx + "}]");
    fsrv.createContext("/", ex -> { byte[] b = fb.getOrDefault(ex.getRequestURI().getPath(), "[]").getBytes(); ex.sendResponseHeaders(200, b.length); ex.getResponseBody().write(b); ex.close(); });
    fsrv.start();
    String fbase = "http://127.0.0.1:" + fsrv.getAddress().getPort();
    ck("gate funding", near(FundingSource.forExchange("gate", fbase).fetch(List.of("BTCUSDT")).get("BTCUSDT").rate(), 0.0002));
    var kfund = FundingSource.forExchange("kucoin", fbase).fetch(List.of("BTCUSDT")).get("BTCUSDT");
    ck("kucoin funding", kfund != null && near(kfund.rate(), 0.0001) && kfund.nextFundingMs() == nx);
    ck("mexc funding", near(FundingSource.forExchange("mexc", fbase).fetch(List.of("BTCUSDT")).get("BTCUSDT").rate(), 0.0003));
    ck("aster funding (binance format)", near(FundingSource.forExchange("aster", fbase).fetch(List.of("BTCUSDT")).get("BTCUSDT").rate(), 0.0004));
    fsrv.stop(0);

    // --- фабрика: шлюз фьючерсов для каждой биржи с перпами (бумажный режим, без старта)
    for (var e : com.hft.exchange.Exchange.values()) {
      if (!e.hasPerp()) continue;
      var tp = com.hft.control.BotController.defaultsFor(e.id());
      var gw = com.hft.exchange.ExchangeFactory.create(e.id(), new ExchangeConfig(e.id(), false, "http://127.0.0.1:1", "", 5000,
          List.of(e == com.hft.exchange.Exchange.HYPERLIQUID ? "BTCUSDC" : "BTCUSDT"), 20, 100, tp), new TradingSettings(tp));
      ck("factory perp " + e, gw.perp() != null && gw.orders().isPerp());
    }

    // --- параметр market: значения по умолчанию и проверки
    ck("binance default perp", com.hft.control.BotController.defaultsFor("binance").isPerp());
    ck("gate default perp", com.hft.control.BotController.defaultsFor("gate").isPerp());
    ck("uniswap default spot", !com.hft.control.BotController.defaultsFor("uniswapv2").isPerp());
    ck("kucoin perp: no testnet by default", !com.hft.control.BotController.defaultsFor("kucoin").testnet());
    ck("hyperliquid default perp", com.hft.control.BotController.defaultsFor("hyperliquid").isPerp());
    ck("perp taker fee from catalog", near(com.hft.control.BotController.defaultsFor("bybit").takerFeePercent(), 0.055));
    var validate = com.hft.control.BotController.class.getDeclaredMethod("validate", String.class, TradingParams.class);
    validate.setAccessible(true);
    ck("uniswap perp rejected", throwsIae(() -> validate.invoke(null, "uniswapv2", TradingParams.DEFAULTS.with(Map.of("market", "perp", "testnet", "false")))));
    ck("mexc perp testnet rejected", throwsIae(() -> validate.invoke(null, "mexc", TradingParams.DEFAULTS.with(Map.of("market", "perp", "testnet", "true")))));
    ck("gate perp ok", !throwsIae(() -> validate.invoke(null, "gate", TradingParams.DEFAULTS.with(Map.of("market", "perp", "testnet", "false")))));
    for (var e : com.hft.exchange.Exchange.values())
      ck("catalog perp " + e, com.hft.exchange.catalog.ExchangeCatalog.supportsPerp(e.id()) == e.hasPerp());
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

    // --- ценовой арбитраж перпов: продать там, где дороже, купить где дешевле, закрыть при схождении
    Map<String,String> big = Map.of("tradingEnabled", "true", "maxPositionQuote", "1000");
    Gw pa = new Gw("binance", "ETHUSDT", 101, 101.1, big);
    Gw pb = new Gw("bybit", "ETHUSDT", 99.9, 100, big);
    var parb = new PerpPriceArbitrage(() -> GlobalParams.DEFAULTS.with(Map.of("perpArbEnabled", "true", "perpArbOrderQuote", "100")),
        () -> List.of(pa, pb), id -> true, id -> 0.05);
    parb.tick(System.currentTimeMillis());
    ck("perp arb opened", parb.openPairs() == 1 && pa.pos.qty("ETHUSDT") < 0 && pb.pos.qty("ETHUSDT") > 0
        && near(-pa.pos.qty("ETHUSDT"), pb.pos.qty("ETHUSDT")));
    parb.tick(System.currentTimeMillis());
    ck("perp arb holds while spread open", parb.openPairs() == 1);
    pa.book("ETHUSDT", 100.4, 100.5); pb.book("ETHUSDT", 100.5, 100.6);       // сошлись: аск A ≤ бид B
    parb.tick(System.currentTimeMillis());
    ck("perp arb closed on convergence", parb.openPairs() == 0 && pa.pos.get("ETHUSDT").isFlat() && pb.pos.get("ETHUSDT").isFlat());
    ck("perp arb profit", (double) parb.stats().get("pnl") > 0);
    // расхождение меньше комиссий — входа нет
    pa.book("ETHUSDT", 100.1, 100.2); pb.book("ETHUSDT", 99.95, 100.0);
    parb.tick(System.currentTimeMillis());
    ck("perp arb: spread below fees - no entry", parb.openPairs() == 0);
    parb.stop();

    // --- cash-and-carry: спот-лонг на бирже со спотом + шорт перпа при положительной ставке
    Gw sp = new Gw("gate", "ADAUSDT", 0.999, 1.0, Map.of("tradingEnabled", "true", "market", "spot", "maxPositionQuote", "1000"));
    sp.bal.set("USDT", 1000, 0);
    Gw pp = new Gw("bybit", "ADAUSDT", 1.0, 1.001, big);
    long tt = System.currentTimeMillis();
    pp.perp.onFunding("ADAUSDT", new Funding(0.0005, 8, tt + 3_600_000, 1.0, tt));   // 0.05% за 8 ч
    GlobalParams[] cg = { GlobalParams.DEFAULTS.with(Map.of("carryEnabled", "true", "carryOrderQuote", "50")) };
    var carry = new FundingCarry(() -> cg[0], () -> List.of(sp, pp), id -> true, id -> 0.05);
    carry.tick(tt);
    ck("carry opened", carry.openPositions() == 1 && sp.bal.free("ADA") > 0 && pp.pos.qty("ADAUSDT") < 0
        && near(sp.bal.free("ADA"), -pp.pos.qty("ADAUSDT")));
    pp.perp.onFunding("ADAUSDT", new Funding(-0.0001, 8, tt + 3_600_000, 1.0, tt));  // ставка ушла в минус
    carry.tick(tt);
    ck("carry closed on rate drop", carry.openPositions() == 0 && pp.pos.get("ADAUSDT").isFlat() && sp.bal.free("ADA") < 1e-6);
    pp.perp.onFunding("ADAUSDT", new Funding(0.00002, 8, tt + 3_600_000, 1.0, tt));  // 0.002%: комиссии 0.2% не окупятся
    var c2 = new FundingCarry(() -> GlobalParams.DEFAULTS.with(Map.of("carryEnabled", "true", "carryMinRatePercent", "0.001")), () -> List.of(sp, pp), id -> true, id -> 0.05);
    c2.tick(tt);
    ck("carry: fees not covered - no entry", c2.openPositions() == 0);
    ck("carry exit < min enforced", throwsIae(() -> GlobalParams.DEFAULTS.with(Map.of("carryExitRatePercent", "0.05"))));

 // --- комиссии в боевом режиме: клиент отдаёт цену без комиссии, OrderService списывает её сразу
    var lm = new MarketDataStore(20, 100); lm.register("BTCUSDT");
    lm.book("BTCUSDT").applySnapshot(new double[]{99}, new double[]{100}, 1, new double[]{100}, new double[]{100}, 1, 1, System.currentTimeMillis());
    com.hft.rest.ExchangeOrderApi fake = new com.hft.rest.ExchangeOrderApi() {
      OrderResult fill(String s, Side side, double q, double px){ return new OrderResult(1, "c", s, side, "FILLED", q, q, px, 0); }
      public OrderResult buyLimit(String s,double q,double p,com.hft.model.OrderEnums.TimeInForce t){ return fill(s,Side.BUY,q,p); }
      public OrderResult sellLimit(String s,double q,double p,com.hft.model.OrderEnums.TimeInForce t){ return fill(s,Side.SELL,q,p); }
      public OrderResult buyMarket(String s,double q){ return fill(s,Side.BUY,q,100); }
      public OrderResult sellMarket(String s,double q){ return fill(s,Side.SELL,q,100); }
      public OrderResult buyMarketForQuote(String s,double q){ return fill(s,Side.BUY,q/100,100); }
      public void cancelOrder(String s,long id){} public int cancelAll(String s){ return 0; }
    };
    var lts = new TradingSettings(TradingParams.DEFAULTS.with(Map.of("tradingEnabled","true","takerFeePercent","0.1","tradeCostQuote","0.5","maxSlippagePercent","5","market","spot")));
    var lb = new BalanceStore(); lb.set("USDT", 1000, 0);
    var lf = new SymbolFilters(); ExchangeSupport.putDefaultFilter(lf, "BTCUSDT");
    var los = new OrderService(fake, lm, lb, lf, new RiskManager(lts, lm, "x"), lts);
    ck("live: fees not in price", !los.feesInPrice());
    los.buyMarket("BTCUSDT", 0.5);
    ck("live spot buy: fee from base, fixed cost from quote", near(lb.free("BTC"), 0.5 * 0.999) && near(lb.free("USDT"), 1000 - 50 - 0.5));
    los.sellMarket("BTCUSDT", 0.4);
    ck("live spot sell: fee from quote", near(lb.free("USDT"), 1000 - 50 - 0.5 + 40 * 0.999 - 0.5));
    ck("costOf live", near(los.costOf(100), 0.1 + 0.5));
    ck("round trip cost %", near(los.roundTripCostPercent(100), 2 * 0.1 + 2 * 0.5 / 100 * 100));
    ck("costOf paper = fixed only", near(new OrderService(new PaperOrderApi(lm, lb, 0, 0.1), lm, lb, lf, new RiskManager(lts, lm, "x"), lts).costOf(100), 0.5));

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
