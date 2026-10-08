package com.hft.strategy;

import com.hft.engine.*;
import com.hft.config.TradingParams;
import com.hft.config.TradingSettings;
import com.hft.model.Tick;
import com.hft.paper.PaperOrderApi;
import com.hft.risk.RiskManager;
import com.hft.store.BalanceStore;
import com.hft.store.MarketDataStore;
import com.hft.store.SymbolFilters;
import java.util.*;
import java.util.function.BooleanSupplier;

/** Треугольный и статистический арбитраж на бумажном исполнении; порядок обработчиков в конвейере. */
public class StrategiesCheck {
  static int pass, fail;
  static void ck(String n, boolean ok){ if(ok) pass++; else {fail++; System.out.println("FAIL "+n);} }
  static boolean await(BooleanSupplier c, long ms) throws Exception { long t=System.currentTimeMillis()+ms; while(System.currentTimeMillis()<t){ if(c.getAsBoolean()) return true; Thread.sleep(20);} return c.getAsBoolean(); }

  static void book(MarketDataStore m, String s, double bid, double ask, double qty) {
    long now = System.currentTimeMillis();
    m.book(s).applySnapshot(new double[]{bid, bid * 0.999}, new double[]{qty, qty}, 2, new double[]{ask, ask * 1.001}, new double[]{qty, qty}, 2, now, now);
  }
  static SymbolFilters filters(String... syms) {
    var f = new SymbolFilters();
    for (String s : syms) f.put(s, new SymbolFilters.Filter(0, 1e12, 1e-8, 0, 1e12, 1e-8, 0.0001));
    return f;
  }
  static Map<String, String> kv(String... a) { Map<String, String> m = new HashMap<>(); for (int i = 0; i < a.length; i += 2) m.put(a[i], a[i + 1]); return m; }
  static Tick tick(String s, double px) { Tick t = new Tick(); t.set(s, px, 1, false, System.currentTimeMillis(), System.nanoTime()); return t; }

  public static void main(String[] a) throws Exception {
    triangular();
    statArb();
    pipelineOrder();
    System.out.println("pass="+pass+" fail="+fail); System.exit(fail==0?0:1);
  }

  static void triangular() throws Exception {
    var cycles = TriangularArbStrategy.buildCycles(Set.of("BTCUSDT", "ETHBTC", "ETHUSDT", "SOLUSDT"), "USDT");
    ck("two directions of one triangle (SOL has no third leg)", cycles.size() == 2
        && cycles.stream().anyMatch(c -> c.name().equals("USDT→BTC→ETH→USDT")) && cycles.stream().anyMatch(c -> c.name().equals("USDT→ETH→BTC→USDT")));
    var c0 = cycles.stream().filter(c -> c.name().equals("USDT→BTC→ETH→USDT")).findFirst().get();
    ck("legs: buy BTC, buy ETH with BTC, sell ETH", c0.legs()[0].buy() && c0.legs()[1].buy() && !c0.legs()[2].buy()
        && c0.legs()[1].symbol().equals("ETHBTC") && c0.legs()[2].to().equals("USDT"));

    var settings = new TradingSettings(TradingParams.DEFAULTS.with(kv("tradingEnabled", "true", "meanReversionEnabled", "false",
        "triangularEnabled", "true", "triMinProfitPercent", "0.2", "triOrderQuote", "50", "takerFeePercent", "0.1", "maxSlippagePercent", "5")));
    var m = new MarketDataStore(5, 50);
    for (String s : List.of("BTCUSDT", "ETHBTC", "ETHUSDT")) m.register(s);
    var bal = new BalanceStore(); bal.set("USDT", 1000, 0);
    var paper = new PaperOrderApi(m, bal, 0.1, 0.1);
    var risk = new RiskManager(settings, m, "t");
    var os = new OrderService(paper, m, bal, filters("BTCUSDT", "ETHBTC", "ETHUSDT"), risk, settings);
    var tri = new TriangularArbStrategy(m, os, "t", settings);
    tri.enable();

    // без расхождения: 100 * 0.05 = 5 = ETHUSDT — круг убыточен из-за комиссий
    book(m, "BTCUSDT", 99.99, 100, 10); book(m, "ETHBTC", 0.04999, 0.05, 100); book(m, "ETHUSDT", 4.999, 5.0, 100);
    var q = tri.evaluate(c0, 0.1, 1000);
    ck("fair prices: cycle loses about 3 fees", q != null && q.profitPct() < -0.25 && q.profitPct() > -0.4);
    tri.onEvent(tick("ETHUSDT", 5), 0, true);
    Thread.sleep(200);
    ck("no trade when unprofitable", tri.executedCount() == 0 && tri.opportunities() == 0);

    // ETH в долларах дороже, чем через BTC: 0.2 ETH * 5.2 = 1.04 на каждый доллар
    book(m, "ETHUSDT", 5.2, 5.201, 100);
    q = tri.evaluate(c0, 0.1, 1000);
    ck("mispricing gives ~3.7% net", q != null && q.profitPct() > 3.5 && q.profitPct() < 3.9);
    tri.onEvent(tick("ETHUSDT", 5.2), 1, true);
    ck("cycle executed on paper", await(() -> tri.executedCount() == 1, 3000));
    ck("profit booked to daily PnL", risk.dailyPnl() > 1 && tri.totalPnl() > 1);
    ck("USDT grew, no BTC/ETH left over", bal.total("USDT") > 1001 && bal.total("BTC") < 1e-6 && bal.total("ETH") < 1e-6);
    tri.onEvent(tick("ETHUSDT", 5.2), 2, true);
    Thread.sleep(200);
    ck("cooldown prevents immediate repeat", tri.executedCount() == 1);
    // стакан устарел — не торгуем
    var stale = new TradingSettings(settings.get().with(Map.of("triCooldownMs", "0", "triMaxBookAgeMs", "10")));
    var tri2 = new TriangularArbStrategy(m, os, "t2", stale); tri2.enable();
    Thread.sleep(50);
    tri2.onEvent(tick("ETHUSDT", 5.2), 0, true);
    Thread.sleep(200);
    ck("stale books block the cycle", tri2.executedCount() == 0);
    // выключена параметром
    var off = new TriangularArbStrategy(m, os, "t3", new TradingSettings(settings.get().with(Map.of("triangularEnabled", "false")))); off.enable();
    book(m, "BTCUSDT", 99.99, 100, 10); book(m, "ETHBTC", 0.04999, 0.05, 100); book(m, "ETHUSDT", 5.2, 5.201, 100);
    off.onEvent(tick("ETHUSDT", 5.2), 0, true);
    Thread.sleep(200);
    ck("disabled by parameter", off.executedCount() == 0);
  }

  static void statArb() throws Exception {
    // математика окна: ln A = 2 ln B + шум -> β ≈ 2, высокая корреляция доходностей
    var pr = new StatArbStrategy.Pair("A", "B", 200);
    Random r = new Random(1);
    double lb = Math.log(100);
    for (int i = 0; i < 200; i++) { lb += r.nextGaussian() * 0.01; pr.add(Math.exp(2 * lb + r.nextGaussian() * 0.002), Math.exp(lb)); }
    ck("beta and correlation estimated", pr.compute() && Math.abs(pr.beta - 2) < 0.1 && pr.corr > 0.9 && Math.abs(pr.z) < 4);
    ck("auto pairs share the quote asset", StatArbStrategy.pairList("", List.of("BTCUSDT", "ETHUSDT", "ETHBTC"), 15).size() == 1
        && StatArbStrategy.pairList("BTCUSDT/ETHUSDT,XXX/YYY", List.of("BTCUSDT", "ETHUSDT"), 15).size() == 1);

    // живой прогон: окно 30 отсчётов по 100 мс; A идёт за B, потом A проваливается и возвращается
    var settings = new TradingSettings(TradingParams.DEFAULTS.with(kv("tradingEnabled", "true", "meanReversionEnabled", "false",
        "statArbEnabled", "true", "statArbWindow", "30", "statArbSampleMs", "100", "statArbEntryZ", "2", "statArbExitZ", "0.5",
        "statArbStopZ", "8", "statArbMinCorrelation", "0.5", "statArbOrderQuote", "50", "maxSlippagePercent", "5")));
    var m = new MarketDataStore(5, 50);
    m.register("SOLUSDT"); m.register("AVAXUSDT");
    var bal = new BalanceStore(); bal.set("USDT", 1000, 0);
    var os = new OrderService(new PaperOrderApi(m, bal, 0.1, 0.1), m, bal, filters("SOLUSDT", "AVAXUSDT"), new RiskManager(settings, m, "s"), settings);
    var sa = new StatArbStrategy(m, os, "s", settings);
    sa.enable();
    double b = 30;
    Random rr = new Random(7);
    for (int i = 0; i < 34; i++) {
      b *= Math.exp(rr.nextGaussian() * 0.004);
      double av = b * 4 * Math.exp(rr.nextGaussian() * 0.0005);
      book(m, "AVAXUSDT", b * 0.9999, b, 1e6); book(m, "SOLUSDT", av * 0.9999, av, 1e6);
      sa.onEvent(tick("SOLUSDT", av), i, true);
      Thread.sleep(105);
    }
    ck("no position while spread is normal", sa.openPositions() == 0);
    double crash = b * 4 * 0.985;                                         // SOL на 1.5% дешевле обычного относительно AVAX
    book(m, "AVAXUSDT", b * 0.9999, b, 1e6); book(m, "SOLUSDT", crash * 0.9999, crash, 1e6);
    sa.onEvent(tick("SOLUSDT", crash), 100, true);
    ck("buys the cheap leg when z <= -entry", await(() -> sa.openPositions() == 1 && bal.total("SOL") > 0, 3000));
    Thread.sleep(105);
    double back = b * 4 * 1.01;                                           // спред вернулся (и с запасом)
    book(m, "AVAXUSDT", b * 0.9999, b, 1e6); book(m, "SOLUSDT", back * 0.9999, back, 1e6);
    sa.onEvent(tick("SOLUSDT", back), 101, true);
    ck("exits when spread reverts, with profit", await(() -> sa.openPositions() == 0 && sa.tradesCount() == 1, 3000) && sa.totalPnl() > 0);
  }

  static void pipelineOrder() throws Exception {
    // стратегия должна видеть тик уже записанным в окно цен
    var m = new MarketDataStore(5, 10); m.register("XUSDT");
    List<Boolean> seen = Collections.synchronizedList(new ArrayList<>());
    TickPipeline p = new TickPipeline(new MarketDataHandler(m), (t, s, e) -> seen.add(m.stats("XUSDT").tickCount() >= s + 1));
    p.start();
    for (int i = 0; i < 2000; i++) p.publish("XUSDT", 100 + i, 1, false, 0, System.nanoTime());
    await(() -> seen.size() == 2000, 3000);
    p.shutdown();
    ck("strategies run after the data handler (2000 ticks)", seen.size() == 2000 && !seen.contains(false));
  }
}
