import com.hft.config.*;
import com.hft.engine.*;
import com.hft.model.*;
import com.hft.paper.PaperOrderApi;
import com.hft.rest.ExchangeOrderApi;
import com.hft.risk.RiskManager;
import com.hft.store.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.atomic.*;

/** Согласованность стакана между потоками и неблокирующая отправка ордеров стратегией. */
public class PerfFixesCheck {
  static int pass, fail;
  static void ck(String n, boolean ok){ if(ok) pass++; else {fail++; System.out.println("FAIL "+n);} }

  public static void main(String[] a) throws Exception {
    // ---- стакан: писатель чередует два снимка, читатели не должны увидеть смесь
    OrderBook ob = new OrderBook("X", 10);
    double[] b1 = {100,99,98,97,96}, q1 = {1,1,1,1,1}, a1 = {101,102,103,104,105};
    double[] b2 = {200,199,198}, q2 = {2,2,2}, a2 = {201,202,203};
    AtomicBoolean stop = new AtomicBoolean();
    Thread w = new Thread(() -> { long i = 0; while (!stop.get()) { if ((i++ & 1) == 0) ob.applySnapshot(b1,q1,5,a1,q1,5,i,i); else ob.applySnapshot(b2,q2,3,a2,q2,3,i,i); } });
    w.start();
    double[] top = new double[4]; long torn = 0, reads = 0;
    long until = System.currentTimeMillis() + 1500;
    while (System.currentTimeMillis() < until) {
      for (int k = 0; k < 1000; k++) {
        reads++;
        if (ob.readTop(top) && !((top[0]==100 && top[2]==101 && top[1]==1) || (top[0]==200 && top[2]==201 && top[1]==2))) torn++;
        double sp = ob.spreadPercent();
        if (!Double.isNaN(sp) && Math.abs(sp - 1/100.5*100) > 1e-9 && Math.abs(sp - 1/200.5*100) > 1e-9) torn++;
        double imb = ob.imbalance(5);
        if (imb != 0) torn++;                       // обе стороны всегда равны в каждом снимке
        double bp = ob.estimateBuyPrice(3);
        if (!Double.isNaN(bp) && Math.abs(bp - 102) > 1e-9 && Math.abs(bp - (2*201+202)/3.0) > 1e-9) torn++;
      }
    }
    stop.set(true); w.join();
    System.out.println("  чтений: " + reads + ", рваных: " + torn);
    ck("no torn reads across threads", torn == 0 && reads > 100_000);

    // ---- стратегия не ждёт биржу
    var app = new TradingSettings(TradingParams.DEFAULTS.with(Map.of("tradingEnabled", "true")));
    MarketDataStore market = new MarketDataStore(20, 20); market.register("BTCUSDT");
    BalanceStore bal = new BalanceStore(); bal.set("USDT", 1000, 0);
    SymbolFilters f = new SymbolFilters(); f.put("BTCUSDT", new SymbolFilters.Filter(0, 1e12, 1e-6, 0, 1e12, 1e-8, 5.0));
    PaperOrderApi paper = new PaperOrderApi(market, bal, 0.1, 0.1);
    AtomicInteger calls = new AtomicInteger();
    ExchangeOrderApi slow = (ExchangeOrderApi) Proxy.newProxyInstance(ExchangeOrderApi.class.getClassLoader(), new Class[]{ExchangeOrderApi.class},
        (p, m, args) -> { if (m.getName().startsWith("buy") || m.getName().startsWith("sell")) { calls.incrementAndGet(); Thread.sleep(1500); }
                          try { return m.invoke(paper, args); } catch (InvocationTargetException e) { throw e.getCause(); } });
    RiskManager risk = new RiskManager(app, market, "test");
    OrderService os = new OrderService(slow, market, bal, f, risk, app);
    MeanReversionStrategy mr = new MeanReversionStrategy(market, os, "test", app);
    mr.enable();
    Runnable book = () -> { long now = System.currentTimeMillis(); market.book("BTCUSDT").applySnapshot(new double[]{99.97,99.96,99.95}, new double[]{50,50,50}, 3, new double[]{100.0,100.01,100.02}, new double[]{10,10,10}, 3, now, now); };
    book.run();
    Tick t = new Tick();
    for (int i = 0; i < 20; i++) { t.set("BTCUSDT", 100 + (i % 2 == 0 ? 0.01 : -0.01), 0, false, 0, System.nanoTime()); market.onTick(t); }
    t.set("BTCUSDT", 99.0, 0, false, 0, System.nanoTime()); market.onTick(t);
    long s0 = System.nanoTime();
    mr.onEvent(t, 0, true);
    double ms = (System.nanoTime() - s0) / 1e6;
    System.out.printf("  onEvent со входом: %.2f мс (биржа отвечает 1500 мс)%n", ms);
    ck("strategy thread does not wait for the exchange", ms < 100 && calls.get() <= 1);
    ck("order in flight marks symbol busy", mr.executor().isBusy("BTCUSDT"));
    mr.onEvent(t, 1, true); mr.onEvent(t, 2, true);
    Thread.sleep(100);
    ck("no duplicate order while in flight", calls.get() == 1);
    long until2 = System.currentTimeMillis() + 4000;
    while (mr.openPositions() == 0 && System.currentTimeMillis() < until2) Thread.sleep(20);
    ck("position appears after exchange answers", mr.openPositions() == 1 && !mr.executor().isBusy("BTCUSDT"));

    // на REST-запасе (не реальное время) — новых входов нет
    MarketDataStore m2 = new MarketDataStore(20, 20); m2.register("BTCUSDT");
    MeanReversionStrategy mr2 = new MeanReversionStrategy(m2, new OrderService(slow, m2, bal, f, new RiskManager(app, m2, "t2"), app), "t2", app);
    mr2.enable(); mr2.setRealtimeSource(() -> false);
    long now = System.currentTimeMillis();
    m2.book("BTCUSDT").applySnapshot(new double[]{99.97}, new double[]{50}, 1, new double[]{100.0}, new double[]{10}, 1, now, now);
    for (int i = 0; i < 20; i++) { t.set("BTCUSDT", 100 + (i % 2 == 0 ? 0.01 : -0.01), 0, false, 0, 0); m2.onTick(t); }
    t.set("BTCUSDT", 99.0, 0, false, 0, 0); m2.onTick(t);
    int before = calls.get();
    mr2.onEvent(t, 0, true); Thread.sleep(50);
    ck("no new entries while feed is not realtime", calls.get() == before && !mr2.executor().isBusy("BTCUSDT"));

    // закрытие ждёт ордера в полёте
    book.run();
    t.set("BTCUSDT", 100.5, 0, false, 0, System.nanoTime());
    mr.closeAll();
    ck("closeAll closes after in-flight orders", mr.openPositions() == 0);

    System.out.println("pass=" + pass + " fail=" + fail);
    System.exit(fail == 0 ? 0 : 1);
  }
}
