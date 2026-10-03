import com.fasterxml.jackson.databind.ObjectMapper;
import com.hft.config.ExchangeConfig;
import com.hft.engine.TickPipeline;
import com.hft.exchange.binance.BinanceMarketDataFeed;
import com.hft.exchange.bybit.BybitMarketDataFeed;
import com.hft.model.Tick;
import com.hft.net.AbstractWsFeed;
import com.hft.store.MarketDataStore;
import com.hft.store.OrderBook;
import com.lmax.disruptor.EventHandler;
import java.lang.reflect.Method;
import java.util.*;
import java.util.concurrent.*;

/** Потоковый разбор сообщений Binance/Bybit: сделки, стакан, снимок+изменения Bybit, скорость против дерева. */
public class FeedParseCheck {
  static int pass, fail;
  static void ck(String n, boolean ok){ if(ok) pass++; else {fail++; System.out.println("FAIL "+n);} }

  record T(String s, double p, double q, boolean m, long t) {}

  public static void main(String[] a) throws Exception {
    BlockingQueue<T> ticks = new LinkedBlockingQueue<>();
    EventHandler<Tick> h = (e, seq, end) -> ticks.add(new T(e.symbol(), e.price(), e.quantity(), e.buyerIsMaker(), e.exchangeTimeMs()));
    TickPipeline pipe = new TickPipeline(h); pipe.start();
    Method onText = AbstractWsFeed.class.getDeclaredMethod("onText", String.class, long.class); onText.setAccessible(true);

    // ---- Binance
    var bcfg = new ExchangeConfig("binance", true, "", "wss://x", 5000, List.of("BTCUSDT", "ETHUSDT"), 3, 100);
    MarketDataStore bm = new MarketDataStore(3, 100); bm.register("BTCUSDT"); bm.register("ETHUSDT");
    var bf = new BinanceMarketDataFeed(bcfg, bm, pipe);
    onText.invoke(bf, "{\"stream\":\"btcusdt@trade\",\"data\":{\"e\":\"trade\",\"E\":1,\"s\":\"BTCUSDT\",\"t\":5,\"p\":\"65000.10\",\"q\":\"0.25\",\"T\":1700000000123,\"m\":true,\"M\":true}}", System.nanoTime());
    T t = ticks.poll(2, TimeUnit.SECONDS);
    ck("binance trade", t != null && t.s().equals("BTCUSDT") && t.p() == 65000.10 && t.q() == 0.25 && t.m() && t.t() == 1700000000123L);
    onText.invoke(bf, "{\"stream\":\"ethusdt@depth5@100ms\",\"data\":{\"lastUpdateId\":42,\"bids\":[[\"3000.5\",\"1\"],[\"3000.4\",\"2\"],[\"3000.3\",\"3\"],[\"3000.2\",\"4\"]],\"asks\":[[\"3000.6\",\"5\"],[\"3000.7\",\"6\"]]}}", System.nanoTime());
    OrderBook eb = bm.book("ETHUSDT");
    ck("binance depth, trimmed to bookDepth", eb.isReady() && eb.bestBid() == 3000.5 && eb.bestAsk() == 3000.6 && eb.bidCount() == 3 && eb.askCount() == 2 && eb.bidQtyAt(2) == 3);
    onText.invoke(bf, "{\"stream\":\"xrpusdt@trade\",\"data\":{\"e\":\"trade\",\"s\":\"XRPUSDT\",\"p\":\"1\",\"q\":\"1\",\"T\":1,\"m\":false}}", System.nanoTime());
    ck("binance: unsubscribed symbol ignored", ticks.poll(200, TimeUnit.MILLISECONDS) == null);
    onText.invoke(bf, "{\"data\":{\"e\":\"trade\",\"s\":\"ETHUSDT\",\"p\":\"3001\",\"q\":\"2\",\"T\":7,\"m\":false},\"stream\":\"ethusdt@trade\"}", System.nanoTime());
    t = ticks.poll(2, TimeUnit.SECONDS);
    ck("binance: field order does not matter", t != null && t.s().equals("ETHUSDT") && t.p() == 3001 && !t.m());

    // ---- Bybit
    var ycfg = new ExchangeConfig("bybit", true, "", "wss://x", 5000, List.of("BTCUSDT"), 3, 100);
    MarketDataStore ym = new MarketDataStore(3, 100); ym.register("BTCUSDT");
    var yf = new BybitMarketDataFeed(ycfg, ym, pipe);
    onText.invoke(yf, "{\"topic\":\"publicTrade.BTCUSDT\",\"type\":\"snapshot\",\"ts\":1,\"data\":[{\"T\":11,\"s\":\"BTCUSDT\",\"S\":\"Sell\",\"v\":\"0.1\",\"p\":\"100.5\",\"L\":\"PlusTick\",\"i\":\"a\",\"BT\":false},{\"T\":12,\"s\":\"BTCUSDT\",\"S\":\"Buy\",\"v\":\"0.2\",\"p\":\"100.6\",\"i\":\"b\",\"BT\":false}]}", System.nanoTime());
    T t1 = ticks.poll(2, TimeUnit.SECONDS), t2 = ticks.poll(2, TimeUnit.SECONDS);
    ck("bybit trades (array)", t1 != null && t2 != null && t1.p() == 100.5 && t1.m() && t2.q() == 0.2 && !t2.m() && t2.t() == 12);
    OrderBook yb = ym.book("BTCUSDT");
    onText.invoke(yf, "{\"topic\":\"orderbook.50.BTCUSDT\",\"type\":\"delta\",\"ts\":1,\"data\":{\"s\":\"BTCUSDT\",\"b\":[[\"99\",\"1\"]],\"a\":[],\"u\":1}}", System.nanoTime());
    ck("bybit: delta before snapshot is not published", !yb.isReady());
    onText.invoke(yf, "{\"topic\":\"orderbook.50.BTCUSDT\",\"type\":\"snapshot\",\"ts\":1,\"data\":{\"s\":\"BTCUSDT\",\"b\":[[\"100\",\"1\"],[\"99\",\"2\"],[\"98\",\"3\"],[\"97\",\"4\"]],\"a\":[[\"101\",\"1\"],[\"102\",\"2\"]],\"u\":2,\"seq\":1}}", System.nanoTime());
    ck("bybit snapshot replaces book", yb.bestBid() == 100 && yb.bestAsk() == 101 && yb.bidCount() == 3 && yb.bidQtyAt(1) == 2);
    // delta: удаляем лучший бид, меняем объём второго, добавляем аск
    onText.invoke(yf, "{\"topic\":\"orderbook.50.BTCUSDT\",\"type\":\"delta\",\"ts\":2,\"data\":{\"s\":\"BTCUSDT\",\"b\":[[\"100\",\"0\"],[\"99\",\"7\"]],\"a\":[[\"100.5\",\"9\"]],\"u\":3,\"seq\":2}}", System.nanoTime());
    ck("bybit delta merged into book (not overwritten)", yb.bestBid() == 99 && yb.bestBidQty() == 7 && yb.bidCount() == 3 && yb.bidPriceAt(2) == 97
        && yb.bestAsk() == 100.5 && yb.askCount() == 3 && yb.askPriceAt(2) == 102);
    onText.invoke(yf, "{\"success\":true,\"ret_msg\":\"subscribe\",\"op\":\"subscribe\",\"conn_id\":\"x\"}", System.nanoTime());
    ck("bybit service message ignored", yb.bestBid() == 99 && ticks.poll(100, TimeUnit.MILLISECONDS) == null);

    // ---- скорость: потоковый разбор против дерева Jackson на сообщении стакана
    StringBuilder sb = new StringBuilder("{\"topic\":\"orderbook.50.BTCUSDT\",\"type\":\"snapshot\",\"ts\":1,\"data\":{\"s\":\"BTCUSDT\",\"b\":[");
    for (int i = 0; i < 50; i++) sb.append(i > 0 ? "," : "").append("[\"").append(60000 - i * 0.5).append("\",\"").append(0.123 + i).append("\"]");
    sb.append("],\"a\":[");
    for (int i = 0; i < 50; i++) sb.append(i > 0 ? "," : "").append("[\"").append(60001 + i * 0.5).append("\",\"").append(0.321 + i).append("\"]");
    sb.append("],\"u\":5,\"seq\":9}}");
    String msg = sb.toString();
    ObjectMapper om = new ObjectMapper();
    double sink = 0;
    for (int w = 0; w < 3; w++) {
      long s0 = System.nanoTime();
      for (int i = 0; i < 20_000; i++) onText.invoke(yf, msg, 0L);
      long stream = System.nanoTime() - s0;
      s0 = System.nanoTime();
      for (int i = 0; i < 20_000; i++) {
        var root = om.readTree(msg); var d = root.get("data");
        for (var l : d.get("b")) sink += l.get(0).asDouble() + l.get(1).asDouble();
        for (var l : d.get("a")) sink += l.get(0).asDouble() + l.get(1).asDouble();
      }
      long tree = System.nanoTime() - s0;
      if (w == 2) {
        System.out.printf("  стакан 50+50: поток %.1f мкс/сообщ., дерево %.1f мкс/сообщ. (%.1fx)%n", stream / 20_000 / 1e3, tree / 20_000 / 1e3, (double) tree / stream);
        ck("streaming parse faster than tree", stream < tree);
      }
    }
    if (sink == 42) System.out.println();
    pipe.shutdown();
    System.out.println("pass="+pass+" fail="+fail); System.exit(fail==0?0:1);
  }
}
