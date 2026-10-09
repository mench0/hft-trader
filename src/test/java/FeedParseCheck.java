import com.hft.config.*;
import com.hft.exchange.catalog.*;
import com.hft.exchange.generic.*;
import com.hft.store.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

/** Стакан Binance (спот) и Bybit (спот и linear) через общий фид: подписка, снимок, изменения, пинг, REST-запас. */
public class FeedParseCheck {
  static int pass, fail;
  static void ck(String n, boolean ok){ if(ok) pass++; else {fail++; System.out.println("FAIL "+n);} }
  static boolean await(BooleanSupplier c, long ms) throws Exception { long t=System.currentTimeMillis()+ms; while(System.currentTimeMillis()<t){ if(c.getAsBoolean()) return true; Thread.sleep(20);} return c.getAsBoolean(); }

  static WsBookFeed feed(String id, String url, MarketDataStore m, boolean perp, AtomicInteger ticks) {
    var params = TradingParams.DEFAULTS.with(Map.of("market", perp ? "perp" : "spot"));
    var cfg = new ExchangeConfig(id, false, "", url, 5000, List.of("BTCUSDT", "ETHUSDT"), 20, 100, params);
    return new WsBookFeed(ExchangeCatalog.find(id).get(), cfg, WsDialects.forExchange(id, cfg).get(), m,
        (sy, px, q, bm, ts, rn) -> ticks.incrementAndGet(), s -> {}, () -> {}).tune(30_000, 50);
  }

  public static void main(String[] a) throws Exception {
    // ───── Binance спот: SUBSCRIBE на depth20@100ms, снимки в обёртке {stream, data}
    try (var srv = new MiniWsServer()) {
      srv.onText = (c, t) -> {
        if (t.contains("\"SUBSCRIBE\"")) {
          c.text("{\"result\":null,\"id\":1}");
          c.text("{\"stream\":\"btcusdt@depth20@100ms\",\"data\":{\"lastUpdateId\":42,\"bids\":[[\"99.5\",\"1\"],[\"99\",\"2\"]],\"asks\":[[\"100\",\"3\"],[\"100.5\",\"4\"]]}}");
        }
      };
      var m = new MarketDataStore(20, 100); m.register("BTCUSDT"); m.register("ETHUSDT");
      var ticks = new AtomicInteger();
      var f = feed("binance", srv.url(), m, false, ticks);
      f.start();
      var b = m.book("BTCUSDT");
      ck("binance: subscribe depth20@100ms for both symbols", await(() -> srv.received.stream().anyMatch(t -> t.contains("btcusdt@depth20@100ms") && t.contains("ethusdt@depth20@100ms")), 5000));
      ck("binance: snapshot applied", await(() -> b.bestBid() == 99.5 && b.bestAsk() == 100 && b.bidCount() == 2, 5000));
      ck("binance: tick published, realtime", ticks.get() > 0 && f.isRealtime());
      srv.conns.get(0).text("{\"stream\":\"btcusdt@depth20@100ms\",\"data\":{\"lastUpdateId\":43,\"bids\":[[\"99.6\",\"1\"]],\"asks\":[[\"99.9\",\"1\"]]}}");
      ck("binance: next snapshot replaces book", await(() -> b.bestBid() == 99.6 && b.bestAsk() == 99.9 && b.bidCount() == 1, 3000));
      ck("binance: no parse errors", ((Number) f.stats().get("parseErrors")).longValue() == 0);
      f.stop();
    }
    ck("binance: default urls", WsDialects.binanceSpot().defaultUrl(false).equals("wss://stream.binance.com:9443/stream")
        && WsDialects.binanceSpot().defaultUrl(true).contains("testnet"));

    // ───── Bybit спот и linear: orderbook.50, снимок + изменения, ответы на подписку, пинг
    for (boolean perp : new boolean[]{false, true}) {
      String tag = perp ? "bybit linear: " : "bybit spot: ";
      try (var srv = new MiniWsServer()) {
        srv.onText = (c, t) -> {
          if (t.contains("\"subscribe\"")) {
            c.text("{\"success\":true,\"ret_msg\":\"subscribe\",\"conn_id\":\"x\",\"op\":\"subscribe\"}");
            c.text("{\"topic\":\"orderbook.50.BTCUSDT\",\"type\":\"delta\",\"ts\":1,\"data\":{\"s\":\"BTCUSDT\",\"b\":[[\"98\",\"1\"]],\"a\":[],\"u\":1,\"seq\":1}}");
            c.text("{\"topic\":\"orderbook.50.BTCUSDT\",\"type\":\"snapshot\",\"ts\":2,\"data\":{\"s\":\"BTCUSDT\",\"b\":[[\"100\",\"1\"],[\"99\",\"2\"]],\"a\":[[\"101\",\"1\"],[\"102\",\"2\"]],\"u\":2,\"seq\":2}}");
          }
          if (t.contains("\"ping\"")) c.text("{\"success\":true,\"ret_msg\":\"pong\",\"conn_id\":\"x\",\"op\":\"ping\"}");
        };
        var m = new MarketDataStore(20, 100); m.register("BTCUSDT"); m.register("ETHUSDT");
        var f = feed("bybit", srv.url(), m, perp, new AtomicInteger());
        f.start();
        var b = m.book("BTCUSDT");
        ck(tag + "subscribe orderbook.50", await(() -> srv.received.stream().anyMatch(t -> t.contains("orderbook.50.BTCUSDT") && t.contains("orderbook.50.ETHUSDT")), 5000));
        ck(tag + "snapshot (delta before it ignored)", await(() -> b.bestBid() == 100 && b.bestAsk() == 101 && b.bidCount() == 2, 5000));
        // изменения: убрать лучший бид, поменять объём второго, добавить аск
        srv.conns.get(0).text("{\"topic\":\"orderbook.50.BTCUSDT\",\"type\":\"delta\",\"ts\":3,\"data\":{\"s\":\"BTCUSDT\",\"b\":[[\"100\",\"0\"],[\"99\",\"7\"]],\"a\":[[\"100.5\",\"9\"]],\"u\":3,\"seq\":3}}");
        ck(tag + "delta merged", await(() -> b.bestBid() == 99 && b.bestBidQty() == 7 && b.bestAsk() == 100.5 && b.askCount() == 3, 3000));
        ck(tag + "no parse errors", ((Number) f.stats().get("parseErrors")).longValue() == 0);
        f.stop();
      }
    }
    var cfgSpot = new ExchangeConfig("bybit", false, "", "", 5000, List.of("BTCUSDT"), 20, 100);
    var cfgPerp = new ExchangeConfig("bybit", false, "", "", 5000, List.of("BTCUSDT"), 20, 100, TradingParams.DEFAULTS.with(Map.of("market", "perp")));
    ck("bybit: urls spot/linear", WsDialects.forExchange("bybit", cfgSpot).get().defaultUrl(false).endsWith("/v5/public/spot")
        && WsDialects.forExchange("bybit", cfgPerp).get().defaultUrl(false).endsWith("/v5/public/linear")
        && WsDialects.forExchange("bybit", cfgSpot).get().pingMessage().contains("ping"));

    // ───── REST-запас: стакан Binance /api/v3/depth и Bybit /v5/market/orderbook
    var bin = Dialects.forExchange("binance", new ExchangeConfig("binance", false, "", "", 5000, List.of("BTCUSDT"), 20, 100));
    ck("binance rest request", bin.request("https://api.binance.com", "BTCUSDT", 20).uri().toString().equals("https://api.binance.com/api/v3/depth?symbol=BTCUSDT&limit=20"));
    var pb = bin.parse("{\"lastUpdateId\":1,\"bids\":[[\"99\",\"1\"]],\"asks\":[[\"100\",\"2\"]]}", "BTCUSDT");
    ck("binance rest parse", pb.bp()[0] == 99 && pb.aq()[0] == 2);
    var by = Dialects.forExchange("bybit", cfgPerp);
    ck("bybit rest request", by.request("https://api.bybit.com", "BTCUSDT", 20).uri().toString().contains("/v5/market/orderbook?category=linear&symbol=BTCUSDT"));
    var yb = by.parse("{\"retCode\":0,\"result\":{\"s\":\"BTCUSDT\",\"b\":[[\"99\",\"1\"]],\"a\":[[\"100\",\"2\"]],\"ts\":5}}", "BTCUSDT");
    ck("bybit rest parse", yb.bp()[0] == 99 && yb.ap()[0] == 100 && yb.tsMs() == 5);
    boolean err = false; try { by.parse("{\"retCode\":10001,\"retMsg\":\"bad\"}", "BTCUSDT"); } catch (Exception e) { err = true; }
    ck("bybit rest error -> exception", err);

    System.out.println("pass=" + pass + " fail=" + fail);
    System.exit(fail == 0 ? 0 : 1);
  }
}
