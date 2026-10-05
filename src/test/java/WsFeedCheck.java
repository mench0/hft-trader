import com.hft.config.ExchangeConfig;
import com.hft.exchange.catalog.*;
import com.hft.exchange.generic.*;
import com.hft.store.*;
import java.io.ByteArrayOutputStream;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.function.BooleanSupplier;
import java.util.zip.GZIPOutputStream;
import com.sun.net.httpserver.HttpServer;

public class WsFeedCheck {
  static int pass, fail;
  static void ck(String n, boolean ok){ if(ok) pass++; else {fail++; System.out.println("FAIL "+n);} }
  static boolean await(BooleanSupplier c, long ms) throws Exception { long t=System.currentTimeMillis()+ms; while(System.currentTimeMillis()<t){ if(c.getAsBoolean()) return true; Thread.sleep(20);} return c.getAsBoolean(); }
  static boolean near(double a,double b){ return Math.abs(a-b)<1e-9; }

  record Rig(WsBookFeed feed, MarketDataStore market, AtomicInteger books, AtomicInteger gaveUp, AtomicInteger ticks) {}

  static Rig rig(String id, String url, List<String> syms, long stale, long backoff) {
    ExchangeInfo info = ExchangeCatalog.find(id).get();
    var cfg = new ExchangeConfig(id, false, "", url, 5000, syms, 20, 100);
    var m = new MarketDataStore(20, 100); syms.forEach(m::register);
    var books = new AtomicInteger(); var gu = new AtomicInteger(); var ticks = new AtomicInteger();
    var f = new WsBookFeed(info, cfg, WsDialects.forExchange(id).get(), m, (sy,px,q,bm,ts,rn) -> ticks.incrementAndGet(), s -> books.incrementAndGet(), gu::incrementAndGet).tune(stale, backoff);
    return new Rig(f, m, books, gu, ticks);
  }

  static byte[] gzip(String s) throws Exception { var bo=new ByteArrayOutputStream(); try(var g=new GZIPOutputStream(bo)){ g.write(s.getBytes()); } return bo.toByteArray(); }

  public static void main(String[] a) throws Exception {
    // ───── OKX: подписка, snapshot, update, удаление уровня, фрагментация
    try (var srv = new MiniWsServer()) {
      var r = rig("okx", srv.url(), List.of("BTCUSDT"), 30000, 50);
      srv.onText = (c, t) -> {
        if (t.contains("\"subscribe\"")) {
          c.text("{\"event\":\"subscribe\",\"arg\":{\"channel\":\"books\",\"instId\":\"BTC-USDT\"}}");
          c.textFragmented("{\"arg\":{\"channel\":\"books\",\"instId\":\"BTC-USDT\"},\"action\":\"snapshot\",\"data\":[{\"asks\":[[\"101\",\"2\",\"0\",\"1\"],[\"100.5\",\"1\",\"0\",\"1\"]],\"bids\":[[\"99\",\"3\",\"0\",\"1\"],[\"99.5\",\"1\",\"0\",\"1\"]],\"ts\":\"1700000000000\"}]}");
        }
      };
      r.feed().start();
      var b = r.market().book("BTCUSDT");
      ck("okx snapshot", await(() -> near(b.bestBid(), 99.5) && near(b.bestAsk(), 100.5), 5000));
      ck("okx subscribe msg", srv.received.stream().anyMatch(t -> t.contains("\"channel\":\"books\"") && t.contains("BTC-USDT")));
      ck("okx connected", r.feed().isConnected());
      srv.conns.get(0).text("{\"arg\":{\"channel\":\"books\",\"instId\":\"BTC-USDT\"},\"action\":\"update\",\"data\":[{\"asks\":[[\"100.5\",\"0\",\"0\",\"0\"],[\"100.7\",\"4\",\"0\",\"1\"]],\"bids\":[[\"99.8\",\"2\",\"0\",\"1\"]],\"ts\":\"1700000000100\"}]}");
      ck("okx update", await(() -> near(b.bestBid(), 99.8) && near(b.bestAsk(), 100.7), 5000));
      ck("okx ask qty", near(b.bestAskQty(), 4));
      srv.conns.get(0).text("pong");
      ck("okx pong text ok", r.feed().stats().get("parseErrors").equals(0L));
      // server ping frame -> JDK pong
      srv.conns.get(0).ping();
      ck("jdk auto-pong", await(() -> srv.pongs.get() >= 1, 3000));
      // добавление символа на лету
      r.feed().stop();
    }

    // ───── переподключение и повторная подписка (с новым снимком) + счётчик сбоев сбрасывается
    try (var srv = new MiniWsServer()) {
      var r = rig("okx", srv.url(), List.of("BTCUSDT"), 30000, 20);
      var n = new AtomicInteger();
      srv.onText = (c, t) -> {
        if (t.contains("\"subscribe\"")) {
          int k = n.incrementAndGet();
          double px = 100 * k;
          c.text("{\"arg\":{\"channel\":\"books\",\"instId\":\"BTC-USDT\"},\"action\":\"snapshot\",\"data\":[{\"asks\":[[\"" + (px + 1) + "\",\"1\"]],\"bids\":[[\"" + px + "\",\"1\"]],\"ts\":\"1\"}]}");
          if (k == 1) new Thread(() -> { try { Thread.sleep(200); } catch (Exception e) {} c.closeSocket(); }).start();
        }
      };
      r.feed().start();
      var b = r.market().book("BTCUSDT");
      ck("reconnect: second snapshot", await(() -> near(b.bestBid(), 200), 8000));
      ck("reconnect: resubscribed", n.get() == 2);
      ck("reconnect counted", ((Long) r.feed().stats().get("reconnects")) >= 1);
      ck("reconnect: failures reset", ((Integer) r.feed().stats().get("consecutiveFailures")) == 0);
      ck("no giveUp", r.gaveUp().get() == 0 && !r.feed().hasGivenUp());
      r.feed().stop();
    }

    // ───── тишина -> переподключение
    try (var srv = new MiniWsServer()) {
      var r = rig("gate", srv.url(), List.of("BTCUSDT"), 700, 20);
      r.feed().start();
      ck("stale: reconnects", await(() -> srv.conns.size() >= 2, 8000));
      r.feed().stop();
    }

    // ───── недоступный сервер -> give-up после 15 попыток
    {
      int port; try (var s = new java.net.ServerSocket(0)) { port = s.getLocalPort(); }
      var r = rig("okx", "ws://127.0.0.1:" + port + "/ws", List.of("BTCUSDT"), 30000, 1);
      r.feed().start();
      ck("giveUp called", await(() -> r.gaveUp().get() == 1, 30000));
      ck("giveUp flag", r.feed().hasGivenUp() && !r.feed().isConnected());
    }

    // ───── Gate
    try (var srv = new MiniWsServer()) {
      var r = rig("gate", srv.url(), List.of("BTCUSDT"), 30000, 50);
      srv.onText = (c, t) -> { if (t.contains("\"subscribe\"")) {
        c.text("{\"time\":1,\"channel\":\"spot.order_book\",\"event\":\"subscribe\",\"result\":{\"status\":\"success\"}}");
        c.text("{\"time\":1,\"channel\":\"spot.order_book\",\"event\":\"update\",\"result\":{\"t\":1700000000000,\"lastUpdateId\":5,\"s\":\"BTC_USDT\",\"bids\":[[\"99\",\"1\"],[\"98\",\"2\"]],\"asks\":[[\"101\",\"3\"],[\"102\",\"1\"]]}}");
      }};
      r.feed().start();
      var b = r.market().book("BTCUSDT");
      ck("gate book", await(() -> near(b.bestBid(), 99) && near(b.bestAsk(), 101) && near(b.bestAskQty(), 3), 5000));
      ck("gate subscribe payload", srv.received.stream().anyMatch(t -> t.contains("spot.order_book") && t.contains("BTC_USDT") && t.contains("\"20\"")));
      // следующий снимок полностью заменяет предыдущий
      srv.conns.get(0).text("{\"time\":2,\"channel\":\"spot.order_book\",\"event\":\"update\",\"result\":{\"t\":1,\"s\":\"BTC_USDT\",\"bids\":[[\"97\",\"1\"]],\"asks\":[[\"103\",\"1\"]]}}");
      ck("gate snapshot replaces", await(() -> near(b.bestBid(), 97) && near(b.bestAsk(), 103), 5000));
      // ошибка биржи не роняет цикл, но считается
      srv.conns.get(0).text("{\"time\":3,\"channel\":\"spot.order_book\",\"event\":\"subscribe\",\"error\":{\"code\":2,\"message\":\"invalid pair\"}}");
      ck("gate error counted", await(() -> ((Long) r.feed().stats().get("parseErrors")) == 1L, 3000));
      ck("gate error text", String.valueOf(r.feed().stats().get("lastError")).contains("invalid pair"));
      r.feed().stop();
    }

    // ───── BingX: gzip + Ping/Pong
    try (var srv = new MiniWsServer()) {
      var r = rig("bingx", srv.url(), List.of("BTCUSDT"), 30000, 50);
      srv.onText = (c, t) -> { if (t.contains("\"sub\"")) {
        try {
          c.binary(gzip("{\"id\":\"x\",\"code\":0,\"msg\":\"\",\"dataType\":\"\",\"data\":null}"));
          c.binary(gzip("{\"code\":0,\"dataType\":\"BTC-USDT@depth20\",\"data\":{\"bids\":[[\"98\",\"1\"],[\"99\",\"2\"]],\"asks\":[[\"102\",\"1\"],[\"101\",\"5\"]]},\"ts\":1700000000000}"));
          c.binary(gzip("Ping"));
        } catch (Exception e) { throw new RuntimeException(e); }
      }};
      r.feed().start();
      var b = r.market().book("BTCUSDT");
      ck("bingx gzip book sorted", await(() -> near(b.bestBid(), 99) && near(b.bestAsk(), 101) && near(b.bestAskQty(), 5), 5000));
      ck("bingx Pong reply", await(() -> srv.received.contains("Pong"), 3000));
      ck("bingx sub msg", srv.received.stream().anyMatch(t -> t.contains("BTC-USDT@depth20") && t.contains("\"reqType\":\"sub\"")));
      ck("bingx no parse errors", r.feed().stats().get("parseErrors").equals(0L));
      r.feed().stop();
    }

    // ───── Hyperliquid
    try (var srv = new MiniWsServer()) {
      var r = rig("hyperliquid", srv.url(), List.of("BTCUSDC"), 30000, 50);
      srv.onText = (c, t) -> { if (t.contains("l2Book")) {
        c.text("{\"channel\":\"subscriptionResponse\",\"data\":{\"method\":\"subscribe\",\"subscription\":{\"type\":\"l2Book\",\"coin\":\"BTC\"}}}");
        c.text("{\"channel\":\"l2Book\",\"data\":{\"coin\":\"BTC\",\"time\":1700000000000,\"levels\":[[{\"px\":\"99\",\"sz\":\"1.5\",\"n\":2},{\"px\":\"98\",\"sz\":\"1\",\"n\":1}],[{\"px\":\"100\",\"sz\":\"2\",\"n\":1}]]}}");
      }};
      r.feed().start();
      var b = r.market().book("BTCUSDC");
      ck("hl book", await(() -> near(b.bestBid(), 99) && near(b.bestAsk(), 100) && near(b.bestBidQty(), 1.5), 5000));
      ck("hl subscribe coin=BTC", srv.received.stream().anyMatch(t -> t.contains("\"coin\":\"BTC\"") && t.contains("\"method\":\"subscribe\"")));
      srv.conns.get(0).text("{\"channel\":\"pong\"}");
      srv.conns.get(0).text("{\"channel\":\"error\",\"data\":\"Already subscribed\"}");
      ck("hl error surfaces", await(() -> String.valueOf(r.feed().stats().get("lastError")).contains("Already subscribed"), 3000));
      r.feed().stop();
    }

    // ───── dYdX: subscribed + batch + удаление уровня
    try (var srv = new MiniWsServer()) {
      var r = rig("dydx", srv.url(), List.of("BTCUSD"), 30000, 50);
      srv.onText = (c, t) -> { if (t.contains("v4_orderbook")) {
        c.text("{\"type\":\"subscribed\",\"connection_id\":\"x\",\"message_id\":1,\"channel\":\"v4_orderbook\",\"id\":\"BTC-USD\",\"contents\":{\"bids\":[{\"price\":\"99\",\"size\":\"1\"},{\"price\":\"98\",\"size\":\"2\"}],\"asks\":[{\"price\":\"101\",\"size\":\"1\"},{\"price\":\"102\",\"size\":\"2\"}]}}");
      }};
      srv.onOpen = c -> c.text("{\"type\":\"connected\",\"connection_id\":\"x\",\"message_id\":0}");
      r.feed().start();
      var b = r.market().book("BTCUSD");
      ck("dydx snapshot", await(() -> near(b.bestBid(), 99) && near(b.bestAsk(), 101), 5000));
      srv.conns.get(0).text("{\"type\":\"channel_batch_data\",\"connection_id\":\"x\",\"message_id\":2,\"id\":\"BTC-USD\",\"channel\":\"v4_orderbook\",\"version\":\"1.0.0\",\"contents\":[{\"bids\":[[\"99\",\"0\"]]},{\"asks\":[[\"100.5\",\"3\"]]},{\"bids\":[[\"99.2\",\"1\"]]}]}");
      ck("dydx batch applied", await(() -> near(b.bestBid(), 99.2) && near(b.bestAsk(), 100.5) && near(b.bestAskQty(), 3), 5000));
      srv.conns.get(0).text("{\"type\":\"channel_data\",\"id\":\"BTC-USD\",\"contents\":{\"bids\":[[\"99.2\",\"0\"]]}}");
      ck("dydx level removed", await(() -> near(b.bestBid(), 98), 5000));
      ck("dydx subscribe batched", srv.received.stream().anyMatch(t -> t.contains("\"batched\":true") && t.contains("BTC-USD")));
      r.feed().stop();
    }

    // ───── перекрещённый стакан не публикуется
    try (var srv = new MiniWsServer()) {
      var r = rig("dydx", srv.url(), List.of("BTCUSD"), 30000, 50);
      srv.onText = (c, t) -> { if (t.contains("v4_orderbook")) {
        c.text("{\"type\":\"subscribed\",\"id\":\"BTC-USD\",\"contents\":{\"bids\":[{\"price\":\"99\",\"size\":\"1\"}],\"asks\":[{\"price\":\"101\",\"size\":\"1\"}]}}");
      }};
      r.feed().start();
      var b = r.market().book("BTCUSD");
      await(() -> near(b.bestBid(), 99), 5000);
      int before = r.books().get();
      srv.conns.get(0).text("{\"type\":\"channel_data\",\"id\":\"BTC-USD\",\"contents\":{\"bids\":[[\"102\",\"1\"]]}}");
      Thread.sleep(400);
      ck("crossed not published", r.books().get() == before && near(b.bestBid(), 99));
      srv.conns.get(0).text("{\"type\":\"channel_data\",\"id\":\"BTC-USD\",\"contents\":{\"bids\":[[\"102\",\"0\"]]}}");
      ck("recovers after uncross", await(() -> r.books().get() > before && near(b.bestBid(), 99), 5000));
      r.feed().stop();
    }

    // ───── 5 ошибок разбора подряд -> переподключение
    try (var srv = new MiniWsServer()) {
      var r = rig("okx", srv.url(), List.of("BTCUSDT"), 30000, 20);
      srv.onOpen = c -> new Thread(() -> { try { Thread.sleep(300); } catch (Exception e) {} for (int i = 0; i < 5; i++) c.text("garbage"); }).start();
      r.feed().start();
      ck("parse errors -> reconnect", await(() -> srv.conns.size() >= 2, 8000));
      r.feed().stop();
    }

    // ───── Hybrid: WS жив -> опрос на паузе; WS нет -> опрос работает
    var hits = new AtomicInteger();
    HttpServer http = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    http.createContext("/", ex -> {
      hits.incrementAndGet();
      byte[] body = "{\"code\":\"0\",\"data\":[{\"asks\":[[\"201\",\"1\",\"0\",\"1\"]],\"bids\":[[\"199\",\"1\",\"0\",\"1\"]],\"ts\":\"5\"}]}".getBytes();
      ex.sendResponseHeaders(200, body.length); ex.getResponseBody().write(body); ex.close();
    });
    http.start();
    String rest = "http://127.0.0.1:" + http.getAddress().getPort();
    try (var srv = new MiniWsServer()) {
      srv.onText = (c, t) -> { if (t.contains("\"subscribe\"")) c.text("{\"arg\":{\"channel\":\"books\",\"instId\":\"BTC-USDT\"},\"action\":\"snapshot\",\"data\":[{\"asks\":[[\"101\",\"1\"]],\"bids\":[[\"99\",\"1\"]],\"ts\":\"1\"}]}"); };
      var info = ExchangeCatalog.find("okx").get();
      var cfg = new ExchangeConfig("okx", false, rest, srv.url(), 5000, List.of("BTCUSDT"), 20, 100);
      var m = new MarketDataStore(20, 100); m.register("BTCUSDT");
      var h = new HybridBookFeed(info, cfg, WsDialects.forExchange("okx").get(), Dialects.forExchange("okx"), m, (sy,px,q,bm,ts,rn) -> {}, s -> {}, () -> {}).grace(500);
      h.start();
      var b = m.book("BTCUSDT");
      ck("hybrid ws book", await(() -> near(b.bestBid(), 99), 5000));
      Thread.sleep(1500);
      ck("hybrid polling paused while ws up", hits.get() == 0 && Boolean.TRUE.equals(h.stats().get("pollingPaused")));
      // WS падает и не возвращается
      srv.accepting = false;
      for (var c : srv.conns) c.closeSocket();
      ck("hybrid falls back to REST", await(() -> near(b.bestBid(), 199) && hits.get() > 0, 15000));
      ck("hybrid polling resumed", Boolean.FALSE.equals(h.stats().get("pollingPaused")));
      // WS возвращается
      srv.accepting = true;
      ck("hybrid polling paused again", await(() -> Boolean.TRUE.equals(h.stats().get("pollingPaused")), 40000));
      Thread.sleep(300);
      srv.conns.get(srv.conns.size() - 1).text("{\"arg\":{\"channel\":\"books\",\"instId\":\"BTC-USDT\"},\"action\":\"snapshot\",\"data\":[{\"asks\":[[\"101\",\"1\"]],\"bids\":[[\"99\",\"1\"]],\"ts\":\"2\"}]}");
      ck("hybrid back to ws", await(() -> near(b.bestBid(), 99), 5000));
      ck("hybrid not given up", !h.hasGivenUp());
      h.stop();
    }
    http.stop(0);

    // ───── каталог/диалекты
    ck("mexc has ws dialect (protobuf)", WsDialects.forExchange("mexc").isPresent() && WsDialects.forExchange("mexc").get().parsesBinary());
    ck("binance no generic ws dialect", WsDialects.forExchange("binance").isEmpty());
    for (String id : List.of("okx","gate","bingx","hyperliquid","dydx")) ck("dialect "+id, WsDialects.forExchange(id).isPresent());
    ck("okx venue", WsDialects.forExchange("okx").get().venueSymbol("BTCUSDT").equals("BTC-USDT"));
    ck("hl venue", WsDialects.forExchange("hyperliquid").get().venueSymbol("BTCUSDC").equals("BTC"));
    ck("dydx venue", WsDialects.forExchange("dydx").get().venueSymbol("BTCUSD").equals("BTC-USD"));
    var okxSub = WsDialects.forExchange("okx").get().subscribe(java.util.stream.IntStream.range(0, 45).mapToObj(i -> "A" + i + "-USDT").toList(), 20);
    ck("okx chunks of 20", okxSub.size() == 3);

    System.out.println("pass=" + pass + " fail=" + fail);
    System.exit(fail == 0 ? 0 : 1);
  }
}
