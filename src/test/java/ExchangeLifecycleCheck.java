import com.hft.config.*;
import com.hft.exchange.ExchangeGateway;
import com.hft.exchange.catalog.*;
import com.hft.exchange.generic.*;
import com.hft.exchange.ExchangeFactory;
import java.util.*;
import java.util.function.BooleanSupplier;

/** Биржи целиком (paper-режим, общий класс GeneralExchange): start -> WS-стакан -> конвейер -> stop. */
public class ExchangeLifecycleCheck {
  static int pass, fail;
  static void ck(String n, boolean ok){ if(ok) pass++; else {fail++; System.out.println("FAIL "+n);} }
  static boolean await(BooleanSupplier c, long ms) throws Exception { long t=System.currentTimeMillis()+ms; while(System.currentTimeMillis()<t){ if(c.getAsBoolean()) return true; Thread.sleep(20);} return c.getAsBoolean(); }

  public static void main(String[] a) throws Exception {
    var app = new TradingSettings(TradingParams.DEFAULTS);
    // OKX: WS-стакан через MiniWsServer
    try (var ws = new MiniWsServer()) {
      ws.onText = (c, t) -> { if (t.contains("\"subscribe\"")) c.text("{\"arg\":{\"channel\":\"books\",\"instId\":\"BTC-USDT\"},\"action\":\"snapshot\",\"data\":[{\"asks\":[[\"101\",\"2\"]],\"bids\":[[\"99\",\"3\"]],\"ts\":\"1\"}]}"); };
      var cfg = new ExchangeConfig("okx", false, "http://127.0.0.1:9", ws.url(), 5000, List.of("BTCUSDT"), 20, 100);
      ExchangeGateway gw = ExchangeFactory.create("okx", cfg, app);
      ck("id", gw.id().equals("okx"));
      gw.start();
      var b = gw.marketData().book("BTCUSDT");
      ck("okx book through WS", await(() -> b.bestBid()==99 && b.bestAsk()==101, 5000));
      ck("okx connected + messages", gw.isConnected() && gw.messageCount() > 0);
      ck("okx paper balance seeded", gw.balances().free("USDT") == 1000);
      ck("okx is paper (no keys)", gw instanceof RequestStatsSource rs && !rs.isLive() && "PAPER".equals(rs.requestStats().get("mode")));
      ck("okx stats expose ws+polling", ((Map<?,?>) ((RequestStatsSource) gw).requestStats().get("marketData")).containsKey("ws"));
      ck("okx symbols", gw.symbols().equals(List.of("BTCUSDT")));
      // тики дошли через конвейер до окна цен
      ck("okx tick pipeline reached price window", await(() -> gw.marketData().stats("BTCUSDT").tickCount() > 0, 5000));
      ck("okx accessors", gw.orders()!=null && gw.risk()!=null && gw.strategy()!=null);
      gw.syncBalances();
      gw.stop();
    }
    // Binance и Bybit — тот же GeneralExchange со своим Netty-фидом
    try (var ws = new MiniWsServer()) {
      ws.onOpen = c -> c.text("{\"stream\":\"btcusdt@depth20@100ms\",\"data\":{\"lastUpdateId\":1,\"bids\":[[\"99\",\"3\"]],\"asks\":[[\"101\",\"2\"]]}}");
      var cfg = new ExchangeConfig("binance", false, "http://127.0.0.1:9", ws.url().replace("/ws", ""), 5000, List.of("BTCUSDT"), 20, 100,
          TradingParams.DEFAULTS.with(Map.of("market", "spot")));
      ExchangeGateway gw = ExchangeFactory.create("binance", cfg, app);
      ck("binance spot is GeneralExchange with Netty feed", gw instanceof GeneralExchange se && se.feed() instanceof NettyBookFeed);
      gw.start();
      var b = gw.marketData().book("BTCUSDT");
      ck("binance book through Netty feed", await(() -> b.bestBid()==99 && b.bestAsk()==101, 5000));
      int before = ws.conns.size();
      for (var c : ws.conns) c.closeSocket();          // обрыв: фид переподключается сам (не в потоке Netty)
      ck("binance Netty feed reconnects", await(() -> ws.conns.size() > before && gw.isConnected(), 8000));
      ck("binance paper balance seeded", gw.balances().free("USDT") == 1000 && !((RequestStatsSource) gw).isLive());
      gw.stop();
    }
    for (String mkt : List.of("spot", "perp")) {
      var cfg = new ExchangeConfig("bybit", false, "http://127.0.0.1:9", "ws://127.0.0.1:9", 5000, List.of("BTCUSDT"), 20, 100,
          TradingParams.DEFAULTS.with(Map.of("market", mkt)));
      ExchangeGateway gw = ExchangeFactory.create("bybit", cfg, app);
      ck("bybit " + mkt + " is GeneralExchange", gw instanceof GeneralExchange se && se.feed() instanceof NettyBookFeed
          && (gw.perp() != null) == mkt.equals("perp"));
    }
    // остальные классы конструируются и стартуют в paper-режиме (стакан возьмут REST-опросом позже; тут — только жизненный цикл)
    List<ExchangeGateway> all = new ArrayList<>();
    String[][] defs = {{"gate","BTCUSDT"},{"mexc","BTCUSDT"},{"hyperliquid","BTCUSDC"}};
    for (String[] d : defs) {
      var cfg = new ExchangeConfig(d[0], false, "http://127.0.0.1:9", "ws://127.0.0.1:9/ws", 5000, List.of(d[1]), 20, 100);
      ExchangeGateway g = ExchangeFactory.create(d[0], cfg, app);
      g.start();
      ck(d[0]+" starts paper", g.id().equals(d[0]) && g.balances().total(d[1].endsWith("USDC")?"USDC":"USDT")==1000 && g instanceof RequestStatsSource r && !r.isLive());
      g.stop();
    }
    var uni = ExchangeFactory.create("uniswapv2", new ExchangeConfig("uniswapv2", false, "http://127.0.0.1:9", "ws://127.0.0.1:9/ws", 5000, List.of("WETHUSDC"), 20, 100), app);
    ck("uniswap constructs in paper", uni.id().equals("uniswapv2") && uni instanceof RequestStatsSource ur && !ur.isLive());
    System.out.println("pass="+pass+" fail="+fail); System.exit(fail==0?0:1);
  }
}
