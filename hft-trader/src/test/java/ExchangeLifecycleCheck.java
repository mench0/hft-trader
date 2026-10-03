import com.hft.config.*;
import com.hft.exchange.ExchangeGateway;
import com.hft.exchange.catalog.*;
import com.hft.exchange.generic.*;
import com.hft.exchange.ExchangeFactory;
import java.util.*;
import java.util.function.BooleanSupplier;

/** Биржи целиком (paper-режим, по образцу BybitExchange): start -> WS-стакан -> конвейер -> stop. */
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
      gw.addSymbol("ETHUSDT");
      ck("okx addSymbol", gw.symbols().contains("ETHUSDT") && await(() -> ws.received.stream().anyMatch(t -> t.contains("ETH-USDT")), 3000));
      gw.removeSymbol("ETHUSDT");
      ck("okx removeSymbol", !gw.symbols().contains("ETHUSDT"));
      ck("okx accessors", gw.orders()!=null && gw.risk()!=null && gw.strategy()!=null);
      gw.syncBalances();
      gw.stop();
    }
    // остальные классы конструируются и стартуют в paper-режиме (стакан возьмут REST-опросом позже; тут — только жизненный цикл)
    List<ExchangeGateway> all = new ArrayList<>();
    String[][] defs = {{"gate","BTCUSDT"},{"mexc","BTCUSDT"},{"bingx","BTCUSDT"},{"lbank","BTCUSDT"},{"hyperliquid","BTCUSDC"}};
    for (String[] d : defs) {
      var cfg = new ExchangeConfig(d[0], false, "http://127.0.0.1:9", "ws://127.0.0.1:9/ws", 5000, List.of(d[1]), 20, 100);
      ExchangeGateway g = ExchangeFactory.create(d[0], cfg, app);
      g.start();
      ck(d[0]+" starts paper", g.id().equals(d[0]) && g.balances().total(d[1].endsWith("USDC")?"USDC":"USDT")==1000 && g instanceof RequestStatsSource r && !r.isLive());
      g.stop();
    }
    var dy = new PaperExchange(ExchangeCatalog.find("dydx").get(), new ExchangeConfig("dydx", false, "http://127.0.0.1:9", "ws://127.0.0.1:9/ws", 5000, List.of("BTCUSD"), 20, 100), app);
    dy.start(); ck("dydx paper starts", dy.id().equals("dydx") && dy.balances().total("USD")==1000); dy.stop();
    var uni = ExchangeFactory.create("uniswapv2", new ExchangeConfig("uniswapv2", false, "http://127.0.0.1:9", "ws://127.0.0.1:9/ws", 5000, List.of("WETHUSDC"), 20, 100), app);
    ck("uniswap constructs in paper", uni.id().equals("uniswapv2") && uni instanceof RequestStatsSource ur && !ur.isLive());
    System.out.println("pass="+pass+" fail="+fail); System.exit(fail==0?0:1);
  }
}
