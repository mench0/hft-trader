import com.fasterxml.jackson.databind.*;
import com.hft.discovery.*;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.util.*;

/** Подбор тикеров: разбор сводок/свечей бирж (фейковый сервер), профили стратегий, бэктест, JSON для админки. */
public class DiscoveryCheck {
  static int pass, fail;
  static void ck(String n, boolean ok){ if(ok) pass++; else {fail++; System.out.println("FAIL "+n);} }
  static ObjectMapper M = new ObjectMapper();

  /** Пила с резкими провалами — mean reversion зарабатывает. */
  static double[] dips(int n, double base) { double[] c = new double[n]; for (int i = 0; i < n; i++) { c[i] = base * (1 + 0.002 * Math.sin(i / 3.0)); if (i % 45 == 44) c[i] = base * 0.985; } return c; }
  /** Падающий тренд — провалы не возвращаются. */
  static double[] trend(int n, double base) { double[] c = new double[n]; for (int i = 0; i < n; i++) c[i] = base * (1 - 0.0008 * i) * (1 + 0.001 * Math.sin(i)); return c; }

  static String binKlines(double[] c) { StringBuilder sb = new StringBuilder("["); for (int i = 0; i < c.length; i++) sb.append(i==0?"":",").append("[").append(1_700_000_000_000L + i*60_000L).append(",\"1\",\"1\",\"1\",\"").append(c[i]).append("\",\"1\"]"); return sb.append("]").toString(); }

  public static void main(String[] a) throws Exception {
    // ---- бэктест на синтетике
    var p = new MeanReversionBacktest.Params(2.0, 0.3, 0.5, 30, 60);
    var good = MeanReversionBacktest.run(dips(500, 100), p, 0.1);
    var bad = MeanReversionBacktest.run(trend(500, 100), p, 0.1);
    ck("backtest: dips are profitable", good.trades() >= 3 && good.netPct() > 0);
    ck("backtest: downtrend is not", bad.netPct() <= 0);

    // ---- фейковые биржи
    double[] up = dips(500, 50), down = trend(500, 20);
    HttpServer s = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    s.createContext("/", ex -> {
      String path = ex.getRequestURI().getPath(), q = String.valueOf(ex.getRequestURI().getQuery());
      String body = new String(ex.getRequestBody().readAllBytes());
      String r = "{}";
      switch (path) {
        case "/bin/api/v3/ticker/24hr" -> r = "[" +
            "{\"symbol\":\"AAAUSDT\",\"lastPrice\":\"50\",\"bidPrice\":\"49.995\",\"askPrice\":\"50.005\",\"highPrice\":\"51.5\",\"lowPrice\":\"48.9\",\"openPrice\":\"49.8\",\"quoteVolume\":\"5000000\",\"count\":90000}," +
            "{\"symbol\":\"TRDUSDT\",\"lastPrice\":\"20\",\"bidPrice\":\"19.999\",\"askPrice\":\"20.001\",\"highPrice\":\"20.5\",\"lowPrice\":\"18\",\"openPrice\":\"18.1\",\"quoteVolume\":\"9000000\"}," +
            "{\"symbol\":\"DIPUSDT\",\"lastPrice\":\"20\",\"bidPrice\":\"19.999\",\"askPrice\":\"20.001\",\"highPrice\":\"20.4\",\"lowPrice\":\"19.6\",\"openPrice\":\"20\",\"quoteVolume\":\"8000000\"}," +
            "{\"symbol\":\"THNUSDT\",\"lastPrice\":\"1\",\"bidPrice\":\"0.995\",\"askPrice\":\"1.005\",\"highPrice\":\"1.05\",\"lowPrice\":\"0.95\",\"openPrice\":\"1\",\"quoteVolume\":\"300000\"}," +
            "{\"symbol\":\"ETHUSDT\",\"lastPrice\":\"100\",\"bidPrice\":\"99.95\",\"askPrice\":\"100\",\"highPrice\":\"103\",\"lowPrice\":\"97\",\"openPrice\":\"100\",\"quoteVolume\":\"90000000\"}," +
            "{\"symbol\":\"ETHBTC\",\"lastPrice\":\"0.05\",\"quoteVolume\":\"1000\"}]";
        case "/bin/api/v3/klines" -> r = binKlines(q.contains("AAAUSDT") ? up : q.contains("DIPUSDT") ? down : dips(500, 100));
        case "/okx/api/v5/market/tickers" -> r = "{\"code\":\"0\",\"data\":[" +
            "{\"instId\":\"ETH-USDT\",\"last\":\"101\",\"bidPx\":\"101\",\"askPx\":\"101.05\",\"open24h\":\"100\",\"high24h\":\"102\",\"low24h\":\"99\",\"volCcy24h\":\"50000000\"}," +
            "{\"instId\":\"AAA-USDT\",\"last\":\"50.02\",\"bidPx\":\"50.01\",\"askPx\":\"50.03\",\"open24h\":\"50\",\"high24h\":\"51\",\"low24h\":\"49\",\"volCcy24h\":\"400000\"}]}";
        case "/okx/api/v5/market/candles" -> r = "{\"code\":\"0\",\"data\":[[\"1700000060000\",\"1\",\"1\",\"1\",\"10.5\"],[\"1700000000000\",\"1\",\"1\",\"1\",\"10\"]]}";
        case "/gate/api/v4/spot/tickers" -> r = "[{\"currency_pair\":\"ETH_USDT\",\"last\":\"100.4\",\"lowest_ask\":\"100.5\",\"highest_bid\":\"100.3\",\"change_percentage\":\"0.4\",\"base_volume\":\"1\",\"quote_volume\":\"2000000\",\"high_24h\":\"101\",\"low_24h\":\"99\"}]";
        case "/gate/api/v4/spot/candlesticks" -> r = "[[\"1700000060\",\"5\",\"10.5\",\"11\",\"9\",\"10\",\"1\",\"true\"],[\"1700000000\",\"5\",\"10\",\"11\",\"9\",\"10\",\"1\",\"true\"]]";
        case "/hl/info" -> r = body.contains("metaAndAssetCtxs")
            ? "[{\"universe\":[{\"name\":\"BTC\",\"szDecimals\":5},{\"name\":\"SOL\",\"szDecimals\":2}]},[{\"dayNtlVlm\":\"900000000\",\"prevDayPx\":\"60000\",\"markPx\":\"60500\",\"midPx\":\"60499.5\",\"impactPxs\":[\"60499\",\"60500\"]},{\"dayNtlVlm\":\"50000000\",\"prevDayPx\":\"150\",\"markPx\":\"151\",\"midPx\":\"151\",\"impactPxs\":[\"150.99\",\"151.01\"]}]]"
            : "[{\"t\":1700000060000,\"c\":\"60010\"},{\"t\":1700000000000,\"c\":\"60000\"}]";
        case "/dydx/v4/perpetualMarkets" -> r = "{\"markets\":{\"BTC-USD\":{\"ticker\":\"BTC-USD\",\"status\":\"ACTIVE\",\"oraclePrice\":\"60450\",\"priceChange24H\":\"450\",\"volume24H\":\"300000000\",\"trades24H\":12000},\"OLD-USD\":{\"status\":\"FINAL_SETTLEMENT\",\"oraclePrice\":\"1\",\"volume24H\":\"1\"}}}";
        case "/dydx/v4/candles/perpetualMarkets/BTC-USD" -> r = "{\"candles\":[{\"startedAt\":\"2024-01-01T00:01:00.000Z\",\"close\":\"60010\"},{\"startedAt\":\"2024-01-01T00:00:00.000Z\",\"close\":\"60000\"}]}";
        default -> { ex.sendResponseHeaders(404, -1); ex.close(); return; }
      }
      byte[] b = r.getBytes(); ex.sendResponseHeaders(200, b.length); ex.getResponseBody().write(b); ex.close();
    });
    s.start();
    String u = "http://127.0.0.1:" + s.getAddress().getPort();

    // ---- разбор сводок/свечей
    var bin = MarketSources.create("binance", u + "/bin").get();
    var tk = bin.tickers();
    ck("binance tickers parsed (non-USD quote skipped)", tk.size() == 5 && tk.stream().noneMatch(t -> t.symbol().equals("ETHBTC")));
    var aaa = tk.stream().filter(t -> t.symbol().equals("AAAUSDT")).findFirst().get();
    ck("binance fields", aaa.base().equals("AAA") && aaa.quote().equals("USDT") && Math.abs(aaa.spreadPct() - 0.02) < 1e-9 && aaa.trades24h() == 90000);
    ck("binance klines sorted closes", bin.closes1m(aaa, 500).length == 500);
    var okx = MarketSources.create("okx", u + "/okx").get();
    var ot = okx.tickers();
    ck("okx tickers", ot.size() == 2 && ot.get(0).symbol().equals("ETHUSDT") && ot.get(0).venueSymbol().equals("ETH-USDT") && ot.get(0).quoteVolume24h() == 5e7);
    double[] oc = okx.closes1m(ot.get(0), 300);
    ck("okx candles newest-first reversed", oc.length == 2 && oc[0] == 10 && oc[1] == 10.5);
    var gate = MarketSources.create("gate", u + "/gate").get();
    var gt = gate.tickers();
    ck("gate tickers", gt.size() == 1 && gt.get(0).symbol().equals("ETHUSDT") && gt.get(0).bid() == 100.3 && Math.abs(gt.get(0).open24h() - 100.4 / 1.004) < 1e-9);
    double[] gc = gate.closes1m(gt.get(0), 100);
    ck("gate candles close index 2", gc.length == 2 && gc[0] == 10 && gc[1] == 10.5);
    var hl = MarketSources.create("hyperliquid", u + "/hl").get();
    var ht = hl.tickers();
    ck("hyperliquid perps", ht.size() == 2 && ht.get(0).symbol().equals("BTCUSDC") && ht.get(0).perp() && ht.get(0).bid() == 60499 && ht.get(0).quoteVolume24h() == 9e8);
    ck("hyperliquid candles", Arrays.equals(hl.closes1m(ht.get(0), 100), new double[]{60000, 60010}));
    var dy = MarketSources.create("dydx", u + "/dydx").get();
    var dt = dy.tickers();
    ck("dydx active markets only", dt.size() == 1 && dt.get(0).symbol().equals("BTCUSD") && dt.get(0).open24h() == 60000 && Double.isNaN(dt.get(0).bid()));
    ck("dydx candles", Arrays.equals(dy.closes1m(dt.get(0), 100), new double[]{60000, 60010}));

    // ---- полный прогон: профили стратегий
    var svc = new DiscoveryService(List.of(bin, okx, gate, hl, dy, MarketSources.create("bybit", u + "/nope").get()),
        List.of(new Profiles.MeanReversion(null, com.hft.config.GlobalParams.DEFAULTS), new Profiles.CrossExchange(com.hft.config.GlobalParams.DEFAULTS), new Profiles.SpreadCapture(com.hft.config.GlobalParams.DEFAULTS)), 15);
    Map<String, Object> res = svc.runOnce();
    JsonNode j = M.valueToTree(res);
    String dump = System.getenv("DISCOVERY_DUMP");
    if (dump != null) java.nio.file.Files.writeString(java.nio.file.Path.of(dump), M.writerWithDefaultPrettyPrinter().writeValueAsString(res));
    ck("exchange status: ok and error reported", j.path("exchanges").path("binance").path("ok").asBoolean() && !j.path("exchanges").path("bybit").path("ok").asBoolean()
        && j.path("exchanges").path("bybit").path("error").asText().contains("404"));
    JsonNode mr = null, arb = null, sc = null;
    for (JsonNode st : j.path("strategies")) switch (st.path("id").asText()) { case "mean-reversion" -> mr = st; case "cross-exchange-arb" -> arb = st; case "spread-capture" -> sc = st; }
    ck("three strategies", mr != null && arb != null && sc != null && mr.path("executable").asBoolean() && !arb.path("executable").asBoolean());
    JsonNode first = null;
    for (JsonNode c : mr.path("candidates")) if (c.path("symbol").asText().equals("AAAUSDT") && c.path("exchange").asText().equals("binance")) first = c;
    ck("MR: AAAUSDT suitable via backtest", first != null && first.path("suitable").asBoolean()
        && first.path("backtest").path("trades").asInt() >= 3 && first.path("backtest").path("netPct").asDouble() > 0);
    ck("MR: suitable sorted first", mr.path("candidates").get(0).path("suitable").asBoolean());
    boolean trdOut = true, dipBad = false, thnOut = true;
    for (JsonNode c : mr.path("candidates")) {
      if (c.path("symbol").asText().equals("TRDUSDT")) trdOut = false;
      if (c.path("symbol").asText().equals("DIPUSDT")) dipBad = !c.path("suitable").asBoolean() && c.path("verdict").asText().equals("не подходит");
      if (c.path("symbol").asText().equals("THNUSDT")) thnOut = false;
    }
    ck("MR: strong trend filtered by ticker stats", trdOut);
    ck("MR: losing backtest marked 'не подходит'", dipBad);
    ck("MR: low volume / wide spread filtered", thnOut);
    ck("MR: no NaN strings in JSON", !M.writeValueAsString(res).contains("NaN"));
    JsonNode a0 = arb.path("candidates").get(0);
    ck("ARB: ETHUSDT buy binance sell okx", a0.path("symbol").asText().equals("ETHUSDT") && a0.path("exchange").asText().equals("binance→okx")
        && a0.path("suitable").asBoolean() && Math.abs(a0.path("metrics").path("netPct").asDouble() - 0.8) < 1e-6);
    boolean perpPair = false;
    for (JsonNode c : arb.path("candidates")) if (c.path("symbol").asText().startsWith("BTC")) perpPair = true;
    ck("ARB: perps not mixed with spot / no bid-ask skipped", !perpPair);
    boolean thnSc = false;
    for (JsonNode c : sc.path("candidates")) if (c.path("symbol").asText().equals("THNUSDT") && c.path("exchange").asText().equals("binance")) thnSc = c.path("suitable").asBoolean();
    ck("SPREAD: wide spread low-liquidity pair suggested", thnSc);
    ck("result has updatedAt and running=false", j.path("updatedAt").asLong() > 0 && !svc.result().get("running").equals(true));
    System.out.println("  MR: " + mr.path("suitable") + " подходят, ARB: " + arb.path("suitable") + ", SPREAD: " + sc.path("suitable"));
    s.stop(0);
    System.out.println("pass=" + pass + " fail=" + fail);
    System.exit(fail == 0 ? 0 : 1);
  }
}
