import com.hft.exchange.generic.*;
import com.hft.exchange.catalog.*;
import com.hft.paper.*;
import com.hft.store.*;
import com.hft.model.OrderEnums.*;
import com.hft.model.OrderResult;
import java.net.http.*;
import java.util.*;
import com.sun.net.httpserver.*;
import java.net.InetSocketAddress;

public class PaperExchangeCheck {
  static int pass, fail;
  static void ck(String n, boolean ok){ if(ok) pass++; else {fail++; System.out.println("FAIL "+n);} }
  static boolean near(double a,double b){ return Math.abs(a-b)<1e-9*Math.max(1,Math.abs(b)); }

  public static void main(String[] a) throws Exception {
    // --- диалекты на образцах
    var okx = Dialects.forExchange("okx").parse("{\"code\":\"0\",\"data\":[{\"asks\":[[\"101\",\"2\",\"0\",\"1\"],[\"100.5\",\"1\",\"0\",\"1\"]],\"bids\":[[\"99\",\"3\",\"0\",\"1\"],[\"99.5\",\"1\",\"0\",\"1\"]],\"ts\":\"1700000000000\"}]}","BTCUSDT");
    ck("okx sorted asks", okx.ap()[0]==100.5 && okx.ap()[1]==101);
    ck("okx sorted bids", okx.bp()[0]==99.5 && okx.bp()[1]==99);
    ck("okx ts", okx.tsMs()==1700000000000L);
    var mexc = Dialects.forExchange("mexc").parse("{\"lastUpdateId\":1,\"bids\":[[\"10\",\"1\"]],\"asks\":[[\"11\",\"2\"]]}","ETHUSDT");
    ck("mexc", mexc.bp()[0]==10 && mexc.aq()[0]==2);
    var gate = Dialects.forExchange("gate").parse("{\"current\":123,\"asks\":[[\"5\",\"1\"]],\"bids\":[[\"4\",\"1\"]]}","ETHUSDT");
    ck("gate", gate.tsMs()==123 && gate.ap()[0]==5);
    var bx = Dialects.forExchange("bingx").parse("{\"code\":0,\"data\":{\"bids\":[[\"4\",\"1\"],[\"4.1\",\"1\"]],\"asks\":[[\"6\",\"1\"],[\"5\",\"1\"]],\"ts\":9}}","ETHUSDT");
    ck("bingx sort", bx.bp()[0]==4.1 && bx.ap()[0]==5);
    var hl = Dialects.forExchange("hyperliquid").parse("{\"coin\":\"BTC\",\"time\":5,\"levels\":[[{\"px\":\"99\",\"sz\":\"1\",\"n\":1}],[{\"px\":\"100\",\"sz\":\"2\",\"n\":1}]]}","BTCUSDC");
    ck("hl", hl.bp()[0]==99 && hl.aq()[0]==2);
    var dy = Dialects.forExchange("dydx").parse("{\"bids\":[{\"price\":\"99\",\"size\":\"1\"}],\"asks\":[{\"price\":\"100\",\"size\":\"1\"}]}","BTCUSD");
    ck("dydx", dy.ap()[0]==100);
    boolean threw=false; try{ Dialects.forExchange("okx").parse("{\"code\":\"51001\"}","BTCUSDT"); }catch(Exception e){threw=true;}
    ck("okx error throws", threw);
    ck("USD split", BalanceStore.baseAsset("BTCUSD").equals("BTC") && BalanceStore.quoteAsset("BTCUSDT").equals("USDT"));
    // request URLs
    ck("okx url", Dialects.forExchange("okx").request("https://www.okx.com","BTCUSDT",20).uri().toString().contains("instId=BTC-USDT"));
    ck("gate url", Dialects.forExchange("gate").request("https://x","BTCUSDT",20).uri().toString().contains("BTC_USDT"));
    ck("hl post", Dialects.forExchange("hyperliquid").request("https://x","BTCUSDC",20).method().equals("POST"));

    // --- Uniswap V2 (формула)
    System.setProperty("x","y");
    // резервы: 1000 WETH, 2_000_000 USDC -> цена 2000
    String r0 = String.format("%064x", new java.math.BigInteger("1000000000000000000000")); // 1000e18
    String r1 = String.format("%064x", new java.math.BigInteger("2000000000000")); // 2e6 * 1e6
    // пул берётся из env, проверяем через рефлексию-обход: создаём диалект с env нельзя, пропускаем если нет
    // --- бумажный движок
    var market = new MarketDataStore(20, 100);
    market.register("BTCUSDT");
    var bal = new BalanceStore();
    var api = new PaperOrderApi(market, bal, 0.0, 0.1);
    market.book("BTCUSDT").applySnapshot(new double[]{99,98}, new double[]{1,5}, 2, new double[]{100,101}, new double[]{1,5}, 2, 1, 1);
    OrderResult m = api.buyMarket("BTCUSDT", 2);
    ck("market buy filled", m.isFilled());
    ck("market buy avg incl fee", near(m.avgPrice(), (100*1+101*1)/2.0*1.001));
    OrderResult big = api.buyMarket("BTCUSDT", 100);
    ck("market partial", big.isPartial() && near(big.executedQty(), 6));
    OrderResult ioc = api.buyLimit("BTCUSDT", 3, 100, TimeInForce.IOC);
    ck("ioc partial at limit", ioc.isPartial() && near(ioc.executedQty(), 1));
    OrderResult iocNo = api.buyLimit("BTCUSDT", 1, 99.5, TimeInForce.IOC);
    ck("ioc no cross expired", iocNo.isRejected());
    OrderResult fok = api.buyLimit("BTCUSDT", 2, 100, TimeInForce.FOK);
    ck("fok insufficient", fok.isRejected() && fok.executedQty()==0);
    OrderResult q = api.buyMarketForQuote("BTCUSDT", 150);
    ck("quote buy", near(q.executedQty(), 1 + 50.0/101));
    OrderResult sell = api.sellMarket("BTCUSDT", 1);
    ck("sell avg fee", near(sell.avgPrice(), 99*0.999));
    // GTC resting + settle
    bal.set("USDT", 1000, 0); bal.set("BTC", 0, 0);
    OrderResult g = api.buyLimit("BTCUSDT", 1, 99.5, TimeInForce.GTC);
    ck("gtc new", "NEW".equals(g.status()) && api.openOrders()==1);
    api.settle("BTCUSDT"); ck("not crossed yet", api.openOrders()==1);
    market.book("BTCUSDT").applySnapshot(new double[]{98}, new double[]{1}, 1, new double[]{99.4}, new double[]{1}, 1, 2, 2);
    api.settle("BTCUSDT");
    ck("settled", api.openOrders()==0 && near(bal.free("BTC"), 1.0) && near(bal.free("USDT"), 1000-99.5));
    OrderResult g2 = api.sellLimit("BTCUSDT", 1, 200, TimeInForce.GTC);
    ck("cancel", api.cancelAll("BTCUSDT")==1 && api.openOrders()==0);

    // --- опрос на фейковом сервере (okx-диалект)
    HttpServer srv = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    int[] hits = {0}; int[] mode = {200};
    srv.createContext("/", ex -> {
      hits[0]++;
      byte[] b = "{\"code\":\"0\",\"data\":[{\"asks\":[[\"100\",\"1\"]],\"bids\":[[\"99\",\"1\"]],\"ts\":\"1\"}]}".getBytes();
      ex.sendResponseHeaders(mode[0], mode[0]==200 ? b.length : -1);
      if (mode[0]==200) ex.getResponseBody().write(b);
      ex.close();
    });
    srv.start();
    String base = "http://127.0.0.1:" + srv.getAddress().getPort();
    var info = new ExchangeInfo("okx","OKX",ExchangeInfo.Kind.CEX_TIER1,ExchangeInfo.Adapter.PAPER_BLIND,base,10,0.08,0.1,"USDT","BTCUSDT","");
    var cfg = new com.hft.config.ExchangeConfig("okx", false, base, "", 5000, List.of("BTCUSDT","ETHUSDT"), 20, 100);
    var mk = new MarketDataStore(20,100); mk.register("BTCUSDT"); mk.register("ETHUSDT");
    int[] ticks={0}, books={0};
    var feed = new PollingBookFeed(info, cfg, Dialects.forExchange("okx"), mk, (sy_,px_,q_,bm_,ts_,rn_)->ticks[0]++, s->books[0]++, ()->{});
    feed.start(); Thread.sleep(1500);
    ck("feed connected", feed.isConnected());
    ck("book filled", mk.book("BTCUSDT").isReady() && mk.book("ETHUSDT").isReady());
    ck("rate <= 10/s", hits[0] <= 17);   // 1.5 c * 10 rps + запас
    ck("ticks", ticks[0] > 4 && books[0] == ticks[0]);
    mode[0]=429; int before = hits[0]; Thread.sleep(2500);
    ck("pause on 429", hits[0] - before <= 2 && (Long)feed.stats().get("rateLimited") >= 1);
    feed.stop(); srv.stop(0);
    System.out.println("passed="+pass+" failed="+fail+" hits="+hits[0]+" stats="+feed.stats());
  }
}
