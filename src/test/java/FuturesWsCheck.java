import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hft.config.*;
import com.hft.exchange.Exchange;
import com.hft.exchange.gate.GateFuturesClient;
import com.hft.exchange.generic.ContractSizes;
import com.hft.exchange.kucoin.KucoinFuturesClient;
import com.hft.exchange.mexc.MexcFuturesClient;
import com.hft.model.OrderEnums.Side;
import com.hft.model.OrderResult;
import com.hft.store.*;
import com.hft.util.Hmac;
import com.sun.net.httpserver.HttpServer;

import java.net.InetSocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;

/**
 * Фьючерсы по WebSocket: ордера и отмены (Gate — WS API, KuCoin — Pro WS API), результаты ордеров, позиции и баланс
 * из приватных потоков (Gate, KuCoin, MEXC). REST при готовом сокете не используется. Запуск: KUCOIN_PASSPHRASE=pp
 */
public class FuturesWsCheck {
  static int pass, fail;
  static void ck(String n, boolean ok){ if(ok) pass++; else {fail++; System.out.println("FAIL "+n);} }
  static boolean await(BooleanSupplier c, long ms) throws Exception { long t=System.currentTimeMillis()+ms; while(System.currentTimeMillis()<t){ if(c.getAsBoolean()) return true; Thread.sleep(20);} return c.getAsBoolean(); }
  static boolean near(double a,double b){ return Math.abs(a-b)<1e-9*Math.max(1,Math.abs(b)); }
  static ObjectMapper m = new ObjectMapper();
  static JsonNode j(String s){ try { return m.readTree(s); } catch(Exception e){ throw new RuntimeException(e); } }
  static final TradingParams PERP = TradingParams.DEFAULTS.with(Map.of("market", "perp", "marketFillWaitMs", "1500"));
  static ExchangeConfig cfg(String id, String url){ return new ExchangeConfig(id, false, url, "", 5000, List.of("BTCUSDT"), 20, 100, PERP); }
  static SymbolFilters filt(double step){ var f=new SymbolFilters(); f.put("BTCUSDT", new SymbolFilters.Filter(step,1e9,step,0,1e9,0.1,1)); return f; }
  static Credentials cr = new Credentials("KEY","SECRET");

  static Map<String,AtomicInteger> hits = new ConcurrentHashMap<>();
  static Map<String,String> routes = new ConcurrentHashMap<>();
  static HttpServer http() throws Exception {
    HttpServer s = HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
    s.createContext("/", ex -> {
      ex.getRequestBody().readAllBytes();
      String key = ex.getRequestMethod()+" "+ex.getRequestURI().getPath();
      hits.computeIfAbsent(key, k->new AtomicInteger()).incrementAndGet();
      byte[] b = routes.getOrDefault(key, "{}").getBytes(); ex.sendResponseHeaders(200, b.length); ex.getResponseBody().write(b); ex.close();
    });
    s.start(); return s;
  }
  static int h(String k){ var a=hits.get(k); return a==null?0:a.get(); }

  public static void main(String[] a) throws Exception {
    var srv = http(); String rest = "http://127.0.0.1:"+srv.getAddress().getPort();
    ContractSizes.put(Exchange.GATE, "BTC_USDT", 0.0001);
    ContractSizes.put(Exchange.KUCOIN, "XBTUSDTM", 0.001);
    ContractSizes.put(Exchange.MEXC, "BTC_USDT", 0.0001);
    gate(rest); kucoin(rest); mexc(rest);
    srv.stop(0);
    System.out.println("FuturesWsCheck: pass=" + pass + " fail=" + fail);
    System.exit(fail > 0 ? 1 : 0);
  }

  // ───────────────────────────── Gate USDT-фьючерсы
  static void gate(String rest) throws Exception {
    routes.put("GET /api/v4/futures/usdt/accounts", "{\"user\":777,\"total\":\"1000\",\"available\":\"800\",\"currency\":\"USDT\"}");
    try (var ws = new MiniWsServer()) {
      ws.onText = (c, t) -> {
        JsonNode n = j(t); String ch = n.path("channel").asText(), ev = n.path("event").asText();
        if (ch.equals("futures.ping")) { c.text("{\"time\":1,\"channel\":\"futures.pong\",\"event\":\"\",\"result\":null}"); return; }
        String rid = n.path("payload").path("req_id").asText();
        if (ch.equals("futures.login")) { c.text("{\"request_id\":\"login\",\"header\":{\"status\":\"200\",\"channel\":\"futures.login\",\"event\":\"api\"},\"data\":{\"result\":{\"uid\":\"777\"}}}"); return; }
        if (ev.equals("subscribe")) {
          c.text("{\"time\":1,\"channel\":\""+ch+"\",\"event\":\"subscribe\",\"result\":{\"status\":\"success\"}}");
          if (ch.equals("futures.balances")) c.text("{\"time\":2,\"channel\":\"futures.balances\",\"event\":\"update\",\"result\":[{\"balance\":950,\"currency\":\"usdt\"}]}");
          return;
        }
        String hdr = "\"header\":{\"status\":\"200\",\"channel\":\""+ch+"\",\"event\":\"api\"}";
        switch (ch) {
          case "futures.order_place" -> {
            String text = n.path("payload").path("req_param").path("text").asText();
            c.text("{\"request_id\":\""+rid+"\",\"ack\":true,"+hdr+",\"data\":{\"result\":{\"req_id\":\""+rid+"\"}}}");
            c.text("{\"request_id\":\""+rid+"\","+hdr+",\"data\":{\"result\":{\"id\":55,\"contract\":\"BTC_USDT\",\"size\":-100,\"left\":-100,\"status\":\"open\",\"text\":\""+text+"\"}}}");
            c.text("{\"time\":3,\"channel\":\"futures.orders\",\"event\":\"update\",\"result\":[{\"id\":55,\"contract\":\"BTC_USDT\",\"size\":-100,\"left\":0,\"fill_price\":\"60000\",\"status\":\"finished\",\"finish_as\":\"filled\",\"text\":\""+text+"\"}]}");
            c.text("{\"time\":3,\"channel\":\"futures.positions\",\"event\":\"update\",\"result\":[{\"contract\":\"BTC_USDT\",\"size\":-100,\"entry_price\":\"60000\"}]}");
          }
          case "futures.order_cancel" -> c.text("{\"request_id\":\""+rid+"\","+hdr+",\"data\":{\"result\":{\"id\":55,\"status\":\"finished\"}}}");
          case "futures.order_cancel_cp" -> c.text("{\"request_id\":\""+rid+"\","+hdr+",\"data\":{\"result\":[{\"id\":1},{\"id\":2},{\"id\":3}]}}");
          default -> {}
        }
      };
      var store = new BalanceStore(); store.set("USDT", 800, 200);
      var positions = new PositionStore();
      var c = new GateFuturesClient(cfg("gate", rest), cr, filt(0.0001));
      c.setPrivateWsUrl(ws.url());
      c.loadPositions(positions);
      c.startStreams(store); c.awaitStreams(5000);
      ck("gate ws ready", c.wsReady());
      String login = ws.received.stream().filter(t -> t.contains("futures.login")).findFirst().orElse("{}");
      String ts = j(login).path("payload").path("timestamp").asText();
      ck("gate futures login sign", j(login).path("payload").path("signature").asText().equals(Hmac.sha512Hex("SECRET", "api\nfutures.login\n\n"+ts)));
      ck("gate subscriptions with user id", await(() -> ws.received.stream().anyMatch(t -> t.contains("\"futures.orders\"") && t.contains("\"777\"") && t.contains("!all"))
          && ws.received.stream().anyMatch(t -> t.contains("\"futures.positions\"")) && ws.received.stream().anyMatch(t -> t.contains("\"futures.balances\"")), 2000));
      ck("gate balance stream", await(() -> near(store.free("USDT"), 750) && c.balancesStreamed(), 3000));

      OrderResult r = c.sellMarket("BTCUSDT", 0.01);
      ck("gate order via WS API", ws.received.stream().anyMatch(t -> t.contains("futures.order_place") && t.contains("\"size\":-100") && t.contains("\"tif\":\"ioc\"")));
      ck("gate no REST order", h("POST /api/v4/futures/usdt/orders") == 0);
      ck("gate fill from stream", r.isFilled() && near(r.executedQty(), 0.01) && near(r.avgPrice(), 60000) && h("GET /api/v4/futures/usdt/orders/55") == 0);
      ck("gate position from stream", await(() -> near(positions.qty("BTCUSDT"), -0.01), 2000));
      c.cancelOrder("BTCUSDT", 55);
      ck("gate cancel via WS", ws.received.stream().anyMatch(t -> t.contains("futures.order_cancel\"") && t.contains("\"order_id\":\"55\"")) && h("DELETE /api/v4/futures/usdt/orders/55") == 0);
      ck("gate cancelAll via WS", c.cancelAll("BTCUSDT") == 3 && h("DELETE /api/v4/futures/usdt/orders") == 0);
      c.stopStreams();
    }
  }

  // ───────────────────────────── KuCoin Futures
  static void kucoin(String rest) throws Exception {
    try (var trade = new MiniWsServer(); var priv = new MiniWsServer()) {
      trade.onOpen = c -> c.text("{\"sessionId\":\"s1\",\"data\":\"welcome\",\"timestamp\":1}");
      trade.onText = (c, t) -> {
        if (!t.startsWith("{")) { c.text("{\"sessionId\":\"s1\",\"data\":\"ok\"}"); return; }   // подпись приветствия
        JsonNode n = j(t); String op = n.path("op").asText(), id = n.path("id").asText();
        if (op.equals("ping")) { c.text("{\"id\":\""+id+"\",\"op\":\"pong\"}"); return; }
        if (op.equals("futures.order")) {
          c.text("{\"id\":\""+id+"\",\"op\":\"futures.order\",\"code\":\"200000\",\"data\":{\"orderId\":\"kfo1\",\"clientOid\":\""+n.path("args").path("clientOid").asText()+"\"}}");
          priv.conns.forEach(p -> {
            p.text("{\"type\":\"message\",\"topic\":\"/contractMarket/tradeOrders\",\"subject\":\"orderChange\",\"data\":{\"orderId\":\"kfo1\",\"symbol\":\"XBTUSDTM\",\"type\":\"match\",\"status\":\"open\",\"side\":\"buy\",\"size\":\"20\",\"filledSize\":\"20\",\"matchSize\":\"20\",\"matchPrice\":\"60000\"}}");
            p.text("{\"type\":\"message\",\"topic\":\"/contractMarket/tradeOrders\",\"subject\":\"orderChange\",\"data\":{\"orderId\":\"kfo1\",\"symbol\":\"XBTUSDTM\",\"type\":\"filled\",\"status\":\"done\",\"side\":\"buy\",\"size\":\"20\",\"filledSize\":\"20\"}}");
            p.text("{\"type\":\"message\",\"topic\":\"/contract/positionAll\",\"subject\":\"position.change\",\"data\":{\"symbol\":\"XBTUSDTM\",\"currentQty\":20,\"avgEntryPrice\":60000}}");
          });
          return;
        }
        if (op.equals("futures.cancel")) c.text("{\"id\":\""+id+"\",\"op\":\"futures.cancel\",\"code\":\"200000\",\"data\":{\"orderId\":\"kfo1\"}}");
      };
      priv.onOpen = c -> c.text("{\"id\":\"w\",\"type\":\"welcome\"}");
      priv.onText = (c, t) -> {
        JsonNode n = j(t);
        if (n.path("type").asText().equals("subscribe")) {
          c.text("{\"id\":\""+n.path("id").asText()+"\",\"type\":\"ack\"}");
          if (n.path("topic").asText().equals("/contractAccount/wallet"))
            c.text("{\"type\":\"message\",\"topic\":\"/contractAccount/wallet\",\"subject\":\"availableBalance.change\",\"data\":{\"currency\":\"USDT\",\"availableBalance\":900,\"holdBalance\":100}}");
        }
      };
      var store = new BalanceStore();
      var positions = new PositionStore();
      var c = new KucoinFuturesClient(cfg("kucoin", rest), cr, filt(0.001));
      c.setWsUrls(trade.url(), priv.url());
      c.loadPositions(positions);
      c.startStreams(store); c.awaitStreams(5000);
      ck("kucoin trade ws ready", c.wsReady());
      ck("kucoin futures subscriptions", await(() -> priv.received.stream().anyMatch(t -> t.contains("/contractMarket/tradeOrders"))
          && priv.received.stream().anyMatch(t -> t.contains("/contract/positionAll")), 2000));
      ck("kucoin wallet stream", await(() -> near(store.free("USDT"), 900) && c.balancesStreamed(), 3000));
      OrderResult r = c.buyMarket("BTCUSDT", 0.02);
      ck("kucoin futures.order via WS", trade.received.stream().anyMatch(t -> t.contains("futures.order") && t.contains("XBTUSDTM") && t.contains("\"size\":20")));
      ck("kucoin no REST order", h("POST /api/v1/orders") == 0);
      ck("kucoin fill from stream", r.isFilled() && near(r.executedQty(), 0.02) && near(r.avgPrice(), 60000) && h("GET /api/v1/orders/kfo1") == 0);
      ck("kucoin position from stream", await(() -> near(positions.qty("BTCUSDT"), 0.02), 2000));
      c.cancelOrder("BTCUSDT", r.orderId());
      ck("kucoin futures.cancel via WS", trade.received.stream().anyMatch(t -> t.contains("futures.cancel")) && h("DELETE /api/v1/orders/kfo1") == 0);
      c.stopStreams();
    }
  }

  // ───────────────────────────── MEXC Contract: ордер по REST, результат по WS
  static void mexc(String rest) throws Exception {
    routes.put("POST /api/v1/private/order/submit", "{\"success\":true,\"code\":0,\"data\":\"mo9\"}");
    try (var ws = new MiniWsServer()) {
      ws.onText = (c, t) -> {
        JsonNode n = j(t);
        String method = n.path("method").asText();
        if (method.equals("login")) {
          c.text("{\"channel\":\"rs.login\",\"data\":\"success\",\"ts\":1}");
          c.text("{\"channel\":\"push.personal.asset\",\"data\":{\"currency\":\"USDT\",\"availableBalance\":500,\"frozenBalance\":10,\"positionMargin\":90}}");
        } else if (method.equals("ping")) c.text("{\"channel\":\"pong\",\"data\":1}");
      };
      var store = new BalanceStore();
      var positions = new PositionStore();
      var c = new MexcFuturesClient(cfg("mexc", rest), cr, filt(0.0001));
      c.setPrivateWsUrl(ws.url());
      c.loadPositions(positions);
      c.startStreams(store); c.awaitStreams(5000);
      ck("mexc ws ready (login)", c.wsReady());
      String login = ws.received.stream().filter(t -> t.contains("\"login\"")).findFirst().orElse("{}");
      String ts = j(login).path("param").path("reqTime").asText();
      ck("mexc login sign", j(login).path("param").path("signature").asText().equals(Hmac.sha256Hex("SECRET", "KEY" + ts)));
      ck("mexc asset stream", await(() -> near(store.free("USDT"), 500) && near(store.locked("USDT"), 100) && c.balancesStreamed(), 3000));
      new Thread(() -> {                                                // исполнение приходит из потока, а не из REST
        try { Thread.sleep(150); } catch (InterruptedException ignored) {}
        ws.conns.forEach(p -> {
          p.text("{\"channel\":\"push.personal.order\",\"data\":{\"orderId\":\"mo9\",\"symbol\":\"BTC_USDT\",\"state\":3,\"vol\":100,\"dealVol\":100,\"dealAvgPrice\":60000,\"side\":3,\"externalOid\":\"x\"}}");
          p.text("{\"channel\":\"push.personal.position\",\"data\":{\"symbol\":\"BTC_USDT\",\"positionType\":2,\"holdVol\":100,\"holdAvgPrice\":60000,\"state\":1}}");
        });
      }).start();
      OrderResult r = c.sellMarket("BTCUSDT", 0.01);
      ck("mexc order via REST (no WS orders at MEXC)", h("POST /api/v1/private/order/submit") == 1);
      ck("mexc fill from stream, no status request", r.isFilled() && near(r.executedQty(), 0.01) && h("GET /api/v1/private/order/get/mo9") == 0);
      ck("mexc short position from stream", await(() -> near(positions.qty("BTCUSDT"), -0.01), 2000));
      c.stopStreams();
    }
  }
}
