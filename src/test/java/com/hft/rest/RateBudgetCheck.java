package com.hft.rest;

import com.hft.rest.RateBudget.Kind;
import com.hft.rest.RateBudget.Limit;
import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.*;
import java.util.*;

/** Общий бюджет запросов: темп, вес, Retry-After, нарастание пауз, заголовки бирж, общий счёт для всех компонентов. */
public class RateBudgetCheck {
  static int pass, fail;
  static void ck(String n, boolean ok){ if(ok) pass++; else {fail++; System.out.println("FAIL "+n);} }
  static HttpHeaders h(String... kv){ Map<String,List<String>> m = new HashMap<>(); for (int i=0;i<kv.length;i+=2) m.put(kv[i], List.of(kv[i+1])); return HttpHeaders.of(m,(a,b)->true); }
  static boolean local(Runnable r){ try { r.run(); return false; } catch (LocalThrottleException e) { return true; } catch (ApiException e) { return false; } }

  public static void main(String[] a) throws Exception {
    // ---- темп: 10/с официально -> 8 жетонов; 16 запросов не быстрее ~1 с
    var b = new RateBudget("t1", List.of(Limit.of("rest", 10, 1000, Kind.PUBLIC, Kind.PRIVATE, Kind.ORDER)));
    long t0 = System.currentTimeMillis();
    for (int i = 0; i < 16; i++) b.acquire(Kind.PUBLIC, 1, 5000);
    long dt = System.currentTimeMillis() - t0;
    ck("paced to 80% of official limit (" + dt + " ms)", dt >= 900 && dt < 2500);

    // ---- вес: запрос весом 4 тратит 4 жетона; ордер и публичный делят одно ведро
    var w = new RateBudget("t2", List.of(Limit.of("weight", 10, 60_000, Kind.PUBLIC, Kind.ORDER), Limit.of("orders", 5, 60_000, Kind.ORDER)));
    ck("weight consumes tokens", w.tryAcquire(Kind.PUBLIC, 4) && w.tryAcquire(Kind.PUBLIC, 4) && !w.tryAcquire(Kind.ORDER, 1));
    var w2 = new RateBudget("t3", List.of(Limit.of("weight", 100, 60_000, Kind.PUBLIC, Kind.ORDER), Limit.of("orders", 5, 60_000, Kind.ORDER)));
    int orders = 0; while (w2.tryAcquire(Kind.ORDER, 1)) orders++;
    ck("order bucket limits orders only (4 of 5 official)", orders == 4 && w2.tryAcquire(Kind.PUBLIC, 1));
    ck("wait longer than maxWait -> LOCAL reject, nothing reserved", local(() -> { try { w2.acquire(Kind.ORDER, 1, 10); } catch (InterruptedException e) {} }) && w2.localRejects() == 1);

    // ---- 429 с Retry-After: пауза из заголовка; повтор — вдвое дольше; блок для всех видов
    var r = new RateBudget("t4", List.of(Limit.of("rest", 100, 1000)));
    long p1 = r.onResponse(429, h("Retry-After", "3"));
    ck("Retry-After honoured", p1 == 3000 && r.blockedForMs() > 2500);
    ck("blocked for every kind", local(() -> { try { r.acquire(Kind.ORDER, 1, 100); } catch (InterruptedException e) {} }));
    long p2 = r.onResponse(429, h());
    ck("repeated limit hit doubles the pause", p2 == 20_000);
    ck("418 (IP ban) pauses at least 2 min", new RateBudget("t5", List.of()).onResponse(418, h()) == 120_000);
    ck("200 is not a limit hit", new RateBudget("t6", List.of()).onResponse(200, h()) == 0);
    ck("limit error code in body pauses", new RateBudget("t7", List.of()).onLimitError("50011") == 10_000);

    // ---- заголовки бирж
    var bn = new RateBudget("t8", List.of(Limit.of("weight", 6000, 60_000, Kind.PUBLIC)));
    bn.onResponse(200, h("X-MBX-USED-WEIGHT-1M", "5900"));
    ck("Binance used weight syncs our count", !bn.tryAcquire(Kind.PUBLIC, 1));
    var by = new RateBudget("t9", List.of(Limit.of("ip", 600, 5000)));
    by.onResponse(200, h("X-Bapi-Limit-Status", "1", "X-Bapi-Limit-Reset-Timestamp", String.valueOf(System.currentTimeMillis() + 2000)));
    ck("Bybit remaining=1 blocks until reset", by.blockedForMs() > 1000 && by.blockedForMs() <= 2000);
    var ku = new RateBudget("t10", List.of(Limit.of("public", 2000, 30_000)));
    ku.onResponse(200, h("gw-ratelimit-remaining", "0", "gw-ratelimit-reset", "1500"));
    ck("KuCoin remaining=0 blocks for reset ms", ku.blockedForMs() > 1000 && ku.blockedForMs() <= 1500);
    var ok = new RateBudget("t11", List.of(Limit.of("public", 2000, 30_000)));
    ok.onResponse(200, h("gw-ratelimit-remaining", "500", "gw-ratelimit-reset", "1500"));
    ck("plenty remaining -> no block", ok.blockedForMs() == 0);

    // ---- один бюджет на биржу на процесс
    ck("RateBudget.of is shared per exchange", RateBudget.of("binance") == RateBudget.of("binance") && RateBudget.of("binance") != RateBudget.of("okx"));
    ck("every live exchange has a WS connect limit", List.of("binance","bybit","okx","gate","mexc","hyperliquid","kucoin","aster","uniswapv2")
        .stream().allMatch(id -> RateLimits.forExchange(id).stream().anyMatch(l -> l.kinds().contains(Kind.WS_CONNECT))));
    ck("Binance weights from docs", RateLimits.weight("binance","GET","/api/v3/account","") == 20 && RateLimits.weight("binance","GET","/api/v3/ticker/24hr","") == 80
        && RateLimits.weight("binance","GET","/api/v3/depth","symbol=X&limit=500") == 25 && RateLimits.weight("binance","POST","/api/v3/order","") == 1);

    // ---- клиент биржи: 429 от сервера ставит на паузу и торговые, и публичные запросы этой биржи
    HttpServer srv = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    srv.createContext("/", ex -> { ex.getResponseHeaders().add("Retry-After", "2"); ex.sendResponseHeaders(429, -1); ex.close(); });
    srv.start();
    var mexc = RateBudget.of("mexc");
    var c = new com.hft.exchange.mexc.MexcRestClient(new com.hft.config.ExchangeConfig("mexc", false, "http://127.0.0.1:" + srv.getAddress().getPort(), "", 5000, List.of("BTCUSDT"), 20, 100),
        new com.hft.config.Credentials("K", "S"), new com.hft.store.SymbolFilters());
    try { c.loadBalances(new com.hft.store.BalanceStore()); } catch (Exception e) {}
    ck("client 429 blocks the shared exchange budget", mexc.blockedForMs() > 1000 && mexc.rateLimitedResponses() == 1);
    ck("public requests of the same exchange wait too", local(() -> { try { mexc.acquire(Kind.PUBLIC, 1, 100); } catch (InterruptedException e) {} }));
    srv.stop(0);

    // ---- свой лимит — своя ошибка (не HTTP 429), ордер отклонён, kill switch; настоящий 429 биржи — тоже kill switch
    var settings = new com.hft.config.TradingSettings(com.hft.config.TradingParams.DEFAULTS.with(Map.of("tradingEnabled", "true", "maxSlippagePercent", "100")));
    var m = new com.hft.store.MarketDataStore(5, 10); m.register("BTCUSDT");
    long now = System.currentTimeMillis();
    m.book("BTCUSDT").applySnapshot(new double[]{99}, new double[]{10}, 1, new double[]{100}, new double[]{10}, 1, now, now);
    var bal = new com.hft.store.BalanceStore(); bal.set("USDT", 1000, 0);
    var f = new com.hft.store.SymbolFilters(); f.put("BTCUSDT", new com.hft.store.SymbolFilters.Filter(0, 1e12, 1e-6, 0, 1e12, 0.01, 0));
    RuntimeException[] toThrow = {new LocalThrottleException(LocalThrottleException.Reason.QUEUE_FULL, 1500, "очередь запросов переполнена")};
    var paper = new com.hft.paper.PaperOrderApi(m, bal, 0, 0);
    ExchangeOrderApi api = (ExchangeOrderApi) java.lang.reflect.Proxy.newProxyInstance(ExchangeOrderApi.class.getClassLoader(), new Class<?>[]{ExchangeOrderApi.class},
        (proxy, method, args) -> { if (method.getName().equals("buyMarket")) throw toThrow[0]; return method.invoke(paper, args); });
    var risk = new com.hft.risk.RiskManager(settings, m, "t");
    var os = new com.hft.engine.OrderService(api, m, bal, f, risk, settings);
    var res = os.buyMarket("BTCUSDT", 0.1);
    ck("local throttle: own error, order rejected", res.executedQty() == 0 && "REJECTED_LOCAL".equals(res.status()));
    ck("local throttle: trading stopped", risk.isStopped());
    ck("local throttle is not RateLimited/429", !(toThrow[0] instanceof RateLimited) && !(toThrow[0] instanceof ApiException));
    risk.resumeTrading();
    toThrow[0] = new ApiException(429, "-1003", "Too many requests", true);
    os.buyMarket("BTCUSDT", 0.1);
    ck("exchange 429: trading stopped", risk.isStopped());

    System.out.println("pass="+pass+" fail="+fail); System.exit(fail==0?0:1);
  }
}
