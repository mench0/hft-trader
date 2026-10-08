package com.hft.exchange.generic;

import java.util.*;

/** Внутренности горячего пути: быстрый разбор чисел, стакан на массивах, потоковый разбор диалектов. */
public class PerfInternalsCheck {
  static int pass, fail;
  static void ck(String n, boolean ok){ if(ok) pass++; else {fail++; System.out.println("FAIL "+n);} }

  public static void main(String[] a) throws Exception {
    // FastJson.parse == Double.parseDouble на случайных десятичных строках
    Random r = new Random(42);
    int bad = 0;
    String[] fixed = {"0","0.0","1","100","0.5","0.000012","123456.789","99999999.99999999","0.30000000000000004","1e-7","12345678901234567890","-3.25","+7.5"};
    List<String> cases = new ArrayList<>(Arrays.asList(fixed));
    for (int i = 0; i < 200_000; i++) {
      int ip = r.nextInt(7), fp = r.nextInt(12);
      StringBuilder sb = new StringBuilder(); sb.append(r.nextInt(ip == 0 ? 1 : (int) Math.pow(10, ip)));
      if (fp > 0) { sb.append('.'); for (int k = 0; k < fp; k++) sb.append((char) ('0' + r.nextInt(10))); }
      cases.add(sb.toString());
    }
    for (String s : cases) { char[] c = s.toCharArray(); if (FastJson.parse(c, 0, c.length) != Double.parseDouble(s)) { bad++; if (bad < 5) System.out.println("  mismatch " + s); } }
    ck("fast number parse == Double.parseDouble (" + cases.size() + " cases)", bad == 0);

    // LocalBook против эталонной TreeMap на случайных операциях
    LocalBook lb = new LocalBook(10_000);           // предел больше числа цен — эталон без обрезки
    TreeMap<Double, Double> bids = new TreeMap<>(Collections.reverseOrder()), asks = new TreeMap<>();
    boolean same = true;
    for (int i = 0; i < 100_000 && same; i++) {
      double p = 1000 + r.nextInt(400) * 0.5; double q = r.nextInt(4) == 0 ? 0 : r.nextInt(100) + 1;
      if (r.nextBoolean()) { lb.applyBid(p, q); if (q == 0) bids.remove(p); else bids.put(p, q); }
      else { lb.applyAsk(p, q); if (q == 0) asks.remove(p); else asks.put(p, q); }
      if (i % 997 == 0) {
        double[] px = new double[50], qx = new double[50];
        int n = lb.topBids(px, qx); int k = 0;
        for (var e : bids.entrySet()) { if (k == n) break; if (px[k] != e.getKey() || qx[k] != e.getValue()) same = false; k++; }
        if (n != Math.min(50, bids.size())) same = false;
        n = lb.topAsks(px, qx); k = 0;
        for (var e : asks.entrySet()) { if (k == n) break; if (px[k] != e.getKey() || qx[k] != e.getValue()) same = false; k++; }
        if (n != Math.min(50, asks.size())) same = false;
      }
    }
    ck("LocalBook == TreeMap reference (100k random ops)", same);

    // предел уровней: память не растёт, лучшие уровни те же, что у эталона
    LocalBook capped = new LocalBook(30);
    TreeMap<Double, Double> ref = new TreeMap<>(Collections.reverseOrder());
    for (int i = 0; i < 200_000; i++) {
      double p = 1000 + r.nextInt(5000) * 0.5; double q = r.nextInt(100) + 1;
      capped.applyBid(p, q); ref.put(p, q);
    }
    double[] cpx = new double[30], cqx = new double[30];
    int cn = capped.topBids(cpx, cqx); int ci = 0; boolean top = cn == 30;
    for (var e : ref.entrySet()) { if (ci == cn) break; if (cpx[ci] != e.getKey() || cqx[ci] != e.getValue()) top = false; ci++; }
    ck("LocalBook capped: size bounded and best levels kept", capped.bidLevels() <= 30 && top);

    // потоковый разбор: строки-числа, числа, объекты {price,size}, {px,sz}
    BookBatch b = new BookBatch(); b.setKnown(List.of("BTC-USDT","BTC","BTC-USD","btc_usdt"));
    String okx = "{\"arg\":{\"channel\":\"books\",\"instId\":\"BTC-USDT\"},\"action\":\"update\",\"data\":[{\"asks\":[[\"101.5\",\"2\",\"0\",\"1\"]],\"bids\":[[\"99\",\"0\",\"0\",\"0\"]],\"ts\":\"1700000000123\",\"checksum\":-5}]}";
    WsDialects.forExchange("okx").get().parse(okx.toCharArray(), okx.length(), b);
    ck("okx stream parse", "BTC-USDT".equals(b.venue) && !b.snapshot && b.an == 1 && b.ap[0] == 101.5 && b.bn == 1 && b.bq[0] == 0 && b.tsMs == 1700000000123L);
    ck("venue string reused (no alloc)", b.venue == "BTC-USDT");
    String hl = "{\"channel\":\"l2Book\",\"data\":{\"coin\":\"BTC\",\"time\":5,\"levels\":[[{\"px\":\"99\",\"sz\":\"1.5\",\"n\":2}],[{\"px\":\"100\",\"sz\":\"2\",\"n\":1}]]}}";
    WsDialects.forExchange("hyperliquid").get().parse(hl.toCharArray(), hl.length(), b);
    ck("hyperliquid objects px/sz", b.venue == "BTC" && b.bq[0] == 1.5 && b.aq[0] == 2 && b.tsMs == 5);
    boolean threw = false;
    String err = "{\"event\":\"error\",\"code\":\"60012\",\"msg\":\"Invalid request\"}";
    try { WsDialects.forExchange("okx").get().parse(err.toCharArray(), err.length(), b); } catch (IllegalStateException e) { threw = e.getMessage().contains("60012"); }
    ck("okx error surfaces", threw);

    // скорость: разбор OKX-снимка 20x20 уровней
    StringBuilder big = new StringBuilder("{\"arg\":{\"channel\":\"books\",\"instId\":\"BTC-USDT\"},\"action\":\"snapshot\",\"data\":[{\"asks\":[");
    for (int i = 0; i < 20; i++) big.append(i == 0 ? "" : ",").append("[\"").append(60000 + i * 0.1).append("\",\"0.").append(1000 + i).append("\",\"0\",\"3\"]");
    big.append("],\"bids\":[");
    for (int i = 0; i < 20; i++) big.append(i == 0 ? "" : ",").append("[\"").append(59999.9 - i * 0.1).append("\",\"0.").append(2000 + i).append("\",\"0\",\"3\"]");
    big.append("],\"ts\":\"1700000000000\"}]}");
    char[] bc = big.toString().toCharArray();
    WsDialect okxD = WsDialects.forExchange("okx").get();
    for (int i = 0; i < 50_000; i++) okxD.parse(bc, bc.length, b);
    long t0 = System.nanoTime(); int N = 200_000;
    for (int i = 0; i < N; i++) okxD.parse(bc, bc.length, b);
    double us = (System.nanoTime() - t0) / 1e3 / N;
    com.fasterxml.jackson.databind.ObjectMapper om = new com.fasterxml.jackson.databind.ObjectMapper(); String bs = big.toString();
    for (int i = 0; i < 50_000; i++) om.readTree(bs);
    t0 = System.nanoTime();
    for (int i = 0; i < N; i++) { var n = om.readTree(bs); for (var row : n.path("data").get(0).path("asks")) Double.parseDouble(row.get(0).asText()); }
    double usTree = (System.nanoTime() - t0) / 1e3 / N;
    System.out.printf("  OKX 20x20: потоковый разбор %.2f мкс/сообщение, дерево Jackson %.2f мкс%n", us, usTree);
    ck("streaming parse faster than tree", us < usTree);

    System.out.println("pass=" + pass + " fail=" + fail);
    System.exit(fail == 0 ? 0 : 1);
  }
}
