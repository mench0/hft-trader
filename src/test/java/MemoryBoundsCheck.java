import com.hft.config.*;
import com.hft.exchange.lbank.LbankRestClient;
import com.hft.rest.SignedCexClient;
import com.hft.store.SymbolFilters;
import com.hft.util.BoundedMap;
import java.lang.reflect.*;
import java.util.*;

/** Долгоживущие карты не растут без предела, и вытеснение не ломает соответствие id. */
public class MemoryBoundsCheck {
  static int pass, fail;
  static void ck(String n, boolean ok){ if(ok) pass++; else {fail++; System.out.println("FAIL "+n);} }

  public static void main(String[] a) throws Exception {
    Map<Integer, Integer> m = BoundedMap.create(100);
    for (int i = 0; i < 10_000; i++) m.put(i, i);
    ck("BoundedMap keeps last N", m.size() == 100 && m.containsKey(9_999) && !m.containsKey(9_899));

    var c = new LbankRestClient(new ExchangeConfig("lbank", false, "http://127.0.0.1:9", "", 5000, List.of("BTCUSDT"), 20, 100),
        new Credentials("KEY", "SECRET"), new SymbolFilters());
    Method reg = SignedCexClient.class.getDeclaredMethod("registerId", String.class); reg.setAccessible(true);
    Method ven = SignedCexClient.class.getDeclaredMethod("venueId", long.class); ven.setAccessible(true);
    Field fl = SignedCexClient.class.getDeclaredField("toLong"); fl.setAccessible(true);
    Field fv = SignedCexClient.class.getDeclaredField("toVenue"); fv.setAccessible(true);
    Field fmax = SignedCexClient.class.getDeclaredField("MAX_TRACKED_ORDERS"); fmax.setAccessible(true);
    int max = fmax.getInt(null);
    long first = (long) reg.invoke(c, "uuid-0");
    long last = 0;
    for (int i = 1; i < max * 3; i++) last = (long) reg.invoke(c, "uuid-" + i);
    ck("string ids are stable", (long) reg.invoke(c, "uuid-" + (max * 3 - 1)) == last);
    ck("recent id round trip", ven.invoke(c, last).equals("uuid-" + (max * 3 - 1)));
    ck("id maps bounded and consistent", ((Map<?, ?>) fl.get(c)).size() == max && ((Map<?, ?>) fv.get(c)).size() == max);
    ck("evicted id no longer resolves to a stale venue id", !ven.invoke(c, first).equals("uuid-0"));
    ck("numeric venue ids are not stored", (long) reg.invoke(c, "123456") == 123456L && ((Map<?, ?>) fl.get(c)).size() == max);

    Field fs = SignedCexClient.class.getDeclaredField("streamed"); fs.setAccessible(true);
    @SuppressWarnings("unchecked") Map<Long, Object> streamed = (Map<Long, Object>) fs.get(c);
    for (long i = 0; i < max * 2L; i++) streamed.put(i, null);
    ck("streamed order states bounded", streamed.size() == max);

    System.out.println("pass="+pass+" fail="+fail); System.exit(fail==0?0:1);
  }
}
