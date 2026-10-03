import com.hft.config.*;
import com.hft.control.BotController;
import com.hft.model.*;
import com.hft.persistence.SqliteStateStore;
import com.hft.risk.RiskManager;
import com.hft.store.MarketDataStore;
import java.lang.reflect.Constructor;
import java.util.*;

/** Торговые параметры по биржам: валидация, применение в риске, дневной PnL, сохранение между рестартами. */
public class TradingParamsCheck {
  static int pass, fail;
  static void ck(String n, boolean ok){ if(ok) pass++; else {fail++; System.out.println("FAIL "+n);} }
  static boolean throwsIae(Runnable r){ try { r.run(); return false; } catch (IllegalArgumentException e) { return true; } }

  public static void main(String[] a) throws Exception {
    // ---- валидация
    TradingParams d = TradingParams.DEFAULTS;
    TradingParams p = d.with(Map.of("maxPositionQuote", "55", "entryZ", "2.5", "exchange", "okx"));
    ck("partial update", p.maxPositionQuote() == 55 && p.entryZ() == 2.5 && p.exitZ() == d.exitZ());
    ck("round trip via string map", d.with(p.toStringMap()).equals(p));
    ck("out of range rejected", throwsIae(() -> d.with(Map.of("maxSlippagePercent", "500"))));
    ck("not a number rejected", throwsIae(() -> d.with(Map.of("entryZ", "abc"))));
    ck("bad boolean rejected", throwsIae(() -> d.with(Map.of("tradingEnabled", "yes"))));
    ck("imbalanceLevels <= bookDepth", throwsIae(() -> d.with(Map.of("bookDepth", "3"))));
    ck("restart-only keys", TradingParams.requiresRestart("bookDepth") && !TradingParams.requiresRestart("entryZ"));

    // ---- риск читает параметры своей биржи на лету
    MarketDataStore market = new MarketDataStore(20, 20); market.register("BTCUSDT");
    long now = System.currentTimeMillis();
    market.book("BTCUSDT").applySnapshot(new double[]{99.9}, new double[]{10}, 1, new double[]{100}, new double[]{10}, 1, now, now);
    TradingSettings s = new TradingSettings(d.with(Map.of("tradingEnabled", "true", "maxPositionQuote", "50")));
    RiskManager risk = new RiskManager(s, market, "x");
    OrderRequest req = OrderRequest.limit("BTCUSDT", OrderEnums.Side.BUY, 100);
    ck("within position limit", risk.check(req, 0.4, 100).allowed());
    ck("over position limit", !risk.check(req, 0.6, 100).allowed());
    s.set(s.get().with(Map.of("maxPositionQuote", "100")));
    ck("new limit applied without restart", risk.check(req, 0.6, 100).allowed());
    s.set(s.get().with(Map.of("tradingEnabled", "false")));
    ck("tradingEnabled=false blocks", !risk.check(req, 0.1, 100).allowed());

    // ---- дневной лимит считается по реализованному результату
    s.set(s.get().with(Map.of("tradingEnabled", "true", "maxDailyLossQuote", "10")));
    risk.recordPnl(-4); risk.recordPnl(-7);
    ck("daily pnl accumulates", Math.abs(risk.dailyPnl() + 11) < 1e-9);
    ck("daily loss trips kill switch", !risk.check(req, 0.1, 100).allowed() && risk.isStopped());
    var dayField = RiskManager.class.getDeclaredField("pnlDay"); dayField.setAccessible(true);
    dayField.setLong(risk, dayField.getLong(risk) - 1);   // будто наступили новые сутки
    ck("daily pnl resets on new UTC day", risk.dailyPnl() == 0);

    // ---- параметры по биржам сохраняются и восстанавливаются (STATE_DB задаётся запускающим скриптом)
    Constructor<AppConfig> ctor = AppConfig.class.getDeclaredConstructor(List.class); ctor.setAccessible(true);
    AppConfig app = ctor.newInstance(List.of());
    BotController c1 = new BotController(app, new SqliteStateStore());
    c1.updateParams("okx", Map.of("maxPositionQuote", "77"));
    c1.updateParams("bybit", Map.of("maxPositionQuote", "33", "entryZ", "3"));
    ck("taker fee default from catalog", c1.params("okx").takerFeePercent() > 0);
    ck("invalid update changes nothing", throwsIae(() -> c1.updateParams("okx", Map.of("maxPositionQuote", "-1"))) && c1.params("okx").maxPositionQuote() == 77);
    ck("unsupported exchange rejected", throwsIae(() -> c1.updateParams("nope", Map.of())));
    BotController c2 = new BotController(app, new SqliteStateStore());
    ck("restored per exchange", c2.params("okx").maxPositionQuote() == 77 && c2.params("bybit").maxPositionQuote() == 33
        && c2.params("bybit").entryZ() == 3 && c2.params("okx").entryZ() == d.entryZ());
    ck("untouched exchange gets defaults", c2.params("mexc").maxPositionQuote() == d.maxPositionQuote());

    System.out.println("pass="+pass+" fail="+fail); System.exit(fail==0?0:1);
  }
}
