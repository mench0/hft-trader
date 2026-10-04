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
  static ExchangeConfig buildCfg(String id, TradingParams p) throws Exception {
    var m = BotController.class.getDeclaredMethod("buildExchangeConfig", String.class, List.class, TradingParams.class); m.setAccessible(true);
    return (ExchangeConfig) m.invoke(null, id, List.of("BTCUSDT"), p);
  }
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

    // ---- параметры только у выбранных бирж; сохраняются и восстанавливаются (STATE_DB задаёт скрипт запуска)
    AppConfig app = AppConfig.defaults();
    BotController c1 = new BotController(app, new SqliteStateStore());
    ck("params of unselected exchange cannot be set", throwsIae(() -> c1.updateParams("okx", Map.of("maxPositionQuote", "77"))));
    c1.selectExchange("okx", List.of("BTCUSDT"), Map.of("maxPositionQuote", "77"));
    c1.selectExchange("bybit", List.of("BTCUSDT"));
    c1.updateParams("bybit", Map.of("maxPositionQuote", "33", "entryZ", "3"));
    ck("taker fee default from catalog", c1.params("okx").takerFeePercent() > 0);
    ck("invalid update changes nothing", throwsIae(() -> c1.updateParams("okx", Map.of("maxPositionQuote", "-1"))) && c1.params("okx").maxPositionQuote() == 77);
    ck("unsupported exchange rejected", throwsIae(() -> c1.selectExchange("nope", List.of("BTCUSDT"))));
    ck("only selected exchanges hold params", c1.configuredExchanges().equals(java.util.Set.of("okx", "bybit")));
    // testnet: по умолчанию — если у биржи он есть; у KuCoin его нет
    ck("testnet default follows catalog", BotController.defaultsFor("binance").testnet() && !BotController.defaultsFor("kucoin").testnet());
    ck("testnet=true rejected where exchange has none", throwsIae(() -> c1.selectExchange("kucoin", List.of("BTCUSDT"), Map.of("testnet", "true"))) && !c1.isSelected("kucoin"));
    c1.selectExchange("gate", List.of("BTCUSDT"), Map.of("testnet", "true"));
    var cfgT = buildCfg("gate", c1.params("gate"));
    c1.updateParams("gate", Map.of("testnet", "false"));
    var cfgM = buildCfg("gate", c1.params("gate"));
    ck("testnet switches REST and WS addresses", cfgT.restUrl().contains("testnet") && cfgT.wsUrl().contains("testnet")
        && cfgM.restUrl().equals("https://api.gateio.ws") && !cfgM.wsUrl().contains("testnet") && cfgT.testnet() && !cfgM.testnet());
    c1.updateParams("gate", Map.of("restUrl", "https://my-proxy.example", "wsUrl", "wss://my-proxy.example/ws"));
    var cfgC = buildCfg("gate", c1.params("gate"));
    ck("custom addresses override catalog", cfgC.restUrl().equals("https://my-proxy.example") && cfgC.wsUrl().equals("wss://my-proxy.example/ws"));
    c1.deselectExchange("gate");
    ck("deselect drops params", !c1.configuredExchanges().contains("gate") && c1.params("gate").restUrl().isEmpty());
    // настройки процесса
    c1.updateGlobal(Map.of("statusLogSec", "120", "discoveryEnabled", "false"));
    ck("discovery can be turned off", c1.discovery() == null);
    ck("invalid global setting rejected", throwsIae(() -> c1.updateGlobal(Map.of("rateLimitSafety", "2"))) && c1.global().rateLimitSafety() == 0.8);
    BotController c2 = new BotController(app, new SqliteStateStore());
    ck("restored per exchange", c2.params("okx").maxPositionQuote() == 77 && c2.params("bybit").maxPositionQuote() == 33
        && c2.params("bybit").entryZ() == 3 && c2.params("okx").entryZ() == d.entryZ());
    ck("deselected exchange not restored", !c2.isSelected("gate") && c2.configuredExchanges().equals(java.util.Set.of("okx", "bybit")));
    ck("global settings restored", c2.global().statusLogSec() == 120 && !c2.global().discoveryEnabled());

    // ---- описания параметров
    ck("every exchange param has a description", TradingParams.SPECS.size() == TradingParams.class.getRecordComponents().length
        && TradingParams.SPECS.values().stream().allMatch(sp -> !sp.help().isBlank()));
    ck("every global setting has a description", GlobalParams.SPECS.size() == GlobalParams.class.getRecordComponents().length);
    ck("restart flags", TradingParams.requiresRestart("testnet") && TradingParams.requiresRestart("bookDepth") && !TradingParams.requiresRestart("entryZ"));
    ck("bad URL / address rejected", throwsIae(() -> d.with(Map.of("restUrl", "ftp://x"))) && throwsIae(() -> d.with(Map.of("uniRouter", "0x12"))));
    ck("integer param rejects fractions", throwsIae(() -> d.with(Map.of("bookDepth", "20.5"))));

    System.out.println("pass="+pass+" fail="+fail); System.exit(fail==0?0:1);
  }
}
