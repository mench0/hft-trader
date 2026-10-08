package com.hft.perp;

import com.hft.config.ExchangeConfig;
import com.hft.rest.ExchangeOrderApi;
import com.hft.store.BalanceStore;
import com.hft.store.FundingStore;
import com.hft.store.FundingStore.Funding;
import com.hft.store.MarketDataStore;
import com.hft.store.PositionStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Фьючерсный счёт биржи: позиции, ставки funding и их опрос, плечо на старте.
 *
 * <ul>
 *   <li>LIVE: плечо выставляется на бирже по каждому символу, позиции загружаются с биржи и
 *       сверяются вместе с балансом; funding списывает сама биржа (баланс подтянется сверкой),
 *       здесь он только считается для статистики.</li>
 *   <li>PAPER: в момент списания (когда у биржи сменилось время следующего funding) по открытой
 *       позиции начисляется или списывается {@code −позиция × mark × ставка} — в бумажный баланс.</li>
 * </ul>
 */
public final class PerpAccount {

    /** Логгер. */
    private static final Logger log = LoggerFactory.getLogger(PerpAccount.class);

    /** Период опроса funding, с (из настроек процесса, применяется при запуске биржи). */
    private static volatile long pollSec = 30;

    /** Задать период опроса funding (BotController, из fundingPollSec). */
    public static void setPollSec(long sec) { pollSec = Math.max(5, sec); }

    /** Биржа. */
    private final String id;
    /** Подключение и параметры. */
    private final ExchangeConfig config;
    /** Рыночные данные (запасная mark-цена). */
    private final MarketDataStore market;
    /** Балансы. */
    private final BalanceStore balances;
    /** Позиции. */
    private final PositionStore positions;
    /** Ставки funding. */
    private final FundingStore funding = new FundingStore();
    /** Клиент биржи или бумажный движок. */
    private final ExchangeOrderApi api;
    /** Бумажный режим: funding начисляется здесь. */
    private final boolean paper;
    /** Источник ставок; null — у биржи нет публичного источника. */
    private final FundingSource source;
    /** Опрос ставок. */
    private ScheduledExecutorService scheduler;
    /** Последняя ошибка опроса (для админки). */
    private volatile String lastError = "";

    /**
     * @param id биржа
     * @param config подключение (символы, REST-адрес фьючерсов, плечо)
     * @param market рыночные данные
     * @param balances балансы
     * @param positions позиции
     * @param api клиент или бумажный движок
     * @param paper бумажный режим
     */
    public PerpAccount(String id, ExchangeConfig config, MarketDataStore market, BalanceStore balances,
                       PositionStore positions, ExchangeOrderApi api, boolean paper) {
        this.id = id;
        this.config = config;
        this.market = market;
        this.balances = balances;
        this.positions = positions;
        this.api = api;
        this.paper = paper;
        this.source = FundingSource.forExchange(id, config.restUrl());
    }

    /** Плечо и позиции (LIVE), затем опрос funding. */
    public void start() {
        if (!paper) {
            int lev = config.params().leverage();
            for (String s : config.symbols()) {
                try { api.setLeverage(s, lev); }
                catch (Exception e) { log.warn("[{}] плечо {}x для {} не выставлено: {}", id, lev, s, e.getMessage()); }
            }
            syncPositions();
        }
        if (source == null) return;
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "funding-" + id);
            t.setDaemon(true);
            return t;
        });
        scheduler.scheduleWithFixedDelay(this::poll, 0, pollSec, TimeUnit.SECONDS);
    }

    /** Остановить опрос. */
    public void stop() {
        ScheduledExecutorService s = scheduler;
        if (s != null) s.shutdownNow();
    }

    /** Сверить позиции с биржей (LIVE). */
    public void syncPositions() {
        if (paper) return;
        try { api.loadPositions(positions); }
        catch (Exception e) { log.warn("[{}] позиции не загружены: {}", id, e.getMessage()); }
    }

    /** Опросить ставки; если прошло время списания — учесть funding по открытым позициям. */
    void poll() {
        try {
            Map<String, Funding> fresh = source.fetch(config.symbols());
            for (var e : fresh.entrySet()) onFunding(e.getKey(), e.getValue());
            lastError = "";
        } catch (Exception e) {
            lastError = e.getMessage() == null ? e.toString() : e.getMessage();
            log.warn("[{}] ставки funding не получены: {}", id, lastError);
        }
    }

    /** Новая ставка символа: сменилось время следующего списания — значит, прошлое списание состоялось. */
    public void onFunding(String symbol, Funding f) {
        double mark = f.markPrice() > 0 ? f.markPrice() : market.referencePrice(symbol);
        Funding withMark = Double.isNaN(f.markPrice()) ? new Funding(f.rate(), f.intervalHours(), f.nextFundingMs(), mark, f.updatedMs()) : f;
        Funding prev = funding.put(symbol, withMark);
        if (prev == null || prev.nextFundingMs() <= 0 || f.nextFundingMs() <= prev.nextFundingMs()) return;
        if (System.currentTimeMillis() < prev.nextFundingMs()) return;
        double pay = positions.accrueFunding(symbol, prev.rate(), mark);
        if (pay == 0) return;
        if (paper) balances.adjust(BalanceStore.quoteAsset(symbol), pay);
        log.info("[{}] funding {}: ставка {}%, {} {}", id, symbol, String.format("%.4f", prev.rate() * 100),
                pay >= 0 ? "получено" : "списано", String.format("%.4f", Math.abs(pay)));
    }

    /** Ставки funding. */
    public FundingStore funding() { return funding; }

    /** Позиции. */
    public PositionStore positions() { return positions; }

    /** Позиции, ставки и ошибки — для админки. */
    public Map<String, Object> stats() {
        Map<String, Object> m = new LinkedHashMap<>(positions.stats());
        m.put("leverage", config.params().leverage());
        List<Map<String, Object>> pos = new ArrayList<>();
        positions.snapshot().forEach((s, p) -> {
            double px = market.referencePrice(s);
            Map<String, Object> x = new LinkedHashMap<>();
            x.put("symbol", s);
            x.put("side", p.qty() > 0 ? "LONG" : "SHORT");
            x.put("qty", Math.abs(p.qty()));
            x.put("entryPrice", p.entryPrice());
            x.put("markPrice", px);
            x.put("notional", p.notional(px));
            x.put("unrealizedPnl", Double.isNaN(px) ? null : p.unrealized(px));
            pos.add(x);
        });
        m.put("positions", pos);
        List<Map<String, Object>> rates = new ArrayList<>();
        funding.snapshot().forEach((s, f) -> {
            Map<String, Object> x = new LinkedHashMap<>();
            x.put("symbol", s);
            x.put("ratePercent", f.rate() * 100);
            x.put("intervalHours", f.intervalHours());
            x.put("per8hPercent", f.ratePer8h() * 100);
            x.put("aprPercent", f.aprPercent());
            x.put("nextFundingMs", f.nextFundingMs());
            rates.add(x);
        });
        m.put("funding", rates);
        if (!lastError.isEmpty()) m.put("fundingError", lastError);
        return m;
    }
}
