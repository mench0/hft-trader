package com.hft.discovery;

import com.hft.exchange.catalog.ExchangeCatalog;
import com.hft.exchange.catalog.ExchangeInfo;

import com.hft.config.GlobalParams;
import com.hft.config.TradingParams;
import java.util.*;
import java.util.function.Function;

/** Профили стратегий: по каким условиям каждая из них выбирает тикеры. */
public final class Profiles {

    /** Утилитный класс — экземпляры не создаются. */
    private Profiles() {}

    /** Комиссия тейкера биржи из каталога, %. */
    static double takerPct(String ex) { return ExchangeCatalog.find(ex).map(ExchangeInfo::takerFeePct).orElse(0.1); }
    /** Комиссия мейкера биржи из каталога, %. */
    static double makerPct(String ex) { return ExchangeCatalog.find(ex).map(ExchangeInfo::makerFeePct).orElse(0.1); }


    /** Процент для причин отбора. */
    static String pct(double v) { return Double.isNaN(v) ? "—" : String.format(Locale.ROOT, "%.3f%%", v); }
    /** Сумма с млн/млрд для причин отбора. */
    static String money(double v) {
        if (Double.isNaN(v)) return "—";
        if (v >= 1e9) return String.format(Locale.ROOT, "%.2f млрд", v / 1e9);
        if (v >= 1e6) return String.format(Locale.ROOT, "%.2f млн", v / 1e6);
        if (v >= 1e3) return String.format(Locale.ROOT, "%.1f тыс", v / 1e3);
        return String.format(Locale.ROOT, "%.0f", v);
    }
    /** Округление до 4 знаков; NaN/бесконечность -> null (в JSON — null, а не строка "NaN"). */
    static Double r4(double v) { return Double.isFinite(v) ? Math.round(v * 1e4) / 1e4 : null; }

    /** Метрики тикера для ответа админки. */
    static Map<String, Object> metrics(TickerSnapshot t) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("last", Double.isFinite(t.last()) ? t.last() : null);
        m.put("quoteVolume24h", r4(t.quoteVolume24h()));
        m.put("spreadPct", r4(t.spreadPct()));
        m.put("rangePct", r4(t.rangePct()));
        m.put("changePct", r4(t.changePct()));
        m.put("perp", t.perp());
        return m;
    }

    // ═════════════════════════ Mean reversion (торгует в боте) ═════════════════════════

    /** Профиль возврата к среднему: отбор по сводке и бэктест по минутным свечам. */
    public static final class MeanReversion implements StrategyProfile {
        /** Параметры бэктеста биржи из админки (null — по умолчанию). */
        private final Function<String, MeanReversionBacktest.Params> paramsFor;
        /** Настройки процесса. */
        private final GlobalParams g;
        /** Минимальный оборот и максимальный спред отбора. */
        private final double minVolume, maxSpread;
        /** Сколько кандидатов на бирже прогонять бэктестом. */
        private final int perExchange;

        /**
         * @param paramsFor параметры стратегии конкретной биржи (из админки) или null — по умолчанию
         * @param g         пороги отбора и бэктеста из настроек процесса
         */
        public MeanReversion(Function<String, MeanReversionBacktest.Params> paramsFor, GlobalParams g) {
            this.paramsFor = paramsFor;
            this.g = g;
            this.minVolume = g.discoveryMinVolume();
            this.maxSpread = g.discoveryMrMaxSpreadPercent();
            this.perExchange = g.discoveryBacktestPerExchange();
        }

        /** Параметры бэктеста, если для биржи ничего не задано. */
        MeanReversionBacktest.Params defaults() {
            TradingParams d = TradingParams.DEFAULTS;
            return new MeanReversionBacktest.Params(d.entryZ(), d.exitZ(), d.stopLossPercent(), g.discoveryBacktestWindow(), g.discoveryBacktestMaxHoldBars());
        }

        /** Идентификатор. */
        public String id() { return "mean-reversion"; }
        /** Название. */
        public String title() { return "Возврат к среднему (mean reversion)"; }
        /** Описание для админки. */
        public String description() {
            return "Покупает, когда цена ушла ниже скользящего среднего на entryZ сигм, продаёт при возврате. "
                    + "Нужны ликвидность, узкий спред и ход цены «пилой», а не трендом. Отбор по сводке 24ч, "
                    + "затем прогон тех же правил по последним минутным свечам с комиссией.";
        }
        /** Стратегия торгуется ботом. */
        public boolean executable() { return true; }
        /** Условия отбора. */
        public List<String> criteria() {
            return List.of("оборот за 24ч ≥ " + money(minVolume),
                    "спред ≤ " + maxSpread + "% (как в стратегии)",
                    "дневной диапазон " + g.discoveryMrMinRangePercent() + "–" + g.discoveryMrMaxRangePercent() + "%",
                    "|изменение за 24ч| ≤ " + Math.round(g.discoveryMrMaxTrend() * 100) + "% диапазона (не тренд)",
                    "бэктест по 1-мин свечам: ≥ 3 сделок и плюс после комиссий");
        }

        /** Отбор и бэктест кандидатов по каждой бирже. */
        public List<Candidate> screen(Map<String, List<TickerSnapshot>> byExchange, ClosesProvider closes) {
            List<Candidate> out = new ArrayList<>();
            for (var e : byExchange.entrySet()) {
                String ex = e.getKey();
                double fee = takerPct(ex);
                MeanReversionBacktest.Params p = Optional.ofNullable(paramsFor == null ? null : paramsFor.apply(ex)).orElse(defaults());
                /** Прошедший первичный отбор тикер с оценкой и причинами. */
                record Pre(TickerSnapshot t, double score, List<String> reasons) {}
                List<Pre> pass = new ArrayList<>();
                for (TickerSnapshot t : e.getValue()) {
                    if (!(t.last() > 0) || !(t.quoteVolume24h() >= minVolume)) continue;
                    List<String> why = new ArrayList<>();
                    why.add("оборот " + money(t.quoteVolume24h()));
                    double sp = t.spreadPct(), range = t.rangePct(), ch = t.changePct();
                    if (!Double.isNaN(sp) && sp > maxSpread) continue;
                    why.add(Double.isNaN(sp) ? "спред неизвестен (сводка без bid/ask)" : "спред " + pct(sp));
                    if (!Double.isNaN(range) && (range < g.discoveryMrMinRangePercent() || range > g.discoveryMrMaxRangePercent())) continue;
                    if (!Double.isNaN(range)) why.add("диапазон " + pct(range));
                    double trend = Double.isNaN(range) || Double.isNaN(ch) || range == 0 ? 0.3 : Math.abs(ch) / range;
                    if (trend > g.discoveryMrMaxTrend()) continue;
                    if (!Double.isNaN(ch)) why.add("изменение " + pct(ch));
                    double s = Math.log10(t.quoteVolume24h())
                            * (Double.isNaN(range) ? 1 : Math.min(range, 10) / ((Double.isNaN(sp) ? 0.05 : sp) + 2 * fee))
                            * (1 - trend);
                    pass.add(new Pre(t, s, why));
                }
                pass.sort((a, b) -> Double.compare(b.score(), a.score()));
                for (int i = 0; i < pass.size(); i++) {
                    Pre pr = pass.get(i);
                    Map<String, Object> m = metrics(pr.t());
                    List<String> why = new ArrayList<>(pr.reasons());
                    if (i >= perExchange) {
                        if (i < perExchange * 3)
                            out.add(new Candidate(ex, pr.t().symbol(), false, "под вопросом", pr.score() * 0.01, add(why, "бэктест не запускался (вне лимита запросов)"), m, null));
                        continue;
                    }
                    double[] c = closes.closes(pr.t(), 500);
                    if (c == null || c.length < p.window() + 20) {
                        out.add(new Candidate(ex, pr.t().symbol(), false, "под вопросом", pr.score() * 0.01, add(why, "нет минутных свечей для бэктеста"), m, null));
                        continue;
                    }
                    MeanReversionBacktest.Result r = MeanReversionBacktest.run(c, p, fee);
                    Map<String, Object> bt = new LinkedHashMap<>();
                    bt.put("bars", r.bars());
                    bt.put("trades", r.trades());
                    bt.put("winRatePct", r4(r.winRate()));
                    bt.put("netPct", r4(r.netPct()));
                    bt.put("avgTradePct", r4(r.avgTradePct()));
                    bt.put("maxDrawdownPct", r4(r.maxDrawdownPct()));
                    bt.put("feePctPerSide", fee);
                    boolean ok = r.trades() >= 3 && r.netPct() > 0;
                    why.add(String.format(Locale.ROOT, "бэктест %d мин: сделок %d, в плюс %.0f%%, итог %s после комиссий",
                            r.bars(), r.trades(), r.winRate(), pct(r.netPct())));
                    if (r.trades() < 3) why.add("слишком мало сигналов");
                    else if (r.netPct() <= 0) why.add("правила стратегии на этих свечах в минусе");
                    double score = ok ? r.netPct() * Math.sqrt(r.trades()) + 1 : r.netPct();
                    out.add(new Candidate(ex, pr.t().symbol(), ok, ok ? "подходит" : "не подходит", score, why, m, bt));
                }
            }
            return sort(out);
        }
    }

    // ═════════════════════════ Межбиржевой арбитраж (сканер) ═════════════════════════

    /** Профиль межбиржевого арбитража (только подбор). */
    public static final class CrossExchange implements StrategyProfile {
        /** Настройки процесса (пороги отбора). */
        private final GlobalParams g;

        /** @param g пороги отбора из настроек процесса */
        public CrossExchange(GlobalParams g) { this.g = g; }

        /** Идентификатор. */
        public String id() { return "cross-exchange-arb"; }
        /** Название. */
        public String title() { return "Межбиржевой арбитраж"; }
        /** Описание для админки. */
        public String description() {
            return "Один и тот же тикер дешевле на одной бирже, чем дороже продаётся на другой, с учётом taker-комиссий обеих. "
                    + "Только сканер: автоматического исполнения в боте нет. Цены сводок снимаются с разницей в секунды — "
                    + "это повод посмотреть на живые стаканы, а не сигнал. Нужны деньги на обеих биржах, переводы не учитываются.";
        }
        /** Ботом не торгуется — только подбор. */
        public boolean executable() { return false; }
        /** Условия отбора. */
        public List<String> criteria() {
            return List.of("тикер есть минимум на двух биржах (спот со спотом, перп с перпом)",
                    "у обеих бирж в сводке есть bid/ask",
                    "чистая разница после двух комиссий > 0 (показываем и близкие, до −0.2%)",
                    "разница > 5% отбрасывается: скорее всего это разные токены с одним тикером");
        }
        /** Пары бирж, где покупка на одной дешевле продажи на другой с учётом комиссий. */
        public List<Candidate> screen(Map<String, List<TickerSnapshot>> byExchange, ClosesProvider closes) {
            Map<String, List<TickerSnapshot>> bySymbol = new HashMap<>();
            for (var l : byExchange.values())
                for (TickerSnapshot t : l)
                    if (t.bid() > 0 && t.ask() > 0 && t.ask() >= t.bid() && t.quoteVolume24h() >= g.discoveryArbMinVolume())
                        bySymbol.computeIfAbsent(t.symbol() + (t.perp() ? ":perp" : ""), k -> new ArrayList<>()).add(t);
            List<Candidate> out = new ArrayList<>();
            for (var e : bySymbol.entrySet()) {
                List<TickerSnapshot> l = e.getValue();
                if (l.size() < 2) continue;
                TickerSnapshot buy = null, sell = null;
                for (TickerSnapshot t : l) {
                    if (buy == null || t.ask() < buy.ask()) buy = t;
                    if (sell == null || t.bid() > sell.bid()) sell = t;
                }
                if (buy.exchange().equals(sell.exchange())) continue;
                double gross = (sell.bid() - buy.ask()) / buy.ask() * 100.0;
                if (Math.abs(gross) > g.discoveryArbMaxGrossPercent()) continue;
                double net = gross - takerPct(buy.exchange()) - takerPct(sell.exchange());
                if (net < -0.2) continue;
                Map<String, Object> m = new LinkedHashMap<>();
                m.put("buyExchange", buy.exchange());
                m.put("buyAsk", buy.ask());
                m.put("sellExchange", sell.exchange());
                m.put("sellBid", sell.bid());
                m.put("grossPct", r4(gross));
                m.put("netPct", r4(net));
                m.put("minVolume24h", r4(Math.min(buy.quoteVolume24h(), sell.quoteVolume24h())));
                m.put("perp", buy.perp());
                boolean ok = net > 0;
                List<String> why = List.of("купить на " + buy.exchange() + " по " + buy.ask() + ", продать на " + sell.exchange() + " по " + sell.bid(),
                        "до комиссий " + pct(gross) + ", после " + pct(net),
                        "меньший оборот из двух " + money(Math.min(buy.quoteVolume24h(), sell.quoteVolume24h())));
                out.add(new Candidate(buy.exchange() + "→" + sell.exchange(), buy.symbol(), ok, ok ? "подходит" : "близко", net, why, m, null));
            }
            return sort(out);
        }
    }

    // ═════════════════════════ Сбор широкого спреда (сканер) ═════════════════════════

    /** Профиль сбора спреда (только подбор). */
    public static final class SpreadCapture implements StrategyProfile {
        /** Настройки процесса (пороги отбора). */
        private final GlobalParams g;

        /** @param g пороги отбора из настроек процесса */
        public SpreadCapture(GlobalParams g) { this.g = g; }

        /** Идентификатор. */
        public String id() { return "spread-capture"; }
        /** Название. */
        public String title() { return "Сбор широкого спреда (маркет-мейкинг на неликвиде)"; }
        /** Описание для админки. */
        public String description() {
            return "Лимитки с обеих сторон внутри широкого спреда на малоликвидных парах: спред должен покрывать две maker-комиссии "
                    + "с запасом. Только сканер: автоматического маркет-мейкинга в боте нет. Риск — застрять с позицией при движении цены.";
        }
        /** Ботом не торгуется — только подбор. */
        public boolean executable() { return false; }
        /** Условия отбора. */
        public List<String> criteria() {
            return List.of("спред ≥ 2×maker-комиссия + 0.1% и ≤ 3%", "оборот за 24ч от 50 тыс до 5 млн", "у биржи в сводке есть bid/ask");
        }
        /** Малоликвидные пары, где спред покрывает две комиссии мейкера с запасом. */
        public List<Candidate> screen(Map<String, List<TickerSnapshot>> byExchange, ClosesProvider closes) {
            List<Candidate> out = new ArrayList<>();
            for (var e : byExchange.entrySet()) {
                double maker = makerPct(e.getKey());
                for (TickerSnapshot t : e.getValue()) {
                    double sp = t.spreadPct(), vol = t.quoteVolume24h();
                    if (Double.isNaN(sp) || !(vol >= g.discoverySpreadMinVolume()) || vol > g.discoverySpreadMaxVolume()) continue;
                    double edge = sp - 2 * maker;
                    if (edge < g.discoverySpreadMinEdgePercent() || sp > g.discoverySpreadMaxPercent()) continue;
                    Map<String, Object> m = metrics(t);
                    m.put("edgePct", r4(edge));
                    m.put("makerFeePct", maker);
                    out.add(new Candidate(e.getKey(), t.symbol(), true, "подходит", edge * Math.log10(vol),
                            List.of("спред " + pct(sp) + ", после двух maker-комиссий остаётся " + pct(edge), "оборот " + money(vol)), m, null));
                }
            }
            return sort(out);
        }
    }

    /** Добавить причину и вернуть список. */
    static List<String> add(List<String> l, String s) { l.add(s); return l; }

    /** Сортировка: подходящие первыми, затем по оценке. */
    static List<Candidate> sort(List<Candidate> l) {
        l.sort(Comparator.comparing(Candidate::suitable).reversed().thenComparing(Comparator.comparingDouble(Candidate::score).reversed()));
        return l.size() > 60 ? new ArrayList<>(l.subList(0, 60)) : l;
    }
}
