package com.hft.metrics;

import com.hft.control.BotController;
import com.hft.exchange.ExchangeGateway;
import com.hft.store.MarketDataStore;
import com.hft.store.OrderBook;
import com.hft.store.PriceWindow;

/**
 * Отдаёт торговые метрики в текстовом формате Prometheus exposition.
 * <p>
 * Почему Prometheus, а не прямая отправка в Grafana: Grafana сама не
 * принимает метрики — она рисует дашборды поверх источника данных
 * (Prometheus, InfluxDB, Graphite и т.д.). Стандартный путь для закрытого
 * контура — поднять рядом Prometheus, который сам стучится в этот
 * эндпоинт по расписанию (pull-модель), и подключить его к Grafana как
 * data source. Никакой отправки наружу из бота не требуется:
 * prometheus.yml просто указывает scrape_configs -> targets:
 * ["<host бота>:8080"], path: /metrics.
 *
 * История хранится только в Prometheus: бот отдаёт текущие значения и ничего не накапливает.
 * Метрики JVM (heap, GC, потоки) нужны, чтобы видеть в Grafana, что память не растёт.
 */
public final class PrometheusExporter {

    /** Все метрики в текстовом формате Prometheus. */
    public String render(BotController controller) {
        StringBuilder sb = new StringBuilder(4096);

        jvm(sb);
        rateBudgets(sb);

        help(sb, "hft_connected", "1 если WebSocket-соединение с биржей активно");
        help(sb, "hft_trading_enabled", "1 если торговля на бирже включена в параметрах");
        counter(sb, "hft_messages_total", "Всего сообщений получено с биржи");
        help(sb, "hft_kill_switch", "1 если торговля остановлена риск-менеджером");
        counter(sb, "hft_orders_accepted_total", "Ордеров прошло проверку риск-менеджера");
        counter(sb, "hft_orders_rejected_total", "Ордеров отклонено риск-менеджером");
        help(sb, "hft_daily_pnl_quote", "Реализованный PnL за сутки UTC после комиссий, в котируемой валюте");
        help(sb, "hft_open_positions", "Открытых позиций у стратегии");
        help(sb, "hft_strategy_enabled", "1 если стратегия включена");
        counter(sb, "hft_tri_opportunities_total", "Найдено прибыльных кругов треугольного арбитража (с учётом комиссий)");
        counter(sb, "hft_tri_executed_total", "Исполнено кругов треугольного арбитража");
        counter(sb, "hft_tri_failed_total", "Прерванных кругов (нога не исполнилась)");
        help(sb, "hft_tri_pnl_quote", "Результат треугольного арбитража с запуска, в базовой валюте круга");
        help(sb, "hft_statarb_positions", "Открытых позиций статистического арбитража");
        counter(sb, "hft_statarb_trades_total", "Закрытых сделок статистического арбитража");
        help(sb, "hft_statarb_pnl_quote", "Результат статистического арбитража с запуска");
        help(sb, "hft_statarb_zscore", "z-score спреда пары");
        help(sb, "hft_order_latency_p50_micros", "Латентность отправки ордера, p50, микросекунды");
        help(sb, "hft_order_latency_p99_micros", "Латентность отправки ордера, p99, микросекунды");

        help(sb, "hft_best_bid", "Лучшая цена покупки в стакане");
        help(sb, "hft_best_ask", "Лучшая цена продажи в стакане");
        help(sb, "hft_spread_percent", "Спред в процентах от середины");
        help(sb, "hft_imbalance", "Дисбаланс стакана от -1 до 1");
        help(sb, "hft_zscore", "Текущее отклонение цены в сигмах от скользящего среднего");
        counter(sb, "hft_tick_count_total", "Сделок получено по инструменту");

        for (ExchangeGateway gw : controller.active().values()) {
            String ex = gw.id();

            line(sb, "hft_connected", ex, gw.isConnected() ? 1 : 0);
            line(sb, "hft_trading_enabled", ex, controller.params(ex).tradingEnabled() ? 1 : 0);
            line(sb, "hft_messages_total", ex, gw.messageCount());
            line(sb, "hft_kill_switch", ex, gw.risk().isStopped() ? 1 : 0);
            line(sb, "hft_orders_accepted_total", ex, gw.risk().acceptedCount());
            line(sb, "hft_orders_rejected_total", ex, gw.risk().rejectedCount());
            line(sb, "hft_daily_pnl_quote", ex, gw.risk().dailyPnl());
            line(sb, "hft_open_positions", ex, gw.strategy().openPositions());
            line(sb, "hft_strategy_enabled", ex, gw.strategy().isEnabled() ? 1 : 0);
            var tri = gw.strategy().triangular();
            line(sb, "hft_tri_opportunities_total", ex, tri.opportunities());
            line(sb, "hft_tri_executed_total", ex, tri.executedCount());
            line(sb, "hft_tri_failed_total", ex, tri.failedCount());
            line(sb, "hft_tri_pnl_quote", ex, tri.totalPnl());
            var sa = gw.strategy().statArb();
            line(sb, "hft_statarb_positions", ex, sa.openPositions());
            line(sb, "hft_statarb_trades_total", ex, sa.tradesCount());
            line(sb, "hft_statarb_pnl_quote", ex, sa.totalPnl());
            for (var pair : sa.zScores().entrySet()) {
                if (Double.isNaN(pair.getValue())) continue;
                sb.append("hft_statarb_zscore{exchange=\"").append(ex).append("\",pair=\"").append(pair.getKey()).append("\"} ")
                  .append(formatNumber(pair.getValue())).append('\n');
            }

            var latency = gw.orders().latency();
            if (latency.count() > 0) {
                line(sb, "hft_order_latency_p50_micros", ex, latency.p50Micros());
                line(sb, "hft_order_latency_p99_micros", ex, latency.p99Micros());
            }

            MarketDataStore market = gw.marketData();
            for (String symbol : market.symbols()) {
                OrderBook book = market.book(symbol);
                if (book != null && book.isReady()) {
                    lineSymbol(sb, "hft_best_bid", ex, symbol, book.bestBid());
                    lineSymbol(sb, "hft_best_ask", ex, symbol, book.bestAsk());
                    lineSymbol(sb, "hft_spread_percent", ex, symbol, book.spreadPercent());
                    lineSymbol(sb, "hft_imbalance", ex, symbol, book.imbalance(5));
                }
                var stats = market.stats(symbol);
                if (stats != null) {
                    lineSymbol(sb, "hft_tick_count_total", ex, symbol, stats.tickCount());
                }
                PriceWindow window = market.window(symbol);
                if (window != null && window.isWarmedUp()) {
                    lineSymbol(sb, "hft_zscore", ex, symbol, window.currentZScore());
                }
            }
        }

        return sb.toString();
    }

    /** Метрики JVM: heap, сборки мусора, потоки. */
    private void jvm(StringBuilder sb) {
        var heap = java.lang.management.ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
        help(sb, "hft_jvm_heap_used_bytes", "Занято heap");
        sb.append("hft_jvm_heap_used_bytes ").append(heap.getUsed()).append('\n');
        help(sb, "hft_jvm_heap_max_bytes", "Предел heap (-Xmx)");
        sb.append("hft_jvm_heap_max_bytes ").append(heap.getMax()).append('\n');
        long gcCount = 0, gcMillis = 0;
        for (var gc : java.lang.management.ManagementFactory.getGarbageCollectorMXBeans()) {
            gcCount += Math.max(0, gc.getCollectionCount());
            gcMillis += Math.max(0, gc.getCollectionTime());
        }
        counter(sb, "hft_jvm_gc_collections_total", "Сборок мусора");
        sb.append("hft_jvm_gc_collections_total ").append(gcCount).append('\n');
        counter(sb, "hft_jvm_gc_seconds_total", "Суммарное время сборок мусора, секунды");
        sb.append("hft_jvm_gc_seconds_total ").append(gcMillis / 1000.0).append('\n');
        help(sb, "hft_jvm_threads", "Живых потоков");
        sb.append("hft_jvm_threads ").append(java.lang.management.ManagementFactory.getThreadMXBean().getThreadCount()).append('\n');
    }

    /** Лимиты запросов: заполненность каждого ведра, ответы «слишком часто», отказы своего бюджета, пауза. */
    private void rateBudgets(StringBuilder sb) {
        var all = com.hft.rest.RateBudget.all();
        if (all.isEmpty()) return;
        help(sb, "hft_rate_used_ratio", "Заполненность ведра лимита запросов (1 = исчерпан наш запас 80% от лимита биржи)");
        counter(sb, "hft_rate_limited_total", "Ответов биржи о превышении лимита (429/418/403 и коды лимита)");
        counter(sb, "hft_rate_local_rejects_total", "Запросов, не отправленных из-за исчерпанного своего бюджета");
        counter(sb, "hft_rate_waited_ms_total", "Суммарное ожидание в очереди лимита, мс");
        help(sb, "hft_rate_blocked_ms", "Сколько ещё мс запросы к бирже приостановлены");
        for (var e : new java.util.TreeMap<>(all).entrySet()) {
            String ex = e.getKey();
            var b = e.getValue();
            for (var bucket : b.buckets()) {
                sb.append("hft_rate_used_ratio{exchange=\"").append(ex).append("\",bucket=\"").append(bucket.get("name")).append("\"} ")
                  .append(formatNumber((Double) bucket.get("usedRatio"))).append('\n');
            }
            line(sb, "hft_rate_limited_total", ex, b.rateLimitedResponses());
            line(sb, "hft_rate_local_rejects_total", ex, b.localRejects());
            line(sb, "hft_rate_waited_ms_total", ex, b.waitedMs());
            line(sb, "hft_rate_blocked_ms", ex, b.blockedForMs());
        }
    }

    /** HELP и TYPE gauge для метрики. */
    private void help(StringBuilder sb, String name, String description) {
        sb.append("# HELP ").append(name).append(' ').append(description).append('\n');
        sb.append("# TYPE ").append(name).append(" gauge\n");
    }

    /** HELP и TYPE counter для метрики. */
    private void counter(StringBuilder sb, String name, String description) {
        sb.append("# HELP ").append(name).append(' ').append(description).append('\n');
        sb.append("# TYPE ").append(name).append(" counter\n");
    }

    /** Значение с меткой exchange. */
    private void line(StringBuilder sb, String name, String exchange, double value) {
        sb.append(name).append("{exchange=\"").append(exchange).append("\"} ")
          .append(formatNumber(value)).append('\n');
    }

    /** Значение с метками exchange и symbol (NaN пропускается). */
    private void lineSymbol(StringBuilder sb, String name, String exchange, String symbol, double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)) return;
        sb.append(name).append("{exchange=\"").append(exchange)
          .append("\",symbol=\"").append(symbol).append("\"} ")
          .append(formatNumber(value)).append('\n');
    }

    /** Число без «.0» для целых. */
    private String formatNumber(double v) {
        if (v == Math.floor(v) && !Double.isInfinite(v)) return String.valueOf((long) v);
        return String.valueOf(v);
    }
}
