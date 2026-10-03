package com.hft.metrics;

import com.hft.control.BotController;
import com.hft.exchange.ExchangeGateway;
import com.hft.store.MarketDataStore;
import com.hft.store.OrderBook;
import com.hft.store.PriceWindow;

/**
 * Отдаёт торговые метрики в текстовом формате Prometheus exposition.
 *
 * Почему Prometheus, а не прямая отправка в Grafana: Grafana сама не
 * принимает метрики — она рисует дашборды поверх источника данных
 * (Prometheus, InfluxDB, Graphite и т.д.). Стандартный путь для закрытого
 * контура — поднять рядом Prometheus, который сам стучится в этот
 * эндпоинт по расписанию (pull-модель), и подключить его к Grafana как
 * data source. Никакой отправки наружу из бота не требуется:
 * prometheus.yml просто указывает scrape_configs -> targets:
 * ["<host бота>:8080"], path: /metrics.
 */
public final class PrometheusExporter {

    public String render(BotController controller) {
        StringBuilder sb = new StringBuilder(4096);

        help(sb, "hft_connected", "1 если WebSocket-соединение с биржей активно");
        help(sb, "hft_messages_total", "Всего сообщений получено с биржи");
        help(sb, "hft_kill_switch", "1 если торговля остановлена риск-менеджером");
        help(sb, "hft_orders_accepted_total", "Ордеров прошло проверку риск-менеджера");
        help(sb, "hft_orders_rejected_total", "Ордеров отклонено риск-менеджером");
        help(sb, "hft_daily_pnl_quote", "Дневной PnL в котируемой валюте");
        help(sb, "hft_open_positions", "Открытых позиций у стратегии");
        help(sb, "hft_strategy_enabled", "1 если стратегия включена");
        help(sb, "hft_order_latency_p50_micros", "Латентность отправки ордера, p50, микросекунды");
        help(sb, "hft_order_latency_p99_micros", "Латентность отправки ордера, p99, микросекунды");

        help(sb, "hft_best_bid", "Лучшая цена покупки в стакане");
        help(sb, "hft_best_ask", "Лучшая цена продажи в стакане");
        help(sb, "hft_spread_percent", "Спред в процентах от середины");
        help(sb, "hft_imbalance", "Дисбаланс стакана от -1 до 1");
        help(sb, "hft_zscore", "Текущее отклонение цены в сигмах от скользящего среднего");
        help(sb, "hft_tick_count_total", "Сделок получено по инструменту");

        for (ExchangeGateway gw : controller.active().values()) {
            String ex = gw.id();

            line(sb, "hft_connected", ex, gw.isConnected() ? 1 : 0);
            line(sb, "hft_messages_total", ex, gw.messageCount());
            line(sb, "hft_kill_switch", ex, gw.risk().isStopped() ? 1 : 0);
            line(sb, "hft_orders_accepted_total", ex, gw.risk().acceptedCount());
            line(sb, "hft_orders_rejected_total", ex, gw.risk().rejectedCount());
            line(sb, "hft_daily_pnl_quote", ex, gw.risk().dailyPnl());
            line(sb, "hft_open_positions", ex, gw.strategy().openPositions());
            line(sb, "hft_strategy_enabled", ex, gw.strategy().isEnabled() ? 1 : 0);

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

    private void help(StringBuilder sb, String name, String description) {
        sb.append("# HELP ").append(name).append(' ').append(description).append('\n');
        sb.append("# TYPE ").append(name).append(" gauge\n");
    }

    private void line(StringBuilder sb, String name, String exchange, double value) {
        sb.append(name).append("{exchange=\"").append(exchange).append("\"} ")
          .append(formatNumber(value)).append('\n');
    }

    private void lineSymbol(StringBuilder sb, String name, String exchange, String symbol, double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)) return;
        sb.append(name).append("{exchange=\"").append(exchange)
          .append("\",symbol=\"").append(symbol).append("\"} ")
          .append(formatNumber(value)).append('\n');
    }

    private String formatNumber(double v) {
        if (v == Math.floor(v) && !Double.isInfinite(v)) return String.valueOf((long) v);
        return String.valueOf(v);
    }
}
