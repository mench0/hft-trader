package com.hft;

import com.hft.admin.AdminServer;
import com.hft.config.AppConfig;
import com.hft.control.BotController;
import com.hft.exchange.ExchangeGateway;
import com.hft.exchange.binance.BinanceExchange;
import com.hft.persistence.SqliteStateStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Точка входа.
 *
 * Ничего не подключается к бирже автоматически, кроме одного случая:
 * если через админку раньше был включён автозапуск
 * (POST /control/autostart?enabled=true), то при рестарте процесса
 * биржи, выбранные на момент последнего сохранения состояния, поднимутся
 * сами. Все настройки (выбор бирж/тикеров, параметры риска и стратегии)
 * читаются из {@code data/state.json} — файл создаётся и обновляется
 * автоматически при любом изменении через API.
 *
 * Без автозапуска — как и раньше, приложение просто поднимает админку
 * и ждёт команд.
 */
public final class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws Exception {
        printBanner();

        AppConfig config = AppConfig.load();
        SqliteStateStore stateStore = new SqliteStateStore();
        BotController controller = new BotController(config, stateStore);

        if (!config.adminEnabled()) {
            throw new IllegalStateException(
                    "Админка выключена (admin.enabled=false), а без неё некому запустить биржи.");
        }

        AdminServer admin = new AdminServer(config, controller);
        admin.start();

        // Сначала — подбор тикеров: сводки всех бирж прогоняются через стратегии,
        // админка показывает подходящие тикеры под каждой стратегией (GET /discovery)
        controller.discovery().start();

        // ---------- Автозапуск, если он был включён в прошлой сессии ----------
        if (controller.autoStart()) {
            log.info("Обнаружен autoStart=true в сохранённом состоянии — поднимаю биржи автоматически");
            try {
                controller.start();
                if (controller.autoTrade()) {
                    log.warn("autoTrade=true — торговля включается автоматически без ручного подтверждения");
                    controller.startTrading("all");
                }
            } catch (Exception e) {
                log.error("Автозапуск не удался, бот остался в режиме настройки: {}", e.getMessage());
            }
        }

        // ---------- Фоновые задачи ----------
        ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2, r -> {
            Thread t = new Thread(r, "scheduler");
            t.setDaemon(true);
            return t;
        });

        scheduler.scheduleAtFixedRate(() -> {
            for (ExchangeGateway gw : controller.active().values()) {
                if (gw instanceof BinanceExchange be) be.syncTime();
            }
        }, 30, 30, TimeUnit.MINUTES);

        scheduler.scheduleAtFixedRate(() -> {
            for (ExchangeGateway gw : controller.active().values()) {
                gw.syncBalances();
            }
        }, 5, 5, TimeUnit.MINUTES);

        scheduler.scheduleAtFixedRate(() -> {
            if (!controller.isRunning()) return;
            var active = controller.active();
            log.info("--- Состояние ({} бирж активно) ---", active.size());
            for (ExchangeGateway gw : active.values()) {
                log.info("[{}] соединение={} сообщений={} принято={} отклонено={} pnl={}",
                        gw.id(), gw.isConnected() ? "активно" : "разорвано", gw.messageCount(),
                        gw.risk().acceptedCount(), gw.risk().rejectedCount(),
                        String.format("%.4f", gw.risk().dailyPnl()));
            }
        }, 60, 60, TimeUnit.SECONDS);

        // ---------- Корректная остановка ----------
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Остановка...");
            for (ExchangeGateway gw : controller.active().values()) {
                gw.strategy().disable();
                if (config.tradingEnabled()) {
                    try { gw.strategy().closeAll(); }
                    catch (Exception e) { log.error("[{}] Не удалось закрыть позиции", gw.id(), e); }
                }
            }
            admin.stop();
            controller.stop();
            scheduler.shutdownNow();
            log.info("Остановлено");
        }));

        log.info("===============================================");
        log.info("Файл состояния: {}", stateStore.filePath().toAbsolutePath());
        log.info("Бот {}. Биржи: {}",
                controller.isRunning() ? "запущен автоматически" : "в режиме настройки",
                controller.isRunning() ? controller.active().keySet() : controller.selection().keySet());
        log.info("Админка: http://localhost:{}/control/status", config.adminPort());
        log.info("Метрики для Grafana/Prometheus: http://localhost:{}/metrics", config.adminPort());
        if (!controller.isRunning()) {
            log.info("");
            log.info("Дальше через API или веб-панель:");
            log.info("  1) POST /control/select?exchange=binance&symbols=BTCUSDT,ETHUSDT");
            log.info("  2) POST /control/start          — поднять соединения");
            log.info("  3) POST /trading/start          — включить реальные ордера");
            log.info("  Чтобы это переживало рестарт процесса:");
            log.info("  4) POST /control/autostart?enabled=true&trade=true");
        }
        log.info("===============================================");
        log.info("Ctrl+C для остановки");

        Thread.currentThread().join();
    }

    private static void printBanner() {
        System.out.println("""
                ┌─────────────────────────────────────┐
                │  HFT Trader — API + персистентность  │
                │  Netty + Disruptor + in-memory       │
                └─────────────────────────────────────┘
                """);
    }
}
