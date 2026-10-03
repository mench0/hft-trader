package com.hft.admin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hft.config.AppConfig;
import com.hft.control.BotController;
import com.hft.exchange.ExchangeGateway;
import com.hft.metrics.PrometheusExporter;
import com.hft.persistence.PersistedState;
import com.hft.store.OrderBook;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * HTTP-управление ботом целиком — от выбора бирж и тикеров до старта
 * торговли и изменения любого параметра риска. Предназначена для того,
 * чтобы отдельный проект (веб-панель) мог полностью настроить и
 * запустить бота, не трогая конфигурационные файлы на сервере.
 *
 * Жизненный цикл через API:
 *   1. GET  /control/status                          — что выбрано, запущен ли бот
 *   2. POST /control/select?exchange=binance&symbols=BTCUSDT,ETHUSDT
 *   3. POST /control/select?exchange=bybit&symbols=BTCUSDT
 *   4. POST /control/deselect?exchange=bybit
 *   5. POST /risk?maxPositionQuote=50&maxDailyLossQuote=20&...
 *   6. POST /control/start                            — поднять WS/REST для выбранных бирж
 *   7. POST /trading/start                             — включить реальную отправку ордеров
 *   8. POST /trading/stop / /control/stop / /trading/panic
 *
 * Данные (только пока бот запущен, т.е. после /control/start):
 *   GET /market?exchange=binance
 *   GET /balances?exchange=binance
 *   GET /status
 *
 * CORS открыт для всех источников — панель управления живёт отдельным
 * проектом на другом порту/домене и обращается сюда через fetch().
 */
public final class AdminServer {

    private static final Logger log = LoggerFactory.getLogger(AdminServer.class);

    private final AppConfig config;
    private final BotController controller;
    private final ObjectMapper mapper = new ObjectMapper();
    private final PrometheusExporter prometheus = new PrometheusExporter();

    private HttpServer server;

    public AdminServer(AppConfig config, BotController controller) {
        this.config = config;
        this.controller = controller;
    }

    public void start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(config.adminPort()), 0);
        server.setExecutor(Executors.newFixedThreadPool(4));

        // Управление выбором и жизненным циклом
        route("/control/status", this::handleControlStatus);
        route("/control/select", this::handleSelect);
        route("/control/deselect", this::handleDeselect);
        route("/control/symbols", this::handleControlSymbols);
        route("/control/start", this::handleControlStart);
        route("/control/stop", this::handleControlStop);
        route("/control/exchanges", this::handleSupportedExchanges);
        route("/exchanges/catalog", this::handleCatalog);
        route("/exchanges/request-stats", this::handleRequestStats);
        route("/discovery", this::handleDiscovery);
        route("/discovery/refresh", this::handleDiscoveryRefresh);
        route("/control/autostart", this::handleAutoStart);

        // Параметры — всё, что можно менять без пересборки
        route("/risk", this::handleRisk);
        route("/strategy/params", this::handleStrategyParams);

        // Торговля поверх уже запущенных подключений
        route("/trading/start", this::handleTradingStart);
        route("/trading/stop", this::handleTradingStop);
        route("/trading/panic", this::handlePanic);

        // Данные и статус
        route("/status", this::handleStatus);
        route("/market", this::handleMarket);
        route("/balances", this::handleBalances);
        route("/order", this::handleOrder);
        route("/cancel-all", this::handleCancelAll);

        // Метрики для Grafana (через Prometheus как data source)
        route("/metrics", this::handleMetrics);

        server.start();
        log.info("Админка запущена на порту {}{}", config.adminPort(),
                config.adminToken().isBlank() ? " (без токена)" : " (с токеном)");
    }

    public void stop() {
        if (server != null) server.stop(1);
    }

    private void route(String path, Handler handler) {
        server.createContext(path, exchange -> {
            try {
                setCors(exchange);
                if ("OPTIONS".equalsIgnoreCase(exchange.getRequestMethod())) {
                    exchange.sendResponseHeaders(204, -1);
                    return;
                }
                if (!authorized(exchange)) {
                    send(exchange, 401, error("Неверный или отсутствующий токен"));
                    return;
                }
                handler.handle(exchange);
            } catch (IllegalStateException | IllegalArgumentException e) {
                send(exchange, 400, error(e.getMessage()));
            } catch (Exception e) {
                log.error("Ошибка в эндпоинте {}", path, e);
                send(exchange, 500, error(e.getMessage()));
            } finally {
                exchange.close();
            }
        });
    }

    private void setCors(HttpExchange ex) {
        ex.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        ex.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
        ex.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type, X-Admin-Token");
    }

    @FunctionalInterface
    private interface Handler {
        void handle(HttpExchange exchange) throws Exception;
    }

    private boolean authorized(HttpExchange ex) {
        String token = config.adminToken();
        if (token.isBlank()) return true;
        String custom = ex.getRequestHeaders().getFirst("X-Admin-Token");
        if (token.equals(custom)) return true;
        String auth = ex.getRequestHeaders().getFirst("Authorization");
        return auth != null && auth.equals("Bearer " + token);
    }

    // ======================= ВЫБОР И ЗАПУСК =======================

    /** GET /discovery — тикеры, подходящие под каждую стратегию (с причинами и бэктестом), и статус бирж. */
    private void handleDiscovery(HttpExchange ex) throws IOException {
        send(ex, 200, mapper.valueToTree(controller.discovery().result()));
    }

    /** POST /discovery/refresh — пересчитать сейчас (в фоне). */
    private void handleDiscoveryRefresh(HttpExchange ex) throws IOException {
        requirePost(ex);
        boolean started = controller.discovery().refreshAsync();
        ObjectNode r = mapper.createObjectNode();
        r.put("started", started);
        r.put("message", started ? "Пересчёт запущен" : "Пересчёт уже идёт");
        send(ex, 202, r);
    }

    /** GET /exchanges/catalog — все биржи с типом, комиссиями, лимитами и статусом адаптера. */
    private void handleCatalog(HttpExchange ex) throws IOException {
        ObjectNode root = mapper.createObjectNode();
        ArrayNode list = root.putArray("exchanges");
        for (var i : com.hft.exchange.catalog.ExchangeCatalog.all()) {
            ObjectNode n = list.addObject();
            n.put("id", i.id());
            n.put("title", i.title());
            n.put("kind", i.kind().name());
            n.put("adapter", i.adapter().name());
            n.put("makerFeePct", i.makerFeePct());
            n.put("takerFeePct", i.takerFeePct());
            n.put("maxRequestsPerSec", i.maxRequestsPerSec());
            n.put("defaultQuote", i.defaultQuote());
            n.put("symbolHint", i.symbolHint());
            n.put("notes", i.notes());
            n.put("selectable", i.adapter() != com.hft.exchange.catalog.ExchangeInfo.Adapter.NOT_IMPLEMENTED);
        }
        send(ex, 200, root);
    }

    /** GET /exchanges/request-stats — время ответа, ошибки и паузы по лимитам для запущенных бирж. */
    private void handleRequestStats(HttpExchange ex) throws IOException {
        ObjectNode root = mapper.createObjectNode();
        for (var gw : controller.resolveTargets("all")) {
            if (gw instanceof com.hft.exchange.generic.RequestStatsSource pe) {
                root.set(gw.id(), mapper.valueToTree(pe.requestStats()));
            }
        }
        send(ex, 200, root);
    }

    private void handleSupportedExchanges(HttpExchange ex) throws IOException {
        ObjectNode root = mapper.createObjectNode();
        ArrayNode list = root.putArray("поддерживаемые");
        controller.supportedExchanges().forEach(list::add);
        send(ex, 200, root);
    }

    private void handleControlStatus(HttpExchange ex) throws IOException {
        ObjectNode root = mapper.createObjectNode();
        root.put("запущен", controller.isRunning());
        ObjectNode sel = root.putObject("выбрано");
        controller.selection().forEach((exId, symbols) -> {
            ArrayNode arr = sel.putArray(exId);
            symbols.forEach(arr::add);
        });
        ArrayNode supported = root.putArray("поддерживаемые_биржи");
        controller.supportedExchanges().forEach(supported::add);
        root.put("торговля_включена", config.tradingEnabled());
        send(ex, 200, root);
    }

    /** POST /control/select?exchange=binance&symbols=BTCUSDT,ETHUSDT */
    private void handleSelect(HttpExchange ex) throws IOException {
        requirePost(ex);
        Map<String, String> q = query(ex);
        String exchangeId = require(q, "exchange");
        String symbolsRaw = require(q, "symbols");
        List<String> symbols = Arrays.stream(symbolsRaw.split(","))
                .map(String::trim).filter(s -> !s.isEmpty()).map(String::toUpperCase).toList();

        controller.selectExchange(exchangeId, symbols);
        send(ex, 200, ok("Биржа " + exchangeId + " выбрана с символами " + symbols));
    }

    /** POST /control/deselect?exchange=bybit */
    private void handleDeselect(HttpExchange ex) throws IOException {
        requirePost(ex);
        String exchangeId = require(query(ex), "exchange");
        controller.deselectExchange(exchangeId);
        send(ex, 200, ok("Биржа " + exchangeId + " убрана из выбора"));
    }

    /** POST /control/symbols?exchange=binance&add=BNBUSDT  или  &remove=BNBUSDT */
    private void handleControlSymbols(HttpExchange ex) throws IOException {
        requirePost(ex);
        Map<String, String> q = query(ex);
        String exchangeId = require(q, "exchange");
        if (q.containsKey("add")) controller.addSymbol(exchangeId, q.get("add"));
        if (q.containsKey("remove")) controller.removeSymbol(exchangeId, q.get("remove"));

        ObjectNode root = mapper.createObjectNode();
        ArrayNode arr = root.putArray(exchangeId);
        controller.selection().getOrDefault(exchangeId, List.of()).forEach(arr::add);
        send(ex, 200, root);
    }

    /** POST /control/autostart?enabled=true&trade=false
     *  Если enabled=true — при следующем запуске процесса бот сам поднимет
     *  соединения для выбранных бирж без ручного /control/start.
     *  trade=true дополнительно сразу включит торговлю (осторожно). */
    private void handleAutoStart(HttpExchange ex) throws IOException {
        requirePost(ex);
        Map<String, String> q = query(ex);
        boolean enabled = Boolean.parseBoolean(q.getOrDefault("enabled", "false"));
        boolean trade = Boolean.parseBoolean(q.getOrDefault("trade", "false"));
        controller.setAutoStart(enabled, trade);
        send(ex, 200, ok("autoStart=" + enabled + " autoTrade=" + trade));
    }

    /** GET /control/start — поднять соединения для всех выбранных бирж. */
    private void handleControlStart(HttpExchange ex) throws Exception {
        requirePost(ex);
        controller.start();
        send(ex, 200, ok("Бот запущен. Активные биржи: " + controller.active().keySet()));
    }

    /** POST /control/stop — разорвать все соединения, выбор сохраняется. */
    private void handleControlStop(HttpExchange ex) throws IOException {
        requirePost(ex);
        controller.stop();
        send(ex, 200, ok("Бот остановлен, выбор бирж/символов сохранён"));
    }

    // ======================= ПАРАМЕТРЫ РИСКА =======================

    /**
     * GET  /risk               — текущие значения
     * POST /risk?maxPositionQuote=50&maxDailyLossQuote=20&maxSlippagePercent=0.2
     *            &feeReservePercent=0.2&maxOrdersPerMinute=20&tradingEnabled=true
     * Любой параметр можно передать отдельно, остальные не меняются.
     */
    private void handleRisk(HttpExchange ex) throws IOException {
        if ("POST".equalsIgnoreCase(ex.getRequestMethod())) {
            Map<String, String> q = query(ex);
            if (q.containsKey("maxPositionQuote")) config.setMaxPositionQuote(Double.parseDouble(q.get("maxPositionQuote")));
            if (q.containsKey("maxDailyLossQuote")) config.setMaxDailyLossQuote(Double.parseDouble(q.get("maxDailyLossQuote")));
            if (q.containsKey("maxSlippagePercent")) config.setMaxSlippagePercent(Double.parseDouble(q.get("maxSlippagePercent")));
            if (q.containsKey("feeReservePercent")) config.setFeeReservePercent(Double.parseDouble(q.get("feeReservePercent")));
            if (q.containsKey("maxOrdersPerMinute")) config.setMaxOrdersPerMinute(Integer.parseInt(q.get("maxOrdersPerMinute")));
            if (q.containsKey("tradingEnabled")) config.setTradingEnabled(Boolean.parseBoolean(q.get("tradingEnabled")));
            controller.persistNow();
            log.info("Параметры риска изменены через админку: {}", q);
        }

        ObjectNode root = mapper.createObjectNode();
        root.put("maxPositionQuote", config.maxPositionQuote());
        root.put("maxDailyLossQuote", config.maxDailyLossQuote());
        root.put("maxSlippagePercent", config.maxSlippagePercent());
        root.put("feeReservePercent", config.feeReservePercent());
        root.put("maxOrdersPerMinute", config.maxOrdersPerMinute());
        root.put("tradingEnabled", config.tradingEnabled());
        send(ex, 200, root);
    }

    private void handleStrategyParams(HttpExchange ex) throws IOException {
        Map<String, String> q = query(ex);
        ExchangeGateway gw = resolveActive(q);
        String exId = gw.id();
        var strategy = gw.strategy();

        // Текущие значения — база, поверх которой применяем то, что прислали
        var current = controller.strategyParamsFor(exId);
        double entryZ = q.containsKey("entryZ") ? Double.parseDouble(q.get("entryZ")) : current.entryZ();
        double exitZ = q.containsKey("exitZ") ? Double.parseDouble(q.get("exitZ")) : current.exitZ();
        double stopLoss = q.containsKey("stopLoss") ? Double.parseDouble(q.get("stopLoss")) : current.stopLossPercent();
        double minImb = q.containsKey("minImbalance") ? Double.parseDouble(q.get("minImbalance")) : current.minImbalance();
        double orderQuote = q.containsKey("orderQuote") ? Double.parseDouble(q.get("orderQuote")) : current.orderQuote();

        var updated = new PersistedState.StrategyParams(entryZ, exitZ, stopLoss, minImb, orderQuote);
        controller.applyStrategyParams(exId, updated); // применяет к живой стратегии и сохраняет на диск

        ObjectNode currentNode = mapper.createObjectNode();
        currentNode.put("entryZ", strategy.entryZ());
        currentNode.put("exitZ", strategy.exitZ());
        currentNode.put("stopLossPercent", strategy.stopLossPercent());
        currentNode.put("minImbalance", strategy.minImbalance());
        currentNode.put("orderQuote", strategy.orderQuote());

        ObjectNode root = mapper.createObjectNode();
        root.put("биржа", exId);
        root.set("текущие", currentNode);
        send(ex, 200, root);
    }

    // ======================= ТОРГОВЛЯ =======================

    private void handleTradingStart(HttpExchange ex) throws IOException {
        requirePost(ex);
        String target = query(ex).getOrDefault("exchange", "all");
        controller.startTrading(target);
        send(ex, 200, ok("Торговля включена"));
    }

    private void handleTradingStop(HttpExchange ex) throws IOException {
        requirePost(ex);
        String target = query(ex).getOrDefault("exchange", "all");
        controller.stopTrading(target);
        send(ex, 200, ok("Торговля остановлена"));
    }

    private void handlePanic(HttpExchange ex) throws IOException {
        requirePost(ex);
        config.setTradingEnabled(false);
        controller.persistNow();
        for (ExchangeGateway gw : controller.resolveTargets(query(ex).getOrDefault("exchange", "all"))) {
            gw.strategy().disable();
            gw.orders().panicClose("Аварийная остановка через админку");
        }
        send(ex, 200, ok("Все ордера отменены, торговля остановлена"));
    }

    // ======================= ДАННЫЕ =======================

    private void handleStatus(HttpExchange ex) throws IOException {
        ObjectNode root = mapper.createObjectNode();
        root.put("бот_запущен", controller.isRunning());
        root.put("торговля_включена", config.tradingEnabled());
        for (var entry : controller.active().entrySet()) {
            ExchangeGateway gw = entry.getValue();
            ObjectNode node = root.putObject(entry.getKey());
            node.put("подключение", gw.isConnected());
            node.put("получено_сообщений", gw.messageCount());
            node.put("kill_switch", gw.risk().isStopped());
            node.put("дневной_pnl", round(gw.risk().dailyPnl(), 4));
            node.put("ордеров_принято", gw.risk().acceptedCount());
            node.put("ордеров_отклонено", gw.risk().rejectedCount());
            node.put("последний_отказ", gw.risk().lastRejectReason());
            node.put("стратегия_активна", gw.strategy().isEnabled());
            node.put("открытых_позиций", gw.strategy().openPositions());
            ArrayNode syms = node.putArray("символы");
            gw.symbols().forEach(syms::add);
        }
        send(ex, 200, root);
    }

    private void handleMarket(HttpExchange ex) throws IOException {
        ExchangeGateway gw = resolveActive(query(ex));
        var market = gw.marketData();
        ObjectNode root = mapper.createObjectNode();
        for (String symbol : market.symbols()) {
            OrderBook book = market.book(symbol);
            var st = market.stats(symbol);
            var window = market.window(symbol);
            ObjectNode node = root.putObject(symbol);
            if (book != null && book.isReady()) {
                node.put("лучший_бид", round(book.bestBid(), 8));
                node.put("лучший_аск", round(book.bestAsk(), 8));
                node.put("середина", round(book.midPrice(), 8));
                node.put("спред_проц", round(book.spreadPercent(), 4));
                node.put("дисбаланс", round(book.imbalance(5), 4));
                node.put("возраст_мс", book.ageMs());
            } else {
                node.put("состояние", "стакан не готов");
            }
            if (st != null) {
                node.put("последняя_цена", round(st.lastPrice(), 8));
                node.put("тиков", st.tickCount());
            }
            if (window != null && window.isWarmedUp()) {
                node.put("z_score", round(window.currentZScore(), 4));
                node.put("изменение_проц", round(window.changePercent(), 4));
            }
        }
        send(ex, 200, root);
    }

    private void handleBalances(HttpExchange ex) throws IOException {
        ExchangeGateway gw = resolveActive(query(ex));
        ObjectNode root = mapper.createObjectNode();
        ObjectNode free = root.putObject("свободно");
        gw.balances().snapshot().forEach((asset, amount) -> {
            if (amount > 0) free.put(asset, round(amount, 8));
        });
        send(ex, 200, root);
    }

    private void handleOrder(HttpExchange ex) throws IOException {
        requirePost(ex);
        Map<String, String> q = query(ex);
        ExchangeGateway gw = resolveActive(q);
        var orders = gw.orders();

        String symbol = require(q, "symbol");
        String side = q.getOrDefault("side", "BUY").toUpperCase();
        String type = q.getOrDefault("type", "MARKET").toUpperCase();
        boolean all = Boolean.parseBoolean(q.getOrDefault("all", "false"));
        double portion = Double.parseDouble(q.getOrDefault("portion", "0"));
        double qty = Double.parseDouble(q.getOrDefault("qty", "0"));
        double price = Double.parseDouble(q.getOrDefault("price", "0"));

        var result = switch (type + "-" + side + "-" + (all ? "ALL" : portion > 0 ? "PORTION" : "QTY")) {
            case "MARKET-BUY-ALL" -> orders.buyMarketAll(symbol);
            case "MARKET-SELL-ALL" -> orders.sellMarketAll(symbol);
            case "MARKET-BUY-PORTION" -> orders.buyMarketPortion(symbol, portion);
            case "MARKET-SELL-PORTION" -> orders.sellMarketPortion(symbol, portion);
            case "MARKET-BUY-QTY" -> orders.buyMarket(symbol, qty);
            case "MARKET-SELL-QTY" -> orders.sellMarket(symbol, qty);
            case "LIMIT-BUY-ALL" -> orders.buyLimitAll(symbol, price);
            case "LIMIT-SELL-ALL" -> orders.sellLimitAll(symbol, price);
            case "LIMIT-BUY-QTY" -> orders.buyLimit(symbol, qty, price);
            case "LIMIT-SELL-QTY" -> orders.sellLimit(symbol, qty, price);
            default -> null;
        };
        if (result == null) { send(ex, 400, error("Неподдерживаемая комбинация параметров")); return; }

        ObjectNode root = mapper.createObjectNode();
        root.put("биржа", gw.id());
        root.put("orderId", result.orderId());
        root.put("статус", result.status());
        root.put("исполнено", round(result.executedQty(), 8));
        root.put("средняя_цена", round(result.avgPrice(), 8));
        send(ex, 200, root);
    }

    private void handleCancelAll(HttpExchange ex) throws IOException {
        requirePost(ex);
        Map<String, String> q = query(ex);
        ExchangeGateway gw = resolveActive(q);
        int count = gw.orders().cancelAll(require(q, "symbol"));
        send(ex, 200, ok("Отменено ордеров: " + count));
    }

    /** GET /metrics — текстовый формат Prometheus exposition, не JSON. */
    private void handleMetrics(HttpExchange ex) throws IOException {
        String body = prometheus.render(controller);
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        ex.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
        ex.sendResponseHeaders(200, bytes.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(bytes); }
    }

    // ======================= СЛУЖЕБНОЕ =======================

    private ExchangeGateway resolveActive(Map<String, String> q) {
        var active = controller.active();
        if (active.isEmpty()) {
            throw new IllegalStateException("Бот не запущен. Сначала POST /control/start");
        }
        String id = q.getOrDefault("exchange", active.keySet().iterator().next());
        ExchangeGateway gw = active.get(id);
        if (gw == null) {
            throw new IllegalArgumentException("Биржа не активна: " + id + ". Активны: " + active.keySet());
        }
        return gw;
    }

    private String require(Map<String, String> q, String key) {
        String v = q.get(key);
        if (v == null || v.isBlank()) throw new IllegalArgumentException("Не указан обязательный параметр: " + key);
        return v;
    }

    private void requirePost(HttpExchange ex) throws IOException {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
            send(ex, 405, error("Требуется POST"));
            throw new IOException("Метод не POST");
        }
    }

    private static Map<String, String> query(HttpExchange ex) {
        Map<String, String> result = new HashMap<>();
        String raw = ex.getRequestURI().getRawQuery();
        if (raw == null || raw.isEmpty()) return result;
        for (String pair : raw.split("&")) {
            int eq = pair.indexOf('=');
            if (eq > 0) {
                result.put(
                        java.net.URLDecoder.decode(pair.substring(0, eq), StandardCharsets.UTF_8),
                        java.net.URLDecoder.decode(pair.substring(eq + 1), StandardCharsets.UTF_8));
            }
        }
        return result;
    }

    private ObjectNode ok(String message) {
        ObjectNode n = mapper.createObjectNode();
        n.put("результат", "успех");
        n.put("сообщение", message);
        return n;
    }

    private ObjectNode error(String message) {
        ObjectNode n = mapper.createObjectNode();
        n.put("результат", "ошибка");
        n.put("сообщение", message == null ? "неизвестная ошибка" : message);
        return n;
    }

    private void send(HttpExchange ex, int code, ObjectNode body) throws IOException {
        byte[] bytes = mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(body);
        ex.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        ex.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(bytes); }
    }

    private static double round(double v, int scale) {
        if (Double.isNaN(v) || Double.isInfinite(v)) return 0;
        double factor = Math.pow(10, scale);
        return Math.round(v * factor) / factor;
    }
}
