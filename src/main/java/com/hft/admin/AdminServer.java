package com.hft.admin;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.hft.config.AppConfig;
import com.hft.control.BotController;
import com.hft.exchange.ExchangeGateway;
import com.hft.metrics.PrometheusExporter;
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
 *   5. POST /exchange/params?exchange=binance&maxPositionQuote=50&entryZ=2.5&...
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

    /** Логгер. */
    private static final Logger log = LoggerFactory.getLogger(AdminServer.class);

    /** Порт и токен админки. */
    private final AppConfig config;
    /** Управление ботом. */
    private final BotController controller;
    /** Сборка JSON-ответов. */
    private final ObjectMapper mapper = new ObjectMapper();
    /** Метрики для /metrics. */
    private final PrometheusExporter prometheus = new PrometheusExporter();

    /** HTTP-сервер JDK. */
    private HttpServer server;

    /**
     * @param config порт и токен
     * @param controller управление ботом
     */
    public AdminServer(AppConfig config, BotController controller) {
        this.config = config;
        this.controller = controller;
    }

    /** Зарегистрировать все эндпоинты и запустить сервер. */
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
        route("/exchange/params/schema", this::handleParamsSchema);
        route("/exchange/params", this::handleExchangeParams);
        route("/settings", this::handleSettings);
        route("/strategies", this::handleStrategies);
        route("/positions", this::handlePositions);
        route("/positions/close", this::handleClosePosition);
        route("/funding", this::handleFunding);

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

        // Переменные окружения (.env): ключи бирж, токен, порт
        route("/env", this::handleEnv);

        // Метрики для Grafana (через Prometheus как data source)
        route("/metrics", this::handleMetrics);

        server.start();
        log.info("Админка запущена на порту {}{}", config.adminPort(),
                config.adminToken().isBlank() ? " (без токена)" : " (с токеном)");
    }

    /** Остановить сервер. */
    public void stop() {
        if (server != null) server.stop(1);
    }

    /** Зарегистрировать обработчик: CORS, OPTIONS, проверка токена, ошибки 400/500 в JSON. */
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

    /** CORS для веб-панели на другом домене. */
    private void setCors(HttpExchange ex) {
        ex.getResponseHeaders().set("Access-Control-Allow-Origin", "*");
        ex.getResponseHeaders().set("Access-Control-Allow-Methods", "GET, POST, OPTIONS");
        ex.getResponseHeaders().set("Access-Control-Allow-Headers", "Content-Type, X-Admin-Token, Authorization");
    }

    /** Обработчик одного эндпоинта. */
    @FunctionalInterface
    private interface Handler {
        /** Обработать запрос. */
        void handle(HttpExchange exchange) throws Exception;
    }

    /** Токен из X-Admin-Token или Authorization: Bearer; пустой токен в конфиге — без проверки. */
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
        var d = controller.discovery();
        if (d == null) { send(ex, 200, ok("Подбор тикеров выключен (POST /settings?discoveryEnabled=true)")); return; }
        send(ex, 200, mapper.valueToTree(d.result()));
    }

    /** POST /discovery/refresh — пересчитать сейчас (в фоне). */
    private void handleDiscoveryRefresh(HttpExchange ex) throws IOException {
        requirePost(ex);
        var d = controller.discovery();
        if (d == null) { send(ex, 400, error("Подбор тикеров выключен (POST /settings?discoveryEnabled=true)")); return; }
        boolean started = d.refreshAsync();
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
            ArrayNode markets = n.putArray("markets");             // какие значения параметра market допустимы
            if (com.hft.exchange.catalog.ExchangeCatalog.supportsPerp(i.id())) markets.add("perp");
            if (com.hft.exchange.catalog.ExchangeCatalog.supportsSpot(i.id())) markets.add("spot");
            com.hft.exchange.catalog.ExchangeCatalog.perp(i.id()).ifPresent(v -> {
                n.put("perpMakerFeePct", v.makerFeePct());
                n.put("perpTakerFeePct", v.takerFeePct());
            });
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

    /** GET /control/exchanges — биржи, которые можно выбрать. */
    private void handleSupportedExchanges(HttpExchange ex) throws IOException {
        ObjectNode root = mapper.createObjectNode();
        ArrayNode list = root.putArray("поддерживаемые");
        controller.supportedExchanges().forEach(list::add);
        send(ex, 200, root);
    }

    /** GET /control/status — запущен ли бот, выбор, поддерживаемые биржи, торговля по биржам. */
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
        ObjectNode trading = root.putObject("торговля_включена");
        controller.selection().keySet().forEach(exId -> trading.put(exId, controller.params(exId).tradingEnabled()));
        send(ex, 200, root);
    }

    /**
     * POST /control/select?exchange=binance&symbols=BTCUSDT,ETHUSDT[&testnet=false&live=true&maxPositionQuote=50…]
     * Выбрать биржу и сразу (необязательно) задать её параметры — любые из GET /exchange/params/schema.
     */
    private void handleSelect(HttpExchange ex) throws IOException {
        requirePost(ex);
        Map<String, String> q = query(ex);
        String exchangeId = require(q, "exchange");
        String symbolsRaw = require(q, "symbols");
        List<String> symbols = Arrays.stream(symbolsRaw.split(","))
                .map(String::trim).filter(s -> !s.isEmpty()).map(String::toUpperCase).toList();
        Map<String, String> params = paramsOnly(q);
        controller.selectExchange(exchangeId, symbols, params);
        ObjectNode root = paramsNode(exchangeId);
        root.put("сообщение", "Биржа " + exchangeId + " выбрана с символами " + symbols);
        send(ex, 200, root);
    }

    /** Из запроса — только ключи параметров биржи; неизвестный ключ — ошибка (опечатка не должна пройти молча). */
    private static Map<String, String> paramsOnly(Map<String, String> q) {
        Map<String, String> out = new HashMap<>(q);
        out.remove("exchange");
        out.remove("symbols");
        var known = com.hft.config.TradingParams.SPECS.keySet();
        for (String k : out.keySet()) {
            if (!known.contains(k)) throw new IllegalArgumentException("Неизвестный параметр: " + k + ". Список: GET /exchange/params/schema");
        }
        return out;
    }

    /** GET /exchange/params/schema — описание всех параметров биржи: по умолчанию, границы, перезапуск, справка. */
    private void handleParamsSchema(HttpExchange ex) throws IOException {
        send(ex, 200, mapper.valueToTree(com.hft.config.TradingParams.schema()));
    }

    /**
     * Настройки процесса (подбор тикеров, фоновые задачи, лимиты запросов).
     * GET /settings — значения и описание; POST /settings?discoveryRefreshMin=30&statusLogSec=120
     */
    private void handleSettings(HttpExchange ex) throws IOException {
        Map<String, String> q = query(ex);
        if ("POST".equalsIgnoreCase(ex.getRequestMethod())) {
            for (String k : q.keySet()) {
                if (!com.hft.config.GlobalParams.SPECS.containsKey(k))
                    throw new IllegalArgumentException("Неизвестная настройка: " + k + ". Доступны: " + com.hft.config.GlobalParams.SPECS.keySet());
            }
            controller.updateGlobal(q);
        }
        ObjectNode root = mapper.createObjectNode();
        root.set("настройки", mapper.valueToTree(controller.global()));
        root.set("описание", mapper.valueToTree(com.hft.config.GlobalParams.schema()));
        if (q.keySet().stream().anyMatch(com.hft.config.GlobalParams::requiresRestart))
            root.put("внимание", "rateLimitSafety применится после перезапуска процесса");
        send(ex, 200, root);
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

    // ======================= ТОРГОВЫЕ ПАРАМЕТРЫ =======================
    /**
     * Торговые параметры каждой биржи: риск, стратегия, глубина стакана
     * и размер ценового окна.
     *
     * <p>Endpoints:
     * <ul>
     *   <li>{@code GET /exchange/params} — параметры выбранных, запущенных
     *       и уже настроенных бирж.</li>
     *   <li>{@code GET /exchange/params?exchange=bybit} — параметры одной биржи.</li>
     *   <li>{@code POST /exchange/params?exchange=bybit&maxPositionQuote=50
     *       &maxDailyLossQuote=20&entryZ=2.5} — изменение параметров.</li>
     * </ul>
     *
     * <p>Любой параметр можно передать отдельно — остальные значения не меняются.
     * Если значение выходит за допустимый диапазон, возвращается {@code 400},
     * и настройки не изменяются.
     *
     * <p>Работающая биржа подхватывает новые значения на следующем тике.
     * Параметры {@code bookDepth} и {@code priceWindow} применяются после
     * выполнения {@code /control/stop} и {@code /control/start}.
     */
    private void handleExchangeParams(HttpExchange ex) throws IOException {
        Map<String, String> q = query(ex);
        String exId = q.get("exchange");
        if ("POST".equalsIgnoreCase(ex.getRequestMethod())) {
            if (exId == null || exId.isBlank()) throw new IllegalArgumentException("Не указан обязательный параметр: exchange");
            Map<String, String> updates = paramsOnly(q);
            controller.updateParams(exId, updates);
            ObjectNode root = paramsNode(exId);
            List<String> restart = updates.keySet().stream().filter(com.hft.config.TradingParams::requiresRestart).sorted().toList();
            if (controller.active().containsKey(exId) && !restart.isEmpty())
                root.put("внимание", restart + " применятся после /control/stop и /control/start");
            send(ex, 200, root);
            return;
        }
        if (exId != null && !exId.isBlank()) { send(ex, 200, paramsNode(exId)); return; }
        ObjectNode root = mapper.createObjectNode();
        for (String id : controller.selection().keySet()) root.set(id, paramsNode(id));   // параметры есть только у выбранных
        send(ex, 200, root);
    }

    /** Параметры биржи с признаками «выбрана» и «есть testnet». */
    private ObjectNode paramsNode(String exId) {
        ObjectNode n = mapper.createObjectNode();
        n.put("биржа", exId);
        n.put("выбрана", controller.isSelected(exId));
        var info = com.hft.exchange.catalog.ExchangeCatalog.find(exId);
        n.put("testnet_доступен", info.map(com.hft.exchange.catalog.ExchangeInfo::hasTestnet).orElse(false));
        if (!controller.isSelected(exId)) n.put("внимание", "биржа не выбрана: показаны значения по умолчанию; задать — POST /control/select?exchange=" + exId + "&symbols=…&параметр=значение");
        n.set("параметры", mapper.valueToTree(controller.params(exId)));
        return n;
    }

    /** GET /strategies?exchange=binance — состояние стратегий: треугольники, пары и их z-score, позиции, результат. */
    private void handleStrategies(HttpExchange ex) throws IOException {
        ExchangeGateway gw = resolveActive(query(ex));
        ObjectNode root = mapper.createObjectNode();
        root.put("биржа", gw.id());
        root.set("стратегии", mapper.valueToTree(gw.strategy().stats()));
        send(ex, 200, root);
    }

    /** GET /positions?exchange=… — позиции по фьючерсам, плечо, funding (все биржи с market=perp или одна). */
    private void handlePositions(HttpExchange ex) throws IOException {
        String target = query(ex).getOrDefault("exchange", "all");
        ObjectNode root = mapper.createObjectNode();
        for (var gw : controller.resolveTargets(target)) {
            if (gw.perp() != null) root.set(gw.id(), mapper.valueToTree(gw.perp().stats()));
        }
        send(ex, 200, root);
    }

    /** POST /positions/close?exchange=…&symbol=… — закрыть позицию рыночным reduceOnly-ордером (symbol=all — все). */
    private void handleClosePosition(HttpExchange ex) throws IOException {
        requirePost(ex);
        Map<String, String> q = query(ex);
        ExchangeGateway gw = resolveActive(q);
        if (gw.perp() == null) { send(ex, 400, error("Биржа " + gw.id() + " торгует спотом — позиций нет")); return; }
        String symbol = require(q, "symbol").toUpperCase();
        List<String> symbols = symbol.equals("ALL") ? List.copyOf(gw.perp().positions().snapshot().keySet()) : List.of(symbol);
        ArrayNode results = mapper.createArrayNode();
        for (String s : symbols) {
            var r = gw.orders().closePosition(s);
            ObjectNode n = results.addObject().put("symbol", s);
            if (r == null) n.put("status", "позиции нет");
            else n.put("status", r.status()).put("executedQty", r.executedQty()).put("avgPrice", r.avgPrice());
        }
        ObjectNode root = mapper.createObjectNode();
        root.put("биржа", gw.id());
        root.set("closed", results);
        send(ex, 200, root);
    }

    /** GET /funding — ставки funding по биржам и состояние funding-арбитража (пары, возможности). */
    private void handleFunding(HttpExchange ex) throws IOException {
        ObjectNode root = mapper.createObjectNode();
        ObjectNode rates = root.putObject("rates");
        for (var gw : controller.resolveTargets("all")) {
            if (gw.perp() != null) rates.set(gw.id(), mapper.valueToTree(gw.perp().stats().get("funding")));
        }
        var fa = controller.fundingArb();
        root.set("arbitrage", fa == null ? mapper.createObjectNode().put("running", false) : mapper.valueToTree(fa.stats()));
        send(ex, 200, root);
    }

    // ======================= ТОРГОВЛЯ =======================

    /** POST /trading/start?exchange=… — включить торговлю. */
    private void handleTradingStart(HttpExchange ex) throws IOException {
        requirePost(ex);
        String target = query(ex).getOrDefault("exchange", "all");
        controller.startTrading(target);
        send(ex, 200, ok("Торговля включена"));
    }

    /** POST /trading/stop?exchange=… — выключить торговлю. */
    private void handleTradingStop(HttpExchange ex) throws IOException {
        requirePost(ex);
        String target = query(ex).getOrDefault("exchange", "all");
        controller.stopTrading(target, "Остановлено через админку");
        send(ex, 200, ok("Торговля остановлена"));
    }

    /** POST /trading/panic?exchange=… — выключить торговлю и отменить все ордера. */
    private void handlePanic(HttpExchange ex) throws IOException {
        requirePost(ex);
        String target = query(ex).getOrDefault("exchange", "all");
        controller.stopTrading(target, "Аварийная остановка через админку");
        for (ExchangeGateway gw : controller.resolveTargets(target)) {
            gw.orders().panicClose("Аварийная остановка через админку");
        }
        send(ex, 200, ok("Все ордера отменены, торговля остановлена"));
    }

    // ======================= ДАННЫЕ =======================

    /** GET /status — состояние активных бирж: соединение, риск, стратегии, символы. */
    private void handleStatus(HttpExchange ex) throws IOException {
        ObjectNode root = mapper.createObjectNode();
        root.put("бот_запущен", controller.isRunning());
        for (var entry : controller.active().entrySet()) {
            ExchangeGateway gw = entry.getValue();
            ObjectNode node = root.putObject(entry.getKey());
            node.put("торговля_включена", controller.params(entry.getKey()).tradingEnabled());
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

    /** GET /market?exchange=… — верх стакана, спред, дисбаланс, z-score по символам. */
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

    /** GET /balances?exchange=… — свободные остатки. */
    private void handleBalances(HttpExchange ex) throws IOException {
        ExchangeGateway gw = resolveActive(query(ex));
        ObjectNode root = mapper.createObjectNode();
        ObjectNode free = root.putObject("свободно");
        gw.balances().snapshot().forEach((asset, amount) -> {
            if (amount > 0) free.put(asset, round(amount, 8));
        });
        send(ex, 200, root);
    }

    /** POST /order — ручной ордер: тип, сторона, объём, доля баланса или весь баланс. */
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

    /** POST /cancel-all?exchange=…&symbol=… — отменить ордера символа. */
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

    /** Переменные, которые читает бот: ключи бирж, их фразы, админка, база. */
    private List<String> knownEnvKeys() {
        List<String> keys = new java.util.ArrayList<>(List.of("ADMIN_TOKEN", "ADMIN_PORT", "ADMIN_ENABLED", "STATE_DB"));
        for (String id : controller.supportedExchanges()) {
            String p = id.toUpperCase();
            keys.add(p + "_API_KEY");
            keys.add(p + "_API_SECRET");
            keys.add(p + "_TESTNET");
            keys.add(p + "_LIVE");
        }
        keys.add("OKX_PASSPHRASE");
        keys.add("KUCOIN_PASSPHRASE");
        keys.add("KUCOIN_UTA");
        return keys;
    }

    /**
     * Управление переменными окружения через админский API.
     *
     * <p>Endpoints:
     * <ul>
     *   <li>
     *     {@code GET /env} — список заданных переменных и источник их значения.
     *     Значения секретов замаскированы.
     *   </li>
     *   <li>
     *     {@code POST /env?BINANCE_API_KEY=…&BINANCE_API_SECRET=…} —
     *     записать переменные в {@code .env}. Пустое значение удаляет переменную.
     *   </li>
     * </ul>
     *
     * <p>Применение изменений:
     * <ul>
     *   <li>
     *     Ключи бирж применяются при следующем {@code /control/start}.
     *   </li>
     *   <li>
     *     {@code ADMIN_*} и {@code STATE_DB} применяются после перезапуска процесса.
     *   </li>
     * </ul>
     */
    private void handleEnv(HttpExchange ex) throws IOException {
        Map<String, String> q = query(ex);
        ObjectNode root = mapper.createObjectNode();
        if ("POST".equalsIgnoreCase(ex.getRequestMethod())) {
            if (q.isEmpty()) throw new IllegalArgumentException("Не указаны переменные: POST /env?ИМЯ=значение");
            List<String> known = knownEnvKeys();
            for (String k : q.keySet())
                if (!known.contains(k)) throw new IllegalArgumentException("Неизвестная переменная: " + k + ". Доступны: " + known);
            com.hft.config.Env.set(q);
            boolean restart = q.keySet().stream().anyMatch(k -> k.startsWith("ADMIN_") || k.equals("STATE_DB")
                    || k.endsWith("_TESTNET") || k.endsWith("_LIVE"));
            root.put("сообщение", "Записано в .env: " + q.keySet() + (restart
                    ? ". ADMIN_*, STATE_DB и режим бирж (_TESTNET/_LIVE) применятся после перезапуска процесса; ключи — при следующем запуске соединений"
                    : ". Ключи применятся при следующем запуске соединений (Остановить → Поднять)"));
        }
        root.put("файл", com.hft.config.Env.file().toString());
        root.put("файл_есть", com.hft.config.Env.fileExists());
        var fileVars = com.hft.config.Env.fileVars();
        ObjectNode vars = root.putObject("переменные");
        for (String k : knownEnvKeys()) {
            String v = com.hft.config.Env.get(k);
            ObjectNode n = vars.putObject(k);
            n.put("задана", v != null);
            n.put("источник", v == null ? "" : fileVars.containsKey(k) ? ".env" : "окружение");
            n.put("значение", v == null ? "" : mask(k, v));
        }
        send(ex, 200, root);
    }

    /** Секреты не показываются: только первые символы ключа; порт и флаги — как есть. */
    private static String mask(String key, String v) {
        if (key.equals("ADMIN_PORT") || key.equals("ADMIN_ENABLED") || key.equals("STATE_DB")
                || key.endsWith("_TESTNET") || key.endsWith("_LIVE")) return v;
        if (key.endsWith("_API_KEY")) return v.substring(0, Math.min(4, v.length())) + "…";
        return "••••••";
    }

    // ======================= СЛУЖЕБНОЕ =======================

    /** Активная биржа из параметра exchange (по умолчанию первая); бот не запущен — IllegalStateException. */
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

    /** Обязательный параметр запроса или IllegalArgumentException. */
    private String require(Map<String, String> q, String key) {
        String v = q.get(key);
        if (v == null || v.isBlank()) throw new IllegalArgumentException("Не указан обязательный параметр: " + key);
        return v;
    }

    /** Только POST; иначе 405. */
    private void requirePost(HttpExchange ex) throws IOException {
        if (!"POST".equalsIgnoreCase(ex.getRequestMethod())) {
            send(ex, 405, error("Требуется POST"));
            throw new IOException("Метод не POST");
        }
    }

    /** Параметры строки запроса (URL-декодированные). */
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

    /** Ответ «успех» с сообщением. */
    private ObjectNode ok(String message) {
        ObjectNode n = mapper.createObjectNode();
        n.put("результат", "успех");
        n.put("сообщение", message);
        return n;
    }

    /** Ответ «ошибка» с сообщением. */
    private ObjectNode error(String message) {
        ObjectNode n = mapper.createObjectNode();
        n.put("результат", "ошибка");
        n.put("сообщение", message == null ? "неизвестная ошибка" : message);
        return n;
    }

    /** Отправить JSON с кодом ответа. */
    private void send(HttpExchange ex, int code, ObjectNode body) throws IOException {
        byte[] bytes = mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(body);
        ex.getResponseHeaders().set("Content-Type", "application/json; charset=utf-8");
        ex.sendResponseHeaders(code, bytes.length);
        try (OutputStream os = ex.getResponseBody()) { os.write(bytes); }
    }

    /** Округлить для ответа; NaN и бесконечность — 0. */
    private static double round(double v, int scale) {
        if (Double.isNaN(v) || Double.isInfinite(v)) return 0;
        double factor = Math.pow(10, scale);
        return Math.round(v * factor) / factor;
    }
}
