package com.hft.rest;

import com.hft.exchange.catalog.ExchangeCatalog;
import com.hft.exchange.catalog.ExchangeInfo;
import com.hft.rest.RateBudget.Kind;
import com.hft.rest.RateBudget.Limit;

import java.util.List;

import static com.hft.rest.RateBudget.Kind.*;

/**
 * Лимиты запросов бирж по их документации (официальные значения; {@link RateBudget} берёт 80% от них).
 * Где у биржи несколько лимитов (на IP, на ключ, на ордера), каждый — отдельное ведро.
 *
 * Значения для обычного (не VIP) уровня аккаунта. Если у вас повышенные лимиты — бот просто будет
 * осторожнее, чем мог бы.
 */
public final class RateLimits {

    /** Утилитный класс — экземпляры не создаются. */
    private RateLimits() {}

    /** Все виды REST-запросов. */
    private static final Kind[] REST = {PUBLIC, PRIVATE, ORDER};

    /** Вёдра лимитов биржи по её документации. */
    public static List<Limit> forExchange(String id) {
        return switch (id) {
            // Binance Spot: вес 6000/мин на IP, ордера 50/10 с и 160 000/сутки на аккаунт, сырые запросы 61 000/5 мин,
            // WS: 5 входящих сообщений/с на соединение, 300 подключений/5 мин на IP
            case "binance" -> List.of(
                    Limit.of("weight", 6000, 60_000, REST),
                    Limit.of("raw", 61_000, 300_000, REST),
                    Limit.of("orders10s", 50, 10_000, ORDER),
                    Limit.of("orders1d", 160_000, 86_400_000, ORDER),
                    Limit.of("wsMessages", 5, 1_000, WS_MESSAGE),
                    Limit.of("wsConnects", 300, 300_000, WS_CONNECT));
            // Aster (API в формате Binance): вес 2400/мин на IP, ордера 1200/мин и 100/10 с на аккаунт
            case "aster" -> List.of(
                    Limit.of("weight", 2400, 60_000, REST),
                    Limit.of("orders1m", 1200, 60_000, ORDER),
                    Limit.of("orders10s", 100, 10_000, ORDER),
                    Limit.of("wsMessages", 10, 1_000, WS_MESSAGE),
                    Limit.of("wsConnects", 300, 300_000, WS_CONNECT));
            // Bybit v5: 600 запросов/5 с на IP; создание/отмена спот-ордеров 20/с на UID, приватные запросы ~10/с
            case "bybit" -> List.of(
                    Limit.of("ip", 600, 5_000, REST),
                    Limit.of("orders", 20, 1_000, ORDER),
                    Limit.of("private", 10, 1_000, PRIVATE),
                    Limit.of("wsMessages", 10, 1_000, WS_MESSAGE),
                    Limit.of("wsConnects", 500, 300_000, WS_CONNECT));
            // OKX: стакан 40/2 с на IP, баланс/ордера-чтение ~10/2 с, создание ордера 60/2 с;
            // WS: 3 подключения/с, 480 запросов subscribe/unsubscribe/login в час
            case "okx" -> List.of(
                    Limit.of("public", 40, 2_000, PUBLIC),
                    Limit.of("private", 10, 2_000, PRIVATE),
                    Limit.of("orders", 60, 2_000, ORDER),
                    Limit.of("wsMessages", 480, 3_600_000, WS_MESSAGE),
                    Limit.of("wsConnects", 3, 1_000, WS_CONNECT));
            // Gate: публичные 200/10 с на эндпоинт, приватные 200/10 с, спот-ордера 10/с на пользователя
            case "gate" -> List.of(
                    Limit.of("public", 200, 10_000, PUBLIC),
                    Limit.of("private", 200, 10_000, PRIVATE),
                    Limit.of("orders", 10, 1_000, ORDER),
                    Limit.of("wsMessages", 50, 1_000, WS_MESSAGE),
                    Limit.of("wsConnects", 100, 60_000, WS_CONNECT));
            // MEXC: 500/10 с на эндпоинт по IP и по UID; ордера — по 5/с держимся осторожно;
            // WS: 100 сообщений/с, не больше 30 подписок на соединение
            case "mexc" -> List.of(
                    Limit.of("public", 500, 10_000, PUBLIC),
                    Limit.of("private", 500, 10_000, PRIVATE),
                    Limit.of("orders", 5, 1_000, ORDER),
                    Limit.of("wsMessages", 100, 1_000, WS_MESSAGE),
                    Limit.of("wsConnects", 100, 60_000, WS_CONNECT));
            // Hyperliquid: вес 1200/мин на IP (info — 2..20, действие — 1); WS: 2000 сообщений/мин, 100 подключений
            case "hyperliquid" -> List.of(
                    Limit.of("weight", 1200, 60_000, REST),
                    Limit.of("wsMessages", 2000, 60_000, WS_MESSAGE),
                    Limit.of("wsConnects", 100, 60_000, WS_CONNECT));
            // KuCoin: публичный пул 2000/30 с на IP, спот-пул 4000/30 с на аккаунт (вес по эндпоинтам);
            // WS: 100 сообщений/10 с на соединение, 30 подключений/мин
            case "kucoin" -> List.of(
                    Limit.of("public", 2000, 30_000, PUBLIC),
                    Limit.of("spot", 4000, 30_000, PRIVATE, ORDER),
                    Limit.of("wsMessages", 100, 10_000, WS_MESSAGE),
                    Limit.of("wsConnects", 30, 60_000, WS_CONNECT));
            // Uniswap — это нода Ethereum (RPC-провайдер): осторожно 4 запроса/с, 1 транзакция/с
            case "uniswapv2" -> List.of(
                    Limit.of("rpc", 5, 1_000, REST),
                    Limit.of("tx", 1.25, 1_000, ORDER),
                    Limit.of("wsMessages", 10, 1_000, WS_MESSAGE),
                    Limit.of("wsConnects", 10, 60_000, WS_CONNECT));
            // Остальные (бумажные адаптеры) — консервативный лимит из каталога
            default -> {
                double rps = ExchangeCatalog.find(id).map(ExchangeInfo::maxRequestsPerSec).orElse(2.0);
                yield List.of(
                        Limit.of("rest", rps / RateBudget.SAFETY, 1_000, REST),
                        Limit.of("wsMessages", 5, 1_000, WS_MESSAGE),
                        Limit.of("wsConnects", 20, 60_000, WS_CONNECT));
            }
        };
    }

    /**
     * Вес запроса по документации биржи (для бирж, считающих вес). Остальным — 1.
     * path без домена, query — строка параметров (может быть пустой).
     */
    public static double weight(String exchange, String method, String path, String query) {
        String q = query == null ? "" : query;
        return switch (exchange) {
            case "binance" -> binanceWeight(method, path, q);
            case "aster" -> asterWeight(method, path, q);
            case "kucoin" -> kucoinWeight(method, path);
            case "hyperliquid" -> path.endsWith("/exchange") ? 1 : 20;   // info по умолчанию 20 (l2Book — 2, см. фид)
            default -> 1;
        };
    }

    /** Вес запроса Binance по пути и параметрам. */
    private static double binanceWeight(String method, String path, String q) {
        boolean symbol = q.contains("symbol=");
        return switch (path) {
            case "/api/v3/order" -> "GET".equals(method) ? 4 : 1;
            case "/api/v3/openOrders" -> "DELETE".equals(method) ? 1 : symbol ? 6 : 80;
            case "/api/v3/account" -> 20;
            case "/api/v3/exchangeInfo" -> 20;
            case "/api/v3/depth" -> depthWeight(limitParam(q, 100));
            case "/api/v3/ticker/24hr" -> symbol ? 2 : 80;
            case "/api/v3/ticker/bookTicker" -> symbol ? 2 : 4;
            case "/api/v3/klines" -> 2;
            case "/api/v3/myTrades" -> 20;
            // USDⓈ-M фьючерсы (свой лимит 2400 веса в минуту, считаем в том же бюджете — консервативно)
            case "/fapi/v1/exchangeInfo" -> 1;
            case "/fapi/v1/depth" -> limitParam(q, 100) <= 50 ? 2 : limitParam(q, 100) <= 100 ? 5 : 10;
            case "/fapi/v1/premiumIndex" -> symbol ? 1 : 10;
            case "/fapi/v2/balance", "/fapi/v2/positionRisk" -> 5;
            case "/fapi/v1/openOrders" -> symbol ? 1 : 40;
            default -> 1;
        };
    }

    /** Вес запроса Aster (формат Binance, свои значения). */
    private static double asterWeight(String method, String path, String q) {
        boolean symbol = q.contains("symbol=");
        if (path.endsWith("/order")) return "GET".equals(method) ? 1 : 1;
        if (path.endsWith("/openOrders")) return "DELETE".equals(method) ? 1 : symbol ? 1 : 40;
        if (path.endsWith("/account")) return 5;
        if (path.endsWith("/exchangeInfo")) return 1;
        if (path.endsWith("/depth")) return depthWeight(limitParam(q, 100)) / 2.5;
        if (path.endsWith("/ticker/24hr")) return symbol ? 1 : 40;
        if (path.endsWith("/klines")) return 2;
        return 1;
    }

    /** Binance: depth до 100 уровней — 5, до 500 — 25, до 1000 — 50, больше — 250. */
    static double depthWeight(int limit) {
        return limit <= 100 ? 5 : limit <= 500 ? 25 : limit <= 1000 ? 50 : 250;
    }

    /** Вес запроса KuCoin по эндпоинту. */
    private static double kucoinWeight(String method, String path) {
        if (path.startsWith("/api/v1/market/orderbook/level2_20")) return 2;
        if (path.startsWith("/api/v1/market/orderbook/level2_100")) return 4;
        if (path.startsWith("/api/v1/market/allTickers")) return 15;
        if (path.startsWith("/api/v1/market/candles")) return 3;
        if (path.startsWith("/api/v2/symbols")) return 4;
        if (path.startsWith("/api/v1/bullet")) return 10;
        if (path.equals("/api/v1/orders") && "POST".equals(method)) return 1;   // hf-ордер 1, обычный 2 — берём осторожно ниже
        if (path.startsWith("/api/v1/orders") && "DELETE".equals(method)) return 3;
        if (path.startsWith("/api/v1/orders")) return 2;
        if (path.startsWith("/api/v1/accounts")) return 5;
        return 2;
    }

    /** Значение limit= из строки параметров или def. */
    private static int limitParam(String q, int def) {
        int i = q.indexOf("limit=");
        if (i < 0) return def;
        int j = i + 6, n = 0;
        while (j < q.length() && Character.isDigit(q.charAt(j))) n = n * 10 + (q.charAt(j++) - '0');
        return n == 0 ? def : n;
    }
}
