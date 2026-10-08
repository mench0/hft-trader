package com.hft.exchange.catalog;

import com.hft.exchange.Exchange;
import com.hft.exchange.catalog.ExchangeInfo.Adapter;
import com.hft.exchange.catalog.ExchangeInfo.Kind;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Реестр всех бирж, о которых шла речь. Админка берёт список отсюда
 * (GET /exchanges/catalog), BotController по этому же реестру создаёт адаптеры.
 *
 * Комиссии и лимиты — приблизительные, из прежних оценок; перед реальными
 * деньгами сверьте с тарифами биржи и своим VIP-уровнем.
 */
public final class ExchangeCatalog {

    /** Утилитный класс — экземпляры не создаются. */
    private ExchangeCatalog() {}

    /** Все биржи каталога по id в порядке добавления. */
    private static final Map<String, ExchangeInfo> ALL = new LinkedHashMap<>();

    /** Добавить биржу в каталог. */
    private static void add(ExchangeInfo i) { ALL.put(i.id(), i); }

    /** Список бирж: адреса, лимиты, комиссии, testnet, заметки. */
    static {
        add(new ExchangeInfo(Exchange.BINANCE.id(), "Binance", Kind.CEX_TIER1, Adapter.NATIVE_LIVE,
                "https://api.binance.com", 20, 0.10, 0.10, "USDT", "BTCUSDT",
                "Полный адаптер: стакан, ордера (WebSocket API) и события аккаунта по WebSocket, REST — запасной. Фьючерсы (market=perp): USDⓈ-M.",
                "wss://stream.binance.com:9443/ws", "https://testnet.binance.vision", "wss://testnet.binance.vision/ws"));
        add(new ExchangeInfo(Exchange.BYBIT.id(), "Bybit", Kind.CEX_TIER1, Adapter.NATIVE_LIVE,
                "https://api.bybit.com", 8, 0.10, 0.10, "USDT", "BTCUSDT",
                "Полный адаптер: стакан, ордера (/v5/trade), исполнения и баланс (/v5/private) по WebSocket, REST — запасной. Блокирует IP многих дата-центров (403). Фьючерсы (market=perp): linear.",
                "wss://stream.bybit.com/v5/public/spot", "https://api-testnet.bybit.com", "wss://stream-testnet.bybit.com/v5/public/spot"));
        add(new ExchangeInfo(Exchange.OKX.id(), "OKX", Kind.CEX_TIER1, Adapter.LIVE_UNVERIFIED,
                "https://www.okx.com", 8, 0.08, 0.10, "USDT", "BTCUSDT",
                "Спот: стакан, ордера, отмены, исполнения и балансы по WebSocket (публичный + приватный канал), REST — запасной. Нужен OKX_PASSPHRASE. Фьючерсы (market=perp): SWAP.",
                "wss://ws.okx.com:8443/ws/v5/public", "https://www.okx.com", "wss://wspap.okx.com:8443/ws/v5/public"));
        add(new ExchangeInfo(Exchange.MEXC.id(), "MEXC", Kind.CEX_TIER3, Adapter.LIVE_UNVERIFIED,
                "https://api.mexc.com", 5, 0.00, 0.05, "USDT", "BTCUSDT",
                "Спот, 0% maker. Стакан, исполнения и баланс по WebSocket (protobuf; стакан — до 30 символов на соединение) + REST-запас; ордера только REST (WS-ордеров у MEXC нет). Не держите большую часть капитала. Фьючерсы (market=perp): MEXC Contract — стакан, исполнения, позиции и баланс по WS; ордера REST (WS-ордеров нет), доступ к ним через API MEXC выдаёт отдельно.",
                "wss://wbs-api.mexc.com/ws", null, null));   // тестовой сети нет
        add(new ExchangeInfo(Exchange.GATE.id(), "Gate", Kind.CEX_TIER3, Adapter.LIVE_UNVERIFIED,
                "https://api.gateio.ws", 5, 0.20, 0.20, "USDT", "BTCUSDT",
                "Спот: стакан, ордера (WS API), исполнения и балансы по WebSocket, REST — запасной. Комиссия указана по умолчанию, проверьте тариф. Фьючерсы (market=perp): USDT-фьючерсы — стакан, ордера (WS API), исполнения, позиции и баланс по WS.",
                "wss://api.gateio.ws/ws/v4/", "https://api-testnet.gateapi.io", "wss://ws-testnet.gate.com/v4/ws/spot"));
        add(new ExchangeInfo(Exchange.HYPERLIQUID.id(), "Hyperliquid", Kind.PERP_DEX, Adapter.LIVE_UNVERIFIED,
                "https://api.hyperliquid.xyz", 5, 0.015, 0.045, "USDC", "BTCUSDC (монета BTC, перп)",
                "Перпы: стакан, ордера, info-запросы (WS post) и исполнения по WebSocket, REST — запасной. Ордера подписываются agent-ключом (EIP-712) через web3j. HYPERLIQUID_API_KEY = адрес аккаунта, _SECRET = ключ agent-кошелька без права вывода. Лимит адреса: 1 действие на $1 оборота.",
                "wss://api.hyperliquid.xyz/ws", "https://api.hyperliquid-testnet.xyz", "wss://api.hyperliquid-testnet.xyz/ws"));
        add(new ExchangeInfo(Exchange.KUCOIN.id(), "KuCoin", Kind.CEX_TIER1, Adapter.LIVE_UNVERIFIED,
                "https://api.kucoin.com", 10, 0.10, 0.10, "USDT", "BTCUSDT",
                "Спот: стакан по WebSocket (level2Depth50, адрес и токен через bullet-public) + REST-запас; ордера и отмены по WS API (wsapi.kucoin.com), исполнения и балансы — приватный поток (bullet-private). Нужен KUCOIN_PASSPHRASE; для UTA-счёта — KUCOIN_UTA=true (uta.order/uta.cancel). Лимиты — пулы весов (публичный 2000/30 с, спот 4000/30 с). Фьючерсы (market=perp): KuCoin Futures (XBTUSDTM) — стакан, ордера (Pro WS API), исполнения, позиции и баланс по WS."));
        add(new ExchangeInfo(Exchange.ASTER.id(), "Aster", Kind.ORDERBOOK_DEX, Adapter.LIVE_UNVERIFIED,
                "https://sapi.asterdex.com", 5, 0.10, 0.10, "USDT", "BTCUSDT",
                "Спот, API v3 в формате Binance: стакан (depth20@100ms), исполнения и баланс (listenKey) по WebSocket + REST-запас; ордера через REST (WS-ордеров нет) с подписью EIP-712 кошельком-агентом. ASTER_API_KEY = адрес основного кошелька, ASTER_API_SECRET = ключ API-кошелька (signer) без права вывода. Комиссии указаны по умолчанию, проверьте тариф. Фьючерсы (market=perp): Aster Futures (/fapi/v3, формат Binance USDⓈ-M), исполнения и позиции по listenKey.",
                "wss://sstream.asterdex.com/stream", "https://sapi.asterdex-testnet.com", "wss://sstream.asterdex-testnet.com/stream"));
        add(new ExchangeInfo(Exchange.UNISWAPV2.id(), "Uniswap V2-совместимые пулы", Kind.AMM_DEX, Adapter.LIVE_UNVERIFIED,
                "https://ethereum-rpc.publicnode.com", 4, 0.30, 0.30, "USDC", "WETHUSDC",
                "Стакан из событий Sync через eth_subscribe и все JSON-RPC вызовы по WebSocket ноды (HTTP — запасной). Свопы через Router02 (web3j). Нужны UNISWAPV2_ROUTER, UNISWAPV2_TOKENS, пулы UNISWAPV2_POOLS; GTC-лимиток и отмены нет. Горячий кошелёк с малой суммой, приватный RPC против сэндвичей."));
    }

    /**
     * Фьючерсный рынок биржи (бессрочные USDT-контракты): свои адреса и комиссии.
     *
     * @param restUrl REST основной сети
     * @param wsUrl WebSocket стакана основной сети
     * @param testnetRestUrl REST тестовой сети (null — нет)
     * @param testnetWsUrl WebSocket тестовой сети
     * @param makerFeePct комиссия мейкера, %
     * @param takerFeePct комиссия тейкера, %
     */
    public record PerpVenue(String restUrl, String wsUrl, String testnetRestUrl, String testnetWsUrl,
                            double makerFeePct, double takerFeePct) {
        /** REST с учётом testnet. */
        public String restUrl(boolean testnet) { return testnet && testnetRestUrl != null ? testnetRestUrl : restUrl; }
        /** WebSocket с учётом testnet. */
        public String wsUrl(boolean testnet) { return testnet && testnetRestUrl != null ? testnetWsUrl : wsUrl; }
        /** У фьючерсов биржи есть тестовая сеть. */
        public boolean hasTestnet() { return testnetRestUrl != null; }
    }

    /** Биржи с фьючерсами. */
    private static final Map<Exchange, PerpVenue> PERP = new java.util.EnumMap<>(Map.of(
            Exchange.BINANCE, new PerpVenue("https://fapi.binance.com", "wss://fstream.binance.com/stream",
                    "https://testnet.binancefuture.com", "wss://fstream.binancefuture.com/stream", 0.02, 0.05),
            Exchange.BYBIT, new PerpVenue("https://api.bybit.com", "wss://stream.bybit.com/v5/public/linear",
                    "https://api-testnet.bybit.com", "wss://stream-testnet.bybit.com/v5/public/linear", 0.02, 0.055),
            Exchange.OKX, new PerpVenue("https://www.okx.com", "wss://ws.okx.com:8443/ws/v5/public",
                    "https://www.okx.com", "wss://wspap.okx.com:8443/ws/v5/public", 0.02, 0.05),
            Exchange.HYPERLIQUID, new PerpVenue("https://api.hyperliquid.xyz", "wss://api.hyperliquid.xyz/ws",
                    "https://api.hyperliquid-testnet.xyz", "wss://api.hyperliquid-testnet.xyz/ws", 0.015, 0.045),
            Exchange.ASTER, new PerpVenue("https://fapi.asterdex.com", "wss://fstream.asterdex.com/stream",
                    "https://fapi.asterdex-testnet.com", "wss://fstream.asterdex-testnet.com/stream", 0.01, 0.035),
            Exchange.GATE, new PerpVenue("https://api.gateio.ws", "wss://fx-ws.gateio.ws/v4/ws/usdt",
                    "https://fx-api-testnet.gateio.ws", "wss://fx-ws-testnet.gateio.ws/v4/ws/usdt", 0.02, 0.05),
            Exchange.KUCOIN, new PerpVenue("https://api-futures.kucoin.com", "", null, null, 0.02, 0.06),   // адрес WS — через bullet-public
            Exchange.MEXC, new PerpVenue("https://contract.mexc.com", "wss://contract.mexc.com/edge", null, null, 0.0, 0.02)));

    static {                                                     // enum и каталог фьючерсов должны совпадать
        for (Exchange e : Exchange.values())
            if (e.hasPerp() != PERP.containsKey(e)) throw new IllegalStateException("каталог фьючерсов не совпадает с Exchange: " + e);
    }

    /** Фьючерсный рынок биржи, если он есть. */
    public static Optional<PerpVenue> perp(String id) { return Exchange.find(id).map(PERP::get); }

    /** У биржи есть фьючерсы (market=perp). */
    public static boolean supportsPerp(String id) { return Exchange.find(id).map(Exchange::hasPerp).orElse(false); }

    /** У биржи есть спот (market=spot); Hyperliquid в боте — только перпы. */
    public static boolean supportsSpot(String id) { return Exchange.find(id).map(Exchange::hasSpot).orElse(false); }

    /** Все биржи каталога. */
    public static List<ExchangeInfo> all() { return List.copyOf(ALL.values()); }

    /** Биржа по id. */
    public static Optional<ExchangeInfo> find(String id) {
        return Optional.ofNullable(ALL.get(id == null ? "" : id.toLowerCase()));
    }

    /** Биржи, которые можно выбрать и запустить (в каталоге только реализованные). */
    public static List<String> runnableIds() {
        return ALL.values().stream().map(ExchangeInfo::id).toList();
    }

    static {                                                     // у каждой биржи enum есть описание в каталоге
        for (Exchange e : Exchange.values())
            if (!ALL.containsKey(e.id())) throw new IllegalStateException("нет описания биржи в каталоге: " + e);
    }
}
