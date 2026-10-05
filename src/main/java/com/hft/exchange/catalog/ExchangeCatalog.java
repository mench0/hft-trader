package com.hft.exchange.catalog;

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
        add(new ExchangeInfo("binance", "Binance", Kind.CEX_TIER1, Adapter.NATIVE_LIVE,
                "https://api.binance.com", 20, 0.10, 0.10, "USDT", "BTCUSDT",
                "Полный адаптер: стакан, ордера (WebSocket API) и события аккаунта по WebSocket, REST — запасной.",
                "wss://stream.binance.com:9443/ws", "https://testnet.binance.vision", "wss://testnet.binance.vision/ws"));
        add(new ExchangeInfo("bybit", "Bybit", Kind.CEX_TIER1, Adapter.NATIVE_LIVE,
                "https://api.bybit.com", 8, 0.10, 0.10, "USDT", "BTCUSDT",
                "Полный адаптер: стакан, ордера (/v5/trade), исполнения и баланс (/v5/private) по WebSocket, REST — запасной. Блокирует IP многих дата-центров (403).",
                "wss://stream.bybit.com/v5/public/spot", "https://api-testnet.bybit.com", "wss://stream-testnet.bybit.com/v5/public/spot"));
        add(new ExchangeInfo("okx", "OKX", Kind.CEX_TIER1, Adapter.LIVE_UNVERIFIED,
                "https://www.okx.com", 8, 0.08, 0.10, "USDT", "BTCUSDT",
                "Спот: стакан, ордера, отмены, исполнения и балансы по WebSocket (публичный + приватный канал), REST — запасной. Нужен OKX_PASSPHRASE.",
                "wss://ws.okx.com:8443/ws/v5/public", "https://www.okx.com", "wss://wspap.okx.com:8443/ws/v5/public"));
        add(new ExchangeInfo("mexc", "MEXC", Kind.CEX_TIER3, Adapter.LIVE_UNVERIFIED,
                "https://api.mexc.com", 5, 0.00, 0.05, "USDT", "BTCUSDT",
                "Спот, 0% maker. Стакан по WebSocket (protobuf, до 30 символов на соединение) + REST-запас; ордера только REST (WS-ордеров у MEXC нет). Не держите большую часть капитала.",
                "wss://wbs-api.mexc.com/ws", null, null));   // тестовой сети нет
        add(new ExchangeInfo("gate", "Gate", Kind.CEX_TIER3, Adapter.LIVE_UNVERIFIED,
                "https://api.gateio.ws", 5, 0.20, 0.20, "USDT", "BTCUSDT",
                "Спот: стакан, ордера (WS API), исполнения и балансы по WebSocket, REST — запасной. Комиссия указана по умолчанию, проверьте тариф.",
                "wss://api.gateio.ws/ws/v4/", "https://api-testnet.gateapi.io", "wss://ws-testnet.gate.com/v4/ws/spot"));
        add(new ExchangeInfo("bingx", "BingX", Kind.CEX_TIER3, Adapter.LIVE_UNVERIFIED,
                "https://open-api.bingx.com", 5, 0.10, 0.10, "USDT", "BTCUSDT",
                "Спот: стакан (depth20, gzip) и исполнения (listenKey) по WebSocket, REST-запас; ордера только REST (WS-ордеров у BingX нет). Порядок уровней может быть обратным, сортируется."));
        add(new ExchangeInfo("hyperliquid", "Hyperliquid", Kind.PERP_DEX, Adapter.LIVE_UNVERIFIED,
                "https://api.hyperliquid.xyz", 5, 0.015, 0.045, "USDC", "BTCUSDC (монета BTC, перп)",
                "Перпы: стакан, ордера, info-запросы (WS post) и исполнения по WebSocket, REST — запасной. Ордера подписываются agent-ключом (EIP-712) через web3j. HYPERLIQUID_API_KEY = адрес аккаунта, _SECRET = ключ agent-кошелька без права вывода. Лимит адреса: 1 действие на $1 оборота.",
                "wss://api.hyperliquid.xyz/ws", "https://api.hyperliquid-testnet.xyz", "wss://api.hyperliquid-testnet.xyz/ws"));
        add(new ExchangeInfo("kucoin", "KuCoin", Kind.CEX_TIER1, Adapter.LIVE_UNVERIFIED,
                "https://api.kucoin.com", 10, 0.10, 0.10, "USDT", "BTCUSDT",
                "Спот: стакан по WebSocket (level2Depth50, адрес и токен через bullet-public) + REST-запас; ордера и отмены по WS API (wsapi.kucoin.com), исполнения и балансы — приватный поток (bullet-private). Нужен KUCOIN_PASSPHRASE; для UTA-счёта — wsTrade=false. Лимиты — пулы весов (публичный 2000/30 с, спот 4000/30 с)."));
        add(new ExchangeInfo("aster", "Aster", Kind.ORDERBOOK_DEX, Adapter.LIVE_UNVERIFIED,
                "https://sapi.asterdex.com", 5, 0.10, 0.10, "USDT", "BTCUSDT",
                "Спот, API v3 в формате Binance: стакан (depth20@100ms), исполнения и баланс (listenKey) по WebSocket + REST-запас; ордера через REST (WS-ордеров нет) с подписью EIP-712 кошельком-агентом. ASTER_API_KEY = адрес основного кошелька, ASTER_API_SECRET = ключ API-кошелька (signer) без права вывода. Комиссии указаны по умолчанию, проверьте тариф.",
                "wss://sstream.asterdex.com/stream", "https://sapi.asterdex-testnet.com", "wss://sstream.asterdex-testnet.com/stream"));
        add(new ExchangeInfo("dydx", "dYdX v4", Kind.PERP_DEX, Adapter.PAPER_BLIND,
                "https://indexer.dydx.trade", 5, 0.01, 0.05, "USD", "BTCUSD (рынок BTC-USD, перп)",
                "Перпы, только paper, стакан по WebSocket (v4_orderbook) + REST-запас: ордера требуют Cosmos-транзакций (protobuf, gRPC) — не реализовано.",
                "wss://indexer.dydx.trade/v4/ws", "https://indexer.v4testnet.dydx.exchange", "wss://indexer.v4testnet.dydx.exchange/v4/ws"));
        add(new ExchangeInfo("uniswapv2", "Uniswap V2-совместимые пулы", Kind.AMM_DEX, Adapter.LIVE_UNVERIFIED,
                "https://ethereum-rpc.publicnode.com", 4, 0.30, 0.30, "USDC", "WETHUSDC",
                "Стакан из событий Sync через eth_subscribe и все JSON-RPC вызовы по WebSocket ноды (HTTP — запасной). Свопы через Router02 (web3j). Нужны UNISWAPV2_ROUTER, UNISWAPV2_TOKENS, пулы UNISWAPV2_POOLS; GTC-лимиток и отмены нет. Горячий кошелёк с малой суммой, приватный RPC против сэндвичей."));
        add(new ExchangeInfo("pancakeswap", "PancakeSwap V2", Kind.AMM_DEX, Adapter.NOT_IMPLEMENTED,
                "", 0, 0.25, 0.25, "USDT", "-", "Тот же формат, что у Uniswap V2: задайте RPC BSC и пулы."));
        add(new ExchangeInfo("raydium", "Raydium (Solana)", Kind.AMM_DEX, Adapter.NOT_IMPLEMENTED,
                "", 0, 0.25, 0.25, "USDC", "-", "Нужен разбор аккаунтов пула Solana, не реализовано."));
        add(new ExchangeInfo("orca", "Orca (Solana)", Kind.AMM_DEX, Adapter.NOT_IMPLEMENTED,
                "", 0, 0.30, 0.30, "USDC", "-", "Концентрированная ликвидность, не реализовано."));
    }

    /** Все биржи каталога. */
    public static List<ExchangeInfo> all() { return List.copyOf(ALL.values()); }

    /** Биржа по id. */
    public static Optional<ExchangeInfo> find(String id) {
        return Optional.ofNullable(ALL.get(id == null ? "" : id.toLowerCase()));
    }

    /** Биржи, которые можно выбрать и запустить. */
    public static List<String> runnableIds() {
        return ALL.values().stream()
                .filter(i -> i.adapter() != Adapter.NOT_IMPLEMENTED)
                .map(ExchangeInfo::id).toList();
    }
}
