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

    private ExchangeCatalog() {}

    private static final Map<String, ExchangeInfo> ALL = new LinkedHashMap<>();

    private static void add(ExchangeInfo i) { ALL.put(i.id(), i); }

    static {
        add(new ExchangeInfo("binance", "Binance", Kind.CEX_TIER1, Adapter.NATIVE_LIVE,
                "https://api.binance.com", 20, 0.10, 0.10, "USDT", "BTCUSDT",
                "Полный адаптер: WS-стакан, REST-ордера, лимитер."));
        add(new ExchangeInfo("bybit", "Bybit", Kind.CEX_TIER1, Adapter.NATIVE_LIVE,
                "https://api.bybit.com", 8, 0.10, 0.10, "USDT", "BTCUSDT",
                "Полный адаптер. Блокирует IP многих дата-центров (403)."));
        add(new ExchangeInfo("okx", "OKX", Kind.CEX_TIER1, Adapter.LIVE_UNVERIFIED,
                "https://www.okx.com", 8, 0.08, 0.10, "USDT", "BTCUSDT",
                "Спот: стакан, ордера, отмены, исполнения и балансы по WebSocket (публичный + приватный канал), REST — запасной. Нужен OKX_PASSPHRASE."));
        add(new ExchangeInfo("mexc", "MEXC", Kind.CEX_TIER3, Adapter.LIVE_UNVERIFIED,
                "https://api.mexc.com", 5, 0.00, 0.05, "USDT", "BTCUSDT",
                "Спот, 0% maker. Стакан только REST-опросом: спотовый WS MEXC отдаёт protobuf, декодера нет. Не держите большую часть капитала."));
        add(new ExchangeInfo("gate", "Gate", Kind.CEX_TIER3, Adapter.LIVE_UNVERIFIED,
                "https://api.gateio.ws", 5, 0.20, 0.20, "USDT", "BTCUSDT",
                "Спот: стакан, ордера (WS API), исполнения и балансы по WebSocket, REST — запасной. Комиссия указана по умолчанию, проверьте тариф."));
        add(new ExchangeInfo("bingx", "BingX", Kind.CEX_TIER3, Adapter.LIVE_UNVERIFIED,
                "https://open-api.bingx.com", 5, 0.10, 0.10, "USDT", "BTCUSDT",
                "Спот: стакан по WebSocket (depth20, gzip) + REST-запас. Порядок уровней может быть обратным, сортируется."));
        add(new ExchangeInfo("lbank", "LBank", Kind.CEX_TIER3, Adapter.LIVE_UNVERIFIED,
                "https://api.lbank.info", 5, 0.10, 0.10, "USDT", "BTCUSDT",
                "Спот, низкая ликвидность: стакан по WebSocket (depth) + REST-запас. Схема подписи по памяти — самая рискованная из клиентов."));
        add(new ExchangeInfo("hyperliquid", "Hyperliquid", Kind.PERP_DEX, Adapter.LIVE_UNVERIFIED,
                "https://api.hyperliquid.xyz", 5, 0.015, 0.045, "USDC", "BTCUSDC (монета BTC, перп)",
                "Перпы: стакан, ордера, info-запросы (WS post) и исполнения по WebSocket, REST — запасной. Ордера подписываются agent-ключом (EIP-712) через web3j. HYPERLIQUID_API_KEY = адрес аккаунта, _SECRET = ключ agent-кошелька без права вывода. Лимит адреса: 1 действие на $1 оборота."));
        add(new ExchangeInfo("dydx", "dYdX v4", Kind.PERP_DEX, Adapter.PAPER_BLIND,
                "https://indexer.dydx.trade", 5, 0.01, 0.05, "USD", "BTCUSD (рынок BTC-USD, перп)",
                "Перпы, только paper, стакан по WebSocket (v4_orderbook) + REST-запас: ордера требуют Cosmos-транзакций (protobuf, gRPC) — не реализовано."));
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

    public static List<ExchangeInfo> all() { return List.copyOf(ALL.values()); }

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
