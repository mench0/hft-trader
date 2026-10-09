package com.hft.exchange;

import com.hft.config.ExchangeConfig;
import com.hft.config.TradingSettings;
import com.hft.exchange.aster.AsterRestClient;
import com.hft.exchange.binance.BinanceMarketDataFeed;
import com.hft.exchange.bybit.BybitMarketDataFeed;
import com.hft.exchange.bybit.BybitRestClient;
import com.hft.rest.BinanceRestClient;
import com.hft.exchange.gate.GateRestClient;
import com.hft.exchange.generic.GeneralExchange;
import com.hft.exchange.hyperliquid.HyperliquidRestClient;
import com.hft.exchange.kucoin.KucoinRestClient;
import com.hft.exchange.mexc.MexcRestClient;
import com.hft.exchange.okx.OkxRestClient;
import com.hft.exchange.uniswapv2.UniswapV2Client;

/** Создание шлюза биржи по её id. */
public final class ExchangeFactory {

    /** Утилитный класс — экземпляры не создаются. */
    private ExchangeFactory() {}

    /** Шлюз биржи по id; неизвестная биржа без адаптера — IllegalArgumentException. */
    public static ExchangeGateway create(String id, ExchangeConfig ec, TradingSettings settings) {
        return switch (Exchange.find(id).orElse(null)) {
            case BINANCE -> ec.params().isPerp()
                    ? new GeneralExchange(id, ec, settings, com.hft.exchange.binance.BinanceFuturesClient::new)   // USDⓈ-M
                    : new GeneralExchange(id, ec, settings, BinanceRestClient::new, BinanceMarketDataFeed::new);
            case BYBIT -> new GeneralExchange(id, ec, settings, BybitRestClient::new, BybitMarketDataFeed::new);   // спот и linear
            case OKX -> new GeneralExchange(id, ec, settings, OkxRestClient::new);
            case MEXC -> new GeneralExchange(id, ec, settings, ec.params().isPerp()
                    ? com.hft.exchange.mexc.MexcFuturesClient::new : MexcRestClient::new);        // contract.mexc.com
            case GATE -> new GeneralExchange(id, ec, settings, ec.params().isPerp()
                    ? com.hft.exchange.gate.GateFuturesClient::new : GateRestClient::new);        // /futures/usdt
            case HYPERLIQUID -> new GeneralExchange(id, ec, settings, HyperliquidRestClient::new);
            case UNISWAPV2 -> new GeneralExchange(id, ec, settings, UniswapV2Client::new);
            case KUCOIN -> new GeneralExchange(id, ec, settings, ec.params().isPerp()
                    ? com.hft.exchange.kucoin.KucoinFuturesClient::new : KucoinRestClient::new);  // api-futures.kucoin.com
            case ASTER -> new GeneralExchange(id, ec, settings, AsterRestClient::new);           // спот и фьючерсы (/fapi/v3) — один клиент
            default -> throw new IllegalArgumentException("Неизвестная или не реализованная биржа: " + id);
        };
    }
}
