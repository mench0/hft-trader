package com.hft.exchange;

import com.hft.config.ExchangeConfig;
import com.hft.config.TradingSettings;
import com.hft.exchange.aster.AsterRestClient;
import com.hft.exchange.binance.BinanceExchange;
import com.hft.exchange.bybit.BybitExchange;
import com.hft.exchange.gate.GateRestClient;
import com.hft.exchange.generic.SignedCexExchange;
import com.hft.exchange.hyperliquid.HyperliquidRestClient;
import com.hft.exchange.kucoin.KucoinRestClient;
import com.hft.exchange.mexc.MexcRestClient;
import com.hft.exchange.okx.OkxRestClient;
import com.hft.exchange.uniswap.UniswapV2Client;

/** Создание шлюза биржи по её id. */
public final class ExchangeFactory {

    /** Утилитный класс — экземпляры не создаются. */
    private ExchangeFactory() {}

    /** Шлюз биржи по id; неизвестная биржа без адаптера — IllegalArgumentException. */
    public static ExchangeGateway create(String id, ExchangeConfig ec, TradingSettings settings) {
        return switch (Exchange.find(id).orElse(null)) {
            case BINANCE -> ec.params().isPerp()
                    ? new SignedCexExchange(id, ec, settings, com.hft.exchange.binance.BinanceFuturesClient::new)   // USDⓈ-M
                    : new BinanceExchange(ec, settings);
            case BYBIT -> new BybitExchange(ec, settings);
            case OKX -> new SignedCexExchange(id, ec, settings, OkxRestClient::new);
            case MEXC -> new SignedCexExchange(id, ec, settings, ec.params().isPerp()
                    ? com.hft.exchange.mexc.MexcFuturesClient::new : MexcRestClient::new);        // contract.mexc.com
            case GATE -> new SignedCexExchange(id, ec, settings, ec.params().isPerp()
                    ? com.hft.exchange.gate.GateFuturesClient::new : GateRestClient::new);        // /futures/usdt
            case HYPERLIQUID -> new SignedCexExchange(id, ec, settings, HyperliquidRestClient::new);
            case UNISWAPV2 -> new SignedCexExchange(id, ec, settings, UniswapV2Client::new);
            case KUCOIN -> new SignedCexExchange(id, ec, settings, ec.params().isPerp()
                    ? com.hft.exchange.kucoin.KucoinFuturesClient::new : KucoinRestClient::new);  // api-futures.kucoin.com
            case ASTER -> new SignedCexExchange(id, ec, settings, AsterRestClient::new);           // спот и фьючерсы (/fapi/v3) — один клиент
            case null, default -> throw new IllegalArgumentException("Неизвестная или не реализованная биржа: " + id);
        };
    }
}
