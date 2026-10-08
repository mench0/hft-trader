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
        return switch (id) {
            case "binance" -> ec.params().isPerp()
                    ? new SignedCexExchange(id, ec, settings, com.hft.exchange.binance.BinanceFuturesClient::new)   // USDⓈ-M
                    : new BinanceExchange(ec, settings);
            case "bybit" -> new BybitExchange(ec, settings);
            case "okx" -> new SignedCexExchange(id, ec, settings, OkxRestClient::new);
            case "mexc" -> new SignedCexExchange(id, ec, settings, MexcRestClient::new);
            case "gate" -> new SignedCexExchange(id, ec, settings, GateRestClient::new);
            case "hyperliquid" -> new SignedCexExchange(id, ec, settings, HyperliquidRestClient::new);
            case "uniswapv2" -> new SignedCexExchange(id, ec, settings, UniswapV2Client::new);
            case "kucoin" -> new SignedCexExchange(id, ec, settings, KucoinRestClient::new);
            case "aster" -> new SignedCexExchange(id, ec, settings, AsterRestClient::new);
            default -> throw new IllegalArgumentException("Неизвестная или не реализованная биржа: " + id);
        };
    }
}
