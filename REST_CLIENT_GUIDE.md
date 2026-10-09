# REST-клиенты бирж

Каждая биржа торгует через реализацию `com.hft.rest.ExchangeOrderApi`; стратегии и админка
обращаются к ней только через `OrderService` (риск-проверки, округление по правилам биржи, лимиты).

## Клиенты

| Биржа | Клиент | Ключи в окружении |
|---|---|---|
| Binance | `rest/BinanceRestClient` | `BINANCE_API_KEY`, `BINANCE_API_SECRET` |
| Bybit | `exchange/bybit/BybitRestClient` | `BYBIT_API_KEY`, `BYBIT_API_SECRET` |
| OKX | `exchange/okx/OkxRestClient` | `OKX_API_KEY`, `OKX_API_SECRET`, `OKX_PASSPHRASE` |
| Gate | `exchange/gate/GateRestClient` | `GATE_API_KEY`, `GATE_API_SECRET` |
| MEXC | `exchange/mexc/MexcRestClient` | `MEXC_API_KEY`, `MEXC_API_SECRET` |
| KuCoin | `exchange/kucoin/KucoinRestClient` | `KUCOIN_API_KEY`, `KUCOIN_API_SECRET`, `KUCOIN_PASSPHRASE` |
| Aster | `exchange/aster/AsterRestClient` | `ASTER_API_KEY` (user), `ASTER_API_SECRET` (signer) |
| Hyperliquid | `exchange/hyperliquid/HyperliquidRestClient` | `HYPERLIQUID_API_KEY` (адрес), `HYPERLIQUID_API_SECRET` (agent-ключ) |
| Uniswap V2 | `exchange/uniswapv2/UniswapV2Client` | `UNISWAPV2_API_KEY`, `UNISWAPV2_API_SECRET` |

Все клиенты, кроме Binance и Bybit, наследуют `SignedClient`: общие HTTP-клиент, подпись,
бюджет запросов (`RateBudget`, 80% официальных лимитов из `RateLimits`), разделение лимитеров ордеров
и фоновых запросов, сопоставление строковых id ордеров с числовыми (`BoundedMap`).
Реальные ордера уходят только при ключах **и** параметре биржи `live=true`; иначе — `PaperOrderApi`
исполняет ордера против живого стакана.

## Методы `ExchangeOrderApi`

| Метод | Что делает |
|---|---|
| `buyLimit/sellLimit(symbol, qty, price, tif)` | лимитный ордер (GTC/IOC/FOK) |
| `buyMarket/sellMarket(symbol, qty)` | рыночный ордер на количество базовой валюты |
| `buyMarketForQuote(symbol, quote)` | рыночная покупка на сумму в котируемой валюте |
| `cancelOrder(symbol, orderId)`, `cancelAll(symbol)` | отмена |
| `orderStatus(symbol, orderId)` | статус и исполнение |
| `loadFilters(symbols)`, `loadBalances(store)` | правила торговли и балансы (без правил LIVE-старт отменяется) |
| `isPerp()` | клиент торгует фьючерсами (`market=perp`) |
| `reduceMarket(symbol, side, qty)` | закрывающий рыночный ордер: на фьючерсах — reduceOnly, на споте — обычный |
| `setLeverage(symbol, leverage)`, `loadPositions(store)` | плечо и открытые позиции (фьючерсы) |

На фьючерсах объём — тоже в монетах базовой валюты (у OKX клиент сам пересчитывает его в контракты по `ctVal`).
Клиенты фьючерсов: `BinanceFuturesClient` (USDⓈ-M), `BybitRestClient` с `category=linear`,
`OkxRestClient` с инструментами `-SWAP`, `HyperliquidRestClient`.

## Ручные ордера через админку

Вкладка «Ордера» веб-админки или напрямую:

```bash
H="X-Admin-Token: $ADMIN_TOKEN"
# рыночная покупка 0.001 BTC
curl -H "$H" -X POST "localhost:8080/order?exchange=binance&symbol=BTCUSDT&side=BUY&type=MARKET&qty=0.001"
# продать 50% баланса базовой валюты
curl -H "$H" -X POST "localhost:8080/order?exchange=binance&symbol=BTCUSDT&side=SELL&type=MARKET&portion=0.5"
# лимитная покупка на весь свободный USDT
curl -H "$H" -X POST "localhost:8080/order?exchange=binance&symbol=BTCUSDT&side=BUY&type=LIMIT&all=true&price=40000"
# отменить все ордера символа
curl -H "$H" -X POST "localhost:8080/cancel-all?exchange=binance&symbol=BTCUSDT"
```

На фьючерсах `side=SELL` открывает или увеличивает шорт, а «весь баланс» — это свободная маржа × `leverage`.
Закрыть позицию — `POST /positions/close?exchange=bybit&symbol=BTCUSDT` (reduceOnly, `symbol=all` — все).

Ответ: `orderId`, `статус`, `исполнено`, `средняя_цена`. Ордера проходят те же риск-проверки, что и
ордера стратегий (`RiskManager`: размер позиции, дневной убыток, частота, возраст данных).

## Безопасность

- Ключи — только с правами на торговлю, **без вывода средств**, с привязкой к IP сервера.
- Начинайте с `testnet=true` (где он есть) или PAPER и минимального `maxPositionQuote`.
- Клиенты всех бирж, кроме Binance и Bybit, не проверялись на живых API — следите за
  `GET /exchanges/request-stats` и логами на первом запуске.
