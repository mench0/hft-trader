# REST-клиенты бирж

Каждая биржа торгует через реализацию `com.hft.rest.ExchangeOrderApi`; стратегии и админка
обращаются к ней только через `OrderService` (риск-проверки, округление по правилам биржи, лимиты).

## Клиенты

| Биржа | Клиент | Ключи в `application.yml` |
|---|---|---|
| Binance | `rest/BinanceRestClient` | `keys.binance`: api-key, api-secret |
| Bybit | `exchange/bybit/BybitRestClient` | `keys.bybit`: api-key, api-secret |
| OKX | `exchange/okx/OkxRestClient` | `keys.okx`: api-key, api-secret, passphrase |
| Gate | `exchange/gate/GateRestClient` | `keys.gate`: api-key, api-secret |
| MEXC | `exchange/mexc/MexcRestClient` | `keys.mexc`: api-key, api-secret |
| BingX | `exchange/bingx/BingxRestClient` | `keys.bingx`: api-key, api-secret |
| KuCoin | `exchange/kucoin/KucoinRestClient` | `keys.kucoin`: api-key, api-secret, passphrase |
| Aster | `exchange/aster/AsterRestClient` | `keys.aster`: api-key (user), api-secret (signer) |
| Hyperliquid | `exchange/hyperliquid/HyperliquidRestClient` | `keys.hyperliquid`: api-key (адрес), api-secret (agent-ключ) |
| Uniswap V2 | `exchange/uniswap/UniswapV2Client` | `keys.uniswapv2`: api-key, api-secret |

Все клиенты, кроме Binance и Bybit, наследуют `SignedCexClient`: общие HTTP-клиент, подпись,
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

Ответ: `orderId`, `статус`, `исполнено`, `средняя_цена`. Ордера проходят те же риск-проверки, что и
ордера стратегий (`RiskManager`: размер позиции, дневной убыток, частота, возраст данных).

## Безопасность

- Ключи — только с правами на торговлю, **без вывода средств**, с привязкой к IP сервера.
- Начинайте с `testnet=true` (где он есть) или PAPER и минимального `maxPositionQuote`.
- Клиенты всех бирж, кроме Binance и Bybit, не проверялись на живых API — следите за
  `GET /exchanges/request-stats` и логами на первом запуске.
