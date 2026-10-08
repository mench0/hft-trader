# Биржи: как бот с ними работает

Принцип: **WebSocket везде, где биржа его даёт; REST — запасной канал** (сокет не готов или оборвался)
и то, чего у биржи по WebSocket нет. Сводка актуальна для кода в этой ветке; формат API каждой
биржи взят из её документации и проверен тестами на поддельном сервере, но **не на живой бирже** —
первый запуск делайте в testnet или в бумажном режиме.

## Сводная таблица

| Биржа | Стакан | Ордера и отмены | Исполнения | Баланс | Testnet |
|---|---|---|---|---|---|
| Binance | WS | **WS** | **WS** | **WS** | есть |
| Bybit | WS | **WS** | **WS** | **WS** | есть |
| OKX | WS | **WS** | **WS** | **WS** | есть (демо) |
| Gate | WS | **WS** | **WS** | **WS** | есть |
| KuCoin | WS | **WS** | **WS** | **WS** | нет |
| Hyperliquid | WS | **WS** | **WS** | **WS** | есть |
| Uniswap V2 | WS (сокет ноды) | **WS** (сокет ноды) | **WS** | **WS** | через `restUrl` |
| MEXC | WS (protobuf) | REST | **WS** (protobuf) | **WS** (protobuf) | нет |
| Aster | WS | REST | **WS** | **WS** | есть |

**REST** в столбце ордеров — это не недоделка, а отсутствие WebSocket-варианта у самой биржи.

Что идёт по REST у всех бирж, независимо от WebSocket: загрузка правил торговли (шаги цены и объёма)
на старте, начальный баланс и его сверка раз в `balanceSyncMs` (по умолчанию 5 минут).

## Фьючерсы (market=perp)

Параметр биржи `market`: `perp` — бессрочные USDT-фьючерсы, `spot` — спот. Фьючерсы есть у всех бирж, кроме
Uniswap V2 (AMM-пулы обмена — фьючерсов не бывает):

| Биржа | Стакан | Ордера и отмены | Исполнения, баланс, позиции | Плечо, сверка позиций | Funding |
|---|---|---|---|---|---|
| Binance USDⓈ-M | WS `fstream` `<symbol>@depth20@100ms` | **WS** `ws-fapi` (`order.place`, `order.cancel`, `order.status`); отмена всех — REST | **WS** listenKey: `ORDER_TRADE_UPDATE`, `ACCOUNT_UPDATE` | REST `/fapi/v1/leverage`, `/fapi/v2/positionRisk` | REST `/fapi/v1/premiumIndex` |
| Bybit linear | WS `/v5/public/linear` | **WS** `/v5/trade` (`category=linear`) | **WS** `/v5/private`: `order`, `wallet`, `position` | REST `/v5/position/set-leverage`, `/v5/position/list` | REST `/v5/market/tickers?category=linear` |
| OKX SWAP | WS `books` по `BTC-USDT-SWAP` | **WS** `order`, `cancel-order` (`tdMode=cross`) | **WS** `orders`, `account`, `positions` | REST `/api/v5/account/set-leverage`, `/positions` | REST `/api/v5/public/funding-rate` |
| Hyperliquid | WS `l2Book` | **WS** `post` (`r` — reduceOnly) | **WS** `orderUpdates`, `userFills` | `updateLeverage`, `clearinghouseState` | REST `metaAndAssetCtxs` |
| Gate USDT-фьючерсы | WS `fx-ws.gateio.ws` `futures.order_book` | REST `/api/v4/futures/usdt/orders` (размер — контракты со знаком, рынок — `price 0` + `ioc`) | REST (статус ордера, `/accounts`) | REST `/positions/{contract}/leverage`, `/positions` | REST `/futures/usdt/contracts` |
| KuCoin Futures | WS `/contractMarket/level2Depth50` (bullet-public фьючерсов) | REST `/api/v1/orders` (лоты, `leverage` в ордере, `marginMode=CROSS`) | REST (статус, `/account-overview`) | REST `/api/v2/changeCrossUserLeverage`, `/positions` | REST `/api/v1/contracts/active` |
| MEXC Contract | WS `contract.mexc.com/edge` `sub.depth.full` | REST `/api/v1/private/order/submit` (сторона 1–4 задаёт открытие/закрытие) | REST (статус, `/account/asset`) | REST `/position/change_leverage`, `/open_positions` | REST `/contract/funding_rate/{symbol}` |
| Aster Futures | WS `fstream.asterdex.com` (формат Binance) | REST `/fapi/v3/order` (подпись EIP-712) | **WS** listenKey: `ORDER_TRADE_UPDATE`, `ACCOUNT_UPDATE` | REST `/fapi/v3/leverage`, `/positionRisk` | REST `/fapi/v1/premiumIndex` |

- Ставки funding читаются по REST раз в `fundingPollSec` (30 с): ставка меняется медленно, отдельный сокет не нужен.
- OKX, Gate, KuCoin и MEXC считают объём в контрактах: бот пересчитывает его в монеты по размеру контракта
  (OKX `ctVal`, Gate `quanto_multiplier`, KuCoin `multiplier`, MEXC `contractSize`), справочник читается один раз.
- KuCoin Futures называет биткоин XBT: BTCUSDT → `XBTUSDTM`.
- У Gate, KuCoin и MEXC фьючерсные ордера идут по REST (исполнение рыночного ордера дочитывается статусом);
  WebSocket-торговля для их фьючерсов — следующий шаг. MEXC выдаёт доступ к фьючерсным ордерам через API отдельно —
  без него ордера отклоняются, а стакан, funding и бумажный режим работают.
- У MEXC режим позиций — раздельный (лонг и шорт отдельно): закрытие идёт reduceOnly-ордером со сторонами «закрыть
  лонг/шорт», позиции в боте складываются со знаком.
- Режим позиций — односторонний (One-way / net), маржа — кросс. Режим хеджирования на бирже включать не нужно.
- Ключи те же, что для спота; у ключа должно быть право торговли фьючерсами (у Binance — «Enable Futures»).
- Testnet фьючерсов: Binance — `testnet.binancefuture.com`, Bybit — `api-testnet.bybit.com`, OKX — демо-торговля, Hyperliquid — testnet,
  Gate — `fx-api-testnet.gateio.ws`, Aster — `fapi.asterdex-testnet.com`. У KuCoin и MEXC тестовой сети фьючерсов нет.
- Ключи те же переменные: `GATE_*`, `KUCOIN_*` (+ `KUCOIN_PASSPHRASE`), `MEXC_*`, `ASTER_*`.

## По биржам

### Binance — спот
- **Стакан:** WS `<symbol>@depth20@100ms` и сделки `<symbol>@trade` (свой фид на Netty).
- **Ордера:** WebSocket API `wss://ws-api.binance.com/ws-api/v3` — `order.place`, `order.cancel`,
  `openOrders.cancelAll`, `order.status`. Каждый запрос подписан ключом (HMAC-SHA256).
- **Исполнения и баланс:** на том же сокете, подписка `userDataStream.subscribe.signature`:
  `executionReport`, `outboundAccountPosition`.
- **REST:** синхронизация времени, правила, баланс на старте; ордера — если сокет не готов.
- **Ключи:** `BINANCE_API_KEY`, `BINANCE_API_SECRET`.

### Bybit — спот (UNIFIED)
- **Стакан:** WS `orderbook.50.<symbol>` и `publicTrade.<symbol>` (свой фид на Netty).
- **Ордера:** торговый сокет `wss://stream.bybit.com/v5/trade` — `order.create`, `order.cancel`.
- **Исполнения и баланс:** приватный сокет `/v5/private` — потоки `order` и `wallet`.
- **REST:** правила, баланс на старте, **отмена всех ордеров символа** (в торговом WS такой команды нет).
- **Ключи:** `BYBIT_API_KEY`, `BYBIT_API_SECRET`.
- Bybit блокирует IP многих дата-центров (ответ 403) — проверьте доступ с вашего VPS.

### OKX — спот
- **Стакан:** WS `books` (публичный сокет).
- **Ордера:** приватный сокет — `order`, `cancel-order`, `batch-cancel-orders`.
- **Исполнения и баланс:** там же — каналы `orders` и `account`.
- **REST:** правила, список открытых ордеров на старте.
- **Ключи:** `OKX_API_KEY`, `OKX_API_SECRET`, `OKX_PASSPHRASE`.

### Gate — спот
- **Стакан:** WS `spot.order_book`.
- **Ордера:** WS API — `spot.order_place`, `spot.order_cancel` и др.
- **Исполнения и баланс:** WS `spot.orders`, `spot.balances`.
- **REST:** правила.
- **Ключи:** `GATE_API_KEY`, `GATE_API_SECRET`.

### KuCoin — спот
- **Стакан:** WS `/spotMarket/level2Depth50` (адрес и токен перед каждым подключением — `bullet-public`).
- **Ордера:** Pro WS API `wss://wsapi.kucoin.com/v1/private` — `spot.order`, `spot.cancel`.
  Сервер присылает приветствие, бот отвечает его подписью — после этого сокет готов.
- **Исполнения и баланс:** приватный поток через `bullet-private` — `/spotMarket/tradeOrdersV2`, `/account/balance`.
- **REST:** правила, баланс на старте, отмена всех ордеров символа.
- **Ключи:** `KUCOIN_API_KEY`, `KUCOIN_API_SECRET`, `KUCOIN_PASSPHRASE`.
- **Счёт UTA (единый торговый счёт):** `KUCOIN_UTA=true` — ордера идут командами `uta.order` / `uta.cancel`
  с `tradeType=SPOT`. Схема аргументов UTA сверена с документацией не полностью — первый ордер на малую сумму.

### Hyperliquid — бессрочные контракты
- **Стакан:** WS `l2Book`.
- **Ордера:** WS `post` (действие подписывается agent-ключом, EIP-712).
- **Исполнения и баланс:** WS `orderUpdates`, `userFills`; состояние аккаунта — info-запросом через WS `post`.
- **REST:** только запасной канал.
- **Ключи:** `HYPERLIQUID_API_KEY` — адрес основного аккаунта, `HYPERLIQUID_API_SECRET` — ключ agent-кошелька без права вывода.

### Uniswap V2 (и совместимые AMM)
- **Цена:** события `Sync` пулов через `eth_subscribe` на WebSocket ноды; первая цена — `eth_call`.
- **Свопы и чтение:** JSON-RPC по сокету ноды, HTTP — запасной.
- Цена меняется раз в блок (~12 с в Ethereum); лимитных ордеров и отмен на AMM нет.
- **Ключи:** `UNISWAPV2_API_KEY`, `UNISWAPV2_API_SECRET` (ключ горячего кошелька с малой суммой);
  роутер, токены и пулы — параметры биржи в админке, RPC ноды — `restUrl`.

### MEXC — спот
- **Стакан:** WS `wss://wbs-api.mexc.com/ws`, канал `spot@public.limit.depth.v3.api.pb@<символ>@<5|10|20>`.
  Данные в **protobuf**, разбор — свой декодер. **Не больше 30 символов на соединение**: символы сверх лимита
  остаются без WS-стакана (в лог пишется предупреждение).
- **Ордера:** **REST** — WebSocket-ордеров у MEXC нет.
- **Исполнения и баланс:** приватный поток через listenKey (`POST /api/v3/userDataStream`, продление раз в 25 минут),
  каналы `spot@private.orders.v3.api.pb` и `spot@private.account.v3.api.pb`, тоже protobuf.
- **Ключи:** `MEXC_API_KEY`, `MEXC_API_SECRET`. Тестовой сети нет.

### Aster — спот (API v3, формат Binance)
- **Стакан:** WS `<symbol>@depth20@100ms`.
- **Ордера:** **REST** (подпись EIP-712 ключом агента) — WebSocket-ордеров у Aster нет.
- **Исполнения и баланс:** приватный поток через listenKey: `executionReport`, `outboundAccountPosition`.
- **Ключи:** `ASTER_API_KEY` — адрес основного кошелька, `ASTER_API_SECRET` — ключ API-кошелька без права вывода.

## Общие правила

- **Отключить WebSocket-торговлю** для биржи: параметр биржи `wsTrade=false` в админке — ордера и приватные
  данные пойдут по REST. Стакан всё равно идёт по WebSocket.
- **Спот или фьючерсы:** параметр биржи `market` (`perp`/`spot`), плечо — `leverage`; Uniswap V2 принимает только `spot`, Hyperliquid — только `perp`.
- **Реальные ордера** уходят только при двух условиях: ключи в `.env` (или окружении) и параметр биржи `live=true`.
  Иначе — бумажный режим на живых данных.
- **Если сокет со стаканом молчит** дольше порога, включается REST-опрос стакана; новые позиции на нём не
  открываются. Когда сокет оживает, опрос встаёт на паузу.
- **Защита от повторных ордеров:**
  - сокет не готов — ордер уходит по REST;
  - ордер ушёл, ответа нет — **вслепую не повторяется**: бот находит его на бирже по своему id ордера и узнаёт, что с ним стало;
  - отказ биржи по WebSocket — это ответ, а не сбой, по REST ордер не повторяется;
  - пять неудачных логинов подряд отключают сокет, остаётся REST.
- **Как проверить на первом запуске:** `GET /exchanges/request-stats` (или вкладка «Обзор» в админке) —
  поля `ws.ready`, `ws.events`, `ws.parseErrors`, `ws.lastError`, `wsFallbacks`. Растущие `parseErrors`
  или частые `wsFallbacks` — признак, что формат биржи отличается от документации.
- Переменные для каждой биржи — в `.env.example`: ключи, `<БИРЖА>_TESTNET`, `<БИРЖА>_LIVE`.
