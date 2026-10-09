# hft-trader: структура проекта

Java 21, Maven, один fat-jar (`target/hft-trader.jar`). Бот стартует без бирж; выбор бирж, тикеров,
параметров и запуск торговли — через веб-админку (отдельный проект hft-admin-panel) или HTTP API.
Состояние хранится в SQLite (`data/state.db`), API-ключи — только в переменных окружения.

```
hft-trader/
├── src/main/java/com/hft/
│   ├── Main.java                     # точка входа: конфиг, SQLite, BotController, AdminServer
│   ├── admin/AdminServer.java        # HTTP API (JDK HttpServer), CORS для внешней админки
│   ├── control/BotController.java    # выбор бирж/символов, параметры, старт/стоп, торговля, автостарт
│   ├── config/                       # AppConfig (порт/токен админки), TradingParams (параметры биржи),
│   │                                 # GlobalParams (настройки процесса), ParamSpec (схема и проверка),
│   │                                 # Credentials (<ID>_API_KEY/_SECRET/_PASSPHRASE), ExchangeConfig
│   ├── persistence/                  # SqliteStateStore, PersistedState
│   ├── exchange/
│   │   ├── Exchange.java             # enum всех бирж: единственное место со строковыми id ("binance"…),
│   │   │                             # по нему выбираются клиенты, диалекты, лимиты, источники funding
│   │   ├── Market.java               # enum рынков SPOT/PERP ("spot"/"perp"); у каждой биржи свой набор
│   │   ├── ExchangeGateway.java      # общий контракт биржи
│   │   ├── ExchangeFactory.java      # id биржи -> реализация
│   │   ├── catalog/                  # ExchangeCatalog/ExchangeInfo: список бирж, комиссии, лимиты, статус адаптера
│   │   ├── binance/, bybit/          # REST-клиенты, ордера и приватные потоки (класс биржи — общий GeneralExchange);
│   │   │                             # BinanceFuturesClient — USDⓈ-M (fapi, ws-fapi, listenKey);
│   │   │                             # фьючерсы остальных: GateFuturesClient, KucoinFuturesClient, MexcFuturesClient,
│   │   │                             # Aster — тот же AsterRestClient с /fapi/v3
│   │   ├── okx/, gate/, mexc/, kucoin/, aster/, hyperliquid/, uniswapv2/
│   │   │                             # REST-клиенты на общем скелете SignedClient
│   │   └── generic/                  # GeneralExchange (класс биржи для всех), ContractSizes (размер контракта перпов), фиды стакана:
│   │                                 # WsBookFeed, PollingBookFeed, HybridBookFeed; диалекты
│   │                                 # Dialects (REST) и WsDialects (WS), LocalBook, FastJson
│   ├── rest/                         # TradingClient (общий контракт клиента), SignedClient, BinanceRestClient, RateBudget/RateLimits/PacedLimiter,
│   │                                 # WsRpcChannel (ордера по WS), WsSender, ExchangeOrderApi
│   ├── engine/                       # ядро исполнения: TickPipeline (Disruptor), MarketDataHandler,
│   │                                 # OrderService (риск, объём, баланс; владельцы позиций, стоп на бирже — syncStop)
│   ├── strategy/                     # все стратегии — спот и перпы вместе:
│   │                                 #   одна биржа (на тиках, StrategySet): MeanReversionStrategy (спот: лонг; перп: лонг и шорт),
│   │                                 #   StatArbStrategy (спот: дешёвая нога; перп: пара лонг/шорт), TriangularArbStrategy (только спот);
│   │                                 #   между биржами (по таймеру, BotController): FundingArbitrageStrategy, PerpPriceArbitrageStrategy (перп/перп),
│   │                                 #   FundingCarryStrategy (спот + шорт перпа); база Strategy, OrderExecutor
│   ├── risk/RiskManager.java         # проверки перед ордером, дневной лимит, kill switch
│   ├── paper/PaperOrderApi.java      # бумажное исполнение против живого стакана (спот и перпы)
│   ├── perp/                         # фьючерсный счёт: PerpAccount (позиции, плечо, опрос funding),
│   │                                 # FundingSource (ставки по REST бирж), PositionGuard (сторож позиций без стратегии
│   │                                 # и стопов на бирже)
│   ├── discovery/                    # подбор тикеров под стратегии: MarketSources (сводки 24ч бирж),
│   │                                 # профили стратегий, бэктест возврата к среднему
│   ├── store/                        # MarketDataStore, OrderBook (StampedLock), PriceWindow, BalanceStore, SymbolFilters,
│   │                                 # PositionStore (позиции перпов), FundingStore (ставки funding)
│   ├── metrics/                      # Latency (HdrHistogram), PrometheusExporter (/metrics)
│   ├── net/                          # WsClient — WebSocket-клиент на Netty для всех бирж (epoll/NIO);
│   │                                 # WsSettings — параметры соединения по биржам (предел сообщения, таймауты, жизнь соединения)
│   ├── crypto/                       # Keccak, Hex, EvmCrypto/Web3jCrypto (Hyperliquid, Uniswap)
│   ├── model/                        # Tick, OrderRequest, OrderResult, OrderEnums
│   └── util/                         # Signer/Hmac, Numbers, BoundedMap
├── src/main/resources/
│   └── logback.xml
├── src/test/java/                    # проверки-раннеры с main(): *Check.java (без JUnit)
├── run.sh                            # запуск с JVM-флагами под низкую задержку
├── .env.example                      # пример переменных окружения
├── README.md                         # основная документация и все эндпоинты
├── EXCHANGES.md                      # как бот работает с каждой биржей: WebSocket или REST
├── REST_CLIENT_GUIDE.md              # REST-клиенты бирж и ручные ордера
└── JVM_CONFIG_GUIDE.md               # JVM-флаги и конфигурация процесса
```

## Поддерживаемые биржи

| Биржа | Рынки | Реализация | Режим по умолчанию |
|---|---|---|---|
| Binance, Bybit | spot, perp | GeneralExchange + BinanceRestClient / BybitRestClient; перпы Binance — BinanceFuturesClient | LIVE при ключах и `live=true` |
| OKX, Gate, MEXC, KuCoin, Aster | spot, perp | GeneralExchange + свой RestClient (фьючерсы — Gate/Kucoin/MexcFuturesClient, OKX SWAP, Aster `/fapi/v3`) | PAPER, LIVE не проверен |
| Hyperliquid | perp | GeneralExchange + HyperliquidRestClient | PAPER, LIVE не проверен |
| Uniswap V2 | spot | GeneralExchange + UniswapV2Client | PAPER, LIVE не проверен |

Полный список с комиссиями — `GET /exchanges/catalog` или вкладка «Биржи» в админке.
Рынок биржи — параметр `market` (enum `Market`): по умолчанию `perp`, если он есть у биржи, иначе `spot`.
Рынок, которого у биржи нет, отклоняется при сохранении параметров.

## Как добавить биржу

1. Константа в enum `exchange/Exchange`: строковый id (ключ в админке и префикс переменных окружения) и рынки `Market`.
2. REST-клиент `exchange/<id>/<Id>RestClient extends SignedClient` (подпись, ордера, баланс, правила).
3. Диалект стакана в `generic/Dialects` (REST) и, если есть WS, в `generic/WsDialects`.
4. Строка в `ExchangeFactory`, `ExchangeCatalog`, лимиты в `rest/RateLimits`, источник сводок в `discovery/MarketSources`.
5. Для перпов — `setLeverage`, `loadPositions`, `reduceMarket` и защитный стоп `placeStopLoss` / `cancelStopLoss`.
6. Проверки в `src/test/java` (разбор ответов на фиктивном сервере `MiniWsServer`).

## Тесты

```bash
mvn -q package -DskipTests
javac -d target/checks -cp target/hft-trader.jar $(find src/test/java -name '*.java')
java -cp target/checks:target/hft-trader.jar WsFeedCheck     # любой *Check
```

Основные проверки: `FuturesCheck`, `FuturesClientsCheck` (фьючерсы), `WsTradeCheck`, `WsFeedCheck` (WebSocket),
`ReconnectCheck` (обрывы связи), `PositionProtectionCheck` (сторож позиций, стоп на бирже), `StrategiesCheck`,
`ExchangeLifecycleCheck` (биржа целиком).
