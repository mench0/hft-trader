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
│   │   ├── ExchangeGateway.java      # общий контракт биржи
│   │   ├── ExchangeFactory.java      # id биржи -> реализация
│   │   ├── catalog/                  # ExchangeCatalog/ExchangeInfo: список бирж, комиссии, лимиты, статус адаптера
│   │   ├── binance/, bybit/          # нативные адаптеры (свой WS-фид на Netty + REST)
│   │   ├── okx/, gate/, mexc/, bingx/, kucoin/, aster/, hyperliquid/, uniswap/
│   │   │                             # REST-клиенты на общем скелете SignedCexClient
│   │   └── generic/                  # SignedCexExchange, PaperExchange (dYdX), фиды стакана:
│   │                                 # WsBookFeed, PollingBookFeed, HybridBookFeed; диалекты
│   │                                 # Dialects (REST) и WsDialects (WS), LocalBook, FastJson
│   ├── rest/                         # SignedCexClient, BinanceRestClient, RateBudget/RateLimits/PacedLimiter,
│   │                                 # WsRpcChannel (ордера по WS), WsSender, ExchangeOrderApi
│   ├── engine/                       # TickPipeline (Disruptor), MarketDataHandler, OrderService, OrderExecutor,
│   │                                 # стратегии: MeanReversion, StatArb, TriangularArb (StrategySet)
│   ├── risk/RiskManager.java         # проверки перед ордером, дневной лимит, kill switch
│   ├── paper/PaperOrderApi.java      # бумажное исполнение против живого стакана
│   ├── discovery/                    # подбор тикеров под стратегии: MarketSources (сводки 24ч бирж),
│   │                                 # профили стратегий, бэктест возврата к среднему
│   ├── store/                        # MarketDataStore, OrderBook (StampedLock), PriceWindow, BalanceStore, SymbolFilters
│   ├── metrics/                      # Latency (HdrHistogram), PrometheusExporter (/metrics)
│   ├── net/AbstractWsFeed.java       # общий Netty WS-клиент для Binance/Bybit
│   ├── crypto/                       # Keccak, Hex, EvmCrypto/Web3jCrypto (Hyperliquid, Uniswap)
│   ├── model/                        # Tick, OrderRequest, OrderResult, OrderEnums
│   └── util/                         # Signer/Hmac, Numbers, BoundedMap, MsgPack
├── src/main/resources/
│   └── logback.xml
├── src/test/java/                    # проверки-раннеры с main(): *Check.java (без JUnit)
├── run.sh                            # запуск с JVM-флагами под низкую задержку
├── .env.example                      # пример переменных окружения
├── README.md                         # основная документация и все эндпоинты
├── REST_CLIENT_GUIDE.md              # REST-клиенты бирж и ручные ордера
└── JVM_CONFIG_GUIDE.md               # JVM-флаги и конфигурация процесса
```

## Поддерживаемые биржи

| Биржа | Реализация | Режим по умолчанию |
|---|---|---|
| Binance, Bybit | BinanceExchange, BybitExchange | LIVE при ключах и `live=true` |
| OKX, Gate, MEXC, BingX, KuCoin, Aster | SignedCexExchange + свой RestClient | PAPER, LIVE не проверен |
| Hyperliquid, Uniswap V2 | SignedCexExchange + HyperliquidRestClient / UniswapV2Client | PAPER, LIVE не проверен |
| dYdX v4 | PaperExchange | только PAPER |

Полный список с комиссиями и заметками — `GET /exchanges/catalog` или вкладка «Биржи» в админке.

## Как добавить биржу

1. REST-клиент `exchange/<id>/<Id>RestClient extends SignedCexClient` (подпись, ордера, баланс, правила).
2. Диалект стакана в `generic/Dialects` (REST) и, если есть WS, в `generic/WsDialects`.
3. Строка в `ExchangeFactory`, `ExchangeCatalog`, лимиты в `rest/RateLimits`, источник сводок в `discovery/MarketSources`.
4. Проверки в `src/test/java` (разбор ответов на фиктивном сервере `MiniWsServer`).

## Тесты

```bash
mvn -q package -DskipTests
javac -d target/checks -cp target/hft-trader.jar $(find src/test/java -name '*.java')
java -cp target/checks:target/hft-trader.jar WsFeedCheck     # любой *Check
```
