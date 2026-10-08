# HFT Trader

Торговый бот на Java 21. Все рыночные данные держатся в оперативной
памяти, ничего не пишется в базу. Без Spring и других тяжёлых фреймворков —
весь граф зависимостей собирается вручную в `Main.java`.

## Что внутри

| Слой | Библиотека | Зачем именно она |
|---|---|---|
| Сеть | Netty 4.1 | Прямой контроль над event loop, буферами и поведением при разрыве. WebSocket написан на голом Netty, без обёрток |
| Конвейер | LMAX Disruptor 4 | Кольцевой буфер без блокировок. Объекты `Tick` выделяются один раз и переиспользуются — в горячем пути нет работы для GC |
| Коллекции | Agrona | Структуры без боксинга от авторов Aeron |
| Метрики | HdrHistogram | Перцентили p50/p99/p999. Среднее время скрывает всплески |
| REST | `java.net.http` (JDK) | HTTP/2, пул соединений и keep-alive из коробки, без лишних зависимостей |
| JSON/YAML | Jackson | Парсинг сообщений биржи и конфигурации |
| Логи | SLF4J + Logback | Без автоконфигурации фреймворков |
| Админка | `com.sun.net.httpserver` (JDK) | Для десятка служебных эндпоинтов Jetty не нужен |

## Путь данных

```
Binance WebSocket
      │  один комбинированный стрим на все символы
      │  @trade (сделки) + @depth20@100ms (стакан)
      ▼
 MarketDataFeed (Netty)          метка времени ставится до разбора JSON
      │
      ▼
 TickPipeline (Disruptor)        кольцо на 4096 переиспользуемых Tick
      │
      ├──► MarketDataHandler ──► MarketDataStore   запись в память
      │
      └──► Strategy ──► OrderService ──► RiskManager ──► BinanceRestClient
```

Обработчики в конвейере идут последовательно: сначала тик попадает в память,
потом стратегия его видит уже в контексте актуального состояния.

## Хранение в памяти

Всё состояние рынка живёт в `MarketDataStore`:

**`OrderBook`** — стакан на двух парах плоских `double[]` (цены и объёмы
отдельно). Не `TreeMap`: массивы не аллоцируют объекты при обновлении, лежат
подряд в памяти и читаются одним кэш-лайном. Что умеет:

- `bestBid()`, `bestAsk()`, `midPrice()` — верх стакана
- `microPrice()` — середина, взвешенная объёмами. Лучший прогноз следующего
  движения, чем простой mid
- `imbalance(levels)` — перевес одной стороны от -1 до +1
- `estimateBuyPrice(qty)` / `estimateSellPrice(qty)` — проход по стакану:
  во сколько реально обойдётся рыночный ордер
- `estimateSlippagePercent(qty, isBuy)` — ожидаемое проскальзывание
- `findWall(bidSide, threshold)` — поиск крупных заявок

**`PriceWindow`** — кольцевой буфер последних N цен. Среднее и дисперсия
считаются инкрементально по Уэлфорду, а не пересчётом по всему окну: иначе на
окне в 1000 элементов это 1000 операций на каждую сделку.

- `zScore(value)`, `currentZScore()` — отклонение в сигмах
- `changePercent()`, `min()`, `max()`

**`BalanceStore`** — балансы. Нужен, чтобы ордер «на весь баланс» не ходил
каждый раз в REST (это +50–200 мс и расход лимита запросов).

## Методы ордеров

Все комбинации тип × объём. Через `OrderService`:

```java
// Лимитные
orders.buyLimit("BTCUSDT", 0.01, 40000);      // заданный объём
orders.buyLimitAll("BTCUSDT", 40000);          // на весь баланс USDT
orders.sellLimit("BTCUSDT", 0.01, 45000);
orders.sellLimitAll("BTCUSDT", 45000);         // весь доступный BTC

// Рыночные
orders.buyMarket("BTCUSDT", 0.01);             // заданный объём
orders.buyMarketAll("BTCUSDT");                // на весь баланс
orders.sellMarket("BTCUSDT", 0.01);
orders.sellMarketAll("BTCUSDT");

// Доля баланса
orders.buyMarketPortion("BTCUSDT", 0.25);      // на четверть баланса
orders.sellMarketPortion("BTCUSDT", 0.5);      // продать половину позиции

// Пассивный вход по стакану
orders.buyLimitAtBid("BTCUSDT", 0.01, 0);      // встать на лучший бид
orders.sellLimitAtAsk("BTCUSDT", 0.01, 1);     // на тик выше лучшего аска

// Отмена
orders.cancel("BTCUSDT", orderId);
orders.cancelAll("BTCUSDT");
orders.panicClose("причина");                   // отменить всё и остановиться
```

Через билдер `OrderRequest` доступны тонкие настройки:

```java
orders.execute(
    OrderRequest.limit("BTCUSDT", Side.BUY, 40000)
        .quantity(0.01)
        .immediateOrCancel()   // исполнить что можно, остаток отменить
);

orders.execute(
    OrderRequest.limit("BTCUSDT", Side.SELL, 45000)
        .balancePortion(0.3)   // 30% доступного объёма
        .fillOrKill()          // только целиком
);
```

**Time In Force** для лимитных ордеров:
- `GTC` — висит в стакане до отмены или исполнения
- `IOC` — исполнить что получится прямо сейчас, остаток снять. Допускает
  частичное исполнение
- `FOK` — целиком или ничего

**Частичное исполнение** видно в `OrderResult`:

```java
OrderResult r = orders.buyLimit("BTCUSDT", 1.0, 40000);
r.executedQty();    // сколько реально исполнилось
r.remainingQty();   // сколько осталось
r.fillRatio();      // 0.3 = исполнилось 30%
r.isPartial();      // статус PARTIALLY_FILLED
```

Как объём «весь баланс» превращается в число: для BUY берутся свободные USDT,
вычитается резерв под комиссию (`fee-reserve-percent`), делится на цену. Для
SELL берётся свободный BTC. Затем результат округляется вниз до шага лота
биржи — `SymbolFilters` знает эти правила, они загружаются при старте из
`/api/v3/exchangeInfo`. Без такого округления биржа отвечает `Filter failure:
LOT_SIZE`, и причина неочевидна.

## Проверки перед отправкой

`RiskManager` пропускает ордер, только если:

- kill switch не взведён и торговля включена в параметрах биржи (`tradingEnabled`)
- не превышен лимит ордеров в минуту `maxOrdersPerMinute` (защита от цикла в стратегии)
- дневной убыток не достиг `maxDailyLossQuote` — иначе kill switch взводится сам.
  Считается реализованный результат закрытых позиций после комиссий (`takerFeePercent`),
  счётчик обнуляется в 00:00 UTC
- данные по символу свежие (стакан не старше `maxDataAgeMs`)
- размер позиции в деньгах не больше `maxPositionQuote`
- для рыночных ордеров ожидаемое проскальзывание не выше
  `maxSlippagePercent` — считается проходом по реальному стакану

Все лимиты задаются для каждой биржи отдельно, см. «Торговые параметры».

## Запуск

Нужна Java 21 и Maven.

```bash
mvn clean package
./run.sh
```

Бот стартует в **режиме настройки** — никакая биржа ещё не подключена.
Дальше всё управление идёт через веб-админку (отдельный проект hft-admin-panel,
см. раздел «Веб-админка») или напрямую через HTTP API (см. ниже).

Минимальный путь через curl:

```bash
# 1. Выбрать биржу и тикеры
curl -X POST "localhost:8080/control/select?exchange=binance&symbols=BTCUSDT,ETHUSDT"

# 2. Поднять WebSocket/REST соединения
curl -X POST localhost:8080/control/start

# 3. Проверить, что данные идут
curl localhost:8080/market?exchange=binance

# 4. Включить реальную торговлю (после того как всё проверено)
curl -X POST "localhost:8080/trading/start?exchange=binance"
```

Без API-ключей биржи (`BINANCE_API_KEY`/`BINANCE_API_SECRET`,
`BYBIT_API_KEY`/`BYBIT_API_SECRET`) бот всё равно поднимет соединения и
будет собирать рыночные данные — они публичные. Не будет работать только
отправка ордеров.

## Веб-админка

Отдельный проект `hft-admin-panel` (статическая страница `index.html`), в этот репозиторий не входит.
Сборка фронтенда не нужна: откройте файл в браузере или положите на любой веб-сервер
(`cd admin-panel && python3 -m http.server 8000`, nginx, GitHub Pages). В шапке укажите адрес API бота
(по умолчанию `http://localhost:8080`) и `ADMIN_TOKEN`; оба хранятся в localStorage браузера.
Страница ходит в API через fetch — CORS у бота открыт.

| Вкладка | Что делает | Эндпоинты |
|---|---|---|
| Обзор | поднять/остановить соединения, торговля вкл/выкл (везде или по бирже), PANIC, автостарт; состояние выбранных бирж (связь, PnL, отказы риска, kill switch) и статистика запросов | `/control/*`, `/trading/*`, `/status`, `/exchanges/request-stats` |
| Биржи | каталог бирж с типом, адаптером, комиссиями и лимитами; выбрать биржу с символами, добавить/убрать символ, убрать биржу | `/exchanges/catalog`, `/control/select`, `/control/symbols`, `/control/deselect` |
| Параметры | форма по схеме параметров выбранной биржи (границы, значение по умолчанию, ⟳ — нужен перезапуск); сохраняются только изменённые поля | `/exchange/params/schema`, `/exchange/params` |
| Рынок | верх стакана, спред, дисбаланс, z-score по символам; балансы; состояние стратегий | `/market`, `/balances`, `/strategies` |
| Ордера | ручной ордер (MARKET/LIMIT, количество, доля или весь баланс), отмена всех ордеров символа | `/order`, `/cancel-all` |
| Подбор | результат подбора тикеров по стратегиям, статус источников бирж, пересчёт, «в выбор» одной кнопкой | `/discovery`, `/discovery/refresh` |
| Настройки | настройки процесса по схеме | `/settings` |

Опасные действия (включение торговли, PANIC, ручной ордер, остановка) спрашивают подтверждение.
Данные обновляются каждые 3 с (флажок «автообновление»); вкладки с формами сами не перерисовываются.
Не открывайте порт админки в интернет: доступ — через SSH-туннель или VPN, и обязательно с `ADMIN_TOKEN`.

## Переменные окружения и файл .env

Ключи бирж, токен и порт админки задаются переменными окружения. Удобнее всего — файлом `.env`:

```bash
cp .env.example .env && chmod 600 .env   # заполнить нужные ключи
./run.sh
```

- Бот сам читает `.env` из рабочей папки (другой путь — `ENV_FILE=/путь/.env`), `source` не нужен.
- Файл перечитывается при изменении: новые ключи бирж применяются при следующем
  `/control/start` («Остановить» → «Поднять соединения»), перезапуск процесса не нужен.
  `ADMIN_*` и `STATE_DB` — только после перезапуска.
- Значение из `.env` важнее переменной, заданной через `export` или systemd `Environment=`.
- Режим каждой биржи: `<БИРЖА>_TESTNET` (тестовая сеть) и `<БИРЖА>_LIVE` (реальные ордера), `true`/`false`.
  Применяются при перезапуске бота — и тогда важнее значения, сохранённого из админки, — а также как значение
  по умолчанию при выборе биржи. Пусто — режим берётся из админки. `testnet=true` для биржи без тестовой сети
  (MEXC, KuCoin) не применяется, в лог пишется предупреждение. Тикеры в `.env` не задаются — они
  выбираются в админке во время работы.
- Из админки: вкладка «Окружение», или API:
  - `GET /env` — какие переменные заданы и откуда (секреты замаскированы, наружу не отдаются);
  - `POST /env?BINANCE_API_KEY=…&BINANCE_API_SECRET=…` — записать в `.env` (пустое значение удаляет строку).
    Принимаются только известные боту имена; файл пишется атомарно с правами 600.
  Секреты при этом идут по сети — используйте только с `ADMIN_TOKEN` и через SSH-туннель.

## Управление на сервере — полный список эндпоинтов

Админка на порту 8080. Если задан `ADMIN_TOKEN`, добавляйте заголовок
`X-Admin-Token` (или `Authorization: Bearer <токен>`). CORS открыт для всех источников —
к API можно обращаться и из внешней панели на другом домене.

### Выбор бирж и тикеров (до старта)

```bash
# Что поддерживается / что сейчас выбрано
curl localhost:8080/control/exchanges
curl localhost:8080/control/status

# Выбрать биржу с тикерами (перезаписывает список для неё)
curl -X POST "localhost:8080/control/select?exchange=binance&symbols=BTCUSDT,ETHUSDT"
curl -X POST "localhost:8080/control/select?exchange=bybit&symbols=BTCUSDT"

# Убрать биржу из выбора совсем
curl -X POST "localhost:8080/control/deselect?exchange=bybit"

# Добавить/убрать один символ у уже выбранной биржи
curl -X POST "localhost:8080/control/symbols?exchange=binance&add=BNBUSDT"
curl -X POST "localhost:8080/control/symbols?exchange=binance&remove=BNBUSDT"
```

Выбор можно менять только пока бот не запущен (`/control/start` ещё не
вызывался, либо после `/control/stop`).

### Жизненный цикл

```bash
curl -X POST localhost:8080/control/start   # поднять соединения для выбранных бирж
curl -X POST localhost:8080/control/stop    # разорвать все соединения, выбор сохраняется
```

### Торговля (поверх уже поднятых соединений)

```bash
curl -X POST "localhost:8080/trading/start?exchange=binance"  # или exchange=all
curl -X POST "localhost:8080/trading/stop?exchange=all"
curl -X POST "localhost:8080/trading/panic?exchange=all"        # отменить всё и заглушить
```

### Параметры биржи — отдельно для каждой выбранной биржи

Все настройки биржи — подключение (testnet, live, адреса), риск, стратегии, работа фидов,
Uniswap — задаются только через админку и хранятся в SQLite. Файл `application.yml` бот не читает:
админка (`ADMIN_*`), путь к базе и API-ключи (`<ID>_API_KEY`, `<ID>_API_SECRET`, `<ID>_PASSPHRASE`) — только в `.env` или окружении.

Параметры есть только у выбранных бирж: их можно передать прямо при выборе, при снятии биржи
с выбора они удаляются. Полный список с границами и справкой — `GET /exchange/params/schema`.

```bash
# выбрать биржу и сразу задать её параметры
curl -X POST "localhost:8080/control/select?exchange=bybit&symbols=BTCUSDT,ETHUSDT&testnet=false&live=true&maxPositionQuote=50"
curl localhost:8080/exchange/params/schema                 # описание всех параметров
```

**Testnet.** Параметр `testnet` (по умолчанию `true`, если у биржи есть тестовая сеть) переключает
REST и WebSocket на тестовые адреса: Binance, Bybit, OKX (демо-торговля), Gate, Hyperliquid, Aster.
У KuCoin и MEXC тестовой сети нет — для них `testnet=true` отклоняется. Для Uniswap
тестовая сеть задаётся адресом RPC (`restUrl`). Свои адреса — параметры `restUrl` и `wsUrl`.

**Настройки процесса** (не биржи): `GET/POST /settings` — интервалы фоновых задач, доля лимита
запросов (`rateLimitSafety`), подбор тикеров (биржи, валюты, период, пороги профилей, бэктест).

```bash
curl localhost:8080/exchange/params                      # все выбранные/настроенные биржи
curl "localhost:8080/exchange/params?exchange=bybit"     # одна биржа
curl -X POST "localhost:8080/exchange/params?exchange=bybit&maxPositionQuote=50&maxDailyLossQuote=20&entryZ=2.5"
```

Любой параметр можно передать отдельно — остальные не меняются. Значение вне
допустимого диапазона или неизвестный параметр — ошибка 400, и не меняется ничего.
Работающая биржа подхватывает новые значения на следующем тике, без перезапуска;
`bookDepth` и `priceWindow` — после `/control/stop` и `/control/start`.

| Параметр | По умолчанию | Что это |
|---|---|---|
| `market` | `perp` у Binance, Bybit, OKX, Hyperliquid; `spot` у остальных | рынок: `perp` — бессрочные фьючерсы, `spot` — спот (после перезапуска биржи), см. [Фьючерсы](#фьючерсы-perp-и-funding-арбитраж) |
| `leverage` | 2 | плечо на фьючерсах, выставляется на бирже при старте |
| `tradingEnabled` | false | торговля на бирже разрешена (также `/trading/start`/`stop`) |
| `maxPositionQuote` | 100 | максимальный размер ордера в котируемой валюте; на фьючерсах — ещё и предел стоимости позиции |
| `maxDailyLossQuote` | 50 | дневной лимит убытка, после него kill switch |
| `maxSlippagePercent` | 0.3 | предел ожидаемого проскальзывания рыночного ордера, % |
| `feeReservePercent` | 0.2 | резерв под комиссию при ордере «на весь баланс», % |
| `takerFeePercent` | из каталога биржи | комиссия тейкера, %: списывается из локального баланса сразу после сделки, вычитается из результата, входит в пороги входа |
| `tradeCostQuote` | 0 | фиксированная стоимость одной сделки помимо комиссии (Uniswap — газ свопа), в котируемой валюте |
| `maxOrdersPerMinute` | 30 | лимит ордеров в минуту |
| `maxDataAgeMs` | 5000 | старше — ордер отклоняется (нет свежих данных) |
| `entryZ` / `exitZ` | 2.0 / 0.3 | пороги входа/выхода по z-score |
| `stopLossPercent` | 0.5 | стоп-лосс, % |
| `minImbalance` / `imbalanceLevels` | 0.15 / 5 | минимальный перевес бидов и по скольким уровням он считается |
| `orderQuote` | 20 | размер сделки стратегии в котируемой валюте |
| `maxBookAgeMs` | 2000 | вход только по стакану не старше |
| `maxSpreadPercent` | 0.1 | не входить при спреде шире, % |
| `positionTimeoutMs` | 3600000 | закрыть позицию по таймауту |
| `bookDepth` / `priceWindow` | 20 / 1000 | глубина стакана и окно цен (после перезапуска биржи) |
| `meanReversionEnabled` / `triangularEnabled` / `statArbEnabled` | true / false / false | какие стратегии работают на бирже (на лету) |
| `triHomeAsset` | USDT | валюта, с которой начинается и где заканчивается треугольный круг |
| `triMinProfitPercent` | 0.15 | минимальная чистая прибыль круга после трёх комиссий, % |
| `triOrderQuote` | 20 | размер круга в `triHomeAsset` |
| `triMaxBookAgeMs` / `triCooldownMs` | 1000 / 3000 | свежесть всех трёх стаканов; пауза перед повтором того же круга |
| `triUnwindOnFail` | true | нога не исполнилась — продать остаток обратно в `triHomeAsset` |
| `statArbPairs` | пусто | пары `A/B,C/D`; пусто — все пары выбранных символов с одной котируемой валютой (до 15) |
| `statArbWindow` / `statArbSampleMs` | 300 / 1000 | окно в отсчётах и шаг отсчётов по времени |
| `statArbEntryZ` / `statArbExitZ` / `statArbStopZ` | 2.0 / 0.5 / 4.0 | вход, выход и стоп по z-score спреда |
| `statArbMinCorrelation` | 0.6 | корреляция доходностей пары не ниже |
| `statArbOrderQuote` / `statArbMaxHoldMs` | 20 / 3600000 | размер позиции; закрыть по таймауту |

## Фьючерсы (perp) и funding-арбитраж

У каждой биржи есть параметр **`market`**: `perp` — бессрочные фьючерсы с расчётом в USDT (USDC у Hyperliquid),
`spot` — спот. Выбирается в админке (вкладка «Параметры») или запросом; применяется после перезапуска биржи.

| Биржа | Фьючерсы | Что используется |
|---|---|---|
| Binance | USDⓈ-M | REST `fapi.binance.com`, стакан `fstream`, ордера WebSocket API `ws-fapi`, исполнения/баланс/позиции — поток по listenKey |
| Bybit | linear | v5 `category=linear`: стакан `/v5/public/linear`, ордера `/v5/trade`, позиции — поток `position` |
| OKX | SWAP | инструменты `BTC-USDT-SWAP`, `tdMode=cross`, объём в контрактах (`ctVal`) пересчитывается в монеты; позиции — канал `positions` |
| Hyperliquid | только перпы | то же, что и раньше; плечо — действие `updateLeverage`, позиции — `clearinghouseState` |
| Gate, KuCoin, MEXC, Aster, Uniswap V2 | нет | только `market=spot`; `market=perp` отклоняется |

Новая биржа с фьючерсами получает `market=perp`, комиссию тейкера по фьючерсному тарифу и плечо `leverage=2`.

```bash
curl -X POST "localhost:8080/control/select?exchange=bybit&symbols=BTCUSDT,ETHUSDT&market=perp&leverage=3"
curl -X POST "localhost:8080/exchange/params?exchange=binance&market=spot"     # вернуть спот
curl "localhost:8080/positions"                                                # позиции, плечо, funding по биржам
curl -X POST "localhost:8080/positions/close?exchange=bybit&symbol=BTCUSDT"    # закрыть рыночным reduceOnly (symbol=all — все)
curl "localhost:8080/funding"                                                  # ставки и состояние funding-арбитража
```

**Как устроено:**
- позиция хранится объёмом со знаком (лонг +, шорт −) и средней ценой входа; обновляется после каждой сделки,
  из приватного потока биржи и сверкой по REST раз в `balanceSyncMs`;
- ордер «на весь баланс» на фьючерсах = свободная маржа × `leverage` / цена, в обе стороны;
- риск: стоимость позиции после сделки ≤ `maxPositionQuote`, начальная маржа (объём × цена / плечо) ≤ свободных средств;
- закрытие идёт **reduceOnly**-ордером: он не может открыть встречную позицию, и его не блокируют ни kill switch,
  ни лимиты входа — чтобы выйти можно было всегда; `/trading/panic` на фьючерсах ещё и закрывает позиции;
- режим позиций — односторонний (One-way / net), маржа — кросс; режим хеджирования не поддержан;
- ставки funding читаются по публичному REST раз в `fundingPollSec` (по умолчанию 30 с; ставка меняется медленно,
  отдельный сокет ради неё не нужен);
- бумажный режим: позиции и реализованный результат как на бирже, funding начисляется по реальным ставкам в момент
  списания; **ликвидация не моделируется** — держите плечо низким.

**Стратегии на фьючерсах:** возврат к среднему открывает и шорты (z ≥ `entryZ` при перевесе асков); статистический
арбитраж держит рыночно-нейтральную пару — лонг дешёвой ноги и шорт дорогой на сумму × |β|; треугольный арбитраж —
только на споте (обмен через три валюты на фьючерсах невозможен).

### Funding-арбитраж

Межбиржевая стратегия: у одной монеты ставка funding на разных биржах разная. Бот открывает **шорт там, где ставка
выше** (шорт получает funding), и **лонг на тот же объём там, где ниже**. Цена монеты на результат почти не влияет,
доход — разница ставок каждый период. Ставки сравниваются приведёнными к 8 часам (у Hyperliquid период — час), монеты
сопоставляются по базовой валюте (BTCUSDT на Binance и BTCUSDC на Hyperliquid — одна монета).

Нужно: минимум две биржи с `market=perp`, общие монеты в выборе, включённая торговля на обеих (`/trading/start`)
и `fundingArbEnabled=true` в настройках процесса (`POST /settings`, вкладка «Настройки» или «Фьючерсы»).

| Настройка (`/settings`) | По умолчанию | Что это |
|---|---|---|
| `fundingArbEnabled` | false | стратегия открывает новые пары |
| `fundingPollSec` | 30 | как часто читать ставки, с (после перезапуска соединений) |
| `fundingArbMinDiffPercent` | 0.03 | вход: разница ставок за 8 ч больше этого, % |
| `fundingArbPaybackPeriods` | 6 | вход, только если разница окупает комиссии полного круга не больше чем за столько периодов по 8 ч (6 = 2 суток) |
| `fundingArbExitDiffPercent` | 0.005 | выход: разница упала ниже, % |
| `fundingArbOrderQuote` | 50 | размер каждой ноги в котируемой валюте (держите ≤ `maxPositionQuote` обеих бирж) |
| `fundingArbMaxPositions` | 3 | сколько пар держать одновременно |
| `fundingArbMaxBasisPercent` | 0.15 | вход, только если цены на двух биржах отличаются не больше, % |
| `fundingArbMaxHoldHours` | 72 | закрыть пару по таймауту, ч |
| `fundingArbSymbols` | пусто | монеты или символы через запятую; пусто — все общие |

**Комиссии.** Полный круг пары — 4 сделки тейкера: вход и выход на каждой из двух бирж, по `takerFeePercent`
каждой биржи (например, Bybit 0.055% + OKX 0.05% → 2 × 0.055 + 2 × 0.05 = 0.21%). Вход разрешён, только если
`разница × fundingArbPaybackPeriods ≥ комиссии круга`. Комиссии вычитаются из результата пары и из отката
неудачного входа и идут в дневной PnL бирж. В бумажном режиме комиссия уже заложена в цену исполнения и второй раз
не вычитается. Задайте `takerFeePercent` по своему тарифу (VIP-уровень, скидки) — бот берёт его оттуда.
`GET /funding` показывает для каждой возможности `roundTripFeePercent` и `paybackPeriods` — за сколько периодов
окупятся комиссии.

Порядок: сначала шорт, затем лонг на исполненный объём; если вторая нога не встала — первая закрывается. Выход —
разница ставок упала, таймаут, на одной из бирж остановлена торговля; при `/control/stop` пары закрываются.
Пока пара открыта, не включайте другие стратегии на тех же символах — у биржи одна позиция на символ.
Риски: ставки могут развернуться раньше, чем окупятся комиссии; при резком движении нога с плечом может быть
ликвидирована — плечо 1–3.

## Стратегии

Все работают на каждой бирже одновременно, каждая включается своим параметром. Состояние —
`GET /strategies?exchange=binance` (треугольники, пары с z-score, позиции, результат). Межбиржевой
funding-арбитраж — в разделе выше.

**Возврат к среднему** — лонг, когда цена ушла ниже скользящего среднего на `entryZ` сигм; на фьючерсах
ещё и шорт, когда выше.

**Треугольный арбитраж** — круг `USDT → A → B → USDT` внутри одной биржи (например, BTCUSDT, ETHBTC,
ETHUSDT — в выборе должны быть все три символа). На каждом тике круг пересчитывается в обе стороны по
лучшим ценам с тремя комиссиями тейкера; размер ограничен `triOrderQuote` и половиной объёма лучших
уровней. При чистой прибыли ≥ `triMinProfitPercent` — три рыночных ордера подряд вне потока конвейера;
количество для следующей ноги берётся по фактическому изменению баланса.
Риски: ноги последовательные, а не атомарные; на ликвидных биржах такие расхождения забирают за
миллисекунды участники с меньшей задержкой.

**Статистический арбитраж** — пары связанных активов. Раз в `statArbSampleMs` берутся середины стаканов,
по окну оценивается β (регрессия ln A на ln B) и z-score спреда `ln A − β·ln B`; пары с корреляцией
доходностей ниже порога не торгуются. Покупается дешёвая нога (z ≤ −entry — A, z ≥ entry — B), на фьючерсах
одновременно шортится дорогая (рыночно-нейтральная позиция); закрытие — при возврате спреда, по стопу или таймауту.
На споте шорт невозможен, и позиция несёт риск движения рынка.

## Лимиты запросов

Один бюджет на биржу на весь процесс: торговый клиент, REST-опрос стакана, подбор тикеров и
WebSocket тратят один и тот же лимит, как его считает биржа (на IP или ключ). Лимиты — по документации
бирж с запасом 20% (`RateLimits`), отдельно на публичные запросы, приватные, ордера, сообщения и
подключения WebSocket; у Binance, Aster, KuCoin и Hyperliquid учитывается вес запроса.

- Ответ 429/418/403 или код лимита в теле останавливает **все** запросы к этой бирже на время из
  `Retry-After` (иначе 10 с / 2 мин / 1 мин); повтор в течение 5 минут — пауза вдвое дольше.
- Заголовки биржи подтягивают наш счёт к её счёту: `X-MBX-USED-WEIGHT-1M`, `X-MBX-ORDER-COUNT-*`
  (Binance, Aster), `X-Bapi-Limit-Status` (Bybit), `X-Gate-RateLimit-*` (Gate), `gw-ratelimit-*` (KuCoin).
- Если ждать разрешения пришлось бы слишком долго, запрос не отправляется (ошибка `LOCAL`), а не висит.
- Лимиты рассчитаны на обычный (не VIP) аккаунт.

### Данные и ручные ордера

```bash
curl localhost:8080/status
curl "localhost:8080/market?exchange=binance"
curl "localhost:8080/balances?exchange=binance"

curl -X POST "localhost:8080/order?exchange=binance&symbol=BTCUSDT&side=BUY&type=MARKET&qty=0.001"
curl -X POST "localhost:8080/order?exchange=binance&symbol=BTCUSDT&side=BUY&type=LIMIT&qty=0.001&price=40000"
curl -X POST "localhost:8080/order?exchange=binance&symbol=BTCUSDT&side=SELL&type=MARKET&all=true"
curl -X POST "localhost:8080/order?exchange=binance&symbol=BTCUSDT&side=BUY&type=MARKET&portion=0.5"

curl -X POST "localhost:8080/cancel-all?exchange=binance&symbol=BTCUSDT"

# фьючерсы (market=perp): SELL открывает или увеличивает шорт
curl "localhost:8080/positions?exchange=bybit"
curl -X POST "localhost:8080/positions/close?exchange=bybit&symbol=all"
curl "localhost:8080/funding"
```

## Персистентность настроек

Всё, что настраивается через API — выбор бирж и тикеров, торговые параметры
каждой биржи — сохраняется в **SQLite** (`data/state.db`) после каждого
изменения и читается автоматически при следующем запуске. В `.env`
остаются только включение, порт и токен админки, путь к базе и ключи; адреса бирж (`testnet`, `restUrl`,
`wsUrl`, `recvWindowMs`) — параметры биржи в админке.

Почему SQLite, а не самодельный JSON-файл:
- атомарность транзакций обеспечивает сама СУБД (UPSERT в одной транзакции),
  а не ручной "временный файл + rename"
- WAL-режим — состояние можно посмотреть напрямую через `sqlite3 data/state.db`,
  не мешая работе бота
- если позже понадобится история изменений или несколько профилей
  конфигурации — это уже готовая база, а не файл, который пришлось бы
  допиливать вручную

**На производительность бота это не влияет.** Запись в БД происходит только
в ответ на HTTP-запрос к админке (смена риск-параметров, выбор биржи и т.п.)
— считанные разы за сессию, а не на каждый тик. Поток Disruptor, который
обрабатывает рыночные данные и генерирует сигналы, вообще не знает о
существовании `SqliteStateStore` — обращение к БД происходит исключительно
в потоке HTTP-сервера админки, полностью отдельно от горячего пути.

Путь к файлу БД можно поменять переменной окружения `STATE_DB`
(по умолчанию `data/state.db`).

**Автозапуск после рестарта.** По умолчанию после перезапуска процесса бот
снова встаёт в режим настройки — соединения нужно поднять вручную через
`/control/start`. Если нужно, чтобы после падения/перезапуска (например,
рестарт systemd) бот сам поднимался с той конфигурацией, что была до этого:

```bash
# Включить автозапуск соединений (без автоматической торговли)
curl -X POST "localhost:8080/control/autostart?enabled=true"

# Включить автозапуск соединений И торговли — использовать осторожно,
# это значит, что после любого рестарта бот сам начнёт слать ордера
curl -X POST "localhost:8080/control/autostart?enabled=true&trade=true"

# Выключить автозапуск обратно
curl -X POST "localhost:8080/control/autostart?enabled=false"
```

## Метрики для Grafana

Бот отдаёт метрики в текстовом формате Prometheus на `/metrics` — это
пассивный эндпоинт, сам бот никуда ничего не отправляет. Стандартная схема
для закрытого контура:

```
Bot (:8080/metrics) <--- pull --- Prometheus <--- Grafana (data source)
```

Пример `prometheus.yml`:

```yaml
scrape_configs:
  - job_name: 'hft-trader'
    scrape_interval: 5s
    static_configs:
      - targets: ['<host-бота>:8080']
    # Если задан ADMIN_TOKEN, Prometheus поддерживает это штатно:
    authorization:
      credentials: '<тот_же_токен_что_и_ADMIN_TOKEN>'
    # AdminServer принимает токен и как Authorization: Bearer, и как X-Admin-Token
```

Метрики, которые отдаются (лейблы `exchange`, где применимо — `symbol`):

- `hft_rate_used_ratio{bucket}`, `hft_rate_limited_total`, `hft_rate_local_rejects_total`,
  `hft_rate_waited_ms_total`, `hft_rate_blocked_ms` — лимиты запросов по каждой бирже
- `hft_tri_opportunities_total`, `hft_tri_executed_total`, `hft_tri_failed_total`, `hft_tri_pnl_quote`,
  `hft_statarb_positions`, `hft_statarb_trades_total`, `hft_statarb_pnl_quote`, `hft_statarb_zscore{pair}`
- `hft_jvm_heap_used_bytes`, `hft_jvm_heap_max_bytes`, `hft_jvm_gc_collections_total`,
  `hft_jvm_gc_seconds_total`, `hft_jvm_threads` — контроль памяти и пауз GC
- `hft_connected`, `hft_messages_total`, `hft_trading_enabled` — состояние соединения и торговли
- `hft_kill_switch`, `hft_orders_accepted_total`, `hft_orders_rejected_total`
- `hft_daily_pnl_quote`, `hft_open_positions`, `hft_strategy_enabled`
- `hft_order_latency_p50_micros`, `hft_order_latency_p99_micros`
- `hft_best_bid`, `hft_best_ask`, `hft_spread_percent`, `hft_imbalance`, `hft_zscore`, `hft_tick_count_total`

Бот хранит только текущее состояние, необходимое для торговли (стакан фиксированной
глубины, окно цен, последние 10 000 ордеров); историю хранит Prometheus. Логи
ротируются: файл до 100 МБ, 7 дней, всего не больше 1 ГБ.

В Grafana дальше — обычный дашборд поверх Prometheus data source: панели
на PnL по времени (`hft_daily_pnl_quote`), латентность ордеров, z-score по
инструментам, счётчик отклонённых риск-менеджером ордеров как индикатор
проблем.



1. Реализовать REST-клиент под её API, `implements ExchangeOrderApi`
2. Реализовать WS-фид, `extends AbstractWsFeed` (общий Netty-код уже есть —
   нужно только описать URL подписки и разбор сообщений)
3. Собрать оба в классе `XxxExchange implements ExchangeGateway`, по образцу
   `BinanceExchange`/`BybitExchange` (клиенту на `SignedCexClient` отдельный класс
   не нужен — хватает `SignedCexExchange`)
4. Добавить `case` в `ExchangeFactory.create()`
5. Добавить биржу в `BotController.SUPPORTED_EXCHANGES`

Дальше она сама появится в `/control/exchanges` и станет доступна через API
и панель без изменений в остальном коде.

## Своя стратегия

Наследуйтесь от `Strategy` и добавьте в конвейер в `Main`:

```java
public final class MyStrategy extends Strategy {

    public MyStrategy(MarketDataStore market, OrderService orders) {
        super("моя-стратегия", market, orders);
    }

    @Override
    protected void onTick(Tick tick) {
        OrderBook book = market.book(tick.symbol());
        PriceWindow window = market.window(tick.symbol());
        if (book == null || !book.isReady() || !window.isWarmedUp()) return;

        double z = window.currentZScore();
        double imbalance = book.imbalance(5);

        if (z < -2.0 && imbalance > 0.2) {
            orders.buyMarket(tick.symbol(), 0.001);
        }
    }
}
```

`MeanReversionStrategy` в проекте — рабочий пример на z-score с
подтверждением по стакану, стоп-лоссом и таймаутом позиции. Параметры
подобраны навскидку: перед реальными деньгами их нужно проверять на истории.

## Что стоит добавить дальше

- **Бэктест на истории.** Сейчас стратегию можно проверить только вживую.
  Загрузите klines с Binance и прогоните `Strategy` на них
- **Запись тиков на диск** (Chronicle Queue) — для последующего анализа
  и отладки стратегий на реальных данных
- **userDataStream** — WebSocket с событиями по вашему аккаунту. Сейчас
  балансы обновляются локально и сверяются раз в 5 минут; стрим даст
  мгновенное уведомление об исполнении ордера

## Предостережения

- Начинайте на Testnet. Проверьте, что ордера проходят, объёмы округляются
  правильно, риск-менеджер отклоняет что должен
- На реальных деньгах ставьте `maxPositionQuote` в несколько долларов,
  пока не убедитесь в поведении бота
- Задайте `ADMIN_TOKEN` — иначе любой, кто достучится до порта, сможет
  торговать вашими деньгами
- Бот пишет `logs/hft-trader.log` и `logs/gc.log` — если начнутся всплески
  задержки, смотрите второй файл

## KuCoin и Aster

- **KuCoin** — спот: стакан по WebSocket (`level2Depth50`; адрес и токен выдаёт `POST /api/v1/bullet-public`
  перед каждым подключением) + REST-запас, ордера через REST. Нужны `KUCOIN_API_KEY`, `KUCOIN_API_SECRET`,
  `KUCOIN_PASSPHRASE`, для реальных ордеров — параметр биржи `live=true`. Тестовой сети у KuCoin нет (песочница выключена с 2023 г.).
- **Aster** — спот, API v3 (`sapi.asterdex.com/api/v3`, формат Binance): стакан по WebSocket
  (`depth20@100ms`) + REST-запас, ордера через REST с подписью EIP-712 кошельком-агентом.
  `ASTER_API_KEY` — адрес основного кошелька (user), `ASTER_API_SECRET` — приватный ключ API-кошелька
  (signer, без права вывода), для реальных ордеров — параметр `live=true`.

Обе не проверялись на живой бирже (из среды разработки биржи недоступны): начинайте с PAPER и малых сумм.

## Биржи из каталога (формат API не проверен на живых биржах)

`GET /exchanges/catalog` — список: Binance, Bybit (полные адаптеры), OKX, MEXC, Gate, KuCoin, Aster,
Hyperliquid, Uniswap V2-пулы (LIVE не проверен), PancakeSwap/Raydium/Orca (пока не реализованы).
Без ключей или с `live=false` любая биржа работает в бумажном режиме на живых данных.
Форматы ответов взяты из документации по памяти и не сверялись с живыми API — сначала прогоните
`GET /exchanges/request-stats` и сравните стакан с сайтом биржи.
Пулы Uniswap V2: параметр биржи `uniPools="WETHUSDC=0xPAIR:true:18:6"` (пара:base это token0:dec base:dec quote).
Виртуальный баланс: параметр биржи `paperStartBalance` (по умолчанию 1000).

## Структура бирж (как BinanceExchange)

| Биржа | Класс биржи | REST-клиент | Режим |
|---|---|---|---|
| Binance, Bybit | BinanceExchange, BybitExchange | свои клиенты + WS | LIVE (проверено раньше только чтением) |
| OKX, MEXC, Gate, KuCoin, Aster | SignedCexExchange (общий класс, биржа задаётся клиентом в ExchangeFactory) | OkxRestClient, MexcRestClient, GateRestClient, KucoinRestClient, AsterRestClient (общий скелет SignedCexClient) | PAPER по умолчанию, LIVE не проверен |
| Hyperliquid, Uniswap V2 | HyperliquidExchange, UniswapV2Exchange | HyperliquidRestClient (EIP-712 через web3j), UniswapV2Client (свопы через Router02) | PAPER по умолчанию, LIVE не проверен и не собирался с настоящим web3j |

LIVE для OKX/MEXC/Gate/KuCoin/Aster включается двумя условиями сразу: `<ID>_API_KEY` + `<ID>_API_SECRET`
(у OKX и KuCoin ещё `<ID>_PASSPHRASE`) и параметр биржи `live=true` в админке. Иначе биржа работает в PAPER и реальных ордеров не шлёт.
Если правила торговли не загрузились, LIVE-старт отменяется. Тесты: src/test/java (простые runner-классы).

### Hyperliquid и Uniswap V2: переменные окружения
- Hyperliquid: `HYPERLIQUID_API_KEY` = адрес основного аккаунта, `HYPERLIQUID_API_SECRET` = ключ agent-кошелька (без права вывода), параметр биржи `live=true`.
- Uniswap V2: `UNISWAPV2_API_KEY` (метка/адрес), `UNISWAPV2_API_SECRET` (ключ горячего кошелька), параметр `live=true`,
  параметры биржи `uniRouter`, `uniTokens="WETH=0x..:18;USDC=0x..:6"`, `uniSlippagePercent`, RPC ноды — `restUrl`; GTC-лимиток и отмены на AMM нет.
- Криптография (secp256k1, keccak, подпись транзакций) — библиотека web3j из pom.xml (класс `Web3jCrypto`); ни её, ни Maven в песочнице не было,
  поэтому `Web3jCrypto` — единственный непроверенный компилятором файл. Остальной код проверен тестами с подменой крипто-слоя.

## WebSocket-стаканы для остальных бирж

OKX, Gate, KuCoin, Aster, MEXC и Hyperliquid получают стакан по WebSocket (`WsBookFeed`, WebSocket из JDK,
без Netty). Форматы подписок и сообщений описаны в `WsDialects` и записаны **по памяти** — против живых
серверов они не проверялись (сеть сборки закрыта). Перед реальными деньгами запустите бота в paper-режиме
и убедитесь по `/exchanges/request-stats`, что `ws.messages` и `ws.bookUpdates` растут, а `parseErrors` = 0.

- Режим `hybrid`: WS — основной канал, REST-опрос на паузе, пока WS жив. Если WS молчит дольше 5 с или
  сдался (15 неудачных подключений подряд), включается REST; когда WS возвращается — опрос снова на паузе.
  Торговля останавливается, только если сдался и запасной канал.
- Переподключение: пауза 0.5 с × 2ⁿ (до 30 с), повторная подписка, свежий снимок; тишина дольше порога или
  5 ошибок разбора подряд — тоже переподключение.
- Перекрещённый стакан (bid ≥ ask) не публикуется.
- MEXC: канал `spot@public.limit.depth.v3.api.pb@<символ>@<5|10|20>`, кадры в protobuf разбирает свой декодер
  (`MexcWsDialect`, без библиотеки); не больше 30 символов на соединение, остальные — без WS-стакана.
- Ордера и приватные потоки — см. следующий раздел.
- Свой адрес WS: `<ID>_WS_URL`.

## Что идёт по WebSocket, а что по REST/RPC

Принцип: сокет — всегда, где он есть; REST — только когда сокет не готов или у биржи его нет.
Подробно по каждой бирже (каналы, ключи, ограничения, что остаётся на REST и почему) — **[EXCHANGES.md](EXCHANGES.md)**.

| Биржа | Стакан | Ордера/отмены | Исполнения | Баланс | Остаётся на REST |
|---|---|---|---|---|---|
| Binance | WS | WS API (`order.place`, `order.cancel`, `openOrders.cancelAll`, `order.status`) | WS `executionReport` (`userDataStream.subscribe.signature`) | WS `outboundAccountPosition` | правила, время, баланс на старте |
| Bybit | WS | WS `/v5/trade` (`order.create`, `order.cancel`); отмена всех — REST | WS `/v5/private` `order` | WS `wallet` | правила, отмена всех, баланс на старте |
| KuCoin | WS | WS API `wsapi.kucoin.com` (`spot.order`, `spot.cancel`); отмена всех — REST | WS `/spotMarket/tradeOrdersV2` (bullet-private) | WS `/account/balance` | правила, отмена всех, баланс на старте |
| OKX | WS | WS (`order`, `cancel-order`, `batch-cancel-orders`) | WS `orders` | WS `account` | правила (`instruments`), список открытых ордеров на старте |
| Gate | WS | WS API (`spot.order_place` и др.) | WS `spot.orders` | WS `spot.balances` | правила |
| Hyperliquid | WS | WS `post` | WS `orderUpdates`, `userFills` | info по WS `post` | — (REST только запасной) |
| Uniswap V2 | WS-RPC: `eth_subscribe` на `Sync` + первый `eth_call` | JSON-RPC по сокету ноды | чтение по сокету | `eth_call` по сокету | HTTP — запасной |
| Aster | WS | REST (WS-ордеров нет) | WS `executionReport` (listenKey) | WS `outboundAccountPosition` | ордера |
| MEXC | WS (protobuf) | REST (WS-ордеров нет) | WS `spot@private.orders.v3.api.pb` (listenKey, protobuf) | WS `spot@private.account.v3.api.pb` | ордера |

Безопасность ордеров по WS (`WsRpcChannel`):
- сокет не готов (нет соединения/логина) — запрос не отправлялся, идём в REST;
- запрос ушёл, ответа нет — исход неизвестен, вслепую не повторяем:
  OKX, Gate, Binance (`origClientOrderId`), Bybit (`orderLinkId`) и KuCoin (`clientOid`) выясняют судьбу ордера через REST; Hyperliquid повторяет **тот же подписанный запрос с тем же nonce**
  (биржа дубликат не исполнит); JSON-RPC повторяется по HTTP (чтения идемпотентны, `eth_sendRawTransaction` с тем же raw даёт тот же хеш);
- бизнес-ошибка биржи по WS — это ответ, а не сбой: на REST не уходим;
- пять неверных логинов подряд отключают WS-канал (остаётся REST);
- отключить WS-торговлю: параметр биржи `wsTrade=false`.

Лимиты: ордера по WS идут через тот же `PacedLimiter`, что и REST; баланс, пришедший по сокету, сверяется с REST раз в 5 минут.
Форматы приватных WS-сообщений взяты из документации бирж и не проверялись на живых биржах (сеть сборки закрыта), только
тестами на поддельном сервере (`BinanceWsCheck`, `BybitWsCheck`, `KucoinWsCheck`, `MexcWsCheck`, `UserStreamCheck`, `WsTradeCheck`):
на первом запуске смотрите `/exchanges/request-stats` — поля `ws.ready`, `ws.parseErrors`, `ws.lastError`, `wsFallbacks`.

## Устройство классов бирж

`SignedCexExchange` (OKX, Gate, MEXC, KuCoin, Aster, Hyperliquid, Uniswap V2 — отличаются только REST-клиентом из `ExchangeFactory`)
собран так же, как `BybitExchange`: свои `MarketDataStore`, `BalanceStore`, `SymbolFilters`,
REST-клиент, `RiskManager`, `OrderService`, конвейер `TickPipeline` (Disruptor) и фид, явные `start()`/`stop()`.
Общие мелочи (режим LIVE/PAPER, стартовый бумажный баланс, сборка WS+REST-фида, остановка при потере данных) — в `ExchangeSupport`.
`start()` в LIVE: поднять WS-каналы → загрузить правила (без них старт падает) → баланс → конвейер → фид.

WS-ордеров нет в документации MEXC и Aster — у них ордера по REST, а события аккаунта
идут по приватному потоку через `listenKey` (`UserStream`: ключ по REST перед подключением, продление раз в 25 минут).
KuCoin в режиме UTA торгует через `uta.order` / `uta.cancel` (`tradeType=SPOT`):
для UTA-счёта задайте `KUCOIN_UTA=true` в `.env` (схема аргументов UTA сверена не полностью — проверьте на малой сумме).

## Производительность: что исправлено

| Было | Стало |
|---|---|
| Стратегия ждала биржу в потоке конвейера (REST до 10 с, паузы лимитера, дочитывание статуса) — тики всех символов стояли | `OrderExecutor`: стратегия ставит задачу и сразу возвращается; по символу одновременно не больше одного ордера; символы параллельно (пул 4) |
| `OrderBook` писался сетевым потоком и читался стратегией без синхронизации — можно было увидеть смесь двух снимков | `StampedLock`: запись под блокировкой, чтение оптимистичное (без блокировки, с проверкой версии); `readTop()` и `copyTo()` — согласованные снимки; тест: 4 млн чтений под нагрузкой, 0 рваных |
| Разбор WS деревом Jackson, `TreeMap<Double,Double>`, новый `Tick` и массивы на каждое сообщение | Потоковый разбор (Jackson streaming) в переиспользуемый буфер, числа без строк (точный быстрый разбор, сверен с `Double.parseDouble` на 200 тыс. случаев), стакан на отсортированных массивах примитивов, тик сразу в кольцо Disruptor |
| Отправка в WS под блокировкой с `join()` — pong из потока чтения мог остановить приём | `WsSender`: отправки выстроены цепочкой future, никто не ждёт; переполнение очереди — сброс сокета |
| REST-запас включался через 5 с по таймеру; стратегия торговала по опросу | Переключение по событию от WS, ожидание 2 с; пока данные с опроса — новых входов нет (`isRealtime=false`), выходы разрешены |
| Один лимитер на ордера и фоновые запросы; отказ лимитера «съедал» слот | Ордера и фон (балансы, статусы, правила) — разные лимитеры; при отказе слот не занимается |

Что осталось по природе: Uniswap — цена раз в блок (~12 с) и газ; у MEXC и Aster ордера только по REST (WS-ордеров у этих бирж нет).

## Подбор тикеров под стратегии

При старте (`Main` → `controller.discovery().start()`) и затем раз в `discoveryRefreshMin` минут (15, настройка `/settings`):

1. Со всех бирж берётся сводка 24ч — по одному публичному запросу на биржу (`MarketSources`).
2. Сводки прогоняются через профили стратегий (`Profiles`):
   - **mean-reversion** (торгует в боте): оборот ≥ `discoveryMinVolume` (1 млн), спред ≤ `discoveryMrMaxSpreadPercent` (0.1%), диапазон 1–25%,
     не тренд; лучшие `discoveryBacktestPerExchange` (8) на биржу прогоняются по 500 минутным свечам
     теми же правилами (`MeanReversionBacktest`, параметры стратегии этой биржи из админки, taker-комиссия с двух сторон);
   - **cross-exchange-arb** и **spread-capture** — только сканеры.
3. Результат — `GET /discovery` (под каждой стратегией: тикеры, вердикт, причины, метрики, бэктест; статус бирж);
   `POST /discovery/refresh` — пересчитать сейчас.

Лимиты: свои мягкие паузы на биржу, свечи — не больше `discoveryKlineBudget` (12) запросов на биржу за прогон,
пауза 2 мин после 429/418/403. Сводки и свечи берутся по REST: это редкий снимок, а не поток.
Биржи — `discoveryExchanges` (по умолчанию все), котировки — `discoveryQuotes` (USDT,USDC,USD); все пороги — в `GET /settings`.
Форматы публичных API записаны по памяти и проверены только на фейковом сервере; ошибка одной биржи видна в статусе и не мешает остальным.

## История изменений

- **2026-10:** полный учёт издержек: комиссия списывается из локального баланса сразу после боевой сделки (спот — из
  полученной валюты, фьючерсы — из USDT); новый параметр `tradeCostQuote` (газ Uniswap и др.); возврат к среднему и
  статарбитраж входят, только если ожидаемый ход (|z| − exitZ)·σ окупает комиссии входа и выхода (+ спред у возврата
  к среднему, обе ноги у статарбитража на фьючерсах); треугольный арбитраж учитывает `tradeCostQuote` трёх сделок.

- **2026-10:** бумажный режим: комиссия больше не вычитается дважды из результата возврата к среднему и
  статистического арбитража (движок уже закладывает её в цену исполнения); на бирже по-прежнему вычитается `takerFeePercent`.

- **2026-10:** фьючерсы: параметр биржи `market` (`perp`/`spot`) и `leverage`; USDT-перпы Binance (USDⓈ-M), Bybit (linear),
  OKX (SWAP) и Hyperliquid; позиции, reduceOnly-закрытие, маржа и плечо в риск-менеджере, бумажный движок для перпов
  с начислением funding; шорты в возврате к среднему, рыночно-нейтральный статарбитраж; межбиржевой funding-арбитраж;
  эндпоинты `/positions`, `/positions/close`, `/funding`; вкладка «Фьючерсы» в админке.
- **2026-10:** funding-арбитраж учитывает комиссии: порог входа по окупаемости круга (`fundingArbPaybackPeriods`),
  комиссии всех сделок пары (и отката) — в результате и дневном PnL.

- **2026-10:** биржа LBank удалена целиком (клиент, диалекты REST/WS, источник подбора тикеров, лимиты, каталог, тесты):
  низкая ликвидность и непроверенная схема подписи. Выбор `exchange=lbank` теперь отклоняется.
- **2026-10:** веб-админка — отдельный проект `hft-admin-panel`, бот только отдаёт API.
- **2026-10:** биржа dYdX v4 удалена целиком (вместе с `PaperExchange`, который был нужен только ей).
- **2026-10:** биржа BingX удалена целиком (клиент, диалекты REST/WS, источник подбора тикеров, лимиты, каталог, тесты).
- **2026-10:** MEXC — исполнения и баланс по приватному WS-потоку (protobuf); KuCoin — режим UTA (`KUCOIN_UTA=true`).
- **2026-10:** чтение `application.yml` убрано (и зависимость jackson-dataformat-yaml): все параметры процесса — только `.env` и окружение.
- **2026-10:** торговля по WebSocket у Binance, Bybit и KuCoin; приватные потоки у Binance, Bybit, KuCoin и Aster;
  стакан MEXC по WebSocket (protobuf). REST везде остаётся запасным каналом.
