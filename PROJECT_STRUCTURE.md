# hft-starter: Краткое резюме структуры

## Структура проекта

```
hft-starter/
├── src/main/java/com/hft/
│   ├── Main.java                      # Точка входа, ручная инициализация компонентов
│   ├── config/
│   │   ├── AppConfig.java             # Загрузка конфига из YAML + переменные окружения
│   │   └── ApiCredentials.java        # API-ключи из переменных окружения
│   ├── net/
│   │   └── BinanceWsClient.java       # WebSocket-клиент на Netty к Binance
│   ├── rest/
│   │   ├── BinanceRestClient.java     # REST-клиент для HTTP-запросов (покупка/продажа)
│   │   ├── RestClientExample.java     # Пример использования REST-клиента
│   │   ├── TickerInfo.java            # POJO для данных тикера
│   │   └── OrderSide.java             # Enum: BUY / SELL
│   ├── engine/
│   │   ├── TickDisruptor.java         # Lock-free кольцевой буфер между стадиями
│   │   └── SignalEngine.java          # Обработчик сигнала (EMA + метрики)
│   ├── model/
│   │   └── Tick.java                  # POJO одного тика рынка
│   ├── metrics/
│   │   └── LatencyTracker.java        # HdrHistogram для честного замера латентности
│   └── util/
│       └── HmacSigner.java            # HMAC-SHA256 подпись запросов
├── src/main/resources/
│   ├── application.yml                # Конфигурация (читается и переопределяется через env)
│   └── logback.xml                    # Конфигурация логирования
├── pom.xml                            # Maven (Netty, Jackson, Disruptor, HdrHistogram, SLF4J)
├── Dockerfile                         # Контейнеризация
├── docker-compose.yml                 # Docker Compose для локальной разработки/VPS
├── run.sh                             # Скрипт запуска с рекомендуемыми JVM-флагами
├── README.md                          # Основная документация
├── DEPLOYMENT.md                      # Инструкции по deployменту на VPS/systemd/Docker
├── .env.example                       # Пример файла с API-ключами (копируй в .env)
└── .gitignore                         # Исключаем ключи, target, логи из git
```

## Ключевые компоненты и их роль

### WebSocket (получение рыночных данных)
- **BinanceWsClient.java** — подключается к Binance через WebSocket
- Парсит JSON сделок в реальном времени
- Публикует в Disruptor с таймстампом получения

### Обработка сигнала (latency-чувствительно)
- **TickDisruptor.java** — lock-free ring buffer без блокировок между потоками
- **SignalEngine.java** — обработчик событий (пока EMA, позже добавите z-score для стат-арба)
- **LatencyTracker.java** — замер задержки обработки с перцентилями (p50/p99/p999)

### Торговля (покупка/продажа)
- **BinanceRestClient.java** — REST API для размещения ордеров
  - `placeLimitOrder()` — разместить LIMIT ордер
  - `cancelOrder()` — отменить ордер
  - `getBalance()` — получить баланс аккаунта
  - `getTickerPrice()` — получить текущую цену
  - Все приватные методы подписываются HMAC-SHA256

### Конфигурация (без переделоя jar)
- **AppConfig.java** — загружает application.yml
- Переопределение через переменные окружения: `BINANCE_USE_TESTNET`, `TRADING_SYMBOLS` и т.д.
- Приоритет: env-переменные > YAML > жёсткие умолчания в коде

## Для чего какой файл

| Если вам нужно... | Файл | Метод |
|---|---|---|
| Запустить локально | `run.sh` или `README.md` | `./run.sh` |
| Настроить JVM | `JVM_CONFIG_GUIDE.md` или `run.sh` | Отредактировать флаги в скрипте |
| Развернуть на VPS | `DEPLOYMENT.md` | systemd или Docker |
| Добавить новый символ | `application.yml` или переменная `TRADING_SYMBOLS` | Отредактировать без пересборки |
| Включить реальную торговлю | `application.yml` + переменная `BINANCE_USE_TESTNET=false` | export + restart |
| Получить текущую цену | `BinanceRestClient.getTickerPrice()` | REST публичный метод |
| Разместить ордер | `BinanceRestClient.placeLimitOrder()` | REST приватный метод (требует ключи) |
| Замерить задержку | `LatencyTracker` + логи каждые 2000 тиков | Смотрите логи |
| Добавить свою стратегию | `SignalEngine.onEvent()` | Заменяйте EMA на z-score для стат-арба |

## Запуск по шагам

### Локальный запуск (разработка)
```bash
mvn clean package
./run.sh
# Выведет: "Конфигурация загружена: testnet=true symbols=[btcusdt, ethusdt]"
# Каждые 200 тиков: "btcusdt: price=... ema=..."
# Каждые 2000 тиков: "Латентность: p50=... p99=... p999=..."
```

### Переключение на реальную торговлю
```bash
export BINANCE_API_KEY="ваш_ключ"
export BINANCE_API_SECRET="ваш_секрет"
export BINANCE_USE_TESTNET=false
export TRADING_MAX_POSITION_SIZE=0.001  # начните с минимума!
./run.sh
```

### На VPS через Docker
```bash
docker build -t hft-starter .
docker run -e BINANCE_USE_TESTNET=false -e TRADING_SYMBOLS="btcusdt" hft-starter
```

### На VPS через systemd
```bash
sudo cp target/hft-starter.jar /opt/hft/
# Отредактируйте /etc/systemd/system/hft-starter.service
sudo systemctl start hft-starter
sudo journalctl -u hft-starter -f
```

## Важные моменты перед реальной торговлей

✅ **Сделайте это:**
1. Тестируйте в Binance Testnet (https://testnet.binance.vision) перед реальными деньгами
2. Начните с минимальной позиции (0.001 BTC, а не 0.5)
3. Мониторьте логи первые часы
4. Убедитесь, что kill-switch работает (systemctl stop / docker stop)
5. Проверьте, что ключи лежат только в переменных окружения, не в коде

❌ **Не делайте:**
- Не коммитьте API-ключи в git
- Не запускайте реальную торговлю без предварительного тестирования в testnet
- Не используйте большие позиции до того, как убедитесь в стратегии
- Не забывайте про остановку бота — "забыл остановить" стоит денег

## Дальнейшие расширения

По мере роста депозита и уверенности в коде:
1. Добавить вторую, третью биржу (просто ещё инстансы BinanceWsClient)
2. Заменить EMA на z-score для статистического арбитража
3. Добавить лимиты на позицию и stop-loss логику
4. Добавить HTTP-эндпоинт для мониторинга в реальном времени
5. Перейти на kernel-bypass (Aeron, DPDK) если понадобится микросекундный бюджет
6. Масштабироваться на 100+ символов и десятки бирж

Но для начала — этот скелет полностью рабочий и достаточный.
