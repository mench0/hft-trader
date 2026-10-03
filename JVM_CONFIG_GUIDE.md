## JVM и GC для вашего проекта

### Почему Java 21 LTS?

- **Стабильность**: LTS версия, поддержка до 2031 года
- **JIT-компиляция**: лучше оптимизирует hot path после прогрева
- **Project Loom готов**, но вам не нужен на этом уровне задержки
- **Без экспериментов**: версии 22/23 могут сломать неожиданные фичи

### Почему G1GC, а не ZGC / Epsilon?

| GC | Паузы | Для вас |
|---|---|---|
| **G1GC** (дефолт) | 1-10 мс | ✅ Идеально. Укладывается в бюджет 5 мс, стабилен на долгой работе |
| ZGC | <1 мс | ❌ Оверкилл. Нужен для микросекундного HFT, у вас это пока не требуется |
| Epsilon | 0 мс | ❌ No-op GC, требует абсолютно нулевых аллокаций в hot path (недостижимо) |

**Вывод**: G1GC по умолчанию — правильный выбор на текущем этапе.

### Рекомендуемые JVM-флаги

```bash
-XX:+UseG1GC                  # Используем G1GC
-XX:MaxGCPauseMillis=10       # Целевая пауза (можно 5 мс для агрессива, 20 мс для щадящего режима)
-XX:+AlwaysPreTouch           # Выделяем память заранее, не на первое обращение
-XX:-UseBiasedLocking         # Отключаем biased locking (даёт непредсказуемые задержки)
-Xms1g -Xmx2g                 # Heap 1-2 ГБ (зависит от размера ордербука)
```

Почему каждый:
- `-XX:MaxGCPauseMillis=10` — 10 мс пауза GC плюс ~300-500 мкс на сигнал дают вам стабильность в бюджете 5 мс на сигнал
- `-XX:+AlwaysPreTouch` — не ждать первого page fault в горячей петле
- `-XX:-UseBiasedLocking` — когда несколько потоков обращаются к одному объекту (Disruptor, Netty), biased lock добавляет непредсказуемые спайки задержки

### Примеры команд запуска

#### Локально (разработка, testnet)
```bash
./run.sh
```

#### С нестандартным heap (если система маленькая)
```bash
java -XX:+UseG1GC -XX:MaxGCPauseMillis=10 \
     -Xms512m -Xmx1g \
     -jar target/hft-starter.jar
```

#### На мощной машине (больше потоков Disruptor)
```bash
java -XX:+UseG1GC -XX:MaxGCPauseMillis=5 \
     -Xms4g -Xmx8g \
     -XX:+UnlockExperimentalVMOptions \
     -XX:G1NewCollectionThreads=4 \
     -jar target/hft-starter.jar
```

---

## Конфигурация без переделоя jar

### Почему это важно?

Представьте: вы запустили бота на VPS, он торгует 7 дней в неделю. Вам нужно:
- Изменить размер позиции (если депозит растёт)
- Добавить новый символ для торговли
- Переключиться с testnet на реальную торговлю

**Без конфигурации из файла**: пересобирать jar через Maven, переделойте на VPS, рестарт — потеря времени и риск ошибки.

**С application.yml**: просто отредактируйте переменные окружения и перезапустите, никакой пересборки.

### Как это работает

**application.yml** — основной файл конфигурации:
```yaml
binance:
  use-testnet: true
trading:
  symbols:
    - btcusdt
  max-position-size: 0.01
  stop-loss-percent: 2.0
```

**Переопределение через переменные окружения**:
```bash
export BINANCE_USE_TESTNET=false
export TRADING_SYMBOLS="bnbusdt,adausdt"
export TRADING_MAX_POSITION_SIZE=0.05
./run.sh
```

Приоритет:
1. Переменные окружения (самый высокий)
2. application.yml (если не переопределено)
3. Жёсткие умолчания в коде (fallback)

### Примеры переходов

#### Локальный testnet (разработка)
```bash
./run.sh
# Читает из application.yml: use-testnet=true, symbols=[btcusdt, ethusdt]
```

#### Переключение на реальную торговлю (осторожно!)
```bash
export BINANCE_USE_TESTNET=false
export BINANCE_API_KEY="ваш_ключ"
export BINANCE_API_SECRET="ваш_секрет"
export TRADING_MAX_POSITION_SIZE=0.001  # начните с минимума!
./run.sh
```

#### На VPS через systemd
В `/etc/systemd/system/hft-starter.service`:
```ini
[Service]
Environment="BINANCE_USE_TESTNET=false"
Environment="TRADING_SYMBOLS=btcusdt"
Environment="TRADING_MAX_POSITION_SIZE=0.01"
```

Потом:
```bash
sudo systemctl restart hft-starter
# Никакой пересборки jar!
```

#### В Docker
```bash
docker run \
  -e BINANCE_USE_TESTNET=false \
  -e TRADING_SYMBOLS="btcusdt,ethusdt" \
  hft-starter
```

---

## Проверка конфигурации при старте

При запуске в логах вы увидите:
```
Конфигурация загружена: testnet=true symbols=[btcusdt, ethusdt] maxPos=0.01
```

Это значит, что конфигурация прочитана правильно.

Если видите ошибку — проверьте:
1. Переменные окружения: `echo $BINANCE_USE_TESTNET`
2. application.yml присутствует в ресурсах
3. Синтаксис YAML правильный (отступы!)

---

## Что дальше?

Когда понадобится ещё больше гибкости:
- Добавить http-эндпоинт (`com.sun.net.httpserver.HttpServer`) для управления конфигурацией на лету
- Использовать ConfigMap в Kubernetes, если развёртываетесь туда
- Добавить reload конфигурации без перезагрузки (сложнее, но возможно)

Но пока application.yml + переменные окружения вполне достаточно для управления параметрами без переделоя jar.
