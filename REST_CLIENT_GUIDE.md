# BinanceRestClient: Полный гайд

Вы запрашивали методы получения тикеров, покупки и продажи. Вот полное описание.

## Что это такое?

`BinanceRestClient.java` — это REST-клиент к Binance API, использующий встроенный `java.net.http.HttpClient` из JDK 21 (никаких сторонних http-библиотек).

**Зачем отдельно от WebSocket?**
- **WebSocket** — для получения потока рыночных данных в реальном времени (каждый тик сделки)
- **REST** — для разовых операций (получить баланс, разместить ордер, отменить, проверить статус)

Оба используются параллельно в реальной торговле.

## Установка и запуск

### 1. Проверьте, что pom.xml содержит Jackson YAML (уже добавлено)
```xml
<dependency>
    <groupId>com.fasterxml.jackson.dataformat</groupId>
    <artifactId>jackson-dataformat-yaml</artifactId>
    <version>${jackson.version}</version>
</dependency>
```

### 2. Соберите проект
```bash
mvn clean package
```

### 3. Протестируйте REST-клиент в Testnet (безопасно)
```bash
# Получите ключи из https://testnet.binance.vision
export BINANCE_API_KEY="ваш_testnet_ключ"
export BINANCE_API_SECRET="ваш_testnet_секрет"

# Запустите пример
java -cp target/hft-starter.jar com.hft.rest.RestClientExample
```

Вывод будет примерно:
```
=== Получение цены ===
Текущая цена BTC/USDT: 42500.50
=== Баланс аккаунта ===
USDT: 1000.0
BTC: 0.001
=== Размещение ордера ===
Ордер размещён с ID: 123456789
Статус ордера: NEW
=== Отмена ордера ===
Ордер 123456789 отменён
Статус после отмены: CANCELED
=== Тест завершён успешно ===
```

## Методы REST-клиента

### Публичные методы (без API-ключей)

#### 1. `getTickerPrice(String symbol)`
Получить текущую цену конкретного инструмента.

```java
BinanceRestClient client = new BinanceRestClient("https://api.binance.com", credentials);
TickerInfo ticker = client.getTickerPrice("BTCUSDT");
System.out.println("BTC/USDT: " + ticker.price());
// Вывод: BTC/USDT: 42500.50
```

**Когда использовать:** Нужна текущая цена для расчёта параметров ордера или для проверки текущего положения рынка.

#### 2. `getAllSymbols()`
Получить список всех торговых пар на Binance (BTCUSDT, ETHUSDT, ...).

```java
List<String> symbols = client.getAllSymbols();
symbols.stream()
       .filter(s -> s.contains("USDT"))
       .limit(10)
       .forEach(System.out::println);
```

**Когда использовать:** Один раз при инициализации, чтобы валидировать, что нужные вам символы существуют.

---

### Приватные методы (требуют API-ключ + секрет)

**Все приватные методы:**
- Требуют BINANCE_API_KEY и BINANCE_API_SECRET из переменных окружения
- Подписываются HMAC-SHA256
- Содержат timestamp для защиты от replay-атак

#### 3. `placeLimitOrder(String symbol, OrderSide side, double quantity, double price)`
Разместить LIMIT ордер на покупку или продажу.

```java
// Разместить ордер на покупку 0.01 BTC по цене 40000 USDT за BTC
long orderId = client.placeLimitOrder("BTCUSDT", OrderSide.BUY, 0.01, 40000.0);
System.out.println("Ордер размещён, ID: " + orderId);
```

**Параметры:**
- `symbol` — торговая пара (например "BTCUSDT")
- `side` — BUY или SELL
- `quantity` — количество в base asset (для BTCUSDT — количество BTC)
- `price` — цена лимита

**Возвращает:** orderId (уникальный номер ордера на бирже)

**Важно:**
- LIMIT ордер выполнится только, если цена рынка дойдёт до вашей цены
- Если цена не дошла — ордер останется open (можно отменить через `cancelOrder()`)
- timeInForce = GTC (Good Till Cancel) — ордер живёт, пока вы не отмените или не выполнится

**Когда использовать:** Основной способ входа в позицию, когда хотите контролировать цену.

#### 4. `cancelOrder(String symbol, long orderId)`
Отменить ордер, если он ещё не выполнился.

```java
client.cancelOrder("BTCUSDT", 123456789);
System.out.println("Ордер отменён");
```

**Параметры:**
- `symbol` — торговая пара (должна совпадать с той, под которую был размещён ордер)
- `orderId` — ID ордера (получаёте при размещении через `placeLimitOrder()`)

**Когда использовать:** 
- При стоп-лосс сценарии (цена рынка движется против вас, срочно отменяете ордер на покупку)
- При выходе из позиции (если ордер на продажу не выполнился)

#### 5. `getBalance()`
Получить текущий баланс всех активов на аккаунте.

```java
var balances = client.getBalance();
for (var entry : balances.entrySet()) {
    System.out.println(entry.getKey() + ": " + entry.getValue());
}
// Вывод:
// USDT: 1000.0
// BTC: 0.05
// ETH: 0.5
```

**Возвращает:** `Map<String, Double>` где ключ — asset (USDT, BTC, ETH), значение — количество.

**Важно:** Возвращает только активы с положительным балансом (free > 0).

**Когда использовать:**
- При инициализации (убедиться, что депозит есть)
- При проверке портфеля
- При расчёте максимального размера позиции

#### 6. `getOrderStatus(String symbol, long orderId)`
Получить статус конкретного ордера.

```java
String status = client.getOrderStatus("BTCUSDT", 123456789);
System.out.println("Статус: " + status);
// Возможные значения: NEW, PARTIALLY_FILLED, FILLED, CANCELED, REJECTED, EXPIRED
```

**Когда использовать:**
- Проверить, выполнился ли ордер
- Узнать сколько из ордера уже выполнилось (PARTIALLY_FILLED)

---

## Пример: полный цикл покупки и продажи

```java
BinanceRestClient client = new BinanceRestClient("https://testnet.binance.vision", credentials);

// Шаг 1: Получить текущую цену
TickerInfo ticker = client.getTickerPrice("BTCUSDT");
double currentPrice = ticker.price();
System.out.println("Текущая цена: " + currentPrice);

// Шаг 2: Проверить баланс USDT
var balances = client.getBalance();
double usdtBalance = balances.getOrDefault("USDT", 0.0);
System.out.println("USDT баланс: " + usdtBalance);

// Шаг 3: Разместить ордер на покупку (по цене на 1% ниже текущей)
double buyPrice = currentPrice * 0.99;
double quantity = 0.01;  // 0.01 BTC
long buyOrderId = client.placeLimitOrder("BTCUSDT", OrderSide.BUY, quantity, buyPrice);
System.out.println("Ордер на покупку размещён, ID: " + buyOrderId);

// Шаг 4: Подождать выполнение (в реальности ждёте через WebSocket или polling)
Thread.sleep(5000);

// Шаг 5: Проверить статус
String buyStatus = client.getOrderStatus("BTCUSDT", buyOrderId);
System.out.println("Статус ордера на покупку: " + buyStatus);

if ("FILLED".equals(buyStatus)) {
    // Шаг 6: Разместить ордер на продажу (по цене на 2% выше цены покупки)
    double sellPrice = buyPrice * 1.02;
    long sellOrderId = client.placeLimitOrder("BTCUSDT", OrderSide.SELL, quantity, sellPrice);
    System.out.println("Ордер на продажу размещён, ID: " + sellOrderId);
    
    // Шаг 7: Проверить финальный баланс
    Thread.sleep(5000);
    var finalBalances = client.getBalance();
    System.out.println("Финальный баланс USDT: " + finalBalances.getOrDefault("USDT", 0.0));
} else {
    // Отменить ордер на покупку, если он не выполнился
    client.cancelOrder("BTCUSDT", buyOrderId);
    System.out.println("Ордер на покупку отменён");
}
```

## Интеграция REST-клиента с основным ботом

Сейчас `Main.java` только слушает WebSocket. Чтобы добавить торговлю, нужно вызывать REST-методы из `SignalEngine`:

```java
// В SignalEngine.java:
public class SignalEngine implements EventHandler<Tick> {
    
    private BinanceRestClient restClient;  // Добавить
    
    @Override
    public void onEvent(Tick tick, long sequence, boolean endOfBatch) {
        // ... расчёты сигнала ...
        
        if (shouldBuy(tick)) {
            // Разместить ордер на покупку
            long orderId = restClient.placeLimitOrder(
                tick.getSymbol(), 
                OrderSide.BUY, 
                0.01,  // количество
                tick.getPrice() * 0.99  // цена на 1% ниже
            );
            log.info("Размещен ордер BUY, ID: {}", orderId);
        }
    }
}
```

Но это уже добавляет **торговую логику**, которая критична — ошибка в коде может стоить денег. На вашем этапе сейчас:

1. ✅ Боту работает получение данных (WebSocket)
2. ✅ Боту работает замер задержки (HdrHistogram)
3. ⏳ Торговая логика — после того, как вы:
   - Протестируете REST-клиент в testnet (RestClientExample)
   - Убедитесь, что баланс, покупка/продажа работают
   - Добавите защиту (stop-loss, max position size, kill switch)
   - Протестируете в testnet целиком

## Важные ограничения Binance API

- **Rate limit**: 1200 запросов в минуту на IP (будьте осторожны с polling)
- **Order qty precision**: зависит от символа (для BTCUSDT — 8 десятичных знаков)
- **Min order value**: обычно ~10 USDT минимум за сделку
- **Testnet не сохраняет историю** после перезагрузки

## Отладка

Если REST-запрос падает с ошибкой:

1. **Проверьте API-ключи:**
   ```bash
   echo $BINANCE_API_KEY
   echo $BINANCE_API_SECRET
   ```

2. **Убедитесь, что используете правильный URL:**
   - Testnet: `https://testnet.binance.vision`
   - Реальный: `https://api.binance.com`

3. **Посмотрите логи:**
   ```java
   // В логе будут детали ошибки от Binance
   log.error("Ошибка: {}", response.body());
   ```

4. **Проверьте синтаксис YAML в application.yml** (отступы!)

---

## Дальше

После того, как REST-клиент заработает:
1. Интегрируйте его в SignalEngine для реальной торговли
2. Добавьте лимиты на позицию
3. Добавьте stop-loss логику
4. Добавьте logging в Chronicle Queue для анализа
5. Только потом переходите на реальные деньги
