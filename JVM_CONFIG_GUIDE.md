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

**Вывод**: G1GC — разумный минимум. Сейчас `run.sh` запускает с `-XX:+UseZGC` (паузы < 1 мс при куче 1 ГБ);
чтобы вернуться на G1, замените в `run.sh` `-XX:+UseZGC` на `-XX:+UseG1GC`.

### Рекомендуемые JVM-флаги

```bash
-XX:+UseG1GC                  # Используем G1GC
-XX:MaxGCPauseMillis=10       # Целевая пауза (можно 5 мс для агрессива, 20 мс для щадящего режима)
-XX:+AlwaysPreTouch           # Выделяем память заранее, не на первое обращение
-Xms1g -Xmx2g                 # Heap 1-2 ГБ (зависит от размера ордербука)
```

Почему каждый:
- `-XX:MaxGCPauseMillis=10` — 10 мс пауза GC плюс ~300-500 мкс на сигнал дают вам стабильность в бюджете 5 мс на сигнал
- `-XX:+AlwaysPreTouch` — не ждать первого page fault в горячей петле
- biased locking в Java 21 уже удалён, флаг `-XX:-UseBiasedLocking` не нужен

### Примеры команд запуска

#### Локально (разработка, testnet)
```bash
./run.sh
```

#### С нестандартным heap (если система маленькая)
```bash
java -XX:+UseG1GC -XX:MaxGCPauseMillis=10 \
     -Xms512m -Xmx1g \
     -jar target/hft-trader.jar
```

#### На мощной машине (больше потоков Disruptor)
```bash
java -XX:+UseG1GC -XX:MaxGCPauseMillis=5 \
     -Xms4g -Xmx8g \
     -XX:+UnlockExperimentalVMOptions \
     -XX:G1NewCollectionThreads=4 \
     -jar target/hft-trader.jar
```

---

## Конфигурация процесса

Файл и окружение задают только админку; всё остальное настраивается на лету через веб-админку
(`http://localhost:8080/`) или API и сохраняется в SQLite (`data/state.db`, путь — `STATE_DB`).

| Что | Где | Перезапуск |
|---|---|---|
| Включена ли админка, порт, токен | `ADMIN_ENABLED`, `ADMIN_PORT`, `ADMIN_TOKEN` или блок `admin` в `application.yml` рядом с jar (`-Dconfig.file=…`) | да |
| API-ключи бирж | только окружение: `<ID>_API_KEY`, `<ID>_API_SECRET`, `<ID>_PASSPHRASE` | да |
| Выбор бирж и тикеров | админка, вкладка «Биржи» (`/control/select`, `/control/symbols`) | нет |
| Параметры биржи (testnet, live, адреса, риск, стратегии, фиды) | вкладка «Параметры» (`/exchange/params`) | помеченные ⟳ — после «Остановить» → «Поднять соединения» |
| Настройки процесса (подбор тикеров, фоновые задачи, доля лимитов) | вкладка «Настройки» (`/settings`) | `rateLimitSafety` — перезапуск процесса |

Приоритет для админки: переменные окружения → `application.yml` → значения по умолчанию.
Блоки `risk` и `exchanges` в `application.yml` больше не читаются (в лог пишется предупреждение).

### systemd

```ini
[Service]
WorkingDirectory=/opt/hft-trader
Environment="ADMIN_TOKEN=длинный-случайный-токен"
Environment="BINANCE_API_KEY=…"
Environment="BINANCE_API_SECRET=…"
ExecStart=/opt/hft-trader/run.sh
Restart=on-failure
```

Порт админки наружу не открывайте: `ssh -L 8080:localhost:8080 server` и открыть `http://localhost:8080/`.
