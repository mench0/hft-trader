#!/usr/bin/env bash
# Запуск с флагами JVM, подобранными под низкую задержку.
set -e

JAR="target/hft-trader.jar"
if [ ! -f "$JAR" ]; then
  echo "Сначала соберите проект: mvn clean package"
  exit 1
fi

JVM_OPTS=(
  # G1GC: паузы в единицы миллисекунд. Для бюджета 5 мс этого достаточно.
  # ZGC имеет смысл только если целитесь в микросекунды.
  "-XX:+UseG1GC"
  "-XX:MaxGCPauseMillis=10"

  # Выделить и коснуться всей памяти на старте, чтобы не ловить
  # page fault в горячем пути
  "-XX:+AlwaysPreTouch"

  # Фиксированный размер кучи: без изменения размера во время работы
  "-Xms1g"
  "-Xmx1g"

  # Большие страницы снижают промахи TLB. Требует настройки в ОС,
  # если не включено — JVM просто проигнорирует
  "-XX:+UseTransparentHugePages"

  # Логи GC: пригодятся, если начнутся всплески задержки
  "-Xlog:gc:logs/gc.log:time,uptime:filecount=5,filesize=10M"
)

mkdir -p logs

echo "Запуск. Конфигурация:"
echo "  ADMIN_PORT=${ADMIN_PORT:-из application.yml или 8080}"
echo "  Торговые параметры бирж — через админку (/exchange/params), хранятся в ${STATE_DB:-data/state.db}"
echo ""

exec java "${JVM_OPTS[@]}" -jar "$JAR" "$@"
