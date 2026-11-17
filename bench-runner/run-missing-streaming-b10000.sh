#!/usr/bin/env bash
# run-missing-streaming-b10000.sh — дособрать прогон streaming b10000.
# Причина пропуска: state_get в run-sweep.sh искал статус по тегу без arm,
# и тег b10000, закрытый прогоном polling, ошибочно закрыл streaming b10000.
# Ждёт завершения работающего оркестратора (править его скрипт на ходу нельзя),
# затем гоняет пропущенный цикл и пересобирает сводку.
set -uo pipefail
cd "$(dirname "$0")/.."

LOGDIR=bench-runner/logs
mkdir -p "$LOGDIR"

# ждать, пока работает основной оркестратор
while pgrep -f "bench-runner/run-sweep.sh" >/dev/null 2>&1; do sleep 60; done
sleep 5   # убедиться, что финальная сводка оркестратора дописана

echo "[start] $(date '+%F %T') streaming b10000 (догон) — лог: $LOGDIR/b10000-streaming.log"
sed -i '' -E "s/^CDC_STREAMING_KAFKA_BATCH_SIZE=.*/CDC_STREAMING_KAFKA_BATCH_SIZE=10000/" .env
t0=$(date +%s)
scripts/test/benchmark.sh --cycle streaming > "$LOGDIR/b10000-streaming.log" 2>&1
rc=$?
if [[ $rc -eq 0 && -f results/benchmark/benchmark-streaming.json ]]; then
  cp results/benchmark/benchmark-streaming.json results/benchmark/benchmark-streaming-b10000.json
  printf 'b10000-streaming\tstreaming\tdone\t%s\n' "$(date '+%F %T')" >> "$LOGDIR/state.tsv"
  echo "[done ] $(date '+%F %T') streaming b10000 (догон) за $(( $(date +%s) - t0 ))с"
else
  [[ -f results/benchmark/benchmark-streaming.json ]] \
    && cp results/benchmark/benchmark-streaming.json results/benchmark/benchmark-streaming-b10000-failed.json
  printf 'b10000-streaming\tstreaming\tfailed(rc=%s)\t%s\n' "$rc" "$(date '+%F %T')" >> "$LOGDIR/state.tsv"
  echo "[FAIL ] $(date '+%F %T') rc=$rc — см. $LOGDIR/b10000-streaming.log"
fi
# вернуть .env к исходному streaming-чанку
sed -i '' -E "s/^CDC_STREAMING_KAFKA_BATCH_SIZE=.*/CDC_STREAMING_KAFKA_BATCH_SIZE=2048/" .env
python3 bench-runner/make-sweep-summary.py || echo "сводка не собралась"
echo "[end  ] $(date '+%F %T') догон завершён"
