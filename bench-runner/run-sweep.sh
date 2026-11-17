#!/usr/bin/env bash
# run-sweep.sh — серия конфигурационных прогонов benchmark.sh (batch/workers sweep).
# Каждый прогон: правка .env под конфиг → полный цикл benchmark.sh --cycle <arm>
# (wipe → чистый стенд → warmup → 5 сценариев s1-s5) → снапшот компактного отчёта
# results/benchmark/benchmark-<arm>.json в файл с тегом конфигурации
# benchmark-<arm>-b<batch>[-w<воркеры>].json (следующий цикл того же arm перезаписал
# бы безымянный файл). Полные артефакты каждого цикла и так уникальны:
# results/raw/<stamp>-benchmark-<arm>/ (timestamped).
#
# Сохранность результатов:
#   - state-файл bench-runner/logs/state.tsv: tag <TAB> arm <TAB> status <TAB> время;
#     повторный запуск скрипта продолжает серию (done пропускается, failed повторяется);
#   - у неуспешного цикла компактный отчёт (если успел собраться) сохраняется
#     как benchmark-<arm>-<tag>-failed.json;
#   - полный stdout/stderr каждого цикла — bench-runner/logs/<tag>.log;
#   - в конце — сводка серии sweep-summary.json + sweep-summary.md по всем тегам.
set -uo pipefail
cd "$(dirname "$0")/.."

LOGDIR="bench-runner/logs"
STATE="$LOGDIR/state.tsv"
mkdir -p "$LOGDIR"
touch "$STATE"

set_env() { # var value — правка строки VAR=... в .env
  sed -i '' -E "s/^$1=.*/$1=$2/" .env
}

state_get() { awk -F'\t' -v t="$1" -v a="$2" '$1==t && $2==a{print $3}' "$STATE" | tail -1; }
state_set() { printf '%s\t%s\t%s\t%s\n' "$1" "$2" "$3" "$(date '+%F %T')" >> "$STATE"; }
state_drop() { # tag — убрать строки тега (перед ретраем)
  STATE_TAG="$1" python3 - <<'PY'
import os
tag = os.environ["STATE_TAG"]
lines = [l for l in open("bench-runner/logs/state.tsv") if not l.startswith(tag + "\t")]
open("bench-runner/logs/state.tsv", "w").writelines(lines)
PY
}

# run_one <arm> <tag> <cfg-fn> [args...]
run_one() {
  local arm=$1 tag=$2; shift 2
  local log="$LOGDIR/$tag.log" rc dur t0
  [[ "$(state_get "$tag" "$arm")" == done ]] && { echo "[skip] $arm $tag — уже done"; return 0; }
  echo "[start] $(date '+%F %T') $arm $tag — лог: $log"
  "$@"   # применить конфиг в .env ДО старта (benchmark.sh читает .env при запуске)
  t0=$(date +%s)
  scripts/test/benchmark.sh --cycle "$arm" >"$log" 2>&1
  rc=$?
  dur=$(( $(date +%s) - t0 ))
  if [[ $rc -eq 0 && -f "results/benchmark/benchmark-$arm.json" ]]; then
    cp "results/benchmark/benchmark-$arm.json" "results/benchmark/benchmark-$arm-$tag.json"
    state_set "$tag" "$arm" done
    echo "[done ] $(date '+%F %T') $arm $tag за ${dur}с"
  else
    # частичный отчёт тоже сохраняем — он может содержать прошедшие сценарии
    [[ -f "results/benchmark/benchmark-$arm.json" ]] \
      && cp "results/benchmark/benchmark-$arm.json" "results/benchmark/benchmark-$arm-$tag-failed.json"
    state_set "$tag" "$arm" "failed(rc=$rc)"
    echo "[FAIL ] $(date '+%F %T') $arm $tag за ${dur}с rc=$rc — см. $log"
  fi
  return 0   # серия не прерывается одним неудачным прогоном
}

# ---------- конфигурации ----------
cfg_polling() { # batch
  set_env CDC_POLLING_POLL_BATCH_SIZE "$1"
  set_env CDC_POLLING_KAFKA_BATCH_SIZE "$1"
}
cfg_streaming() { # batch
  set_env CDC_STREAMING_KAFKA_BATCH_SIZE "$1"
}
cfg_competing() { # batch workers
  set_env CDC_COMPETING_DRAIN_BATCH_SIZE "$1"
  set_env CDC_COMPETING_KAFKA_BATCH_SIZE "$1"   # kafka-чанк = пачке (tuned-конвенция)
  set_env CDC_COMPETING_WORKER_COUNT "$2"
}

SWEEP_START=$(date '+%F %T')
echo "=== batch-sweep: старт $SWEEP_START (13 полных циклов) ==="

# порядок: polling (текущий дефолт b10000) → streaming b1000/5000/10000 →
# competing b{1000,5000,10000} × w{1,5,10}
run_one polling   b10000    cfg_polling 10000
run_one streaming b1000     cfg_streaming 1000
run_one streaming b5000     cfg_streaming 5000
run_one streaming b10000    cfg_streaming 10000
for b in 1000 5000 10000; do
  for w in 1 5 10; do
    run_one competing "b${b}-w${w}" cfg_competing "$b" "$w"
  done
done

# один автоматический ретрай неуспешных (транзиентные сбои Docker/сети)
RETRY=$(awk -F'\t' '$3!="done" && !seen[$1]++ {print $1"\t"$2}' "$STATE")
if [[ -n "$RETRY" ]]; then
  echo "=== ретрай неуспешных тегов ==="
  while IFS=$'\t' read -r rtag rarm; do
    [[ -z "$rtag" ]] && continue
    state_drop "$rtag"
    case "$rtag" in
      b10000)    [[ "$rarm" == polling ]]   && cfg_polling 10000  || cfg_streaming 10000 ;;
      b1000)     cfg_streaming 1000 ;;
      b5000)     cfg_streaming 5000 ;;
      b1000-w*)  cfg_competing 1000  "${rtag##*-w}" ;;
      b5000-w*)  cfg_competing 5000  "${rtag##*-w}" ;;
      b10000-w*) cfg_competing 10000 "${rtag##*-w}" ;;
    esac
    run_one "$rarm" "$rtag" true   # конфиг применён выше
  done <<< "$RETRY"
fi

# вернуть streaming-ручку к исходному значению (.env до серии: 2048);
# competing остаётся на последнем прогоне b10000-w10 = tuned-дефолт 10/10000/10000;
# polling в серии гонялся ровно на дефолтных 10000/10000 — без изменений
set_env CDC_STREAMING_KAFKA_BATCH_SIZE 2048

echo "=== сводка серии ($SWEEP_START → $(date '+%F %T')) ==="
cat "$STATE"

# ---------- итоговая сводка по всем сохранённым отчётам ----------
python3 bench-runner/make-sweep-summary.py || echo "сводка не собралась — см. ошибки выше"

FAILS=$(awk -F'\t' '$3!="done" && !seen[$1]++' "$STATE" | wc -l | tr -d ' ')
echo "=== серия завершена $(date '+%F %T'), неуспешных тегов: $FAILS ==="
exit 0
