#!/usr/bin/env bash
# benchmark.sh — этап 9: изолированные benchmark-циклы A/B (requirements.md 11).
#   Цикл A (polling):  полный wipe → чистый стенд → одинаковые сценарии →
#                      артефакты/отчёт → полное удаление тестового состояния (§11 шаг 7).
#   Цикл B (streaming): инфраструктура заново → ТЕ ЖЕ сценарии с теми же параметрами →
#                      артефакты/отчёт (стенд остаётся поднятым — исходное состояние владельца).
# Метрики (§13.4 — точные итоги из фактических данных, Prometheus только дополнительно):
#   Writer (длительность, фактический rate, p50/p95/p99 commit/batch), CDC (пропускная
#   способность доставки по сэмплам 1с, e2e p50/p95/p99 + средний e2e по дельтам гистограммы,
#   счётчики ошибок/ретраев), полный цикл доставки до валидатора (drain), дубликаты,
#   per-role CPU/RAM по docker stats (cAdvisor не отдаёт per-container — deviations 7),
#   нагрузка на postgres: дельты снапшотов до/после сценария — pg_stat_statements
#   (per-statement, включая drain-DELETE competing), pg_stat_user_tables(_indexes),
#   pg_stat_wal, pg_stat_database, checkpointer/bgwriter; CPU postgres — отдельно
#   leader и standbys.
#
# Использование:
#   scripts/test/benchmark.sh --cycle all                     # A(polling)+wipe+B(streaming)+wipe+C(competing)
#   scripts/test/benchmark.sh --cycle polling|streaming|competing  # один цикл (polling завершится wipe)
#   Опции: --skip-wipe   не делать начальный wipe (стенд уже чистый/поднят под этот arm)
#          --only s1,s3  подмножество сценариев (имена без суффиксов, см. SCENARIOS)
# Артефакты: results/raw/<stamp>-benchmark-<arm>/{benchmark-report.json, docker-stats.csv,
#   runs/<stamp>-<runId8>/{result.json,timeseries.csv,validation-verdict.json},
#   scenarios/<имя>.json, cdc-samples/<имя>.tsv}
#   + компактная копия отчёта в results/benchmark/benchmark-<arm>.json (коммитится).
set -uo pipefail
cd "$(dirname "$0")/../.."

if [[ ! -f .env ]]; then
  echo "ERROR: .env не найден. Скопируйте: cp .env.example .env"
  exit 1
fi
set -a; source .env; set +a

DC="docker compose"
WBASE="http://localhost:${WRITER_HTTP_PORT:-18080}"
VBASE="http://localhost:${KAFKA_VALIDATOR_HTTP_PORT:-18083}"
PPORT="${CDC_POLLING_HTTP_PORT:-18081}"
SPORT="${CDC_STREAMING_HTTP_PORT:-18082}"
CPORT="${CDC_COMPETING_HTTP_PORT:-18084}"
WV6PORT="${CDC_WALV6_HTTP_PORT:-18086}"

CYCLE_ARG="all"
SKIP_WIPE=0
ONLY=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    --cycle) CYCLE_ARG="${2:-}"; shift 2 ;;
    --skip-wipe) SKIP_WIPE=1; shift ;;
    --only) ONLY="${2:-}"; shift 2 ;;
    *) echo "неизвестный аргумент: $1"; exit 1 ;;
  esac
done
[[ "$CYCLE_ARG" == all || "$CYCLE_ARG" == polling || "$CYCLE_ARG" == streaming || "$CYCLE_ARG" == competing || "$CYCLE_ARG" == walv6 ]] \
  || { echo "--cycle должен быть all|polling|streaming|competing|walv6"; exit 1; }

ARM=""; CDC_PORT=""   # ha-common.sh требует их после source
source scripts/test/ha-common.sh
PASS=0; FAIL=0

# ---------- Сценарии (ОДИНАКОВЫЕ параметры для обеих реализаций, §11) ----------
# Постоянная нагрузка — минимум 5-10 минут (требование владельца, этап 9):
# короткие прогоны не выходят на установившийся режим (GC/checkpoint/окна scrape).
# Порядок фиксирован; warmup не входит в отчёт сценариев (JIT/пулы/прогрев валидатора).
S1_TOTAL="${BENCH_S1_TOTAL:-30000}"
S2_TOTAL="${BENCH_S2_TOTAL:-60000}"
S3_TOTAL="${BENCH_S3_TOTAL:-20000}"
S4_TOTAL="${BENCH_S4_TOTAL:-15000}"
S4_SIZE="${BENCH_S4_SIZE:-4096}"
S5_TOTAL="${BENCH_S5_TOTAL:-4000000}"

SCEN_NAMES=(s1-steady-50 s2-steady-200 s3-burst s4-large-4k s5-throughput)
SCEN_DESCS=(
  "равномерная низкая нагрузка ${S1_TOTAL}@50/s"
  "равномерная высокая нагрузка ${S2_TOTAL}@200/s"
  "backlog-drain: burst ${S3_TOTAL} без rate-limit (короткий по смыслу: скорость выборки)"
  "крупные сообщения ${S4_TOTAL}@50/s x${S4_SIZE}B"
  "max throughput, pre-seeded backlog ${S5_TOTAL} без rate-limit: Writer пишет при ОСТАНОВЛЕННОМ CDC (напрямую через HAProxy, из измерения исключено), после завершения записи CDC стартует и дрейнит чисто; узкое место по построению — только CDC; drain считается с момента state=running (бут контейнера отдельно в cdcBootMs)"
)
SCEN_SPECS=(
  "{\"totalMessages\":${S1_TOTAL},\"ratePerSecond\":50,\"messageSizeBytes\":256,\"batchSize\":50,\"parallelism\":4}"
  "{\"totalMessages\":${S2_TOTAL},\"ratePerSecond\":200,\"messageSizeBytes\":256,\"batchSize\":50,\"parallelism\":4}"
  "{\"totalMessages\":${S3_TOTAL},\"messageSizeBytes\":256,\"batchSize\":100,\"parallelism\":8}"
  "{\"totalMessages\":${S4_TOTAL},\"ratePerSecond\":50,\"messageSizeBytes\":${S4_SIZE},\"batchSize\":50,\"parallelism\":4}"
  "{\"totalMessages\":${S5_TOTAL},\"messageSizeBytes\":256,\"batchSize\":100,\"parallelism\":8}"
)
WARMUP_SPEC='{"totalMessages":500,"ratePerSecond":100,"messageSizeBytes":256,"batchSize":50,"parallelism":4}'

# Спека прогона для текущего arm. Параметры НАГРУЗКИ одинаковы для всех arm (§11);
# arm competing дополнительно получает writeMode=insertOnly — переключатель пути
# ДОСТАВКИ, а не нагрузки: Writer оставляет строки в outbox-таблице, их разбирает
# cdc-competing (DELETE … SKIP LOCKED … RETURNING). arm walv6 получает writeMode=walEmit
# (безтабличная пакетная запись в WAL через pg_logical_emit_message). Остальные arm
# идут с дефолтом writeMode=insertDelete (таблица пустая, сообщение живёт в WAL).
spec_for_arm() { # spec-json → spec-json для текущего ARM
  if [[ "$ARM" == "competing" ]]; then
    python3 -c 'import json,sys; d=json.loads(sys.argv[1]); d["writeMode"]="insertOnly"; print(json.dumps(d))' "$1"
    return 0
  fi
  if [[ "$ARM" == "walv6" ]]; then
    python3 -c 'import json,sys; d=json.loads(sys.argv[1]); d["writeMode"]="walEmit"; print(json.dumps(d))' "$1"
    return 0
  fi
  echo "$1"
}

scenario_enabled() { # short-name без суффикса (s1, s3...)
  [[ -z "$ONLY" ]] && return 0
  local n short
  for n in "${SCEN_NAMES[@]}"; do
    short="${n%%-*}"
    [[ "$short" == "$1" ]] || continue
    [[ ",$ONLY," == *",$n,"* || ",$ONLY," == *",$short,"* ]] && return 0
  done
  return 1
}

# ---------- Сэмплирование CDC-доставки (1с) ----------
# ts_ms,state,delivered,wal_lag_bytes — растёт messagesDelivered = сообщения дошли до Kafka.
cdc_sampler() { # out_tsv
  local out=$1 line
  echo "ts_ms,state,delivered,wal_lag_bytes" > "$out"
  while [[ -e /tmp/bench-sampler-on ]]; do
    line=$(curl -s --max-time 10 "http://localhost:$CDC_PORT/api/cdc/state" | python3 -c '
import json, sys
try:
    d = json.load(sys.stdin)
except Exception:
    raise SystemExit
# без f-строк с \"-экранированием: системный python3 (3.9) их не парсит
print("{},{},{}".format(d.get("state", "?"), d.get("messagesDelivered", 0), d.get("walLagBytes", 0)))' 2>/dev/null)
    [[ -n "$line" ]] && echo "$(now_ms),$line" >> "$out"
    sleep 1
  done
}

# ---------- Снапшот статистик PostgreSQL (primary через HAProxy) ----------
# Дельта до/после сценария изолирует нагрузку arm'а на БД от остального стенда:
#   statements — pg_stat_statements по БД outbox: drain-DELETE у competing vs
#     INSERT+DELETE Writer'а у polling/streaming vs peek/advance у polling;
#   table/indexes — pg_stat_user_tables(_indexes) по outbox_messages
#     (сканирования индексов, dead tuples, autovacuum после mass DELETE);
#   wal/db/checkpointer/bgwriter — объём WAL, транзакции, буферы, temp files.
# Одна строка JSON на снапшот; при недоступности psql файл пуст (дельты не считаются).
pg_snap() { # out_json
  psql_primary -Atc "
SELECT json_build_object(
  'statements', (SELECT coalesce(json_agg(row_to_json(s)), '[]'::json) FROM (
      SELECT coalesce(queryid::text, ' utility') AS queryid,
             pg_get_userbyid(userid) AS rolname,
             left(query, 200) AS query, calls,
             round(total_exec_time::numeric, 1) AS total_exec_ms,
             round(mean_exec_time::numeric, 3) AS mean_exec_ms, rows,
             shared_blks_hit, shared_blks_read, shared_blks_dirtied, shared_blks_written,
             temp_blks_read, temp_blks_written,
             round(shared_blk_read_time::numeric, 1) AS blk_read_ms,
             round(shared_blk_write_time::numeric, 1) AS blk_write_ms,
             wal_records, wal_bytes
      FROM pg_stat_statements
      WHERE dbid = (SELECT oid FROM pg_database WHERE datname = 'outbox')
      ORDER BY total_exec_time DESC LIMIT 500) s),
  'table', (SELECT coalesce(json_agg(row_to_json(t)), '[]'::json) FROM (
      SELECT seq_scan, seq_tup_read, idx_scan, n_tup_ins, n_tup_upd, n_tup_del,
             n_tup_hot_upd, n_live_tup, n_dead_tup, vacuum_count, autovacuum_count,
             analyze_count, autoanalyze_count
      FROM pg_stat_user_tables
      WHERE schemaname = 'public' AND relname = 'outbox_messages') t),
  'indexes', (SELECT coalesce(json_agg(row_to_json(i)), '[]'::json) FROM (
      SELECT indexrelname, idx_scan, idx_tup_read, idx_tup_fetch
      FROM pg_stat_user_indexes
      WHERE schemaname = 'public' AND relname = 'outbox_messages') i),
  'wal', (SELECT to_json(w) FROM (SELECT wal_records, wal_fpi, wal_bytes, wal_buffers_full
      FROM pg_stat_wal) w),
  'db', (SELECT to_json(d) FROM (SELECT xact_commit, xact_rollback, blks_hit, blks_read,
      tup_returned, tup_fetched, tup_inserted, tup_updated, tup_deleted,
      temp_files, temp_bytes, deadlocks
      FROM pg_stat_database WHERE datname = 'outbox') d),
  'checkpointer', (SELECT to_json(c) FROM (SELECT num_timed, num_requested,
      round(write_time::numeric, 0) AS write_time, round(sync_time::numeric, 0) AS sync_time,
      buffers_written FROM pg_stat_checkpointer) c),
  'bgwriter', (SELECT to_json(b) FROM (SELECT buffers_clean, maxwritten_clean, buffers_alloc
      FROM pg_stat_bgwriter) b),
  'timeline', (SELECT timeline_id FROM pg_control_checkpoint())
)::text" > "$1" 2>/dev/null || :
}

# Средний e2e за сценарий по дельтам гистограммы Prometheus (sum/count — счётчики):
# точный per-сценарный средний e2e, в отличие от rolling-процентилей CDC (окно 4096).
prom_hist_pair() { # metric_base_without_suffix → "sum count" (или " ")
  local q sum count
  sum=$(prom_query "$1_sum{job=\"cdc-$ARM\"}" | python3 -c 'import json,sys; r=json.load(sys.stdin)["data"]["result"]; print(r[0]["value"][1] if r else "")' 2>/dev/null)
  count=$(prom_query "$1_count{job=\"cdc-$ARM\"}" | python3 -c 'import json,sys; r=json.load(sys.stdin)["data"]["result"]; print(r[0]["value"][1] if r else "")' 2>/dev/null)
  echo "${sum:- } ${count:- }"
}

# ---------- Ожидание доставки всего прогона (валидатор; бюджет больше, чем в ha-common) ----------
wait_run_unique_long() { # runId expected timeout_s → unique на stdout
  local u=""
  for _ in $(seq 1 "$3"); do
    u=$(vget "/api/validation/runs/$1" | python3 -c 'import json,sys; d=json.load(sys.stdin); print(d.get("uniqueIds", 0))' 2>/dev/null)
    [[ "$u" == "$2" ]] && break
    sleep 1
  done
  echo "${u:-0}"
}

# ---------- Один сценарий ----------
# Глобальные входы: ARM, CYCLE_DIR, DOCKER_STATS_CSV (общий на цикл).
# Глобальные выходы: SCEN_JSON (путь), SCEN_RUN_ID, SCEN_WRITTEN.
run_scenario() { # idx
  local idx=$1 name desc spec run written t0 t1 t2 u code verdict
  name="${SCEN_NAMES[$idx]}"; desc="${SCEN_DESCS[$idx]}"; spec="$(spec_for_arm "${SCEN_SPECS[$idx]}")"
  section "Сценарий $name: $desc"

  [[ "$(writer_state_field 'state')" != "running" ]] || { bad "$name: Writer ещё занят прошлым прогоном"; return 1; }

  curl -s --max-time 10 "http://localhost:$CDC_PORT/api/cdc/state" -o /tmp/bench-cdc-before.json
  local e2e_sum0 e2e_cnt0
  read -r e2e_sum0 e2e_cnt0 <<< "$(prom_hist_pair "cdc_${ARM}_e2e_latency_milliseconds")"
  # снапшот статистик PG до старта Writer'а + лидер (для разбивки CPU leader/standbys)
  local pg_leader
  pg_leader=$(leader_name)
  pg_snap /tmp/bench-pg-before.json

  mkdir -p "$CYCLE_DIR/scenarios" "$CYCLE_DIR/cdc-samples"
  touch /tmp/bench-sampler-on
  cdc_sampler "$CYCLE_DIR/cdc-samples/$name.tsv" &
  local sampler_pid=$!

  # s5: pre-seeded backlog — Writer пишет при остановленном CDC, после завершения
  # записи CDC стартует и дрейнит в чистом виде (скорость записи вне измерения,
  # узкое место по построению — только CDC). Слот за время простоя не инвалидируется
  # (max_slot_wal_keep_size=-1, patroni.yml) — WAL копится и выбирается после старта.
  local pre_seed=0 t1c=""
  [[ "$name" == s5-* ]] && pre_seed=1
  if [[ "$pre_seed" == 1 ]]; then
    $DC stop "cdc-$ARM" >/dev/null 2>&1
    local cdc_ctr="streaming-polling-outbox-cdc-${ARM}-1" cdc_down=0
    for _ in $(seq 1 30); do
      [[ "$(docker inspect -f '{{.State.Running}}' "$cdc_ctr" 2>/dev/null)" == "false" ]] && { cdc_down=1; break; }
      sleep 1
    done
    if [[ "$cdc_down" != 1 ]]; then
      bad "$name: не удалось остановить cdc-$ARM за 30с"
      rm -f /tmp/bench-sampler-on; wait "$sampler_pid" 2>/dev/null
      return 1
    fi
    ok "$name: CDC остановлен — backlog пишется без потребителя (pre-seed)"
  fi

  t0=$(now_ms)
  run=$(run_writer "$spec")
  t1=$(now_ms)
  if [[ "$run" == FAIL:* ]]; then
    bad "$name: прогон не completed ($run)"
    rm -f /tmp/bench-sampler-on; wait "$sampler_pid" 2>/dev/null
    return 1
  fi
  SCEN_RUN_ID="$run"
  ok "$name: Writer completed за $(( t1 - t0 ))мс (runId=${run:0:8})"
  writer_results
  cp /tmp/ha-results.json /tmp/bench-writer.json
  written=$(writer_field 'd["writtenMessages"]')
  SCEN_WRITTEN="$written"
  ok "$name: written=$written, фактический rate=$(python3 -c "print(f'{$written / max($(writer_field 'd["durationSeconds"]'), 0.001):.0f}')") msg/s"

  # s5: старт CDC после завершения записи — дрейн чистый, от момента state=running
  if [[ "$pre_seed" == 1 ]]; then
    $DC start "cdc-$ARM" >/dev/null 2>&1
    if ! wait_cdc_state running 300; then
      bad "$name: CDC не перешёл в running за 300с после старта: $(cdc_state_raw)"
      rm -f /tmp/bench-sampler-on; wait "$sampler_pid" 2>/dev/null
      return 1
    fi
    t1c=$(now_ms)
    ok "$name: CDC поднят за $(( t1c - t1 ))мс — начинается дрейн"
  fi

  local wait_budget=1800
  [[ "$pre_seed" == 1 ]] && wait_budget=5400
  u=$(wait_run_unique_long "$run" "$written" "$wait_budget")
  t2=$(now_ms)
  rm -f /tmp/bench-sampler-on; wait "$sampler_pid" 2>/dev/null
  if [[ "$pre_seed" == 1 ]]; then
    [[ "$u" == "$written" ]] \
      && ok "$name: доставка завершена: цикл $(( t2 - t0 ))мс = запись $(( t1 - t0 ))мс + бут CDC $(( t1c - t1 ))мс + дрейн $(( t2 - t1c ))мс" \
      || bad "$name: доставлено $u из $written за ${wait_budget}с"
  else
    [[ "$u" == "$written" ]] \
      && ok "$name: доставка завершена: цикл $(( t2 - t0 ))мс, drain после Writer $(( t2 - t1 ))мс" \
      || bad "$name: доставлено $u из $written за ${wait_budget}с"
  fi

  curl -s --max-time 10 "http://localhost:$CDC_PORT/api/cdc/state" -o /tmp/bench-cdc-after.json
  # pending-статистики postgres (table/db/wal) бэкенды флашат с задержкой:
  # непрерывно работающие (drain-воркеры) — за ~1-3с, idle-подключения Writer'а —
  # замерено на стенде ~9с после завершения прогона. Без паузы after-снапшот
  # теряет Writer-хвост (ins/wal/xact). 15с покрывает худший случай; pg_stat_statements
  # обновляется сразу и паузой не искажается. Побочный эффект: в дельту competing
  # попадают пустые poll-вызовы drain-воркеров за паузу — это реальная стоимость
  # steady-state поллинга пустой очереди, оставляем.
  sleep 15
  pg_snap /tmp/bench-pg-after.json
  code=$(validate_run "$run" true)
  if [[ "$code" == "200" ]]; then
    assert_verdict_pass_dup "$name"
  else
    bad "$name: validate → $code: $(cat /tmp/ha-verdict.json)"
  fi
  cp /tmp/ha-verdict.json "/tmp/ha-verdict-$run.json"

  local e2e_sum1 e2e_cnt1
  read -r e2e_sum1 e2e_cnt1 <<< "$(prom_hist_pair "cdc_${ARM}_e2e_latency_milliseconds")"

  # артефакты §13.4 (коллектор работает с последним завершённым прогоном — это наш run)
  local collector_out run_dir
  collector_out=$(./scripts/reports/collect-results.sh "$run" --arm "$ARM" \
    --notes "benchmark $name: $desc" --out "$CYCLE_DIR/runs" 2>&1)
  echo "$collector_out" | sed 's/^/  /' >&2
  run_dir=$(echo "$collector_out" | grep '^артефакты:' | sed 's/^артефакты: //; s|/result\.json.*||')
  [[ -n "$run_dir" ]] && cp "/tmp/ha-verdict-$run.json" "$run_dir/validation-verdict.json" \
    && ok "$name: артефакты прогона: $run_dir"

  # per-сценарный JSON: тайминги, дельты счётчиков, сэмплы доставки, CPU/RAM по ролям
  SCEN_JSON="$CYCLE_DIR/scenarios/$name.json"
  SCEN_NAME="$name" SCEN_DESC="$desc" SCEN_SPEC="$spec" SCEN_RUN_ID="$run" \
  SCEN_T0="$t0" SCEN_T1="$t1" SCEN_T2="$t2" SCEN_T1C="$t1c" \
  E2E_SUM0="$e2e_sum0" E2E_CNT0="$e2e_cnt0" E2E_SUM1="$e2e_sum1" E2E_CNT1="$e2e_cnt1" \
  CDC_BEFORE="/tmp/bench-cdc-before.json" CDC_AFTER="/tmp/bench-cdc-after.json" \
  PG_BEFORE="/tmp/bench-pg-before.json" PG_AFTER="/tmp/bench-pg-after.json" PG_LEADER="$pg_leader" \
  WRITER_JSON="/tmp/bench-writer.json" VERDICT_JSON="/tmp/ha-verdict.json" \
  SAMPLES_TSV="$CYCLE_DIR/cdc-samples/$name.tsv" DOCKER_STATS_CSV="$DOCKER_STATS_CSV" \
  SCEN_OUT="$SCEN_JSON" BENCH_ARM="$ARM" python3 <<'PY'
import csv, json, os
from datetime import datetime

def load(p, d=None):
    try:
        return json.load(open(p))
    except Exception:
        return d

name = os.environ["SCEN_NAME"]
writer = load(os.environ["WRITER_JSON"], {})
before = load(os.environ["CDC_BEFORE"], {})
after = load(os.environ["CDC_AFTER"], {})
verdict = load(os.environ["VERDICT_JSON"], {})
t0, t1, t2 = (int(os.environ[k]) for k in ("SCEN_T0", "SCEN_T1", "SCEN_T2"))
_t1c = os.environ.get("SCEN_T1C", "")
t1c = int(_t1c) if _t1c.isdigit() else None
pre_seeded = t1c is not None

def f(d, k):
    v = d.get(k)
    return v if isinstance(v, (int, float)) else 0

scen = {
    "name": name,
    "description": os.environ["SCEN_DESC"],
    "spec": json.loads(os.environ["SCEN_SPEC"]),
    "runId": os.environ["SCEN_RUN_ID"],
    "arm": os.environ.get("BENCH_ARM", ""),
    "timings": {
        "t0EpochMs": t0,
        "t2EpochMs": t2,
        "writerMs": t1 - t0,
        "deliveryTotalMs": t2 - t0,       # старт записи → всё подтверждено валидатором
                                         # (s5 pre-seed: включает фазу записи и бут CDC)
        "drainMs": t2 - (t1c if pre_seeded else t1),
                                         # s5 pre-seed: с момента старта CDC — чистый дрейн;
                                         # остальные: Writer finished → всё доставлено
        **({"cdcBootMs": t1c - t1, "preSeededBacklog": True} if pre_seeded else {}),
    },
    "writer": {
        "written": f(writer, "writtenMessages"),
        "failed": f(writer, "failedMessages"),
        "ambiguousUnresolved": f(writer, "ambiguousUnresolved"),
        "durationSeconds": writer.get("durationSeconds"),
        "writerRatePerSecond": round(f(writer, "writtenMessages") / max(writer.get("durationSeconds") or 1e-3, 1e-3), 1),
        "commitP50Ms": writer.get("commitP50Ms"), "commitP95Ms": writer.get("commitP95Ms"), "commitP99Ms": writer.get("commitP99Ms"),
        "batchP50Ms": writer.get("batchP50Ms"), "batchP95Ms": writer.get("batchP95Ms"), "batchP99Ms": writer.get("batchP99Ms"),
        "reconnects": f(writer, "reconnects"),
    },
    "verdict": {
        "verdict": verdict.get("verdict"),
        "missing": verdict.get("missingWrittenIdsCount"),
        "unexpectedExtra": verdict.get("unexpectedExtraIdsCount"),
        "duplicates": (verdict.get("kafka") or {}).get("duplicateRecords"),
        "uniqueIds": (verdict.get("kafka") or {}).get("uniqueIds"),
    },
    "cdc": {
        # pre-seed: контейнер CDC рестартован внутри сценария — счётчики in-memory
        # обнулились, дельта от before (счётчики ПРОШЛОГО процесса) кривая (минус
        # warmup-хвост); базой берём 0 — after и есть дельта текущего процесса
        "deliveredDelta": f(after, "messagesDelivered") - (0 if pre_seeded else f(before, "messagesDelivered")),
        "readDelta": f(after, "messagesRead") - (0 if pre_seeded else f(before, "messagesRead")),
        "readErrors": f(after, "readErrors") - (0 if pre_seeded else f(before, "readErrors")),
        "kafkaErrors": f(after, "kafkaErrors") - (0 if pre_seeded else f(before, "kafkaErrors")),
        "retries": f(after, "retries") - (0 if pre_seeded else f(before, "retries")),
        "reconnects": f(after, "reconnects") - (0 if pre_seeded else f(before, "reconnects")),
        "walLagBytesAfter": after.get("walLagBytes"),
        "e2eP50Ms": after.get("e2eP50Ms"), "e2eP95Ms": after.get("e2eP95Ms"), "e2eP99Ms": after.get("e2eP99Ms"),
    },
}

# средний e2e за сценарий: дельты счётчиков гистограммы Prometheus
try:
    ds = float(os.environ["E2E_SUM1"]) - float(os.environ["E2E_SUM0"])
    dc = float(os.environ["E2E_CNT1"]) - float(os.environ["E2E_CNT0"])
    if pre_seeded:
        # рестарт CDC обнулил гистограмму: sum/cnt после — это ВЕСЬ дрен-процесс,
        # дельта с before (warmup-хвост старого процесса) только занижала бы mean
        ds, dc = float(os.environ["E2E_SUM1"]), float(os.environ["E2E_CNT1"])
    if dc > 0:
        scen["cdc"]["e2eMeanMs"] = round(ds / dc, 1)
        scen["cdc"]["e2eSamples"] = int(dc)
except Exception:
    pass

# дельты статистик postgres: снапшоты pg_snap до/после сценария (нагрузка arm'а на БД)
def pg_load(path):
    try:
        txt = open(path).read().strip()
        return json.loads(txt) if txt else None
    except Exception:
        return None

pg_before = pg_load(os.environ.get("PG_BEFORE", ""))
pg_after = pg_load(os.environ.get("PG_AFTER", ""))
scen["postgres"] = {"leader": os.environ.get("PG_LEADER") or None,
                    "snapshots": bool(pg_before and pg_after)}
if pg_before and pg_after:
    # статистики не реплицируются: failover между снапшотами делает дельты
    # мусором — детектим по смене timeline (pg_control_checkpoint)
    if pg_before.get("timeline") != pg_after.get("timeline"):
        scen["postgres"]["snapshots"] = False
        scen["postgres"]["snapshotWarning"] = (
            f"failover между снапшотами (timeline {pg_before.get('timeline')}"
            f" -> {pg_after.get('timeline')}): дельты postgres не считаются")
    def fnum(v):
        try:
            return float(v)
        except (TypeError, ValueError):
            return 0.0

    # statements: кумулятивные счётчики → дельта по queryid
    STMT_F = ("calls", "total_exec_ms", "rows", "shared_blks_hit", "shared_blks_read",
              "shared_blks_dirtied", "shared_blks_written", "temp_blks_read", "temp_blks_written",
              "blk_read_ms", "blk_write_ms", "wal_records", "wal_bytes")
    bmap = {s["queryid"]: s for s in pg_before.get("statements") or []}
    stmts = []
    for a in pg_after.get("statements") or []:
        b = bmap.get(a["queryid"], {})
        d = {"queryid": a["queryid"], "query": a["query"], "rolname": a.get("rolname")}
        for k in STMT_F:
            d["d_" + k] = round(fnum(a.get(k)) - fnum(b.get(k)), 3)
        if any(d["d_" + k] for k in STMT_F):
            stmts.append(d)
    # собственные снапшот-запросы — не нагрузка сценария
    stmts = [s for s in stmts if "pg_stat_statements" not in s["query"]
             and "json_build_object" not in s["query"]]
    for s in stmts:
        s["mean_exec_ms"] = round(s["d_total_exec_ms"] / s["d_calls"], 3) if s["d_calls"] else None
        # роль по тексту запроса: Writer и drain различаются даже в DELETE
        # (drain: DELETE … FOR UPDATE SKIP LOCKED … RETURNING; Writer insertDelete:
        # DELETE … WHERE id = ANY($1)). peek/advance — SECURITY DEFINER-обёртки
        # polling-CDC (exec time внешнего SELECT включает работу функции).
        q = s["query"].lstrip()
        ql = q.lower()
        if "outbox_messages" in ql:
            if q.startswith("DELETE"):
                s["role"] = "drain-delete" if ("skip locked" in ql or "returning" in ql) else "writer-delete"
            elif q.startswith("INSERT"):
                s["role"] = "writer-insert"
            else:
                s["role"] = "outbox-other"
        elif "peek_outbox_changes" in ql or "pg_logical_slot_peek" in ql:
            s["role"] = "polling-peek"
        elif "advance_outbox_slot" in ql or "pg_replication_slot_advance" in ql:
            s["role"] = "polling-advance"
        elif "emit_outbox_frame" in ql or "pg_logical_emit_message" in ql:
            s["role"] = "writer-wal-emit"
        elif "outbox_slot_status" in ql:
            s["role"] = "slot-status"
        else:
            s["role"] = "other"
    stmts.sort(key=lambda s: -s["d_total_exec_ms"])
    scen["postgres"]["statements"] = stmts

    # таблица: счётчики — дельта; n_live_tup/n_dead_tup — гейджи, значение ПОСЛЕ
    bt = (pg_before.get("table") or [{}])[0]
    at = (pg_after.get("table") or [{}])[0]
    tdel, tafter = {}, {}
    for k, v in at.items():
        if k in ("n_live_tup", "n_dead_tup"):
            tafter[k] = v
        else:
            tdel["d_" + k] = round(fnum(v) - fnum(bt.get(k)), 3)
    scen["postgres"]["table"] = {"delta": tdel, "after": tafter}

    bix = {i["indexrelname"]: i for i in pg_before.get("indexes") or []}
    scen["postgres"]["indexes"] = [
        {"indexrelname": i["indexrelname"],
         **{"d_" + k: round(fnum(i.get(k)) - fnum(bix.get(i["indexrelname"], {}).get(k)), 3)
            for k in ("idx_scan", "idx_tup_read", "idx_tup_fetch")}}
        for i in pg_after.get("indexes") or []]

    # wal / db / checkpointer / bgwriter — счётчики, дельта
    for section in ("wal", "db", "checkpointer", "bgwriter"):
        b, a = pg_before.get(section) or {}, pg_after.get(section) or {}
        scen["postgres"][section] = {"d_" + k: round(fnum(a.get(k)) - fnum(b.get(k)), 3)
                                     for k in a}

# сэмплы доставки: скорость = рост delivered. Каденс сэмплера ~1.5-2с (curl+python),
# поэтому peak — максимум СКОРОСТИ МЕЖДУ СОСЕДНИМИ сэмплами (нормировано к 1с),
# avg — активное окно (первый→последний рост); при <2 ростов (burst короче каденса)
# avg берётся overall = delivered/окно сценария. Разрешение = samplerRows.
try:
    rows = []
    with open(os.environ["SAMPLES_TSV"]) as fh:
        rd = csv.DictReader(fh)
        for r in rd:
            rows.append((int(r["ts_ms"]), int(r["delivered"])))
    grows = [(rows[i][0], rows[i][1]) for i in range(1, len(rows)) if rows[i][1] > rows[i-1][1]]
    if len(grows) >= 2:
        span_ms = grows[-1][0] - grows[0][0]
        if span_ms > 0:
            scen["cdc"]["deliveryRateAvgPerSecond"] = round(
                (grows[-1][1] - grows[0][1]) / (span_ms / 1000), 1)
    peak = 0.0
    for i in range(1, len(rows)):
        dt = (rows[i][0] - rows[i-1][0]) / 1000
        if dt > 0:
            peak = max(peak, (rows[i][1] - rows[i-1][1]) / dt)
    scen["cdc"]["deliveryRatePeakPerSecond"] = round(peak, 1)
    scen["cdc"]["samplerRows"] = len(rows)
except Exception:
    pass
# overall-скорость всегда доступна (не зависит от каденса сэмплера)
try:
    tot_s = max((t2 - t0) / 1000, 1e-3)
    scen["cdc"]["deliveryRateOverallPerSecond"] = round(
        scen["cdc"]["deliveredDelta"] / tot_s, 1)
except Exception:
    pass

# per-role CPU/RAM из docker stats (срез 5с по всему циклу → окно сценария).
# Burst-сценарии короче интервала сэмплера → окно расширяется до >=12с
# (паддинг фиксируется в resourceWindowMs; cpuAvg за паддинг занижен, cpuMax честен).
def stats_for(container_pred):
    pad_b, pad_a = 2000, 2000
    span = t2 - t0
    if span < 12000:
        extra = 12000 - span
        pad_b += extra // 2
        pad_a += extra - extra // 2
    lo, hi = t0 - pad_b, t2 + pad_a
    sums = []  # на срез: суммарный cpu% по роли
    mems = []
    try:
        with open(os.environ["DOCKER_STATS_CSV"]) as fh:
            rd = csv.DictReader(fh)
            buckets = {}
            for r in rd:
                try:
                    ts = datetime.strptime(r["timestamp"], "%Y-%m-%dT%H:%M:%S%z").timestamp() * 1000
                except Exception:
                    continue
                if not (lo <= ts <= hi):
                    continue
                if not container_pred(r["container"]):
                    continue
                b = buckets.setdefault(int(ts // 5000), {"cpu": 0.0, "mem": 0})
                try:
                    b["cpu"] += float(r["cpu_percent"] or 0)
                    b["mem"] += int(r["mem_bytes"] or 0)
                except Exception:
                    pass
            sums = [b["cpu"] for b in buckets.values()]
            mems = [b["mem"] for b in buckets.values()]
    except Exception:
        pass
    if not sums:
        return None
    return {
        "cpuAvgPercent": round(sum(sums) / len(sums), 1),
        "cpuMaxPercent": round(max(sums), 1),
        "memAvgBytes": round(sum(mems) / len(mems)),
        "memMaxBytes": max(mems),
        "slices": len(sums),
        "resourceWindowMs": hi - lo,
    }

def role_stats(prefixes):
    return stats_for(lambda c: any(c.startswith(p) for p in prefixes))

proj = "streaming-polling-outbox"
pg_nodes = [f"{proj}-pg{i}-1" for i in (1, 2, 3)]
leader_node = os.environ.get("PG_LEADER") or ""
leader_ctr = f"{proj}-{leader_node}-1"
pg_leader_stats = stats_for(lambda c: c == leader_ctr) if leader_ctr in pg_nodes else None
if pg_leader_stats is not None:
    pg_leader_stats = {"node": leader_node, **pg_leader_stats}
scen["resources"] = {
    "cdc": role_stats([f"{proj}-cdc-"]),
    "postgresql": role_stats(pg_nodes),  # сумма 3 нод (как раньше — совместимость)
    "pgLeader": pg_leader_stats,
    "pgStandbys": stats_for(lambda c: c in pg_nodes and c != leader_ctr),
    "kafka": role_stats([f"{proj}-kafka1"]),
}

json.dump(scen, open(os.environ["SCEN_OUT"], "w"), ensure_ascii=False, indent=2, sort_keys=True)

d, c, v = scen["timings"], scen["cdc"], scen["verdict"]
print(f"    → writer {scen['writer']['writerRatePerSecond']} msg/s"
      f" | доставка {d['deliveryTotalMs']}мс (drain {d['drainMs']}мс)"
      f" | cdc avg {c.get('deliveryRateAvgPerSecond','?')} пик {c.get('deliveryRatePeakPerSecond','?')} msg/s"
      f" | e2e p50/p95/p99 {c.get('e2eP50Ms','?')}/{c.get('e2eP95Ms','?')}/{c.get('e2eP99Ms','?')}мс"
      f" | дубликатов {v['duplicates']}")
pg = scen.get("postgres") or {}
if pg.get("snapshots"):
    lead = (scen.get("resources") or {}).get("pgLeader") or {}
    wal_mb = round((pg.get("wal") or {}).get("d_wal_bytes", 0) / 1e6, 1)
    drain = next((s for s in pg.get("statements") or []
                  if s.get("role") == "drain-delete" and s.get("mean_exec_ms") is not None), None)
    drain_txt = f" | drain-delete mean {drain['mean_exec_ms']}мс/батч x{int(drain['d_calls'])}" if drain else ""
    print(f"    → pg: leader cpu avg {lead.get('cpuAvgPercent','?')}% max {lead.get('cpuMaxPercent','?')}%"
          f" | xact {round((pg.get('db') or {}).get('d_xact_commit', 0))}"
          f" | wal {wal_mb}МБ | tup del {round((pg.get('db') or {}).get('d_tup_deleted', 0))}{drain_txt}")
PY
  [[ $? -eq 0 && -f "$SCEN_JSON" ]] && ok "$name: сводка: $SCEN_JSON" || bad "$name: сводка не собрана"
}

# ---------- Готовность свежего стенда ----------
wait_stand_ready() {
  local info slot_st i
  section "Готовность стенда (arm=$ARM)"
  wait_cluster_ready 600 && ok "Patroni: 3 ноды running/streaming, leader=$(leader_name)" \
    || { bad "кластер Patroni не собрался за 600с"; return 1; }
  slot_st=""
  for i in $(seq 1 75); do
    slot_st=$(slot_field wal_status)
    [[ "$slot_st" == "reserved" || "$slot_st" == "extended" ]] && break
    sleep 4
  done
  [[ "$slot_st" == "reserved" || "$slot_st" == "extended" ]] && ok "слот: wal_status=$slot_st" \
    || { bad "слот не готов (wal_status='$slot_st') за 300с"; return 1; }
  local synced
  synced=$(wait_slot_synced 600)
  [[ -n "$synced" ]] && ok "слот synced на standby ($synced)" || bad "слот не synced за 600с"
  wait_kafka_healthy 300 && ok "Kafka: брокер healthy" || { bad "Kafka не healthy"; return 1; }
  for i in $(seq 1 3); do wait_validator_ready && break; sleep 5; done
  [[ "$(curl -s -o /dev/null -w '%{http_code}' --max-time 4 "$VBASE/ready")" == "200" ]] \
    && ok "KafkaValidator /ready=200" || { bad "валидатор не готов"; return 1; }
  wait_cdc_state running 600 && ok "CDC $ARM: state=running" \
    || { bad "CDC не running: $(cdc_state_raw)"; return 1; }
  local wstate
  for i in $(seq 1 30); do
    wstate=$(curl -s --max-time 5 "$WBASE/api/generation/state" 2>/dev/null)
    [[ -n "$wstate" ]] && break
    sleep 2
  done
  [[ -n "$wstate" ]] && ok "Writer отвечает" || { bad "Writer недоступен"; return 1; }
}

# ---------- Итоговый отчёт цикла ----------
cycle_report() { # → benchmark-report.json + копия в results/benchmark/
  local total=0 verdict_ok=0 s j
  mkdir -p results/benchmark
  BENCH_ARM="$ARM" CYCLE_DIR="$CYCLE_DIR" CYCLE_STAMP="$CYCLE_STAMP" \
  REPORT="$CYCLE_DIR/benchmark-report.json" SUMMARY="results/benchmark/benchmark-$ARM.json" \
  python3 <<'PY'
import csv, glob, json, os

arm = os.environ["BENCH_ARM"]
cycle_dir = os.environ["CYCLE_DIR"]
scens = []
for p in sorted(glob.glob(os.path.join(cycle_dir, "scenarios", "*.json"))):
    try:
        scens.append(json.load(open(p)))
    except Exception:
        pass

# версии/гит — из первого собранного прогона
vers, git = {}, {}
runs = sorted(glob.glob(os.path.join(cycle_dir, "runs", "*", "result.json")))
if runs:
    r = json.load(open(runs[0]))
    vers = r.get("versions", {})
    git = r.get("git", {})

# ресурсы за весь блок сценариев [первый t0 .. последний t2]: честное сравнение
# CPU/RAM между реализациями на ОДИНАКОВОЙ работе (per-сценарные окна коротки для 5с-сэмплера)
def cycle_stats_for(container_pred, lo, hi):
    from datetime import datetime
    sums, mems = [], []
    try:
        with open(os.path.join(cycle_dir, "docker-stats.csv")) as fh:
            rd = csv.DictReader(fh)
            buckets = {}
            for r in rd:
                try:
                    ts = datetime.strptime(r["timestamp"], "%Y-%m-%dT%H:%M:%S%z").timestamp() * 1000
                except Exception:
                    continue
                if not (lo <= ts <= hi):
                    continue
                if not container_pred(r["container"]):
                    continue
                b = buckets.setdefault(int(ts // 5000), {"cpu": 0.0, "mem": 0})
                try:
                    b["cpu"] += float(r["cpu_percent"] or 0)
                    b["mem"] += int(r["mem_bytes"] or 0)
                except Exception:
                    pass
            sums = [b["cpu"] for b in buckets.values()]
            mems = [b["mem"] for b in buckets.values()]
    except Exception:
        pass
    if not sums:
        return None
    return {"cpuAvgPercent": round(sum(sums) / len(sums), 1),
            "cpuMaxPercent": round(max(sums), 1),
            "memAvgBytes": round(sum(mems) / len(mems)),
            "memMaxBytes": max(mems), "slices": len(sums)}

def cycle_role_resources(prefixes, lo, hi):
    return cycle_stats_for(lambda c: any(c.startswith(p) for p in prefixes), lo, hi)

# слияние postgres-дельт сценариев цикла: statements суммируются по queryid,
# счётчики wal/db/... — суммой; гейджи таблицы (n_*_tup) — после последнего сценария
def merge_pg(scens):
    out = {"statements": {}, "table": {}, "wal": {}, "db": {}, "checkpointer": {}, "bgwriter": {}}
    for s in scens:
        pg = s.get("postgres") or {}
        if not pg.get("snapshots"):
            continue
        for st in pg.get("statements") or []:
            m = out["statements"].setdefault(
                st["queryid"], {"queryid": st["queryid"], "query": st["query"],
                                "rolname": st.get("rolname"), "role": st.get("role")})
            for k, v in st.items():
                if k.startswith("d_"):
                    m[k] = round(m.get(k, 0) + v, 3)
        for sec in ("wal", "db", "checkpointer", "bgwriter"):
            for k, v in (pg.get(sec) or {}).items():
                if k.startswith("d_"):
                    out[sec][k] = round(out[sec].get(k, 0) + v, 3)
        t = pg.get("table") or {}
        for k, v in (t.get("delta") or {}).items():
            out["table"][k] = round(out["table"].get(k, 0) + v, 3)
        if t.get("after"):
            out["table"]["after"] = t["after"]  # гейджи: значение после последнего сценария
    stmts = sorted(out["statements"].values(), key=lambda s: -s.get("d_total_exec_ms", 0))
    for s in stmts:
        s["mean_exec_ms"] = round(s["d_total_exec_ms"] / s["d_calls"], 3) if s.get("d_calls") else None
    out["statements"] = stmts
    return out

t0s = [s["timings"]["t0EpochMs"] for s in scens if s.get("timings", {}).get("t0EpochMs")]
t2s = [s["timings"]["t2EpochMs"] for s in scens if s.get("timings", {}).get("t2EpochMs")]
resources = {}
if t0s and t2s:
    proj = "streaming-polling-outbox"
    pg_nodes = [f"{proj}-pg{i}-1" for i in (1, 2, 3)]
    # лидер цикла: самый частый среди снятых в сценариях (failover внутри цикла
    # сломал бы вердикты — не ожидается; при расхождении видно по per-scenario)
    leaders = [s.get("postgres", {}).get("leader") for s in scens
               if s.get("postgres", {}).get("leader")]
    leader_node = max(set(leaders), key=leaders.count) if leaders else ""
    leader_ctr = f"{proj}-{leader_node}-1"
    # при коротком цикле (единственный burst-сценарий) окно [t0..t2] может не
    # contain ни одного 5с-среза docker stats → цикловые pg-ресурсы None
    # (per-scenario окна с паддингом остаются источником)
    pg_leader_cycle = None
    if leader_ctr in pg_nodes:
        st = cycle_stats_for(lambda c: c == leader_ctr, min(t0s), max(t2s))
        if st is not None:
            pg_leader_cycle = {"node": leader_node, **st}
    resources = {
        "window": {"fromMs": min(t0s), "toMs": max(t2s)},
        "cdc": cycle_role_resources([f"{proj}-cdc-"], min(t0s), max(t2s)),
        "postgresql": cycle_role_resources(pg_nodes, min(t0s), max(t2s)),
        "pgLeader": pg_leader_cycle,
        "pgStandbys": cycle_stats_for(lambda c: c in pg_nodes and c != leader_ctr,
                                      min(t0s), max(t2s)),
        "kafka": cycle_role_resources([f"{proj}-kafka1"], min(t0s), max(t2s)),
        "writer": cycle_role_resources([f"{proj}-writer-"], min(t0s), max(t2s)),
    }

cycle_labels = {
    "polling": "A (polling)",
    "streaming": "B (streaming)",
    "competing": "C (competing)",
    "walv6": "D (walv6)",
}
report = {
    "schema": "streaming-polling-outbox/benchmark/v1",
    "cycle": cycle_labels.get(arm, arm),
    "arm": arm,
    "startedAt": os.environ["CYCLE_STAMP"],
    "git": git,
    "versions": vers,
    "resources": resources,
    "postgres": merge_pg(scens),
    "scenarios": scens,
    "totals": {
        "scenarios": len(scens),
        "verdictsPass": sum(1 for s in scens if s.get("verdict", {}).get("verdict") == "pass"),
        "messagesDelivered": sum(s.get("cdc", {}).get("deliveredDelta", 0) for s in scens),
        "duplicatesTotal": sum(s.get("verdict", {}).get("duplicates") or 0 for s in scens),
        "readErrors": sum(s.get("cdc", {}).get("readErrors", 0) for s in scens),
        "kafkaErrors": sum(s.get("cdc", {}).get("kafkaErrors", 0) for s in scens),
    },
}
json.dump(report, open(os.environ["REPORT"], "w"), ensure_ascii=False, indent=2, sort_keys=True)
json.dump(report, open(os.environ["SUMMARY"], "w"), ensure_ascii=False, indent=2, sort_keys=True)
print(f"  отчёт цикла: {os.environ['REPORT']}")
print(f"  компактная копия (коммитится): {os.environ['SUMMARY']}")
t = report["totals"]
print(f"  сценариев {t['scenarios']}, pass {t['verdictsPass']}/{t['scenarios']}, "
      f"доставлено {t['messagesDelivered']}, дубликатов {t['duplicatesTotal']}, "
      f"readErrors {t['readErrors']}, kafkaErrors {t['kafkaErrors']}")
pgm = report.get("postgres") or {}
if pgm.get("statements"):
    lead = (resources.get("pgLeader") or {})
    print(f"  postgres: leader {lead.get('node','?')} cpu avg {lead.get('cpuAvgPercent','?')}%"
          f" | standbys cpu avg {(resources.get('pgStandbys') or {}).get('cpuAvgPercent','?')}%"
          f" | wal {round(pgm.get('wal', {}).get('d_wal_bytes', 0) / 1e6, 1)}МБ"
          f" | xact {round(pgm.get('db', {}).get('d_xact_commit', 0))}")
    for s in pgm["statements"][:5]:
        print(f"    {s.get('role','?'):>14}  mean {s.get('mean_exec_ms')}мс  calls {int(s.get('d_calls', 0))}"
              f"  wal {round(s.get('d_wal_bytes', 0) / 1e6, 2)}МБ  blks r/w "
              f"{int(s.get('d_shared_blks_read', 0))}/{int(s.get('d_shared_blks_dirtied', 0))}"
              f"  | {s['query'][:100]}")
PY
}

# ---------- Один цикл (§11) ----------
run_cycle() { # arm
  ARM=$1
  case "$ARM" in
    polling)   CDC_PORT=$PPORT;   ARM_LABEL="A" ;;
    streaming) CDC_PORT=$SPORT;   ARM_LABEL="B" ;;
    competing) CDC_PORT=$CPORT;   ARM_LABEL="C" ;;
    walv6)     CDC_PORT=$WV6PORT; ARM_LABEL="D" ;;
    *)         CDC_PORT=$SPORT;   ARM_LABEL="?" ;;
  esac
  CYCLE_STAMP=$(date +%Y-%m-%dT%H:%M:%S)
  CYCLE_DIR="results/raw/$(date +%Y%m%d-%H%M%S)-benchmark-$ARM"

  section "ЦИКЛ $ARM_LABEL (arm=$ARM): чистая инфраструктура (§11)"
  if [[ "$SKIP_WIPE" == "0" ]]; then
    ./scripts/reset/wipe.sh || { bad "wipe не удался"; return 1; }
  else
    ok "начальный wipe пропущен (--skip-wipe): стенд должен быть чистым и поднятым под arm=$ARM"
  fi
  mkdir -p "$CYCLE_DIR"
  $DC --profile "$ARM" up -d --build > "$CYCLE_DIR/compose-up.log" 2>&1 \
    && ok "docker compose --profile $ARM up -d --build (лог: $CYCLE_DIR/compose-up.log)" \
    || { bad "compose up не удался — см. $CYCLE_DIR/compose-up.log"; return 1; }
  wait_stand_ready || { bad "стенд не готов, цикл прерван"; return 1; }

  section "Warmup (не входит в сценарии: JIT, пулы, прогрев валидатора)"
  local wrun wu
  wrun=$(run_writer "$(spec_for_arm "$WARMUP_SPEC")")
  if [[ "$wrun" != FAIL:* ]]; then
    writer_results
    wu=$(wait_run_unique_long "$wrun" "$(writer_field 'd["writtenMessages"]')" 300)
    validate_run "$wrun" true >/dev/null
    [[ "$(vd 'd["verdict"]')" == "pass" && "$wu" == "$(writer_field 'd["writtenMessages"]')" ]] \
      && ok "warmup: 500 сообщений доставлены, вердикт pass" \
      || bad "warmup не прошёл (вердикт $(vd 'd["verdict"]'), unique=$wu)"
  else
    bad "warmup-прогон не completed ($wrun)"
  fi

  # сэмплер docker stats на весь блок сценариев (срез 5с; cAdvisor не отдаёт per-container)
  DOCKER_STATS_CSV="$CYCLE_DIR/docker-stats.csv"
  ./scripts/reports/sample-docker-stats.sh "$DOCKER_STATS_CSV" 5 >/dev/null 2>&1 &
  local stats_pid=$!
  sleep 3

  local idx
  for idx in "${!SCEN_NAMES[@]}"; do
    scenario_enabled "${SCEN_NAMES[$idx]%%-*}" || continue
    run_scenario "$idx" || bad "сценарий ${SCEN_NAMES[$idx]} прерван"
  done

  kill "$stats_pid" 2>/dev/null
  wait "$stats_pid" 2>/dev/null

  section "Отчёт цикла (arm=$ARM)"
  cycle_report
}

# ---------- Сводка по всем arm (файлы benchmark-<arm>.json, какие есть) ----------
# Обобщение прежней пары A/B на N arm: polling/streaming/competing. Пишется при
# наличии минимум двух arm-файлов (как раньше — только после --cycle all).
arms_summary() {
  python3 <<'PY'
import json, os
known = ("polling", "competing", "streaming", "walv6")
arms = {}
for arm in known:
    path = f"results/benchmark/benchmark-{arm}.json"
    if os.path.isfile(path):
        arms[arm] = json.load(open(path))
if len(arms) < 2:
    raise SystemExit
def pick(s, *path):
    v = s
    for k in path:
        v = (v or {}).get(k)
    return v
names = []
for data in arms.values():
    for s in data.get("scenarios", []):
        if s["name"] not in names:
            names.append(s["name"])
rows = []
for name in names:
    row = {"scenario": name}
    for arm, data in arms.items():
        s = next((x for x in data.get("scenarios", []) if x["name"] == name), None)
        if not s:
            continue
        row[arm] = {
            "deliveryTotalMs": pick(s, "timings", "deliveryTotalMs"),
            "drainMs": pick(s, "timings", "drainMs"),
            "cdcRateOverall": pick(s, "cdc", "deliveryRateOverallPerSecond"),
            "cdcRateAvg": pick(s, "cdc", "deliveryRateAvgPerSecond"),
            "cdcRatePeak": pick(s, "cdc", "deliveryRatePeakPerSecond"),
            "e2eMeanMs": pick(s, "cdc", "e2eMeanMs"),
            "e2eP95Ms": pick(s, "cdc", "e2eP95Ms"),
            "duplicates": pick(s, "verdict", "duplicates"),
            "pgLeaderCpuAvgPercent": pick(s, "resources", "pgLeader", "cpuAvgPercent"),
            "pgStandbysCpuAvgPercent": pick(s, "resources", "pgStandbys", "cpuAvgPercent"),
            "pgWalMb": round((pick(s, "postgres", "wal", "d_wal_bytes") or 0) / 1e6, 1),
            "pgXactCommit": pick(s, "postgres", "db", "d_xact_commit"),
            "drainDeleteMeanMs": next(
                (st.get("mean_exec_ms") for st in (pick(s, "postgres", "statements") or [])
                 if st.get("role") == "drain-delete"), None),
            "pollingPeekMeanMs": next(
                (st.get("mean_exec_ms") for st in (pick(s, "postgres", "statements") or [])
                 if st.get("role") == "polling-peek"), None),
        }
    rows.append(row)
out = {"schema": "streaming-polling-outbox/benchmark-summary/v1", "arms": list(arms), "scenarios": rows}
json.dump(out, open("results/benchmark/summary.json", "w"), ensure_ascii=False, indent=2, sort_keys=True)
print("  сводка arm'ов (%s): results/benchmark/summary.json" % ", ".join(arms))
for r in rows:
    parts = []
    for arm in arms:
        m = r.get(arm) or {}
        parts.append("%s: drain %s / rate avg %s / peak %s msg/s / e2e p95 %s ms" % (
            arm, str(m.get("drainMs")), str(m.get("cdcRateAvg")),
            str(m.get("cdcRatePeak")), str(m.get("e2eP95Ms"))))
    print("  %-16s %s" % (r["scenario"], " | ".join(parts)))
    pgparts = []
    for arm in arms:
        m = r.get(arm) or {}
        pgparts.append("%s: pg leader cpu %s%% / wal %sМБ / xact %s" % (
            arm, str(m.get("pgLeaderCpuAvgPercent")), str(m.get("pgWalMb")),
            str(m.get("pgXactCommit") and round(m.get("pgXactCommit")))))
    print("  %-16s %s" % ("", " | ".join(pgparts)))
PY
}

# ---------- Запуск ----------
if [[ "$CYCLE_ARG" == "all" || "$CYCLE_ARG" == "polling" ]]; then
  run_cycle polling
  # §11 шаг 7 цикла A: полное удаление тестового состояния перед циклом B / после одиночного A
  section "Цикл A завершён: полное удаление тестового состояния (§11 шаг 7)"
  ./scripts/reset/wipe.sh && ok "стенд вытерт после цикла A"
  [[ "$CYCLE_ARG" == "polling" ]] \
    && echo "Стенд остановлен (§11). Поднять заново: docker compose --profile streaming up -d --build"
fi
if [[ "$CYCLE_ARG" == "all" || "$CYCLE_ARG" == "streaming" ]]; then
  [[ "$CYCLE_ARG" == "all" ]] && SKIP_WIPE=0   # только что вытерли — полный путь цикла B
  run_cycle streaming
fi
if [[ "$CYCLE_ARG" == "all" || "$CYCLE_ARG" == "competing" ]]; then
  [[ "$CYCLE_ARG" == "all" ]] && SKIP_WIPE=0   # полный путь цикла C: чистая инфраструктура
  run_cycle competing
fi
if [[ "$CYCLE_ARG" == "all" || "$CYCLE_ARG" == "walv6" ]]; then
  [[ "$CYCLE_ARG" == "all" ]] && SKIP_WIPE=0
  run_cycle walv6
fi

[[ "$CYCLE_ARG" == "all" ]] && { section "Сводка всех arm"; arms_summary; }

finish
