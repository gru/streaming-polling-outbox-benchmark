# QUICKSTART: стенд и его запуск

Короткая инструкция. Подробности сравнения arm и методика — в [README.md](README.md).

## Состав стенда: что для чего

| Компонент | Роль |
|---|---|
| **etcd ×3** | DCS: кворум для выборов лидера Patroni |
| **pg1–pg3** (Patroni + PostgreSQL 17) | HA-кластер с синхронной репликацией. БД `outbox`: таблица `public.outbox_messages` в publication `outbox_publication`, failover-слот `outbox_slot` |
| **HAProxy** `:5432` | Единая точка входа к текущему primary |
| **PgBouncer** `:6432` | Пул соединений Writer'а (transaction pooling); replication-трафик идёт мимо него, напрямую через HAProxy |
| **kafka1–3** (KRaft) | Брокеры; топик `outbox.events` |
| **writer** `:18080` | Генератор нагрузки: INSERT+DELETE в outbox-таблицу одной транзакцией — таблица всегда пустая, сообщение живёт в WAL |
| **cdc-polling** (profile `polling`) `:18081` | Реализация A: чтение слота SQL-функциями `pg_logical_slot_peek_binary_changes` + `pg_replication_slot_advance` |
| **cdc-streaming** (profile `streaming`) `:18082` | Реализация B: чтение слота протоколом логической репликации (Npgsql) |
| **kafka-validator** `:18083` | Независимый консюмер: вердикт доставки (0 потерь / 0 дубликатов), фиксирует момент полного дрейна прогона |
| **prometheus** `:9090` | Метрики участников; источник e2e-гистограмм для benchmark |
| **init-db, kafka-init** | One-shot инициализаторы (роли, БД, publication, слот, peek/advance-функции; создание топика) |

Две реализации CDC никогда не запускаются одновременно — они читают один слот (для этого и нужны compose-профили).

## Запуск

Нужен только Docker (сборка .NET идёт внутри multi-stage образов, локальный SDK не требуется). По пиковым прогонам контейнеры суммарно занимают ~5 ГБ RAM — выделяйте Docker VM память с запасом.

```bash
# 1. Конфиг с паролями (обязателен, compose без него не стартует)
cp .env.example .env   # и заполнить значения

# 2. Полный benchmark-цикл: A (polling) → wipe → B (streaming)
#    Сам пересоздаёт стенд под каждую реализацию, гонит warmup + 5 сценариев, собирает отчёты
scripts/test/benchmark.sh --cycle all
# Одна реализация: --cycle polling | --cycle streaming

# 3. Результаты
#   results/benchmark/summary.json            — сводка A/B (главный файл)
#   results/benchmark/benchmark-<arm>.json    — компактные отчёты по реализациям
#   results/raw/<stamp>-benchmark-<arm>/      — полные артефакты (gitignored)
```

Стенд вручную (если нужен без benchmark'а):

```bash
# Поднять под реализацию polling (для streaming — соответственно --profile streaming)
docker compose --profile polling up -d --build

# Переключить реализацию на живом стенде (обе сразу нельзя — один слот)
docker compose stop cdc-polling
docker compose --profile streaming up -d --no-deps --build cdc-streaming
# (--no-deps обязателен: без него compose может пересоздать pg-контейнеры)

# Полный сброс (контейнеры, сети, volumes)
scripts/reset/wipe.sh
```
