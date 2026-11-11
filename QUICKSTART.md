# QUICKSTART: стенд и его запуск

Короткая инструкция. Подробности сравнения arm и методика — в [README.md](README.md).

## Состав стенда: что для чего

| Компонент | Роль |
|---|---|
| **etcd ×3** | DCS: кворум для выборов лидера Patroni |
| **pg1–pg3** (Patroni + PostgreSQL 17) | HA-кластер с синхронной репликацией. БД `outbox`: таблица `public.outbox_messages` в publication `outbox_publication`, failover-слот `outbox_slot` |
| **HAProxy** `:5432` | Единая точка входа к текущему primary |
| **PgBouncer** `:6432` | Пул соединений Writer'а (transaction pooling); replication-трафик идёт мимо него, напрямую через HAProxy |
| **kafka1** (KRaft, 1 узел) | Топик `outbox.events`. Один broker+controller без реплик: Kafka — транспорт, а не объект измерения (кластер из 3 брокеров на одной машине душил запись раньше, чем упиралось чтение outbox) |
| **writer** `:18080` | Генератор нагрузки. Режим `writeMode=insertDelete` (дефолт): INSERT+DELETE одной транзакцией — таблица всегда пустая, сообщение живёт в WAL. Режим `writeMode=insertOnly` (для arm competing): строки копятся в таблице, чистит их читатель |
| **cdc-polling** (profile `polling`) `:18081` | Реализация A: чтение слота SQL-функциями `pg_logical_slot_peek_binary_changes` + `pg_replication_slot_advance` |
| **cdc-streaming** (profile `streaming`) `:18082` | Реализация B: чтение слота протоколом логической репликации (Npgsql) |
| **cdc-competing** (profile `competing`) `:18084` | Реализация C: таблица-очередь конкурирующими потребителями — N воркеров (по умолчанию 5×2000) разбирают таблицу `DELETE … FOR UPDATE SKIP LOCKED … RETURNING` в транзакции поверх публикации в Kafka (at-least-once); роль `sys_drainer`, слот не использует |
| **kafka-validator** `:18083` | Независимый консюмер: вердикт доставки (0 потерь / 0 дубликатов), фиксирует момент полного дрейна прогона |
| **prometheus** `:9090` | Метрики участников; источник e2e-гистограмм для benchmark |
| **init-db, kafka-init** | One-shot инициализаторы (роли, БД, publication, слот, peek/advance-функции; создание топика) |

Реализации CDC никогда не запускаются одновременно (compose-профили): polling/streaming делят один слот, competing меняет сам путь записи (Writer в `insertOnly` оставляет строки в таблице).

## Запуск

Нужен только Docker (сборка .NET идёт внутри multi-stage образов, локальный SDK не требуется). По пиковым прогонам контейнеры суммарно занимают ~5 ГБ RAM — выделяйте Docker VM память с запасом.

```bash
# 1. Конфиг с паролями (обязателен, compose без него не стартует)
cp .env.example .env   # и заполнить значения

# 2. Полный benchmark-цикл: A (polling) → wipe → B (streaming) → wipe → C (competing)
#    Сам пересоздаёт стенд под каждую реализацию, гонит warmup + 5 сценариев, собирает отчёты
scripts/test/benchmark.sh --cycle all
# Одна реализация: --cycle polling | --cycle streaming | --cycle competing

# 3. Результаты
#   results/benchmark/summary.json            — сводка всех arm (главный файл)
#   results/benchmark/benchmark-<arm>.json    — компактные отчёты по реализациям
#   results/raw/<stamp>-benchmark-<arm>/      — полные артефакты (gitignored)
```

Стенд вручную (если нужен без benchmark'а):

```bash
# Поднять под реализацию polling (для streaming/competing — соответственно --profile streaming|competing)
docker compose --profile polling up -d --build

# Переключить реализацию на живом стенде (обе сразу нельзя — один слот)
docker compose stop cdc-polling
docker compose --profile streaming up -d --no-deps --build cdc-streaming
# (--no-deps обязателен: без него compose может пересоздать pg-контейнеры)

# Полный сброс (контейнеры, сети, volumes)
scripts/reset/wipe.sh
```
