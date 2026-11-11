-- Роль и гранты arm competing (cdc-competing): drain outbox-таблицы конкурирующими
-- потребителями (третий подход бенчмарка, решение владельца).
-- Идемпотентно; выполняется от имени postgres на primary (через HAProxy),
-- подключённым к БД outbox (роль/гранты — кластерные и табличные, но CONNECT
-- выдаётся в прикладной БД).
--
-- ОТКЛОНЕНИЕ ОТ ТЗ: подход сознательно нарушает правило «SELECT-polling таблицы
-- запрещён» (01-db-publication.sql) — это и есть предмет сравнения бенчмарка:
-- Writer в режиме writeMode=insertOnly оставляет строки в таблице, а N воркеров
-- cdc-competing разбирают их запросом
--   DELETE … WHERE id IN (SELECT id … ORDER BY id FOR UPDATE SKIP LOCKED LIMIT n)
--     RETURNING id, topic, body, created_at
-- внутри транзакции поверх публикации в Kafka (COMMIT только после delivery —
-- at-least-once). Доступ к таблице выдан ТОЛЬКО этой роли и только под этот arm:
-- sys_app (polling) и sys_replication (streaming) таблицу по-прежнему не видят.

-- sys_drainer: только drain outbox-таблицы (SELECT 4 колонок + DELETE);
-- прав репликации и записи нет.
SELECT format('CREATE ROLE sys_drainer LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOINHERIT PASSWORD %L',
              :'sys_drainer_password')
WHERE NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'sys_drainer') \gexec

ALTER ROLE sys_drainer LOGIN NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION NOINHERIT PASSWORD :'sys_drainer_password';

\connect outbox

-- CONNECT на прикладную БД (слот этой роли не нужен — arm работает с таблицей).
GRANT CONNECT ON DATABASE outbox TO sys_drainer;

-- USAGE на схему public: без него имя таблицы в запросе не разрешится.
GRANT USAGE ON SCHEMA public TO sys_drainer;

-- DELETE требует SELECT на колонки, читаемые в условии (id в подзапросе) и на
-- возвращаемые RETURNING колонки — выдаём колоночный SELECT ровно на 4 колонки,
-- полнотабличный SELECT не даём (по образцу колоночного SELECT(id) у sys_writer).
-- FOR UPDATE подзапроса (SKIP LOCKED) требует UPDATE хотя бы на одну колонку —
-- блокировка строки считается правом записи; колоночного UPDATE(id) достаточно,
-- менять данные drain'еру незачем.
GRANT SELECT (id, topic, body, created_at), UPDATE (id), DELETE ON public.outbox_messages TO sys_drainer;
