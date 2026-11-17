-- Безтабличный путь записи для arm walv6 (CdcWalV6):
-- пакетная запись SuperFrame одной бинарной записью в WAL через
-- pg_logical_emit_message(true, 'outbox.events', p_frame) без таблицы,
-- без B-Tree индексов, без DELETE и без мёртвых строк / TOAST.
-- Чтение выполняется штатным плагином pgoutput (proto_version=1, messages=true) на PG 17.

\connect outbox

CREATE OR REPLACE FUNCTION public.emit_outbox_frame(p_frame bytea)
RETURNS pg_lsn
LANGUAGE sql
VOLATILE
PARALLEL UNSAFE
SECURITY DEFINER
SET search_path = pg_catalog
AS $$
  SELECT pg_catalog.pg_logical_emit_message(true, 'outbox.events', p_frame);
$$;

ALTER FUNCTION public.emit_outbox_frame(bytea) OWNER TO postgres;
REVOKE ALL ON FUNCTION public.emit_outbox_frame(bytea) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION public.emit_outbox_frame(bytea) TO sys_writer;
