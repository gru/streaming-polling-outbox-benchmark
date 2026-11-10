namespace CdcCommon.PgOutput;

/// <summary>
/// План колонок Relation-записи public.outbox_messages: слот для каждой позиции
/// колонки. Строится один раз на Relation (OutboxRowParser.BuildColumnPlan) и
/// переиспользуется для всех Insert-строк этого отношения.
/// </summary>
/// <remarks>
/// В отличие от streaming-outbox, OID-контракт (TypeIds) не хранится: оба пути
/// бенчмарка работают на pgoutput proto_version=1, где значения колонок всегда
/// приходят текстом — бинарного разбора значений нет.
/// </remarks>
public sealed record OutboxColumnPlan(OutboxColumnSlot[] Slots);
