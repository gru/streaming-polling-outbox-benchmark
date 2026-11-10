using System.Text;

namespace CdcCommon.PgOutput;

/// <summary>
/// Разбор бинарных записей pgoutput (proto_version=1) из peek-выборки.
///
/// Формат (PostgreSQL, Logical Replication Message Format): первый байт — тип
/// записи; целые — big-endian; значения колонок — текстовое представление типа
/// ('t' + int32 длина + байты; 'n' = NULL; 'u' = неизменённый TOAST).
/// Пользовательское событие = Insert-запись на public.outbox_messages; Delete
/// игнорируется (Writer делает INSERT+DELETE в одной транзакции, таблица пустая);
/// B/R/C — служебные (R кэшируется в пределах батча: каждая peek-сессия
/// декодирования объявляет relations заново).
///
/// Маппинг колонок и валидация значений строки — общее ядро OutboxRowParser
/// (на Relation строится OutboxColumnPlan, переиспользуемый для всех Insert
/// батча); здесь — только чтение wire-формата и группировка по транзакциям.
///
/// События публикуются только для транзакций, чей Commit присутствует в батче:
/// peek возвращает выборку, выровненную по границам транзакций (upto_nchanges
/// проверяется после декодирования очередной транзакции), но парсер не
/// полагается на это и не отдаёт «хвосты» без подтверждения.
/// </summary>
public static class PgOutputParser
{
    public static PgOutputBatch Parse(IReadOnlyList<RawSlotRecord> records)
    {
        var relations = new Dictionary<int, PgOutputRelation>();
        var plans = new Dictionary<int, OutboxColumnPlan>();
        var pendingByXid = new Dictionary<uint, List<PendingEvent>>();
        var events = new List<OutboxEvent>();
        var counts = new SortedDictionary<char, long>();
        Lsn? lastCommitLsn = null;
        Lsn? lastReceivedLsn = null;

        foreach (var rec in records)
        {
            if (lastReceivedLsn is null || rec.Lsn > lastReceivedLsn)
                lastReceivedLsn = rec.Lsn;

            if (rec.Data.Length == 0)
            {
                counts['?'] = counts.GetValueOrDefault('?') + 1;
                continue;
            }

            var kind = (char)rec.Data[0];
            counts[kind] = counts.GetValueOrDefault(kind) + 1;
            var reader = new PgOutputReader(rec.Data.AsSpan(1));

            switch (kind)
            {
                case 'B': // Begin: final_lsn, commit_ts, xid — структуру проверяем, содержимое не нужно
                    reader.ReadInt64();
                    reader.ReadInt64();
                    reader.ReadInt32();
                    break;

                case 'R':
                {
                    var relation = ParseRelation(ref reader);
                    relations[relation.RelationId] = relation;
                    if (relation.Is(OutboxRowParser.OutboxSchema, OutboxRowParser.OutboxTable))
                        plans[relation.RelationId] = OutboxRowParser.BuildColumnPlan(ColumnNames(relation));
                    break;
                }

                case 'I':
                    ParseInsert(ref reader, rec, relations, plans, pendingByXid);
                    break;

                case 'D': // Delete: relation_id, 'K'/'O' (ключ/старый кортеж), TupleData —
                          // Writer удаляет те же строки; событие НЕ порождается
                    reader.ReadInt32();
                    reader.ReadByte();
                    reader.SkipTupleData();
                    break;

                case 'C': // Commit: commit_lsn, end_lsn, commit_ts; события транзакции получают строчный LSN commit-записи
                {
                    reader.ReadInt64();
                    reader.ReadInt64();
                    reader.ReadInt64();
                    lastCommitLsn = rec.Lsn;
                    if (pendingByXid.Remove(rec.Xid, out var committed))
                    {
                        foreach (var p in committed)
                            events.Add(new OutboxEvent(p.Id, p.Topic, p.BodyJson, p.CreatedAtUtc, rec.Lsn, rec.Xid));
                    }
                    break;
                }

                // Update/Type/Origin/Truncate/Message не возникают в outbox_publication
                // (у Writer только INSERT/DELETE); неизвестное содержимое пропускаем
                // с подсчётом — запись самодостаточна, это не ошибка.
                case 'U':
                case 'Y':
                case 'O':
                case 'T':
                case 'M':
                    break;

                default:
                    break;
            }
        }

        var incomplete = 0;
        foreach (var list in pendingByXid.Values)
            incomplete += list.Count;

        return new PgOutputBatch
        {
            Events = events,
            LastCommitLsn = lastCommitLsn,
            LastReceivedLsn = lastReceivedLsn,
            RecordCounts = counts,
            IncompleteTransactionEventCount = incomplete,
        };
    }

    private static PgOutputRelation ParseRelation(ref PgOutputReader reader)
    {
        var relationId = reader.ReadInt32();
        var schema = reader.ReadCString();
        var name = reader.ReadCString();
        var replicaIdentity = reader.ReadByte(); // 'd'/'i'/'n'/'f' — для парсинга не нужен, но присутствует в записи
        var columns = new List<PgOutputColumn>();
        var ncols = reader.ReadInt16();
        for (var i = 0; i < ncols; i++)
        {
            var flags = reader.ReadByte();
            var columnName = reader.ReadCString();
            var typeOid = unchecked((uint)reader.ReadInt32());
            var typeModifier = reader.ReadInt32();
            columns.Add(new PgOutputColumn(flags, columnName, typeOid, typeModifier));
        }
        return new PgOutputRelation(relationId, schema, name, columns);
    }

    private static void ParseInsert(ref PgOutputReader reader, RawSlotRecord rec,
        Dictionary<int, PgOutputRelation> relations, Dictionary<int, OutboxColumnPlan> plans,
        Dictionary<uint, List<PendingEvent>> pendingByXid)
    {
        var relationId = reader.ReadInt32();
        var tupleKind = reader.ReadByte();
        if (tupleKind != (byte)'N')
            throw new PgOutputParseException(
                $"Insert с неожидаемым видом кортежа '{(char)tupleKind}' (ожидался 'N' — новая строка), xid={rec.Xid}");

        if (!relations.TryGetValue(relationId, out var relation))
            throw new PgOutputParseException(
                $"Insert для relation_id={relationId} без предшествующей Relation-записи в батче (xid={rec.Xid})");

        var ncols = reader.ReadInt16();
        var values = new string?[ncols];
        for (var i = 0; i < ncols; i++)
        {
            var valueKind = reader.ReadByte();
            switch (valueKind)
            {
                case (byte)'t':
                    var len = reader.ReadInt32();
                    if (len < 0)
                        throw new PgOutputParseException($"отрицательная длина значения колонки ({len})");
                    values[i] = Encoding.UTF8.GetString(reader.ReadBytes(len));
                    break;
                case (byte)'n':
                case (byte)'u':
                    values[i] = null;
                    break;
                default:
                    throw new PgOutputParseException($"неизвестный вид значения колонки '{(char)valueKind}'");
            }
        }

        if (!relation.Is(OutboxRowParser.OutboxSchema, OutboxRowParser.OutboxTable))
            return; // публикация содержит только outbox_messages; чужие записи просто не считаем событиями

        if (!plans.TryGetValue(relationId, out var plan))
            throw new PgOutputParseException(
                $"Insert для relation_id={relationId} без плана колонок (Relation-запись без обязательных колонок?; xid={rec.Xid})");

        // Слот-диспатч по плану, построенному один раз на Relation (pgoutput
        // присылает значения позиционно, в порядке колонок Relation-записи)
        string? idText = null;
        string? topic = null;
        string? bodyJson = null;
        string? createdAtText = null;
        for (var i = 0; i < ncols && i < plan.Slots.Length; i++)
        {
            switch (plan.Slots[i])
            {
                case OutboxColumnSlot.Id: idText = values[i]; break;
                case OutboxColumnSlot.Topic: topic = values[i]; break;
                case OutboxColumnSlot.Body: bodyJson = values[i]; break;
                case OutboxColumnSlot.CreatedAt: createdAtText = values[i]; break;
            }
        }

        var created = OutboxRowParser.Parse(idText, topic, bodyJson, createdAtText, rec.Xid);
        if (!pendingByXid.TryGetValue(rec.Xid, out var list))
            pendingByXid[rec.Xid] = list = new List<PendingEvent>();
        list.Add(new PendingEvent(created.Id, created.Topic, created.BodyJson, created.CreatedAtUtc));
    }

    private static string[] ColumnNames(PgOutputRelation relation)
    {
        var names = new string[relation.Columns.Count];
        for (var i = 0; i < relation.Columns.Count; i++)
            names[i] = relation.Columns[i].Name;
        return names;
    }

    private sealed record PendingEvent(Guid Id, string Topic, string BodyJson, DateTimeOffset CreatedAtUtc);
}
