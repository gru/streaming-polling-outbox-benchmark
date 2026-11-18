using System.Buffers;
using System.Buffers.Binary;
using System.Buffers.Text;
using System.Text;
using CdcCommon;
using CdcCommon.PgOutput;
using Npgsql.Replication.PgOutput;
using Npgsql.Replication.PgOutput.Messages;

namespace CdcWalV6.Replication;

/// <summary>
/// Подготовленная запись для отправки в Kafka без промежуточных объектов кучи:
/// Key — реальный 36-байтный UTF-8 UUID (честное партиционирование по ключу, идентично остальным вариантам),
/// Value — готовый UTF-8 JSON конверт {"id","timestamp","topic","body"}.
/// </summary>
public readonly record struct PreparedRecord(byte[] Key, byte[] Value, long CreatedAtUtcTicks);

public readonly record struct FlushBatch(PreparedRecord[]? RentedEvents, int Count, Lsn TransactionEndLsn, uint Xid);

public readonly record struct DecodedFrame(char Kind, FlushBatch? Completed);

/// <summary>
/// Zero-alloc декодер pgoutput (proto v1, binary=true, messages=true) для безтабличного варианта V6:
/// - основной путь: читает транзакционные LogicalDecodingMessage (prefix='outbox.events')
///   и распаковывает бинарный SuperFrame [0x42][int32 count][int32 len_i][utf8_envelope_i]...
///   без парсинга/пересборки JSON (только срез готовых UTF-8 байт конверта и 36-байтного UUID-ключа);
/// - запасной совместимый путь: поддерживает Relation + Insert из public.outbox_messages (для канареек).
/// </summary>
public sealed class WalSuperFrameDecoder
{
    private const long PostgresEpochTicks = 630_822_816_000_000_000L; // 2000-01-01T00:00:00Z

    private static ReadOnlySpan<byte> AllowedTopicUtf8 => "outbox.events"u8;
    private static ReadOnlySpan<byte> PrefixIdBytes => "{\"id\":\""u8;
    private static ReadOnlySpan<byte> MidTopicBodyBytes => "\",\"topic\":\"outbox.events\",\"body\":"u8;
    private static ReadOnlySpan<byte> MidTimestampBytes => ",\"timestamp\":\""u8;
    private static ReadOnlySpan<byte> SuffixBytes => "\"}"u8;

    private PreparedRecord[] _txBuffer = new PreparedRecord[512];
    private int _txCount;
    private byte[] _scratch = new byte[65536];
    private uint _currentXid;
    private OutboxColumnPlan? _columnPlan;

    public int IncompleteEventCount => _txCount;

    public async ValueTask<DecodedFrame> ProcessAsync(PgOutputReplicationMessage message, CancellationToken ct)
    {
        switch (message)
        {
            case BeginMessage begin:
                _currentXid = begin.TransactionXid;
                ClearTxBuffer();
                return new DecodedFrame('B', null);

            case LogicalDecodingMessage logicalMsg:
                await OnLogicalMessageAsync(logicalMsg, ct).ConfigureAwait(false);
                return new DecodedFrame('M', null);

            case RelationMessage relation:
                OnRelation(relation);
                return new DecodedFrame('R', null);

            case InsertMessage insert:
                await OnInsertAsync(insert, ct).ConfigureAwait(false);
                return new DecodedFrame('I', null);

            case CommitMessage commit:
                return new DecodedFrame('C', CompleteTransaction(commit));

            case UpdateMessage:
                return new DecodedFrame('U', null);
            case DeleteMessage:
                return new DecodedFrame('D', null);
            case TruncateMessage:
                return new DecodedFrame('T', null);
            case TypeMessage:
                return new DecodedFrame('Y', null);
            case OriginMessage:
                return new DecodedFrame('O', null);
            default:
                return new DecodedFrame('?', null);
        }
    }

    private async ValueTask OnLogicalMessageAsync(LogicalDecodingMessage msg, CancellationToken ct)
    {
        var total = 0;
        while (true)
        {
            if (total == _scratch.Length)
                EnsureScratchCapacity(_scratch.Length * 2);

            var read = await msg.Data.ReadAsync(_scratch.AsMemory(total), ct).ConfigureAwait(false);
            if (read == 0)
                break;
            total += read;
        }

        if (!string.Equals(msg.Prefix, OutboxRowParser.AllowedTopic, StringComparison.Ordinal))
            return;

        UnpackSuperFrameToTxBuffer(_scratch.AsSpan(0, total), _currentXid);
    }

    public void UnpackSuperFrameToTxBuffer(ReadOnlySpan<byte> frame, uint xid)
    {
        if (frame.Length < 5 || frame[0] != 0x42)
        {
            throw new PgOutputParseException(
                $"Некорректный заголовок SuperFrame в LogicalDecodingMessage (len={frame.Length}, xid={xid})");
        }

        var count = BinaryPrimitives.ReadInt32LittleEndian(frame.Slice(1, 4));
        if (count < 0)
            throw new PgOutputParseException($"Отрицательное число записей SuperFrame ({count}, xid={xid})");
        if (count == 0)
            return;

        EnsureTxCapacity(_txCount + count);

        var pos = 5;
        for (var i = 0; i < count; i++)
        {
            if (pos + 4 > frame.Length)
                throw new PgOutputParseException($"Обрезан размер конверта #{i} в SuperFrame (xid={xid})");

            var envLen = BinaryPrimitives.ReadInt32LittleEndian(frame.Slice(pos, 4));
            pos += 4;
            if (envLen < 84 || pos + envLen > frame.Length)
                throw new PgOutputParseException($"Некорректная длина конверта #{i} ({envLen}, xid={xid})");

            var slice = frame.Slice(pos, envLen);
            pos += envLen;

            // Формат конверта WalSuperFrameMessageFactory:
            // [0..6]:   {"id":"          (7 байт)
            // [7..42]:  <UUID>           (36 байт)
            // [43..57]: ","timestamp":"  (15 байт)
            // [58..81]: <ISO-8601 UTC>   (24 байта: YYYY-MM-DDTHH:mm:ss.fffZ)
            var key = slice.Slice(7, 36).ToArray();
            var utcTicks = ParseIso8601UtcTicks(slice.Slice(58, 24), xid);
            var env = slice.ToArray();

            _txBuffer[_txCount++] = new PreparedRecord(key, env, utcTicks);
        }

        if (pos != frame.Length)
        {
            throw new PgOutputParseException(
                $"Лишние байты в хвосте SuperFrame ({frame.Length - pos} Б, xid={xid})");
        }
    }

    public static long ParseIso8601UtcTicks(ReadOnlySpan<byte> ts24, uint xid)
    {
        if (ts24.Length != 24 ||
            ts24[4] != (byte)'-' || ts24[7] != (byte)'-' || ts24[10] != (byte)'T' ||
            ts24[13] != (byte)':' || ts24[16] != (byte)':' || ts24[19] != (byte)'.' || ts24[23] != (byte)'Z')
        {
            throw new PgOutputParseException($"Некорректный ISO-8601 UTC штамп времени в SuperFrame (xid={xid})");
        }

        var year = Read4Digits(ts24.Slice(0, 4), xid);
        var month = Read2Digits(ts24.Slice(5, 2), xid);
        var day = Read2Digits(ts24.Slice(8, 2), xid);
        var hour = Read2Digits(ts24.Slice(11, 2), xid);
        var minute = Read2Digits(ts24.Slice(14, 2), xid);
        var second = Read2Digits(ts24.Slice(17, 2), xid);
        var ms = Read3Digits(ts24.Slice(20, 3), xid);

        return new DateTime(year, month, day, hour, minute, second, ms, DateTimeKind.Utc).Ticks;
    }

    private void OnRelation(RelationMessage relation)
    {
        if (!IsOutboxRelation(relation))
            return;
        var names = new string[relation.Columns.Count];
        for (var i = 0; i < relation.Columns.Count; i++)
            names[i] = relation.Columns[i].ColumnName;
        _columnPlan = OutboxRowParser.BuildColumnPlan(names);
    }

    private async ValueTask OnInsertAsync(InsertMessage insert, CancellationToken ct)
    {
        if (!IsOutboxRelation(insert.Relation))
            return;

        var plan = _columnPlan ?? throw new PgOutputParseException(
            $"Insert для {OutboxRowParser.OutboxSchema}.{OutboxRowParser.OutboxTable} без предшествующего Relation (xid={_currentXid})");

        int idOff = -1, idLen = 0;
        int topicOff = -1, topicLen = 0;
        int bodyOff = -1, bodyLen = 0;
        int tsOff = -1, tsLen = 0;
        bool idBin = false, topicBin = false, bodyBin = false, tsBin = false;
        var scratchPos = 0;

        var position = 0;
        await foreach (var value in insert.NewRow.WithCancellation(ct).ConfigureAwait(false))
        {
            if (position < plan.Slots.Length)
            {
                var slot = plan.Slots[position];
                if (slot != OutboxColumnSlot.Ignored)
                {
                    if (value.Kind is TupleDataKind.Null or TupleDataKind.UnchangedToastedValue)
                        throw new PgOutputParseException($"Колонка {slot} пуста ({value.Kind}) в Insert (xid={_currentXid})");

                    if (value.Kind == TupleDataKind.BinaryValue)
                    {
                        var len = value.Length;
                        EnsureScratchCapacity(scratchPos + len);
                        using var stream = value.GetStream();
                        await stream.ReadExactlyAsync(_scratch.AsMemory(scratchPos, len), ct).ConfigureAwait(false);
                        switch (slot)
                        {
                            case OutboxColumnSlot.Id: idOff = scratchPos; idLen = len; idBin = true; break;
                            case OutboxColumnSlot.Topic: topicOff = scratchPos; topicLen = len; topicBin = true; break;
                            case OutboxColumnSlot.Body: bodyOff = scratchPos; bodyLen = len; bodyBin = true; break;
                            case OutboxColumnSlot.CreatedAt: tsOff = scratchPos; tsLen = len; tsBin = true; break;
                        }
                        scratchPos += len;
                    }
                    else if (value.Kind == TupleDataKind.TextValue)
                    {
                        var text = await value.Get<string>(ct).ConfigureAwait(false);
                        var maxBytes = Encoding.UTF8.GetMaxByteCount(text.Length);
                        EnsureScratchCapacity(scratchPos + maxBytes);
                        var written = Encoding.UTF8.GetBytes(text, _scratch.AsSpan(scratchPos));
                        switch (slot)
                        {
                            case OutboxColumnSlot.Id: idOff = scratchPos; idLen = written; idBin = false; break;
                            case OutboxColumnSlot.Topic: topicOff = scratchPos; topicLen = written; topicBin = false; break;
                            case OutboxColumnSlot.Body: bodyOff = scratchPos; bodyLen = written; topicBin = false; break;
                            case OutboxColumnSlot.CreatedAt: tsOff = scratchPos; tsLen = written; tsBin = false; break;
                        }
                        scratchPos += written;
                    }
                }
            }
            position++;
        }

        if (idOff < 0 || topicOff < 0 || bodyOff < 0 || tsOff < 0)
            throw new PgOutputParseException($"Неполный набор колонок outbox в Insert (xid={_currentXid})");

        _ = topicBin;
        var record = BuildTableRowRecord(
            _scratch.AsSpan(idOff, idLen), idBin,
            _scratch.AsSpan(topicOff, topicLen),
            _scratch.AsSpan(bodyOff, bodyLen), bodyBin,
            _scratch.AsSpan(tsOff, tsLen), tsBin,
            _currentXid);

        EnsureTxCapacity(_txCount + 1);
        _txBuffer[_txCount++] = record;
    }

    private static PreparedRecord BuildTableRowRecord(
        ReadOnlySpan<byte> idRaw, bool idBinary,
        ReadOnlySpan<byte> topicRaw,
        ReadOnlySpan<byte> bodyRaw, bool bodyBinary,
        ReadOnlySpan<byte> tsRaw, bool tsBinary,
        uint xid)
    {
        var key = new byte[36];
        if (idBinary)
        {
            var guid = new Guid(idRaw, bigEndian: true);
            Utf8Formatter.TryFormat(guid, key, out _, 'D');
        }
        else
        {
            idRaw.CopyTo(key);
        }

        if (!topicRaw.SequenceEqual(AllowedTopicUtf8))
            throw new PgOutputParseException($"Запрещённый topic '{Encoding.UTF8.GetString(topicRaw)}' (xid={xid})");

        var bodyUtf8 = bodyBinary ? bodyRaw.Slice(1) : bodyRaw;
        long utcTicks;
        if (tsBinary)
        {
            var pgMicros = BinaryPrimitives.ReadInt64BigEndian(tsRaw);
            utcTicks = PostgresEpochTicks + pgMicros * TimeSpan.TicksPerMicrosecond;
        }
        else
        {
            utcTicks = OutboxRowParser.ParseTimestamp(Encoding.UTF8.GetString(tsRaw), xid).UtcTicks;
        }

        var envLen = PrefixIdBytes.Length + 36 + MidTopicBodyBytes.Length + bodyUtf8.Length + MidTimestampBytes.Length + 24 + SuffixBytes.Length;
        var env = new byte[envLen];
        var span = env.AsSpan();
        var pos = 0;

        PrefixIdBytes.CopyTo(span.Slice(pos)); pos += PrefixIdBytes.Length;
        key.AsSpan().CopyTo(span.Slice(pos, 36)); pos += 36;
        MidTopicBodyBytes.CopyTo(span.Slice(pos)); pos += MidTopicBodyBytes.Length;
        bodyUtf8.CopyTo(span.Slice(pos, bodyUtf8.Length)); pos += bodyUtf8.Length;
        MidTimestampBytes.CopyTo(span.Slice(pos)); pos += MidTimestampBytes.Length;
        FormatIso8601Utc(new DateTime(utcTicks, DateTimeKind.Utc), span.Slice(pos, 24)); pos += 24;
        SuffixBytes.CopyTo(span.Slice(pos));

        return new PreparedRecord(key, env, utcTicks);
    }

    public FlushBatch CompleteTransaction(CommitMessage commit)
    {
        var transactionEndLsn = new Lsn((ulong)commit.TransactionEndLsn);
        return DrainTxBuffer(transactionEndLsn);
    }

    public FlushBatch DrainTxBuffer(Lsn transactionEndLsn)
    {
        var count = _txCount;
        if (count == 0)
            return new FlushBatch(null, 0, transactionEndLsn, _currentXid);

        var rented = ArrayPool<PreparedRecord>.Shared.Rent(count);
        Array.Copy(_txBuffer, 0, rented, 0, count);
        ClearTxBuffer();
        return new FlushBatch(rented, count, transactionEndLsn, _currentXid);
    }

    private void ClearTxBuffer()
    {
        if (_txCount > 0)
        {
            _txBuffer.AsSpan(0, _txCount).Clear();
            _txCount = 0;
        }
    }

    private void EnsureTxCapacity(int required)
    {
        if (required <= _txBuffer.Length)
            return;
        var next = _txBuffer.Length;
        while (next < required)
            next *= 2;
        Array.Resize(ref _txBuffer, next);
    }

    private void EnsureScratchCapacity(int required)
    {
        if (required <= _scratch.Length)
            return;
        var next = _scratch.Length;
        while (next < required)
            next *= 2;
        var grown = new byte[next];
        Buffer.BlockCopy(_scratch, 0, grown, 0, _scratch.Length);
        _scratch = grown;
    }

    private static bool IsOutboxRelation(RelationMessage relation) =>
        string.Equals(relation.Namespace, OutboxRowParser.OutboxSchema, StringComparison.Ordinal) &&
        string.Equals(relation.RelationName, OutboxRowParser.OutboxTable, StringComparison.Ordinal);

    private static int Read2Digits(ReadOnlySpan<byte> s, uint xid)
    {
        var d0 = s[0] - (byte)'0';
        var d1 = s[1] - (byte)'0';
        if ((uint)d0 > 9 || (uint)d1 > 9)
            throw new PgOutputParseException($"Некорректные цифры даты в SuperFrame (xid={xid})");
        return d0 * 10 + d1;
    }

    private static int Read3Digits(ReadOnlySpan<byte> s, uint xid)
    {
        var d0 = s[0] - (byte)'0';
        var d1 = s[1] - (byte)'0';
        var d2 = s[2] - (byte)'0';
        if ((uint)d0 > 9 || (uint)d1 > 9 || (uint)d2 > 9)
            throw new PgOutputParseException($"Некорректные миллисекунды в SuperFrame (xid={xid})");
        return d0 * 100 + d1 * 10 + d2;
    }

    private static int Read4Digits(ReadOnlySpan<byte> s, uint xid)
    {
        var d0 = s[0] - (byte)'0';
        var d1 = s[1] - (byte)'0';
        var d2 = s[2] - (byte)'0';
        var d3 = s[3] - (byte)'0';
        if ((uint)d0 > 9 || (uint)d1 > 9 || (uint)d2 > 9 || (uint)d3 > 9)
            throw new PgOutputParseException($"Некорректный год в SuperFrame (xid={xid})");
        return d0 * 1000 + d1 * 100 + d2 * 10 + d3;
    }

    private static void FormatIso8601Utc(DateTime utc, Span<byte> dest24)
    {
        dest24[0] = (byte)('0' + (utc.Year / 1000));
        dest24[1] = (byte)('0' + ((utc.Year / 100) % 10));
        dest24[2] = (byte)('0' + ((utc.Year / 10) % 10));
        dest24[3] = (byte)('0' + (utc.Year % 10));
        dest24[4] = (byte)'-';
        dest24[5] = (byte)('0' + (utc.Month / 10));
        dest24[6] = (byte)('0' + (utc.Month % 10));
        dest24[7] = (byte)'-';
        dest24[8] = (byte)('0' + (utc.Day / 10));
        dest24[9] = (byte)('0' + (utc.Day % 10));
        dest24[10] = (byte)'T';
        dest24[11] = (byte)('0' + (utc.Hour / 10));
        dest24[12] = (byte)('0' + (utc.Hour % 10));
        dest24[13] = (byte)':';
        dest24[14] = (byte)('0' + (utc.Minute / 10));
        dest24[15] = (byte)('0' + (utc.Minute % 10));
        dest24[16] = (byte)':';
        dest24[17] = (byte)('0' + (utc.Second / 10));
        dest24[18] = (byte)('0' + (utc.Second % 10));
        dest24[19] = (byte)'.';
        dest24[20] = (byte)('0' + (utc.Millisecond / 100));
        dest24[21] = (byte)('0' + ((utc.Millisecond / 10) % 10));
        dest24[22] = (byte)('0' + (utc.Millisecond % 10));
        dest24[23] = (byte)'Z';
    }
}
