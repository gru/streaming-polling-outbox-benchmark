using System.Buffers.Binary;
using System.Buffers.Text;

namespace Writer.Generation;

/// <summary>
/// Упакованный батч для безтабличной записи в WAL (writeMode=walEmit):
/// [0x42 'B'][int32 count][int32 len_1][utf8_envelope_1]...[int32 len_N][utf8_envelope_N].
/// Псевдослучайный alphanumeric payload той же длины, что и в табличном MessageFactory,
/// исключает искусственное завышение степени сжатия wal_compression=lz4.
/// </summary>
public sealed record PackedBatch(Guid[] Ids, byte[] WalFrameBytes, long TotalBodyBytes);

public static class WalSuperFrameMessageFactory
{
    public const string Topic = "outbox.events";
    public const string EventType = "TestEvent";

    private const string Alphabet = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

    private static ReadOnlySpan<byte> PrefixIdBytes => "{\"id\":\""u8;
    private static ReadOnlySpan<byte> MidTimestampBytes => "\",\"timestamp\":\""u8;
    private static ReadOnlySpan<byte> MidTopicBodyBytes => "\",\"topic\":\"outbox.events\",\"body\":{\"eventType\":\"TestEvent\",\"runId\":\""u8;
    private static ReadOnlySpan<byte> MidPayloadBytes => "\",\"payload\":\""u8;
    private static ReadOnlySpan<byte> SuffixBytes => "\"}}"u8;

    private static readonly byte[] RandomAsciiPool = CreateRandomAsciiPool(131072);

    private static byte[] CreateRandomAsciiPool(int length)
    {
        var pool = new byte[length];
        var rng = new Random(42);
        for (var i = 0; i < length; i++)
            pool[i] = (byte)Alphabet[rng.Next(Alphabet.Length)];
        return pool;
    }

    public static PackedBatch CreatePackedBatch(Guid runId, int count, int payloadBytes)
    {
        payloadBytes = Math.Max(0, payloadBytes);
        var ids = new Guid[count];

        Span<byte> runIdUtf8 = stackalloc byte[36];
        Utf8Formatter.TryFormat(runId, runIdUtf8, out _, 'D');

        var ts = DateTimeOffset.UtcNow;
        Span<byte> tsUtf8 = stackalloc byte[24];
        FormatIso8601Utc(ts, tsUtf8);

        var singleBodyLen = 19 + 1 + 10 + 36 + 12 + payloadBytes + 2;
        var singleEnvLen = PrefixIdBytes.Length + 36 + MidTimestampBytes.Length + 24 +
                           MidTopicBodyBytes.Length + 36 + MidPayloadBytes.Length + payloadBytes + SuffixBytes.Length;

        var totalFrameLen = 5 + count * (4 + singleEnvLen);
        var frame = new byte[totalFrameLen];
        var span = frame.AsSpan();
        span[0] = 0x42; // 'B' — Binary Batch SuperFrame
        BinaryPrimitives.WriteInt32LittleEndian(span.Slice(1, 4), count);

        var pos = 5;
        var poolWindow = Math.Max(1, RandomAsciiPool.Length - Math.Min(payloadBytes, RandomAsciiPool.Length));
        var randOffset = Random.Shared.Next(0, poolWindow);

        for (var i = 0; i < count; i++)
        {
            var id = MessageFactory.CreateUuidV7();
            ids[i] = id;

            BinaryPrimitives.WriteInt32LittleEndian(span.Slice(pos, 4), singleEnvLen);
            pos += 4;

            PrefixIdBytes.CopyTo(span.Slice(pos));
            pos += PrefixIdBytes.Length;
            Utf8Formatter.TryFormat(id, span.Slice(pos, 36), out _, 'D');
            pos += 36;

            MidTimestampBytes.CopyTo(span.Slice(pos));
            pos += MidTimestampBytes.Length;
            tsUtf8.CopyTo(span.Slice(pos, 24));
            pos += 24;

            MidTopicBodyBytes.CopyTo(span.Slice(pos));
            pos += MidTopicBodyBytes.Length;
            runIdUtf8.CopyTo(span.Slice(pos, 36));
            pos += 36;

            MidPayloadBytes.CopyTo(span.Slice(pos));
            pos += MidPayloadBytes.Length;

            if (payloadBytes <= RandomAsciiPool.Length)
            {
                var offset = (randOffset + i * 31) % poolWindow;
                RandomAsciiPool.AsSpan(offset, payloadBytes).CopyTo(span.Slice(pos, payloadBytes));
            }
            else
            {
                var rem = payloadBytes;
                var dstPos = pos;
                while (rem > 0)
                {
                    var take = Math.Min(rem, RandomAsciiPool.Length);
                    RandomAsciiPool.AsSpan(0, take).CopyTo(span.Slice(dstPos, take));
                    dstPos += take;
                    rem -= take;
                }
            }
            pos += payloadBytes;

            SuffixBytes.CopyTo(span.Slice(pos));
            pos += SuffixBytes.Length;
        }

        return new PackedBatch(ids, frame, (long)count * singleBodyLen);
    }

    private static void FormatIso8601Utc(DateTimeOffset utc, Span<byte> dest24)
    {
        Write4Digits(dest24.Slice(0, 4), utc.Year);
        dest24[4] = (byte)'-';
        Write2Digits(dest24.Slice(5, 2), utc.Month);
        dest24[7] = (byte)'-';
        Write2Digits(dest24.Slice(8, 2), utc.Day);
        dest24[10] = (byte)'T';
        Write2Digits(dest24.Slice(11, 2), utc.Hour);
        dest24[13] = (byte)':';
        Write2Digits(dest24.Slice(14, 2), utc.Minute);
        dest24[16] = (byte)':';
        Write2Digits(dest24.Slice(17, 2), utc.Second);
        dest24[19] = (byte)'.';
        Write3Digits(dest24.Slice(20, 3), utc.Millisecond);
        dest24[23] = (byte)'Z';
    }

    private static void Write2Digits(Span<byte> s, int val)
    {
        s[0] = (byte)('0' + (val / 10));
        s[1] = (byte)('0' + (val % 10));
    }

    private static void Write3Digits(Span<byte> s, int val)
    {
        s[0] = (byte)('0' + (val / 100));
        s[1] = (byte)('0' + ((val / 10) % 10));
        s[2] = (byte)('0' + (val % 10));
    }

    private static void Write4Digits(Span<byte> s, int val)
    {
        s[0] = (byte)('0' + (val / 1000));
        s[1] = (byte)('0' + ((val / 100) % 10));
        s[2] = (byte)('0' + ((val / 10) % 10));
        s[3] = (byte)('0' + (val % 10));
    }
}
