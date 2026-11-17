using System.Buffers;
using System.Text;
using System.Text.Json;
using CdcCommon;
using CdcWalV6.Replication;
using Shouldly;
using Writer.Generation;
using Xunit;

namespace CdcCommon.UnitTests;

public sealed class WalSuperFrameDecoderTests
{
    [Theory]
    [InlineData(1, 256)]
    [InlineData(100, 256)]
    [InlineData(500, 256)]
    [InlineData(50, 7680)]
    public void CreatePackedBatch_UnpackSuperFrame_RoundTripsAllIdsAndEnvelopes(int count, int payloadBytes)
    {
        var runId = Guid.Parse("aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee");
        var packed = WalSuperFrameMessageFactory.CreatePackedBatch(runId, count, payloadBytes);
        packed.Ids.Length.ShouldBe(count);
        packed.Ids.Distinct().Count().ShouldBe(count);

        var decoder = new WalSuperFrameDecoder();
        decoder.UnpackSuperFrameToTxBuffer(packed.WalFrameBytes, xid: 777);
        decoder.IncompleteEventCount.ShouldBe(count);

        var flushed = decoder.DrainTxBuffer(new Lsn(0x1000));
        flushed.Count.ShouldBe(count);
        flushed.RentedEvents.ShouldNotBeNull();

        try
        {
            for (var i = 0; i < count; i++)
            {
                var rec = flushed.RentedEvents[i];
                rec.Key.Length.ShouldBe(36);
                Encoding.UTF8.GetString(rec.Key).ShouldBe(packed.Ids[i].ToString("D"));

                Envelope.TryParse(rec.Value, out var consumed).ShouldBeTrue();
                consumed.ShouldNotBeNull();
                consumed.Id.ShouldBe(packed.Ids[i]);
                consumed.Topic.ShouldBe("outbox.events");
                consumed.TimestampUtc.UtcTicks.ShouldBe(rec.CreatedAtUtcTicks);

                using var bodyDoc = JsonDocument.Parse(consumed.BodyJson);
                bodyDoc.RootElement.GetProperty("eventType").GetString().ShouldBe("TestEvent");
                bodyDoc.RootElement.GetProperty("runId").GetGuid().ShouldBe(runId);
                bodyDoc.RootElement.GetProperty("payload").GetString()!.Length.ShouldBe(payloadBytes);
            }
        }
        finally
        {
            ArrayPool<PreparedRecord>.Shared.Return(flushed.RentedEvents, clearArray: true);
        }
    }
}
