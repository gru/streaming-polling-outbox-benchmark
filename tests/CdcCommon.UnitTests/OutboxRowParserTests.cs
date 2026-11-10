using CdcCommon;
using CdcCommon.PgOutput;
using Shouldly;
using Xunit;

namespace CdcCommon.UnitTests;

public sealed class OutboxRowParserTests
{
    private static readonly string[] SchemaColumns = { "id", "topic", "body", "created_at" };

    [Fact]
    public void BuildColumnPlan_SchemaColumns_MapsSlotsInOrder()
    {
        var plan = OutboxRowParser.BuildColumnPlan(SchemaColumns);

        plan.Slots.ShouldBe(new[]
        {
            OutboxColumnSlot.Id,
            OutboxColumnSlot.Topic,
            OutboxColumnSlot.Body,
            OutboxColumnSlot.CreatedAt,
        });
    }

    [Fact]
    public void BuildColumnPlan_ReorderedAndExtraColumns_MapsByPosition()
    {
        var plan = OutboxRowParser.BuildColumnPlan(new[] { "created_at", "note", "id", "topic", "body" });

        plan.Slots.ShouldBe(new[]
        {
            OutboxColumnSlot.CreatedAt,
            OutboxColumnSlot.Ignored,
            OutboxColumnSlot.Id,
            OutboxColumnSlot.Topic,
            OutboxColumnSlot.Body,
        });
    }

    [Fact]
    public void BuildColumnPlan_UnknownColumn_BecomesIgnored()
    {
        var plan = OutboxRowParser.BuildColumnPlan(new[] { "id", "topic", "body", "created_at", "outbox_partition" });

        plan.Slots[4].ShouldBe(OutboxColumnSlot.Ignored);
    }

    [Theory]
    [InlineData(new[] { "topic", "body", "created_at" }, "id")]
    [InlineData(new[] { "id", "body", "created_at" }, "topic")]
    [InlineData(new[] { "id", "topic", "created_at" }, "body")]
    [InlineData(new[] { "id", "topic", "body" }, "created_at")]
    public void BuildColumnPlan_MissingRequiredColumn_FailsWithColumnName(string[] columns, string expectedMissing)
    {
        var error = Should.Throw<PgOutputParseException>(() => OutboxRowParser.BuildColumnPlan(columns));

        error.Message.ShouldContain(expectedMissing);
        error.Message.ShouldContain(string.Join(", ", columns));
    }

    [Fact]
    public void BuildColumnPlan_MissingAllRequiredColumns_ListsEveryColumn()
    {
        var error = Should.Throw<PgOutputParseException>(() => OutboxRowParser.BuildColumnPlan(new[] { "note" }));

        error.Message.ShouldContain("id");
        error.Message.ShouldContain("topic");
        error.Message.ShouldContain("body");
        error.Message.ShouldContain("created_at");
    }

    [Fact]
    public void Parse_FullRow_ProducesEventWithZeroCommitLsn()
    {
        var id = Guid.NewGuid();

        var message = OutboxRowParser.Parse(
            id.ToString(),
            "outbox.events",
            "{\"runId\":\"...\"}",
            "2026-10-17 06:42:11.123456+00",
            xid: 42);

        message.Id.ShouldBe(id);
        message.Topic.ShouldBe("outbox.events");
        message.BodyJson.ShouldBe("{\"runId\":\"...\"}");
        message.CreatedAtUtc.ShouldBe(new DateTimeOffset(2026, 10, 17, 6, 42, 11, 123, 456, TimeSpan.Zero));
        message.CommitLsn.ShouldBe(Lsn.Zero);
        message.Xid.ShouldBe(42u);
    }

    [Fact]
    public void Parse_AllColumnsNull_FailsListingEveryColumn()
    {
        var error = Should.Throw<PgOutputParseException>(() => OutboxRowParser.Parse(null, null, null, null, 7));

        error.Message.ShouldContain("id");
        error.Message.ShouldContain("topic");
        error.Message.ShouldContain("body");
        error.Message.ShouldContain("created_at");
    }

    [Fact]
    public void Parse_SingleNullColumn_FailsWithThatColumnOnly()
    {
        var error = Should.Throw<PgOutputParseException>(() =>
            OutboxRowParser.Parse(Guid.NewGuid().ToString(), "outbox.events", null, "2026-10-17 06:42:11+00", 7));

        error.Message.ShouldContain("body");
        error.Message.ShouldNotContain("topic");
    }

    [Fact]
    public void Parse_IdNotUuid_FailsWithColumnAndValue()
    {
        var error = Should.Throw<PgOutputParseException>(() =>
            OutboxRowParser.Parse("not-a-uuid", "outbox.events", "{}", "2026-10-17 06:42:11+00", 7));

        error.Message.ShouldContain("id");
        error.Message.ShouldContain("not-a-uuid");
    }

    [Fact]
    public void Parse_ForbiddenTopic_FailsWithAllowedTopic()
    {
        var error = Should.Throw<PgOutputParseException>(() =>
            OutboxRowParser.Parse(Guid.NewGuid().ToString(), "other.topic", "{}", "2026-10-17 06:42:11+00", 7));

        error.Message.ShouldContain("other.topic");
        error.Message.ShouldContain(OutboxRowParser.AllowedTopic);
    }

    [Fact]
    public void Parse_GarbageTimestamp_Fails()
    {
        Should.Throw<PgOutputParseException>(() =>
            OutboxRowParser.Parse(Guid.NewGuid().ToString(), "outbox.events", "{}", "not-a-timestamp", 7));
    }

    [Theory]
    [InlineData("2026-10-17 06:42:11+03", 3, 0, 0)]        // целочасовое смещение PG +HH
    [InlineData("2026-10-17 06:42:11+0300", 3, 0, 0)]      // компактное +HHMM
    [InlineData("2026-10-17 06:42:11.5+03", 3, 500, 0)]    // дробные секунды с +HH
    [InlineData("2026-10-17 06:42:11.123456+00", 6, 123, 456)]
    public void Parse_TimestampOffsets_NormalizesToUtc(string text, int expectedHour, int expectedMillisecond, int expectedMicrosecond)
    {
        var message = OutboxRowParser.Parse(
            Guid.NewGuid().ToString(), "outbox.events", "{}", text, 7);

        message.CreatedAtUtc.UtcDateTime.ShouldBe(
            new DateTime(2026, 10, 17, expectedHour, 42, 11, expectedMillisecond, expectedMicrosecond, DateTimeKind.Utc));
    }
}
