using FsCheck;
using FsCheck.Fluent;
using FsCheck.Xunit;
using OutboxLab.Core.Model;

namespace OutboxLab.UnitTests.Model;

public sealed class EventCodecTests
{
    [Property(MaxTest = 300)]
    public Property Decode_inverts_encode_for_any_event_and_size() =>
        Prop.ForAll(
            (from aggregate in ArbMap.Default.GeneratorFor<int>()
             from sequence in ArbMap.Default.GeneratorFor<long>()
             from producedAt in ArbMap.Default.GeneratorFor<long>()
             select new OutboxEvent(aggregate, sequence, producedAt)).ToArbitrary(),
            Gen.Choose(EventCodec.HeaderSize, EventCodec.MaxPayloadSize).ToArbitrary(),
            (e, size) =>
            {
                var payload = EventCodec.Encode(e, size);
                return payload.Length == size && EventCodec.Decode(payload) == e;
            });

    [Fact]
    public void Payloads_of_different_messages_differ_beyond_the_header()
    {
        var a = EventCodec.Encode(new OutboxEvent(1, 1, 0), 64);
        var b = EventCodec.Encode(new OutboxEvent(1, 2, 0), 64);

        a.AsSpan(EventCodec.HeaderSize).SequenceEqual(b.AsSpan(EventCodec.HeaderSize)).Should().BeFalse();
    }

    [Theory]
    [InlineData(EventCodec.HeaderSize - 1)]
    [InlineData(EventCodec.MaxPayloadSize + 1)]
    public void Encode_rejects_sizes_outside_the_storable_range(int size)
    {
        var act = () => EventCodec.Encode(new OutboxEvent(1, 1, 1), size);

        act.Should().Throw<ArgumentOutOfRangeException>();
    }

    [Fact]
    public void Decode_rejects_a_short_payload()
    {
        var act = () => EventCodec.Decode(new byte[EventCodec.HeaderSize - 1]);

        act.Should().Throw<FormatException>().WithMessage("*at least 24*");
    }

    [Fact]
    public void Decode_rejects_a_payload_without_the_magic()
    {
        var payload = EventCodec.Encode(new OutboxEvent(7, 9, 11), 32);
        payload[0] ^= 0xFF;

        var act = () => EventCodec.Decode(payload);

        act.Should().Throw<FormatException>().WithMessage("*OBX1*");
    }

    [Fact]
    public void Layout_is_big_endian_and_stable()
    {
        var payload = EventCodec.Encode(new OutboxEvent(0x01020304, 0x05, 0x06), EventCodec.HeaderSize);

        Convert.ToHexString(payload).Should().Be("4F425831" + "01020304" + "0000000000000005" + "0000000000000006");
    }
}
