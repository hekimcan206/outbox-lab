using System.Buffers.Binary;

namespace OutboxLab.Core.Model;

/// <summary>
/// Fixed binary layout for the outbox payload, identical on every engine (PostgreSQL <c>bytea</c>, Oracle <c>RAW</c>,
/// Advanced Queuing RAW payload and logical-decoding message content), so payload size is never a hidden variable.
/// </summary>
/// <remarks>
/// Layout (big endian): magic "OBX1" (4) | aggregate id (4) | sequence (8) | produced-at timestamp (8) | filler.
/// The filler is derived from the sequence so payloads differ and do not compress to nothing.
/// </remarks>
public static class EventCodec
{
    public const int HeaderSize = 24;
    public const int MaxPayloadSize = 2000; // Oracle RAW limit with MAX_STRING_SIZE=STANDARD.

    private const uint Magic = 0x4F425831; // "OBX1"

    public static byte[] Encode(OutboxEvent e, int payloadSize)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(payloadSize, HeaderSize);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(payloadSize, MaxPayloadSize);

        var buffer = new byte[payloadSize];
        var span = buffer.AsSpan();
        BinaryPrimitives.WriteUInt32BigEndian(span, Magic);
        BinaryPrimitives.WriteInt32BigEndian(span[4..], e.AggregateId);
        BinaryPrimitives.WriteInt64BigEndian(span[8..], e.Sequence);
        BinaryPrimitives.WriteInt64BigEndian(span[16..], e.ProducedAt);

        var filler = (byte)(e.Sequence * 31 + e.AggregateId);
        for (var i = HeaderSize; i < buffer.Length; i++)
        {
            buffer[i] = filler++;
        }

        return buffer;
    }

    public static OutboxEvent Decode(ReadOnlySpan<byte> payload)
    {
        if (payload.Length < HeaderSize)
        {
            throw new FormatException($"Outbox payload is {payload.Length} bytes; at least {HeaderSize} are required.");
        }

        if (BinaryPrimitives.ReadUInt32BigEndian(payload) != Magic)
        {
            throw new FormatException("Outbox payload does not start with the OBX1 magic.");
        }

        return new OutboxEvent(
            BinaryPrimitives.ReadInt32BigEndian(payload[4..]),
            BinaryPrimitives.ReadInt64BigEndian(payload[8..]),
            BinaryPrimitives.ReadInt64BigEndian(payload[16..]));
    }
}
