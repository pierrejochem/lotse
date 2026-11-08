using System;
using System.Text;

namespace Lotse.Record;

/// <summary>
/// An immutable sequence of bytes that may share storage with the buffer it was cut from. Keys, values and header
/// parts of records are byte strings: slicing a batch does not copy them.
/// </summary>
public sealed class ByteString : IEquatable<ByteString>
{
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);

    /// <summary>The empty byte string.</summary>
    public static readonly ByteString Empty = new(Array.Empty<byte>(), 0, 0);

    private readonly byte[] _array;
    private readonly int _offset;
    private readonly int _length;

    private ByteString(byte[] array, int offset, int length)
    {
        _array = array;
        _offset = offset;
        _length = length;
    }

    /// <summary>A copy of <paramref name="bytes"/>.</summary>
    public static ByteString CopyOf(byte[] bytes) => CopyOf(bytes, 0, bytes.Length);

    /// <summary>A copy of <c>bytes[offset, offset + length)</c>.</summary>
    public static ByteString CopyOf(byte[] bytes, int offset, int length)
    {
        CheckFromIndexSize(offset, length, bytes.Length);
        if (length == 0)
        {
            return Empty;
        }
        byte[] copy = new byte[length];
        Buffer.BlockCopy(bytes, offset, copy, 0, length);
        return new ByteString(copy, 0, length);
    }

    /// <summary>
    /// A view of <c>bytes[offset, offset + length)</c> without copying. The caller must not change the array
    /// afterwards.
    /// </summary>
    public static ByteString View(byte[] bytes, int offset, int length)
    {
        CheckFromIndexSize(offset, length, bytes.Length);
        return length == 0 ? Empty : new ByteString(bytes, offset, length);
    }

    /// <summary>The UTF-8 bytes of <paramref name="s"/>.</summary>
    public static ByteString Utf8(string s) => CopyOf(Encoding.UTF8.GetBytes(s));

    /// <summary>The number of bytes.</summary>
    public int Size => _length;

    /// <summary>Whether there are no bytes.</summary>
    public bool IsEmpty => _length == 0;

    /// <summary>The bytes, without copying.</summary>
    public ReadOnlySpan<byte> Span => new(_array, _offset, _length);

    /// <summary>The byte at <paramref name="index"/>, as an unsigned value.</summary>
    public int At(int index)
    {
        if ((uint)index >= (uint)_length)
        {
            throw new ArgumentOutOfRangeException(nameof(index));
        }
        return _array[_offset + index];
    }

    /// <summary>A copy of the bytes.</summary>
    public byte[] ToByteArray()
    {
        byte[] copy = new byte[_length];
        Buffer.BlockCopy(_array, _offset, copy, 0, _length);
        return copy;
    }

    /// <summary>The bytes <c>[from, to)</c>, sharing the storage.</summary>
    public ByteString Slice(int from, int to)
    {
        if (from < 0 || to < from || to > _length)
        {
            throw new ArgumentOutOfRangeException(nameof(from));
        }
        return to == from ? Empty : new ByteString(_array, _offset + from, to - from);
    }

    /// <summary>Copies the bytes into <paramref name="dst"/> at <paramref name="dstOffset"/>.</summary>
    public void CopyTo(byte[] dst, int dstOffset) => Buffer.BlockCopy(_array, _offset, dst, dstOffset, _length);

    /// <summary>The bytes as UTF-8 text; invalid sequences become replacement characters.</summary>
    public string ToUtf8() => Encoding.UTF8.GetString(_array, _offset, _length);

    /// <summary>The bytes as UTF-8 text, or <c>null</c> if they are not valid UTF-8.</summary>
    public string? TryToUtf8Strict()
    {
        try
        {
            return StrictUtf8.GetString(_array, _offset, _length);
        }
        catch (DecoderFallbackException)
        {
            return null;
        }
    }

    // The storage, for decoders in this assembly that read without copying.
    internal byte[] Storage => _array;

    internal int Offset => _offset;

    /// <inheritdoc/>
    public bool Equals(ByteString? other) => other is not null && Span.SequenceEqual(other.Span);

    /// <inheritdoc/>
    public override bool Equals(object? obj) => obj is ByteString other && Equals(other);

    /// <inheritdoc/>
    public override int GetHashCode()
    {
        int h = 1;
        foreach (byte b in Span)
        {
            h = unchecked(31 * h + (sbyte)b);
        }
        return h;
    }

    /// <inheritdoc/>
    public override string ToString()
    {
        var sb = new StringBuilder("ByteString[").Append(_length).Append(" bytes");
        int shown = Math.Min(_length, 16);
        if (shown > 0)
        {
            sb.Append(": ");
        }
        for (int i = 0; i < shown; i++)
        {
            sb.Append(_array[_offset + i].ToString("x2"));
        }
        return sb.Append(shown < _length ? "..." : "").Append(']').ToString();
    }

    private static void CheckFromIndexSize(int offset, int length, int size)
    {
        if (offset < 0 || length < 0 || offset > size - length)
        {
            throw new ArgumentOutOfRangeException(nameof(offset), $"range [{offset}, {offset}+{length}) is outside {size} bytes");
        }
    }
}
