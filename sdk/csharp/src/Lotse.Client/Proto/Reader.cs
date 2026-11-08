using System;
using System.Collections.Generic;
using System.Text;
using Lotse.Record;

namespace Lotse.Proto;

/// <summary>
/// Reads fields from a frame, refusing anything that runs past its end. The conventions are those of
/// <see cref="Writer"/>. The reader takes the array without copying it, and the <see cref="ByteString"/>s it
/// returns point into it: the caller must not change the array afterwards. Not thread-safe.
/// </summary>
public sealed class Reader
{
    private const int NullString = 0xFFFF;
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);

    private readonly byte[] _buf;
    private readonly int _end;
    private int _pos;

    /// <summary>Reads <paramref name="frame"/>, which the caller hands over.</summary>
    public Reader(byte[] frame)
        : this(frame, 0, frame.Length)
    {
    }

    /// <summary>Reads <c>frame[offset, offset + length)</c>.</summary>
    public Reader(byte[] frame, int offset, int length)
    {
        if (offset < 0 || length < 0 || offset > frame.Length - length)
        {
            throw new ArgumentOutOfRangeException(nameof(offset));
        }
        _buf = frame;
        _pos = offset;
        _end = offset + length;
    }

    /// <summary>The number of bytes not read yet.</summary>
    public int Remaining => _end - _pos;

    /// <summary>Fails unless everything was read.</summary>
    public void Finish()
    {
        if (_pos != _end)
        {
            throw DecodeException.Malformed("trailing bytes");
        }
    }

    private void Need(long n)
    {
        if (_end - _pos < n)
        {
            throw DecodeException.TruncatedFrame();
        }
    }

    /// <summary>A <c>u8</c>.</summary>
    public byte U8()
    {
        Need(1);
        return _buf[_pos++];
    }

    /// <summary>A boolean: any non-zero byte is true, as the broker reads it.</summary>
    public bool Bool() => U8() != 0;

    /// <summary>A <c>u16</c>.</summary>
    public ushort U16()
    {
        Need(2);
        ushort v = (ushort)(_buf[_pos] | _buf[_pos + 1] << 8);
        _pos += 2;
        return v;
    }

    /// <summary>A <c>u32</c>.</summary>
    public uint U32()
    {
        Need(4);
        uint v = 0;
        for (int i = 0; i < 4; i++)
        {
            v |= (uint)_buf[_pos + i] << (8 * i);
        }
        _pos += 4;
        return v;
    }

    /// <summary>An <c>i32</c>.</summary>
    public int I32() => unchecked((int)U32());

    /// <summary>A <c>u64</c>.</summary>
    public ulong U64()
    {
        Need(8);
        ulong v = 0;
        for (int i = 0; i < 8; i++)
        {
            v |= (ulong)_buf[_pos + i] << (8 * i);
        }
        _pos += 8;
        return v;
    }

    /// <summary>An <c>i64</c>.</summary>
    public long I64() => unchecked((long)U64());

    /// <summary>A ZigZag varint.</summary>
    public long Varint()
    {
        try
        {
            var d = Record.Varint.GetI64(_buf, _pos, _end);
            _pos += d.Length;
            return d.Value;
        }
        catch (RecordFormatException)
        {
            // The broker does not tell a short varint from an overlong one either.
            throw DecodeException.TruncatedFrame();
        }
    }

    // A count of at least 0, each element taking at least minSize bytes: counts the frame cannot hold are refused
    // before anything is allocated.
    private int Count(int minSize)
    {
        long n = Varint();
        if (n < 0)
        {
            throw DecodeException.Malformed("negative count");
        }
        if (n > Remaining / Math.Max(minSize, 1))
        {
            throw DecodeException.Malformed("count exceeds the frame");
        }
        return (int)n;
    }

    /// <summary>A string that must not be null.</summary>
    public string String() => OptString() ?? throw DecodeException.Malformed("null string");

    /// <summary>A string that may be null.</summary>
    public string? OptString()
    {
        int len = U16();
        if (len == NullString)
        {
            return null;
        }
        Need(len);
        try
        {
            string s = StrictUtf8.GetString(_buf, _pos, len);
            _pos += len;
            return s;
        }
        catch (DecoderFallbackException)
        {
            throw DecodeException.Malformed("string is not UTF-8");
        }
    }

    /// <summary>Length-prefixed bytes, sharing the frame's array.</summary>
    public ByteString Bytes()
    {
        uint len = U32();
        Need(len);
        ByteString output = ByteString.View(_buf, _pos, (int)len);
        _pos += (int)len;
        return output;
    }

    /// <summary>An array; <paramref name="minSize"/> is the smallest encoding of one element.</summary>
    public List<T> Array<T>(int minSize, Func<Reader, T> f)
    {
        int n = Count(minSize);
        var output = new List<T>(n);
        for (int i = 0; i < n; i++)
        {
            output.Add(f(this));
        }
        return output;
    }

    /// <summary>Tagged fields, in the order sent.</summary>
    public List<Tag> Tags() =>
        Array(5, r =>
        {
            long tag = r.Varint();
            if (tag < 0)
            {
                throw DecodeException.Malformed("negative tag");
            }
            return new Tag(tag, r.Bytes());
        });
}
