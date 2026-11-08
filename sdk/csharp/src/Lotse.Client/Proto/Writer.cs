using System;
using System.Collections.Generic;
using System.Text;
using Lotse.Record;

namespace Lotse.Proto;

/// <summary>
/// Writes fields into a frame. Conventions, shared by every message:
/// <code>
/// u8/u16/u32/u64/i32/i64  fixed width, little-endian
/// varint                  ZigZag varint (array counts, tags)
/// string                  u16 length + UTF-8 (0xFFFF = null), at most 65,534 bytes
/// bytes                   u32 length + data
/// array&lt;T&gt;              varint count + elements
/// tagged fields           varint count + {tag varint, bytes}, at the end of most structures
/// optional                one byte 0 or 1, then the value if 1
/// </code>
/// Not thread-safe.
/// </summary>
public sealed class Writer
{
    private const int NullString = 0xFFFF;
    private static readonly UTF8Encoding StrictUtf8 = new(false, true);

    private byte[] _buf = new byte[64];
    private int _size;

    /// <summary>The bytes written so far.</summary>
    public byte[] ToByteArray()
    {
        byte[] copy = new byte[_size];
        Buffer.BlockCopy(_buf, 0, copy, 0, _size);
        return copy;
    }

    private void Reserve(int extra)
    {
        long needed = (long)_size + extra;
        if (needed > int.MaxValue - 8)
        {
            throw new InvalidOperationException("frame too large");
        }
        if (needed > _buf.Length)
        {
            long grown = Math.Max(needed, Math.Min((long)_buf.Length * 2, int.MaxValue - 8));
            System.Array.Resize(ref _buf, (int)grown);
        }
    }

    /// <summary>A <c>u8</c>.</summary>
    public Writer U8(byte v)
    {
        Reserve(1);
        _buf[_size++] = v;
        return this;
    }

    /// <summary>A boolean as one byte, 0 or 1.</summary>
    public Writer Bool(bool v) => U8(v ? (byte)1 : (byte)0);

    /// <summary>A <c>u16</c>.</summary>
    public Writer U16(ushort v)
    {
        Reserve(2);
        _buf[_size++] = (byte)v;
        _buf[_size++] = (byte)(v >> 8);
        return this;
    }

    /// <summary>A <c>u32</c>.</summary>
    public Writer U32(uint v) => Fixed32(v);

    /// <summary>An <c>i32</c>.</summary>
    public Writer I32(int v) => Fixed32(unchecked((uint)v));

    private Writer Fixed32(uint v)
    {
        Reserve(4);
        for (int i = 0; i < 4; i++)
        {
            _buf[_size++] = (byte)(v >> (8 * i));
        }
        return this;
    }

    /// <summary>A <c>u64</c>.</summary>
    public Writer U64(ulong v)
    {
        Reserve(8);
        for (int i = 0; i < 8; i++)
        {
            _buf[_size++] = (byte)(v >> (8 * i));
        }
        return this;
    }

    /// <summary>An <c>i64</c>.</summary>
    public Writer I64(long v) => U64(unchecked((ulong)v));

    /// <summary>A ZigZag varint.</summary>
    public Writer Varint(long v)
    {
        Reserve(Record.Varint.MaxLen64);
        _size += Record.Varint.PutI64(_buf, _size, v);
        return this;
    }

    /// <summary>A string; strings are short names, at most 65,534 UTF-8 bytes.</summary>
    public Writer String(string s)
    {
        if (s is null)
        {
            throw new ArgumentNullException(nameof(s), "null string where the protocol needs one; use OptString");
        }
        byte[] utf8;
        try
        {
            utf8 = StrictUtf8.GetBytes(s);
        }
        catch (EncoderFallbackException e)
        {
            throw new ArgumentException("string is not valid Unicode", nameof(s), e);
        }
        if (utf8.Length >= NullString)
        {
            throw new ArgumentException($"a string of at most 65,534 bytes, got {utf8.Length}", nameof(s));
        }
        U16((ushort)utf8.Length);
        Reserve(utf8.Length);
        Buffer.BlockCopy(utf8, 0, _buf, _size, utf8.Length);
        _size += utf8.Length;
        return this;
    }

    /// <summary>A string that may be <c>null</c>.</summary>
    public Writer OptString(string? s) => s is null ? U16(NullString) : String(s);

    /// <summary>Length-prefixed bytes.</summary>
    public Writer Bytes(ByteString b)
    {
        U32((uint)b.Size);
        Reserve(b.Size);
        b.CopyTo(_buf, _size);
        _size += b.Size;
        return this;
    }

    /// <summary>An array, each element written by <paramref name="f"/>.</summary>
    public Writer Array<T>(IReadOnlyList<T> items, Action<Writer, T> f)
    {
        Varint(items.Count);
        foreach (T item in items)
        {
            f(this, item);
        }
        return this;
    }

    /// <summary>Tagged fields, in ascending tag order.</summary>
    public Writer Tags(IReadOnlyList<Tag> tags)
    {
        Varint(tags.Count);
        foreach (Tag t in tags)
        {
            Varint(t.Number);
            Bytes(t.Value);
        }
        return this;
    }

    /// <summary>An empty block of tagged fields.</summary>
    public Writer NoTags() => Varint(0);
}
