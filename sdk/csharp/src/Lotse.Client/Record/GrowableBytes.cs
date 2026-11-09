using System;

namespace Lotse.Record;

internal sealed class GrowableBytes
{
    private byte[] _data;
    private int _size;

    internal GrowableBytes(int capacity)
    {
        _data = new byte[Math.Max(capacity, 16)];
    }

    internal int Size => _size;

    internal byte[] Storage => _data;

    private void Reserve(int extra)
    {
        long needed = (long)_size + extra;
        if (needed > MaxArrayLength)
        {
            throw new RecordFormatException(RecordFormatErrorKind.TooLarge, "buffer too large");
        }
        if (needed > _data.Length)
        {
            long grown = Math.Max(needed, Math.Min((long)_data.Length * 2, MaxArrayLength));
            System.Array.Resize(ref _data, (int)grown);
        }
    }

    private const int MaxArrayLength = int.MaxValue - 8;

    internal void Put(byte b)
    {
        Reserve(1);
        _data[_size++] = b;
    }

    internal void Put(byte[] src, int off, int len)
    {
        Reserve(len);
        Buffer.BlockCopy(src, off, _data, _size, len);
        _size += len;
    }

    internal void Put(ByteString s)
    {
        Reserve(s.Size);
        s.CopyTo(_data, _size);
        _size += s.Size;
    }

    internal void PutVarint(long signed)
    {
        Reserve(Varint.MaxLen64);
        _size += Varint.PutI64(_data, _size, signed);
    }
}
