namespace Lotse.Record;

/// <summary>A header of a record. The key is never null; the value may be.</summary>
public sealed record Header(ByteString Key, ByteString? Value);
