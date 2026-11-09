namespace Lotse.Proto;

/// <summary>
/// The header of a request frame: <c>api_key u16 | api_version u16 | correlation_id u32 | flags u16 |
/// tagged_fields</c>.
/// </summary>
/// <param name="ApiKey">What is asked.</param>
/// <param name="ApiVersion">In which version.</param>
/// <param name="CorrelationId">Echoed in the response; 0 is reserved for push frames.</param>
/// <param name="Flags">See <see cref="Proto.Flags"/>.</param>
public sealed record RequestHeader(ApiKey ApiKey, ushort ApiVersion, uint CorrelationId, ushort Flags);
