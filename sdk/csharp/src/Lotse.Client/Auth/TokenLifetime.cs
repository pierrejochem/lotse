using System;

namespace Lotse.Auth;

/// <summary>
/// How long an access token lives (ADR 0133): one of five steps, counted from when the metadata leader records it.
/// The value is the code that travels in <c>CreateToken</c>.
/// </summary>
public enum TokenLifetime : byte
{
    /// <summary>7 days.</summary>
    Week = 1,

    /// <summary>30 days.</summary>
    Month = 2,

    /// <summary>90 days.</summary>
    Quarter = 3,

    /// <summary>180 days.</summary>
    HalfYear = 4,

    /// <summary>365 days.</summary>
    Year = 5,
}

/// <summary>What a <see cref="TokenLifetime"/> means.</summary>
public static class TokenLifetimes
{
    /// <summary>The lifetime as a span of days.</summary>
    /// <param name="lifetime">The lifetime.</param>
    /// <exception cref="ArgumentOutOfRangeException">Not one of the five steps.</exception>
    public static TimeSpan Duration(this TokenLifetime lifetime) => lifetime switch
    {
        TokenLifetime.Week => TimeSpan.FromDays(7),
        TokenLifetime.Month => TimeSpan.FromDays(30),
        TokenLifetime.Quarter => TimeSpan.FromDays(90),
        TokenLifetime.HalfYear => TimeSpan.FromDays(180),
        TokenLifetime.Year => TimeSpan.FromDays(365),
        _ => throw new ArgumentOutOfRangeException(nameof(lifetime), lifetime, "a token lifetime's code is 1 to 5"),
    };

    /// <summary>The lifetime a code names.</summary>
    /// <param name="code">The code on the wire.</param>
    /// <exception cref="ArgumentOutOfRangeException">For anything but 1 to 5.</exception>
    public static TokenLifetime FromCode(byte code) =>
        code is >= 1 and <= 5
            ? (TokenLifetime)code
            : throw new ArgumentOutOfRangeException(nameof(code), code, "a token lifetime's code is 1 to 5");
}
