using System;
using Lotse.Proto;

namespace Lotse.Client;

/// <summary>
/// Why one attempt failed, thrown inside the retry loop only. <c>Retry</c> may be tried again at once, after metadata
/// is refreshed; <c>RetryMaybeDone</c> may be tried again, but the request may have been carried out, so only an
/// idempotent produce does; <c>Fail</c> ends the call with an error.
/// </summary>
internal sealed class Attempt : Exception
{
    internal enum Kind
    {
        Retry,
        RetryMaybeDone,
        Fail,
    }

    private Attempt(Kind kind, string why, ClientException? error)
        : base(why)
    {
        Outcome = kind;
        Error = error;
    }

    internal Kind Outcome { get; }

    internal ClientException? Error { get; }

    internal static Attempt Retry(string why) => new(Kind.Retry, why, null);

    internal static Attempt RetryMaybeDone(string why) => new(Kind.RetryMaybeDone, why, null);

    internal static Attempt Fail(ClientException error) => new(Kind.Fail, error.Message, error);
}

/// <summary>Why a request got no response.</summary>
internal sealed class CallFailure : Exception
{
    internal enum Kind
    {
        /// <summary>It never left: retrying cannot duplicate anything.</summary>
        NotSent,

        /// <summary>It may have reached the broker.</summary>
        Lost,

        /// <summary>The connection could not authenticate: the request never left, and trying again will not help.</summary>
        Denied,

        /// <summary>The node refused the request (a role is missing): not carried out, and trying again will not help.</summary>
        Refused,
    }

    private CallFailure(Kind kind, string why, ErrorCode? code)
        : base(why)
    {
        Outcome = kind;
        Code = code;
    }

    internal Kind Outcome { get; }

    internal ErrorCode? Code { get; }

    internal static CallFailure NotSent(string why) => new(Kind.NotSent, why, null);

    internal static CallFailure Lost(string why) => new(Kind.Lost, why, null);

    internal static CallFailure Denied(string why) => new(Kind.Denied, why, null);

    internal static CallFailure Refused(ErrorCode code, string why) => new(Kind.Refused, why, code);

    /// <summary>A connection that could not authenticate, or a refusal, fails at once; anything else is tried again.</summary>
    internal Attempt ToAttempt() => Outcome switch
    {
        Kind.Denied => Attempt.Fail(new ClientAuthenticationException(Message)),
        Kind.Refused => Attempt.Fail(new BrokerErrorException(Code!.Value, Message)),
        _ => Attempt.Retry(Describe()),
    };

    internal string Describe() => Outcome == Kind.Refused ? $"{Code}: {Message}" : Message;
}
