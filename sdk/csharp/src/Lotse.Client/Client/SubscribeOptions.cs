using System;

namespace Lotse.Client;

/// <summary>
/// How a queue consumer is subscribed.
/// <para>
/// Credit is how many messages the queue's leader may have pushed to the consumer and not yet counted as used. At most
/// that many messages wait unprocessed in the client. A consumer that works through a message slowly wants a small
/// credit, so that the messages it is not working on can go to others.
/// </para>
/// <para>Exclusive subscriptions are not offered: the broker refuses them. A single active consumer is a setting of the queue.</para>
/// </summary>
public sealed class SubscribeOptions
{
    /// <summary>The credit used when none is given: 64 messages.</summary>
    public const int DefaultCredit = 64;

    private SubscribeOptions(int credit)
    {
        Credit = credit;
    }

    /// <summary>The defaults: <see cref="DefaultCredit"/> messages of credit.</summary>
    public static SubscribeOptions Default { get; } = new(DefaultCredit);

    /// <summary>The credit in messages, at least 1.</summary>
    public int Credit { get; }

    /// <summary>Options with room for <paramref name="credit"/> messages at a time; a value below 1 is taken as 1.</summary>
    public static SubscribeOptions WithCredit(int credit) => new(Math.Max(1, credit));
}
