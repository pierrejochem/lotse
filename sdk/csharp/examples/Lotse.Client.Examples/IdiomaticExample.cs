using System;
using System.Threading;
using System.Threading.Tasks;
using Lotse.Client;

namespace Lotse.Examples;

/// <summary>
/// The shape of the C# client: a queue read as an <see cref="System.Collections.Generic.IAsyncEnumerable{T}"/>, a
/// group's records read the same way, and a <see cref="Transaction"/> in an <c>await using</c> that aborts unless it is
/// committed. Writes three notifications and acks them, writes ten orders and invoices them in one transaction, and
/// reads the invoices back. Uses its own resources (names ending in <c>-cs</c>).
/// </summary>
internal static class IdiomaticExample
{
    public static async Task RunAsync()
    {
        using var client = new LotseClient(Env.Config().WithTransactionalId("example-billing-cs").Build());
        await CreateStreamAsync(client, "example-orders-cs");
        await CreateStreamAsync(client, "example-invoices-cs");
        try
        {
            await client.CreateQueueAsync("example-notify-cs", QueueOptions.CreateBuilder().WithAckTimeout(TimeSpan.FromSeconds(30)).Build());
        }
        catch (ResourceExistsException)
        {
            // made by an earlier run
        }

        // A queue as an IAsyncEnumerable: it ends when the token is cancelled, here after the third message.
        for (int i = 0; i < 3; i++)
        {
            await client.ProduceAsync("example-notify-cs", 0, OutMessage.FromValue($"notification-{i}"));
        }
        using (var stop = new CancellationTokenSource(TimeSpan.FromSeconds(20)))
        {
            await using QueueConsumer notifications = await client.SubscribeAsync("example-notify-cs", cancellationToken: stop.Token);
            int seen = 0;
            await foreach (Delivery delivery in notifications.DeliveriesAsync(stop.Token))
            {
                Console.WriteLine($"notified {delivery.Value!.ToUtf8()}");
                await notifications.AckAsync(delivery, stop.Token);
                if (++seen == 3)
                {
                    break;
                }
            }
        }

        // One transaction per poll: the invoices and the group's offsets commit together, or neither does.
        for (int i = 0; i < 10; i++)
        {
            await client.ProduceAsync("example-orders-cs", 0, OutMessage.FromValue($"order-{i}").WithKey($"customer-{i % 3}"));
        }
        GroupConfig config = GroupConfig.CreateBuilder().WithAutoCommit(false).Build();
        await using (GroupConsumer billing = await client.JoinGroupAsync("example-billing-cs", new[] { "example-orders-cs" }, config))
        {
            int invoiced = 0;
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(20));
            while (invoiced < 10 && !deadline.IsCancellationRequested)
            {
                var orders = await billing.PollAsync(TimeSpan.FromMilliseconds(500), deadline.Token);
                if (orders.Count == 0)
                {
                    continue;
                }
                try
                {
                    await using Transaction txn = await client.BeginTransactionAsync(deadline.Token);
                    foreach (GroupRecord order in orders)
                    {
                        await txn.ProduceAsync("example-invoices-cs", 0, OutMessage.FromValue($"invoice for {order.Record.Value!.ToUtf8()}"), deadline.Token);
                    }
                    await txn.SendOffsetsAsync(billing, deadline.Token);
                    await txn.CommitAsync(deadline.Token);
                    invoiced += orders.Count;
                    Console.WriteLine($"invoiced {invoiced} orders");
                }
                catch (ClientException e)
                {
                    // The block's await using has aborted the transaction: read the orders again.
                    Console.WriteLine($"transaction failed, reading again: {e.Message}");
                    billing.Rewind();
                }
            }
        }

        // A group's records as an IAsyncEnumerable: a new group starts at the log's start.
        using (var stop = new CancellationTokenSource(TimeSpan.FromSeconds(20)))
        {
            await using GroupConsumer audit = await client.JoinGroupAsync("example-audit-cs", new[] { "example-invoices-cs" });
            int read = 0;
            await foreach (GroupRecord invoice in audit.RecordsAsync(stop.Token))
            {
                Console.WriteLine(invoice.Record.Value!.ToUtf8());
                if (++read == 10)
                {
                    break;
                }
            }
        }
    }

    private static async Task CreateStreamAsync(LotseClient client, string name)
    {
        try
        {
            await client.CreateStreamAsync(name, partitions: 1, replication: 3);
        }
        catch (ResourceExistsException)
        {
            // made by an earlier run
        }
    }
}
