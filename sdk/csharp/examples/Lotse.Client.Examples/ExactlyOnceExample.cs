using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Lotse.Client;

namespace Lotse.Examples;

/// <summary>
/// Consume, transform, produce, exactly once: a group member reads orders, and each batch of invoices it writes
/// commits in one transaction with the group's offsets. A crash between the two leaves neither. Run the stream
/// example first: it writes the orders.
/// </summary>
internal static class ExactlyOnceExample
{
    public static async Task RunAsync()
    {
        // One running client per transactional id: a second one fences the first.
        using var client = new LotseClient(Env.Config().WithTransactionalId("example-billing-1").Build());
        try
        {
            await client.CreateStreamAsync("example-invoices", partitions: 1, replication: 3);
        }
        catch (ResourceExistsException)
        {
            // made by an earlier run
        }
        GroupConfig config = GroupConfig.CreateBuilder().WithAutoCommit(false).Build(); // offsets go with the transaction
        await using GroupConsumer group = await client.JoinGroupAsync("example-billing", new[] { "example-orders" }, config);
        Stopwatch clock = Stopwatch.StartNew();
        while (clock.Elapsed < TimeSpan.FromSeconds(20))
        {
            IReadOnlyList<GroupRecord> orders = await group.PollAsync(TimeSpan.FromMilliseconds(500)); // heartbeats too: call it often
            if (orders.Count == 0)
            {
                continue;
            }
            try
            {
                await using Transaction txn = await client.BeginTransactionAsync(); // leaving the block uncommitted aborts
                foreach (GroupRecord order in orders)
                {
                    await txn.ProduceAsync("example-invoices", 0, OutMessage.FromValue($"invoice for {order.Record.Value!.ToUtf8()}"));
                }
                await txn.SendOffsetsAsync(group);
                await txn.CommitAsync();
                Console.WriteLine($"invoiced {orders.Count} orders");
            }
            catch (ClientException e)
            {
                // Not committed: read the same orders again from the group's committed offsets.
                Console.WriteLine($"transaction failed, reading again: {e.Message}");
                group.Rewind();
            }
        }
    }
}
