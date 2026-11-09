using System;
using System.Threading.Tasks;
using Lotse.Client;
using Lotse.Record;

namespace Lotse.Examples;

/// <summary>Creates a stream, writes ten messages to it and reads them back.</summary>
internal static class StreamExample
{
    public static async Task RunAsync()
    {
        using var client = new LotseClient(Env.Config().Build());
        try
        {
            await client.CreateStreamAsync("example-orders", partitions: 2, replication: 3);
        }
        catch (ResourceExistsException)
        {
            // made by an earlier run
        }
        for (int i = 0; i < 10; i++)
        {
            // The partition is the caller's to choose; here, by key.
            string customer = $"customer-{i % 3}";
            int partition = (int)(JavaHash(customer) % 2 + 2) % 2;
            ProduceResult r = await client.ProduceAsync(
                "example-orders", partition, OutMessage.FromValue($"order-{i}").WithKey(customer));
            Console.WriteLine($"order-{i} at partition {partition}, offset {r.BaseOffset}");
        }
        for (int partition = 0; partition < 2; partition++)
        {
            ulong offset = 0;
            while (true)
            {
                Fetched f = await client.FetchAsync("example-orders", partition, offset, TimeSpan.FromMilliseconds(200));
                foreach (BatchRecord record in f.CommittedMessages())
                {
                    Console.WriteLine($"read {record.Value!.ToUtf8()} from partition {partition}");
                }
                ulong next = f.NextOffset(offset);
                if (next == offset)
                {
                    break; // nothing more for now
                }
                offset = next;
            }
        }
    }

    // Java's String.hashCode, so the partitions match the Java and Kotlin examples' and a stream written by one is read by the other.
    private static long JavaHash(string s)
    {
        int h = 0;
        foreach (char c in s)
        {
            h = unchecked(31 * h + c);
        }
        return h;
    }
}
