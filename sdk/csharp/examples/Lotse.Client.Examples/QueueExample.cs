using System;
using System.Threading.Tasks;
using Lotse.Client;

namespace Lotse.Examples;

/// <summary>
/// A work queue with a dead-letter queue: jobs that fail are tried again after a delay, and a job that fails three
/// times moves to the dead-letter queue.
/// </summary>
internal static class QueueExample
{
    public static async Task RunAsync()
    {
        using var client = new LotseClient(Env.Config().Build());
        await CreateIfMissingAsync(client, "example-jobs-dead", QueueOptions.Defaults);
        await CreateIfMissingAsync(client, "example-jobs", QueueOptions.CreateBuilder()
            .WithMaxDeliveries(3)
            .WithDeadLetterQueue("example-jobs-dead")
            .WithAckTimeout(TimeSpan.FromSeconds(30))
            .Build());
        for (int i = 0; i < 5; i++)
        {
            await client.ProduceAsync("example-jobs", 0, OutMessage.FromValue($"job-{i}"));
        }
        await using (QueueConsumer jobs = await client.SubscribeAsync("example-jobs", SubscribeOptions.WithCredit(16)))
        {
            while (true)
            {
                Delivery? job = await jobs.PollAsync(TimeSpan.FromSeconds(5));
                if (job is null)
                {
                    break; // nothing for 5 s: done
                }
                string name = job.Value!.ToUtf8();
                if (name == "job-3")
                {
                    // Fails every time: back in one second, until max.deliveries retires it.
                    Console.WriteLine($"{name} failed (delivery {job.DeliveryCount})");
                    await jobs.NackAfterAsync(job, TimeSpan.FromSeconds(1));
                    continue;
                }
                Console.WriteLine($"done {name}");
                await jobs.AckAsync(job);
            }
        }
        // The retired job, with where it came from and why.
        await using (QueueConsumer dead = await client.SubscribeAsync("example-jobs-dead"))
        {
            Delivery? job = await dead.PollAsync(TimeSpan.FromSeconds(5));
            if (job is not null)
            {
                Console.WriteLine($"dead letter {job.Value!.ToUtf8()}: {job.DeadLetter?.Reason ?? "?"} after {job.DeadLetter?.Deliveries ?? 0} deliveries");
                await dead.AckAsync(job);
            }
        }
    }

    private static async Task CreateIfMissingAsync(LotseClient client, string name, QueueOptions options)
    {
        try
        {
            await client.CreateQueueAsync(name, options);
        }
        catch (ResourceExistsException)
        {
            // made by an earlier run
        }
    }
}
