using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Lotse.Net;
using Lotse.Proto;
using Lotse.Record;
using Xunit;

namespace Lotse.Client.Tests.Client;

/// <summary>
/// Three real <c>lotse</c> processes on loopback (plain TCP, no authentication) and a client that creates streams,
/// produces, fetches, and keeps going when the leader of a partition is killed. Skipped, with a message, when
/// <c>LOTSE_BIN</c> is not set.
/// </summary>
[Collection("real cluster")]
public sealed class RealClusterTests : IClassFixture<RealCluster>, IAsyncLifetime
{
    private readonly RealCluster _cluster;
    private LotseClient? _client;

    public RealClusterTests(RealCluster cluster) => _cluster = cluster;

    public Task InitializeAsync()
    {
        if (_cluster.Available)
        {
            _client = new LotseClient(ClientConfig.CreateBuilder(_cluster.Bootstrap)
                .PlaintextOnLoopback()
                .WithRequestTimeout(TimeSpan.FromSeconds(5))
                .WithRetryFor(TimeSpan.FromSeconds(60))
                .Build());
        }
        return Task.CompletedTask;
    }

    public Task DisposeAsync()
    {
        _client?.Dispose();
        return Task.CompletedTask;
    }

    private LotseClient Client => _client!;

    private async Task<int> LeaderOfAsync(string stream, int partition)
    {
        DateTime deadline = DateTime.UtcNow.AddSeconds(60);
        while (DateTime.UtcNow < deadline)
        {
            Response.Metadata m = await Client.MetadataAsync(new[] { stream });
            foreach (Messages.ResourceMetadata r in m.Resources)
            {
                foreach (Messages.PartitionMetadata p in r.Partitions)
                {
                    if (r.Name == stream && p.Partition == (uint)partition && p.Leader != 0)
                    {
                        return (int)p.Leader;
                    }
                }
            }
            await Task.Delay(100);
        }
        throw new Xunit.Sdk.XunitException($"no leader for {stream}/{partition}\n{_cluster.Log(1)}");
    }

    /// <summary>Reads a partition from its start until <paramref name="count"/> records are seen.</summary>
    private async Task<List<BatchRecord>> ReadAllAsync(string stream, int partition, int count, LotseClient? reader = null)
    {
        reader ??= Client;
        var output = new List<BatchRecord>();
        ulong offset = 0;
        DateTime deadline = DateTime.UtcNow.AddSeconds(60);
        while (output.Count < count && DateTime.UtcNow < deadline)
        {
            Fetched f = await reader.FetchAsync(stream, partition, offset, TimeSpan.FromMilliseconds(200));
            output.AddRange(f.CommittedMessages());
            offset = f.NextOffset(offset);
        }
        return output;
    }

    private static List<string> Texts(IEnumerable<BatchRecord> records) => records.Select(r => r.Value!.ToUtf8()).ToList();

    [LotseFact]
    public async Task AStreamIsCreatedProducedToAndReadBack()
    {
        ulong id = await Client.CreateStreamAsync("orders", 2, 3);
        Assert.NotEqual(0UL, id);
        var again = await Assert.ThrowsAsync<ResourceExistsException>(() => Client.CreateStreamAsync("orders", 2, 3));
        Assert.Equal(id, again.ResourceId); // creating it again says which one exists

        long before = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        var offsets = new List<ulong>();
        for (int i = 0; i < 10; i++)
        {
            ProduceResult r = await Client.ProduceAsync(
                "orders", i % 2, OutMessage.FromValue("order-" + i).WithKey("k" + i).WithHeader("n", i.ToString()));
            offsets.Add(r.BaseOffset);
        }
        long after = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

        for (int partition = 0; partition < 2; partition++)
        {
            List<BatchRecord> got = await ReadAllAsync("orders", partition, 5);
            Assert.Equal(5, got.Count);
            for (int j = 0; j < 5; j++)
            {
                int i = 2 * j + partition;
                BatchRecord rec = got[j];
                Assert.Equal("order-" + i, rec.Value!.ToUtf8());
                Assert.Equal("k" + i, rec.Key!.ToUtf8());
                Assert.Equal(i.ToString(), rec.Headers[0].Value!.ToUtf8());
                Assert.InRange(rec.Timestamp, before, after); // stamped with the time of the produce
                Assert.Equal(offsets[i], rec.Offset); // the offset the produce returned
            }
        }
    }

    [LotseFact]
    public async Task ALongPollWaitsAtTheBrokerAndAnOffsetPastTheEndIsOutOfRange()
    {
        await Client.CreateStreamAsync("poll", 1, 3);
        await Client.ProduceAsync("poll", 0, OutMessage.FromValue("one"));
        // The log holds control entries as well as records, so the end is found by reading, not by counting.
        ulong end = 0;
        for (Fetched f = await Client.FetchAsync("poll", 0, 0, TimeSpan.FromMilliseconds(50));
             !f.Records.IsEmpty;
             f = await Client.FetchAsync("poll", 0, end, TimeSpan.FromMilliseconds(50)))
        {
            end = f.NextOffset(end);
        }
        var clock = Stopwatch.StartNew();
        Fetched waited = await Client.FetchAsync("poll", 0, end, TimeSpan.FromMilliseconds(300));
        // The broker may append an entry of its own meanwhile (a membership change), which ends the wait early:
        // either something came, or the broker held the request for the whole wait.
        Assert.True(!waited.Records.IsEmpty || clock.ElapsedMilliseconds >= 200, $"returned empty after only {clock.ElapsedMilliseconds} ms");
        if (waited.Records.IsEmpty)
        {
            Assert.Equal(end, waited.NextOffset(end)); // nothing read: stays where it was
        }

        var e = await Assert.ThrowsAsync<OffsetOutOfRangeException>(
            () => Client.FetchAsync("poll", 0, 1_000_000, TimeSpan.FromMilliseconds(10)));
        Assert.Equal(0UL, e.LogStart);
    }

    [LotseFact]
    public async Task AnUnknownStreamIsReportedAtOnce()
    {
        var clock = Stopwatch.StartNew();
        await Assert.ThrowsAsync<UnknownStreamException>(() => Client.ProduceAsync("nope", 0, OutMessage.FromValue("x")));
        Assert.True(clock.Elapsed < TimeSpan.FromSeconds(10));
    }

    [LotseFact]
    public async Task ListOffsetsFindsTheStartTheEndAndATime()
    {
        // The end of a log moves when a new leader appends a note of its own term, so the scenario runs again, with a
        // new stream, if a leader moved while it ran; with every leader where it was, a failure is a failure.
        await RealWaits.RetryIfLeaderMovedAsync(Client, async (steady, attempt) =>
        {
            string offsets = attempt == 1 ? "offsets" : "offsets-" + attempt;
            await Client.CreateStreamAsync(offsets, 1, 3);
            await RealWaits.PinLeaderAsync(Client, offsets);
            await steady.WatchAsync(offsets);
            long t0 = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            ProduceResult a = await Client.ProduceAsync(offsets, 0, OutMessage.FromValue("a").WithTimestamp(t0 + 1_000));
            ProduceResult b = await Client.ProduceAsync(offsets, 0, OutMessage.FromValue("b").WithTimestamp(t0 + 5_000));
            ProduceResult c = await Client.ProduceAsync(offsets, 0, OutMessage.FromValue("c").WithTimestamp(t0 + 9_000));

            Assert.Equal(0UL, await Client.ListOffsetAsync(offsets, 0, OffsetAt.Earliest));
            Assert.Equal(c.BaseOffset + 1, await Client.ListOffsetAsync(offsets, 0, OffsetAt.Latest));
            // Batches are found by their newest record: the first with a record at or after the time.
            Assert.Equal(a.BaseOffset, await Client.ListOffsetAsync(offsets, 0, OffsetAt.Time(t0)));
            Assert.Equal(a.BaseOffset, await Client.ListOffsetAsync(offsets, 0, OffsetAt.Time(t0 + 1_000)));
            Assert.Equal(b.BaseOffset, await Client.ListOffsetAsync(offsets, 0, OffsetAt.Time(t0 + 1_001)));
            Assert.Equal(b.BaseOffset, await Client.ListOffsetAsync(offsets, 0, OffsetAt.Time(t0 + 5_000)));
            Assert.Equal(c.BaseOffset, await Client.ListOffsetAsync(offsets, 0, OffsetAt.Time(t0 + 9_000)));
            // No batch that new yet: the latest offset.
            Assert.Equal(c.BaseOffset + 1, await Client.ListOffsetAsync(offsets, 0, OffsetAt.Time(t0 + 9_001)));
        });
    }

    [LotseFact]
    public async Task ASentSequenceIsRecognisedAgainSoARetryCannotWriteTwice()
    {
        await Client.CreateStreamAsync("dedupe", 1, 3);
        int leader = await LeaderOfAsync("dedupe", 0);
        Response.Metadata m = await Client.MetadataAsync(new[] { "dedupe" });
        ulong resource = m.Resources.First(r => r.Name == "dedupe").ResourceId;
        var options = ConnectionOptions.CreateBuilder().PlaintextOnLoopback().Build();
        await using Connection conn = await Connection.OpenAsync(_cluster.AddressOf(leader), options);

        Response.FindCoordinator find = await conn.CallAsync<Response.FindCoordinator>(
            new Request.FindCoordinator(Messages.CoordinatorType.Producer, string.Empty));
        await using Connection coordinator = await Connection.OpenAsync(new Address(find.Host, find.Port), options);
        Response.InitProducer ids = await coordinator.CallAsync<Response.InitProducer>(new Request.InitProducer(null, 60_000));
        Assert.True(ids.Error.IsOk);

        var builder = new BatchBuilder();
        builder.Push(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(), (byte[]?)null, new byte[] { 1 });
        byte[] bytes = builder.Finish().ToByteArray();
        Batch.SetProducer(bytes, ids.ProducerId, ids.ProducerEpoch, 0);
        var request = new Request.Produce(
            null, 5_000, new[] { new Messages.ProduceResource(resource, new[] { new Messages.ProducePartition(0, ByteString.View(bytes, 0, bytes.Length)) }) });

        ulong first = (await conn.CallAsync<Response.Produce>(request)).Resources[0].Partitions[0].BaseOffset;
        Messages.ProducePartitionResponse again = (await conn.CallAsync<Response.Produce>(request)).Resources[0].Partitions[0];
        Assert.True(again.Error.IsOk);
        Assert.Equal(first, again.BaseOffset); // the leader recognised the batch and answered with its offset
        Assert.Single(await ReadAllAsync("dedupe", 0, 1));
        await Task.Delay(300);
        Assert.Single((await Client.FetchAsync("dedupe", 0, 0, TimeSpan.FromMilliseconds(50))).CommittedMessages());
    }

    [LotseFact]
    public async Task KillingTheLeaderMidStreamNeitherLosesNorDuplicatesAnIdempotentProduce()
    {
        await Client.CreateStreamAsync("idem", 1, 3);
        int leader = await LeaderOfAsync("idem", 0);
        const int Total = 150;
        int done = 0;
        Task killer = Task.Run(async () =>
        {
            while (Volatile.Read(ref done) < 40)
            {
                await Task.Delay(2);
            }
            await _cluster.KillAsync(leader);
        });
        var offsets = new List<ulong>();
        for (int i = 0; i < Total; i++)
        {
            offsets.Add((await Client.ProduceAsync("idem", 0, OutMessage.FromValue("m-" + i))).BaseOffset);
            Interlocked.Increment(ref done);
        }
        await killer;

        List<string> values = Texts(await ReadAllAsync("idem", 0, Total));
        Assert.Equal(Enumerable.Range(0, Total).Select(i => "m-" + i).ToList(), values); // every message once, in order
        Assert.Equal(Total, offsets.Distinct().Count()); // each produce got its own offset
        for (int i = 1; i < Total; i++)
        {
            Assert.True(offsets[i] > offsets[i - 1], "offsets rise");
        }
        Assert.NotEqual(leader, await LeaderOfAsync("idem", 0)); // another node leads now
        await _cluster.RestartAsync(leader);
    }

    [LotseFact]
    public async Task AClientFollowsTheNewLeaderAfterAFailover()
    {
        await Client.CreateStreamAsync("failover", 1, 3);
        await Client.ProduceAsync("failover", 0, OutMessage.FromValue("before"));
        int old = await LeaderOfAsync("failover", 0);
        Assert.Equal((uint)old, Client.RouteOf("failover", 0)!.Value.Leader);

        await _cluster.KillAsync(old);
        await Client.ProduceAsync("failover", 0, OutMessage.FromValue("after"));
        Assert.NotEqual((uint)old, Client.RouteOf("failover", 0)!.Value.Leader); // the route moved to the new leader

        Assert.Equal(new[] { "before", "after" }, Texts(await ReadAllAsync("failover", 0, 2)));
        await _cluster.RestartAsync(old);
        await Client.ProduceAsync("failover", 0, OutMessage.FromValue("again"));
        Assert.Equal(3, (await ReadAllAsync("failover", 0, 3)).Count);
    }

    [LotseFact]
    public async Task AClientWithoutIdempotenceWritesTooAndNeverWritesTwice()
    {
        await Client.CreateStreamAsync("plain", 1, 3);
        using var plain = new LotseClient(ClientConfig.CreateBuilder(_cluster.Bootstrap)
            .PlaintextOnLoopback()
            .WithIdempotence(false)
            .WithRetryFor(TimeSpan.FromSeconds(60))
            .Build());
        for (int i = 0; i < 5; i++)
        {
            await plain.ProduceAsync("plain", 0, OutMessage.FromValue("p-" + i));
        }
        Assert.Equal(new[] { "p-0", "p-1", "p-2", "p-3", "p-4" }, Texts(await ReadAllAsync("plain", 0, 5)));
    }

    // ---- compression against a real node (ADR 0058, ADR 0084) ----

    private static ByteString Compressed(Compression codec, string tag, int records)
    {
        var b = new BatchBuilder(new BatchOptions(new Attributes(codec, false, false, BatchKind.Data), 0, 0, -1));
        long now = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        for (int i = 0; i < records; i++)
        {
            b.Push(now, System.Text.Encoding.UTF8.GetBytes($"{tag}-key-{i}"), System.Text.Encoding.UTF8.GetBytes($"{tag}-" + new string('x', 5_000) + i));
        }
        return b.Finish();
    }

    [LotseFact]
    public async Task CompressedBatchesRoundTripThroughARealNodeAndComeBackCompressed()
    {
        await Client.CreateStreamAsync("packed", 1, 3);
        // The leader decodes each compressed batch once when it is produced and refuses one that does not decode,
        // so an accepted batch is proof that the frames written here are ones the Rust decoders read.
        await Client.ProduceBatchAsync("packed", 0, Compressed(Compression.Zstd, "z", 3));
        await Client.ProduceBatchAsync("packed", 0, Compressed(Compression.Lz4, "l", 3));
        await Client.ProduceAsync("packed", 0, OutMessage.FromValue("plain"));

        var batches = new List<Batch>();
        ulong offset = 0;
        for (int i = 0; i < 100 && batches.Sum(b => (int)b.RecordCount) < 7; i++)
        {
            Fetched f = await Client.FetchAsync("packed", 0, offset, TimeSpan.FromMilliseconds(200));
            batches.AddRange(f.CommittedBatches());
            offset = f.NextOffset(offset);
        }
        // Served as the producer sent them.
        Assert.Equal(
            new[] { Compression.Zstd, Compression.Lz4, Compression.None },
            batches.Select(b => b.Attributes.Compression).ToArray());
        Assert.True(batches[0].Length < 3 * 5_000 / 10);
        List<BatchRecord> records = batches.SelectMany(b => b.Records()).ToList();
        Assert.Equal(7, records.Count);
        for (int i = 0; i < 3; i++)
        {
            Assert.Equal($"z-key-{i}", records[i].Key!.ToUtf8());
            Assert.Equal($"z-" + new string('x', 5_000) + i, records[i].Value!.ToUtf8());
            Assert.Equal($"l-key-{i}", records[3 + i].Key!.ToUtf8());
            Assert.Equal($"l-" + new string('x', 5_000) + i, records[3 + i].Value!.ToUtf8());
        }
        Assert.Equal("plain", records[6].Value!.ToUtf8());
        Assert.Equal(Enumerable.Range(0, 7).Select(i => records[0].Offset + (ulong)i).ToArray(), records.Select(r => r.Offset).ToArray());
    }

    [LotseFact]
    public async Task ABatchWhoseBlockDoesNotDecodeIsRefusedByTheNode()
    {
        await Client.CreateStreamAsync("garbled", 1, 3);
        foreach (Compression codec in new[] { Compression.Zstd, Compression.Lz4 })
        {
            byte[] bytes = Compressed(codec, "g", 2).ToByteArray();
            Array.Fill(bytes, (byte)0x5a, Batch.HeaderLen + 6, bytes.Length - Batch.HeaderLen - 6);
            Batch.PutU32(bytes, 21, Batch.Crc(bytes, 25, bytes.Length)); // the checksum is right; the block is not a frame
            var e = await Assert.ThrowsAsync<BrokerErrorException>(
                () => Client.ProduceBatchAsync("garbled", 0, ByteString.View(bytes, 0, bytes.Length)));
            Assert.Equal(ErrorCode.CorruptBatch, e.Code);
        }
    }
}
