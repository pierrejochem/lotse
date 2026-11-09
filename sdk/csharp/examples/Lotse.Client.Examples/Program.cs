using System;
using System.Threading.Tasks;

namespace Lotse.Examples;

/// <summary>One executable, one command per example: <c>dotnet run --project examples/Lotse.Client.Examples -- stream</c>.</summary>
internal static class Program
{
    private const string Usage = "usage: Lotse.Client.Examples <stream | queue | exactly-once | idiomatic>";

    public static async Task<int> Main(string[] args)
    {
        Func<Task>? example = args.Length == 1
            ? args[0] switch
            {
                "stream" => StreamExample.RunAsync,
                "queue" => QueueExample.RunAsync,
                "exactly-once" => ExactlyOnceExample.RunAsync,
                "idiomatic" => IdiomaticExample.RunAsync,
                _ => null,
            }
            : null;
        if (example is null)
        {
            Console.Error.WriteLine(Usage);
            return 2;
        }
        await example();
        return 0;
    }
}
