using System.Text.Json;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Soenneker.OpenApi.Fixer;
using Soenneker.OpenApi.Fixer.Abstract;
using Soenneker.OpenApi.Fixer.Registrars;

if (args.Length < 2)
    throw new ArgumentException("Usage: fix <source> <destination> | repair <generated-directory> | audit <specification> <generated-directory>");
if (args[0] == "audit")
{
    await GeneratedCodeAudit.Run(args[1], args[2]);
    return;
}
var services = new ServiceCollection();
services.AddLogging(b => b.AddConsole().SetMinimumLevel(LogLevel.Warning));
services.AddSingleton<IConfiguration>(new ConfigurationBuilder().Build());
services.AddOpenApiFixerAsSingleton();
await using ServiceProvider provider = services.BuildServiceProvider();
IOpenApiFixer fixer = provider.GetRequiredService<IOpenApiFixer>();
if (args[0] == "fix")
    await fixer.Fix(args[1], args[2], new OpenApiFixerOptions { Int32IdTransform = true });
else if (args[0] == "repair")
{
    OpenApiGeneratedClientRepairReport report = await fixer.RepairGeneratedClient(args[1]);
    File.WriteAllText(Path.Combine(args[1], "repair-report.json"), JsonSerializer.Serialize(report));
    if (report.Warnings.Count > 0)
        throw new InvalidOperationException(string.Join("; ", report.Warnings));
    Dictionary<string, string> before = Snapshot(args[1]);
    await fixer.SanitizeGeneratedEnumMembers(args[1]);
    Dictionary<string, string> after = Snapshot(args[1]);
    string[] changed = before.Keys.Union(after.Keys).Where(p => before.GetValueOrDefault(p) != after.GetValueOrDefault(p)).ToArray();
    foreach (string path in changed)
        File.WriteAllText(path + ".before", before.GetValueOrDefault(path));
    File.WriteAllText(Path.Combine(args[1], "repair-verification.json"), JsonSerializer.Serialize(new { files = after.Count, nonIdempotent = changed }));
    if (changed.Length != 0)
        throw new InvalidOperationException("Generated repair is not idempotent: " + string.Join(", ", changed.Take(20)));
}
else throw new ArgumentException("Unknown command: " + args[0]);

static Dictionary<string, string> Snapshot(string directory) => Directory.EnumerateFiles(directory, "*.cs", SearchOption.AllDirectories)
    .ToDictionary(p => p, File.ReadAllText);
