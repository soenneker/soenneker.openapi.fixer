using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    public async ValueTask<OpenApiGeneratedClientRepairReport> RepairGeneratedClient(string generatedRoot, CancellationToken cancellationToken = default)
    {
        var warnings = new List<string>();
        if (!Directory.Exists(generatedRoot))
            return new OpenApiGeneratedClientRepairReport { FileCount = 0, ChangedFiles = [], Warnings = ["Generated directory does not exist."] };
        if (await ReadGeneratedUnionSchemas(generatedRoot, cancellationToken) == null)
            warnings.Add("Local schema metadata could not be loaded from kiota-lock.json; schema-dependent repairs are unavailable.");
        Dictionary<string, string> before = await HashGeneratedFiles(generatedRoot, cancellationToken);
        await RepairGeneratedClientCore(generatedRoot, cancellationToken);
        Dictionary<string, string> after = await HashGeneratedFiles(generatedRoot, cancellationToken);
        return new OpenApiGeneratedClientRepairReport
        {
            FileCount = after.Count,
            ChangedFiles = after.Where(p => !before.TryGetValue(p.Key, out string? hash) || hash != p.Value)
                .Select(p => Path.GetRelativePath(generatedRoot, p.Key)).Order(StringComparer.Ordinal).ToArray(),
            Warnings = warnings
        };
    }

    private static async ValueTask<Dictionary<string, string>> HashGeneratedFiles(string root, CancellationToken cancellationToken)
    {
        var result = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (string path in Directory.EnumerateFiles(root, "*.cs", SearchOption.AllDirectories))
        {
            cancellationToken.ThrowIfCancellationRequested();
            await using FileStream stream = File.OpenRead(path);
            result[path] = Convert.ToHexString(await SHA256.HashDataAsync(stream, cancellationToken));
        }
        return result;
    }
}
