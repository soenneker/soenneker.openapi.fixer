using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static async ValueTask<Dictionary<string, Dictionary<string, int>>> ReadGeneratedRouteAliases(string root, CancellationToken ct)
    {
        var aliases = new Dictionary<string, Dictionary<string, int>>(StringComparer.Ordinal);
        string lockPath = Path.Combine(root, "kiota-lock.json");
        if (!File.Exists(lockPath)) return aliases;
        JsonNode? configuration = JsonNode.Parse(await File.ReadAllTextAsync(lockPath, ct));
        string? location = configuration?["descriptionLocation"]?.GetValue<string>();
        if (string.IsNullOrWhiteSpace(location) || Uri.TryCreate(location, UriKind.Absolute, out Uri? uri) && !uri.IsFile) return aliases;
        string sourcePath = Uri.TryCreate(location, UriKind.Absolute, out Uri? fileUri) && fileUri.IsFile ? fileUri.LocalPath : Path.GetFullPath(Path.Combine(root, location));
        if (!File.Exists(sourcePath)) return aliases;
        // These metadata repairs consume the fixer's JSON output; raw YAML has no fixer annotations.
        if (Path.GetExtension(sourcePath).Equals(".yaml", StringComparison.OrdinalIgnoreCase) ||
            Path.GetExtension(sourcePath).Equals(".yml", StringComparison.OrdinalIgnoreCase)) return aliases;
        JsonNode? document = JsonNode.Parse(await File.ReadAllTextAsync(sourcePath, ct));
        if (document?["paths"] is not JsonObject paths) return aliases;
        foreach ((string path, JsonNode? item) in paths)
            if (item is JsonObject operations)
                foreach ((string method, JsonNode? operation) in operations)
                    if (operation is JsonObject op && op["x-original-trailing-slashes"] is JsonValue value && value.TryGetValue(out int count) && count > 0)
                    {
                        string signature = Regex.Replace(path, @"\{[^}]+\}", "{}");
                        if (!aliases.TryGetValue(signature, out Dictionary<string, int>? methods)) aliases[signature] = methods = new(StringComparer.Ordinal);
                        methods[method.ToUpperInvariant()] = count;
                    }
        return aliases;
    }

    private static string RepairGeneratedRouteAliases(string source, Dictionary<string, Dictionary<string, int>> aliases)
    {
        if (aliases.Count == 0) return source;
        Match template = Regex.Match(source, "base\\(requestAdapter, \"\\{\\+baseurl\\}(?<path>[^\"]+)\"");
        if (!template.Success) return source;
        string path = template.Groups["path"].Value;
        int query = path.IndexOf("{?", StringComparison.Ordinal);
        if (query >= 0) path = path[..query];
        string signature = Regex.Replace(path, @"\{[^}]+\}", "{}");
        if (!aliases.TryGetValue(signature, out Dictionary<string, int>? methods)) return source;
        foreach ((string method, int count) in methods)
        {
            string slashes = new('/', count);
            string replacement = "(UrlTemplate.Contains(\"{?\", global::System.StringComparison.Ordinal) ? UrlTemplate.Insert(UrlTemplate.IndexOf(\"{?\", global::System.StringComparison.Ordinal), \"" +
                slashes + "\") : UrlTemplate + \"" + slashes + "\")";
            source = source.Replace("new RequestInformation(Method." + method + ", UrlTemplate, PathParameters)",
                "new RequestInformation(Method." + method + ", " + replacement + ", PathParameters)", StringComparison.Ordinal);
        }
        return source;
    }
}
