using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private async ValueTask<string> BundleLocalSchemaReferences(string json, string sourcePath, CancellationToken cancellationToken)
    {
        if (JsonNode.Parse(json) is not JsonObject root)
            return json;

        StringComparer pathComparer = OperatingSystem.IsWindows() ? StringComparer.OrdinalIgnoreCase : StringComparer.Ordinal;
        var documents = new Dictionary<string, JsonObject>(pathComparer) { [sourcePath] = root };
        var imported = new Dictionary<string, Dictionary<string, string>>(pathComparer);
        var pending = new Queue<(JsonObject Document, string Path, string? Name)>();
        pending.Enqueue((root, sourcePath, null));
        bool changed = false;

        while (pending.TryDequeue(out var entry))
        {
            cancellationToken.ThrowIfCancellationRequested();
            var references = new List<(JsonObject Owner, string Key, string Reference)>();
            OpenApiJsonSchemaWalker.Visit(entry.Document, (schema, _) =>
            {
                if (schema["$ref"] is JsonValue value && value.TryGetValue(out string? reference))
                    references.Add((schema, "$ref", reference));
                if (schema["discriminator"]?["mapping"] is JsonObject mapping)
                    foreach ((string key, JsonNode? node) in mapping)
                        if (node is JsonValue mapped && mapped.TryGetValue(out string? target) && target.Contains('#'))
                            references.Add((mapping, key, target));
                return false;
            });

            foreach (var reference in references)
            {
                cancellationToken.ThrowIfCancellationRequested();
                int hash = reference.Reference.IndexOf('#');
                if (hash < 0 || !reference.Reference[(hash + 1)..].StartsWith("/components/schemas/", StringComparison.Ordinal))
                    continue;
                string location = reference.Reference[..hash];
                if (location.Length == 0 && pathComparer.Equals(entry.Path, sourcePath))
                    continue;
                if (Uri.TryCreate(location, UriKind.Absolute, out Uri? uri) && !uri.IsFile)
                    continue;
                string targetPath = location.Length == 0 ? entry.Path : uri?.IsFile == true ? uri.LocalPath :
                    Path.GetFullPath(Path.Combine(Path.GetDirectoryName(entry.Path)!, Uri.UnescapeDataString(location)));
                string pointer = Uri.UnescapeDataString(reference.Reference[(hash + 1)..]);
                if (pathComparer.Equals(targetPath, sourcePath))
                {
                    reference.Owner[reference.Key] = "#" + pointer;
                    changed = true;
                    continue;
                }
                if (!documents.TryGetValue(targetPath, out JsonObject? document))
                {
                    // Remote and unavailable dependencies remain references for the caller to resolve.
                    if (!File.Exists(targetPath))
                        continue;
                    document = JsonNode.Parse(await File.ReadAllTextAsync(targetPath, cancellationToken).ConfigureAwait(false)) as JsonObject ??
                               throw new InvalidOperationException($"Expected a JSON document in schema dependency '{targetPath}'.");
                    documents.Add(targetPath, document);
                }
                if (!imported.TryGetValue(targetPath, out Dictionary<string, string>? names))
                    imported[targetPath] = names = new Dictionary<string, string>(StringComparer.Ordinal);
                if (!names.TryGetValue(pointer, out string? name))
                {
                    JsonNode? target = document;
                    foreach (string segment in pointer[1..].Split('/'))
                        target = target is JsonObject obj ? obj[segment.Replace("~1", "/", StringComparison.Ordinal).Replace("~0", "~", StringComparison.Ordinal)] : null;
                    if (target is not JsonObject schema)
                        throw new InvalidOperationException($"Unresolved schema reference '{reference.Reference}' in '{entry.Path}'.");
                    JsonObject components = (root["components"] ??= new JsonObject()).AsObject();
                    JsonObject schemas = (components["schemas"] ??= new JsonObject()).AsObject();
                    string baseName = new((Path.GetFileNameWithoutExtension(targetPath) + "_" + pointer.Split('/')[^1])
                        .Select(static ch => char.IsAsciiLetterOrDigit(ch) || ch == '_' ? ch : '_').ToArray());
                    name = baseName;
                    for (int suffix = 2; schemas.ContainsKey(name); suffix++)
                        name = baseName + "_" + suffix;
                    names.Add(pointer, name);
                    var copy = (JsonObject)schema.DeepClone();
                    schemas[name] = copy;
                    // A separate wrapper lets the structural walker visit only this imported schema.
                    var wrapper = new JsonObject { ["components"] = new JsonObject { ["schemas"] = new JsonObject { [name] = copy.DeepClone() } } };
                    pending.Enqueue((wrapper, targetPath, name));
                }
                reference.Owner[reference.Key] = "#/components/schemas/" + name;
                changed = true;
            }
            if (entry.Name != null)
                root["components"]!["schemas"]![entry.Name] = entry.Document["components"]!["schemas"]![entry.Name]!.DeepClone();
        }
        return changed ? root.ToJsonString() : json;
    }
}
