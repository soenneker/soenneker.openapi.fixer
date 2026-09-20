using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    public async ValueTask FixOperationLinks(string directoryPath, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(directoryPath);
        var documents = new List<(string Path, JsonObject Root, List<(JsonObject Node, string Pointer)> Nodes)>();
        var operations = new Dictionary<string, List<(string Path, string Pointer)>>(StringComparer.Ordinal);
        foreach (string path in Directory.EnumerateFiles(Path.GetFullPath(directoryPath), "*.json", new EnumerationOptions
                 { RecurseSubdirectories = true, IgnoreInaccessible = false, AttributesToSkip = FileAttributes.ReparsePoint }).Order(StringComparer.Ordinal))
        {
            cancellationToken.ThrowIfCancellationRequested();
            JsonNode? parsed = JsonNode.Parse(await File.ReadAllTextAsync(path, cancellationToken).ConfigureAwait(false));
            if (parsed is not JsonObject root || !root.ContainsKey("openapi"))
                continue;
            var nodes = new List<(JsonObject Node, string Pointer)>();
            VisitLinkContainers(root, (node, pointer, kind) =>
            {
                if (kind == "operation" && node["operationId"] is JsonValue value && value.TryGetValue(out string? id))
                {
                    if (!operations.TryGetValue(id, out var targets))
                        operations[id] = targets = [];
                    targets.Add((path, pointer));
                }
                if (kind == "links")
                    nodes.Add((node, pointer));
            });
            documents.Add((path, root, nodes));
        }

        foreach (var document in documents)
        {
            bool changed = false;
            foreach (var container in document.Nodes)
            foreach ((string name, JsonNode? node) in container.Node.ToArray())
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (node is not JsonObject link || link.ContainsKey("$ref") || link.ContainsKey("operationRef") ||
                    link["operationId"] is not JsonValue value || !value.TryGetValue(out string? id))
                    continue;
                operations.TryGetValue(id, out var targets);
                var local = targets?.Where(target => target.Path == document.Path).ToArray() ?? [];
                if (local.Length == 1)
                    continue;
                if (local.Length == 0 && targets is { Count: 1 })
                {
                    var target = targets[0];
                    string relative = Path.GetRelativePath(Path.GetDirectoryName(document.Path)!, target.Path).Replace(Path.DirectorySeparatorChar, '/');
                    link.Remove("operationId");
                    link["operationRef"] = string.Join('/', relative.Split('/').Select(Uri.EscapeDataString)) + target.Pointer;
                }
                else
                {
                    container.Node.Remove(name);
                    _logger.LogWarning("Removed unresolved or ambiguous operation link {Link} at {Pointer} in {FilePath}: operationId {OperationId}.",
                        name, container.Pointer, document.Path, id);
                }
                changed = true;
            }
            if (changed)
                await File.WriteAllTextAsync(document.Path, document.Root.ToJsonString(), cancellationToken).ConfigureAwait(false);
        }
    }

    private static void VisitLinkContainers(JsonObject root, Action<JsonObject, string, string> visitor)
    {
        var pending = new Stack<(JsonObject Node, string Pointer, string Kind)>();
        pending.Push((root, "#", "document"));
        while (pending.TryPop(out var entry))
        {
            var (node, pointer, kind) = entry;
            visitor(node, pointer, kind);
            void Add(JsonNode? child, string key, string childKind)
            {
                if (child is JsonObject obj)
                    pending.Push((obj, pointer + "/" + Uri.EscapeDataString(key.Replace("~", "~0", StringComparison.Ordinal).Replace("/", "~1", StringComparison.Ordinal)), childKind));
            }
            void Map(string key, string childKind) => Add(node[key], key, "map:" + childKind);
            switch (kind)
            {
                case "document":
                    Map("paths", "path");
                    Map("webhooks", "path");
                    Add(node["components"], "components", "components");
                    break;
                case "components":
                    Map("pathItems", "path");
                    Map("callbacks", "callback");
                    Map("responses", "response");
                    break;
                case "path":
                    foreach ((string method, JsonNode? operation) in node)
                        if (method is "get" or "put" or "post" or "delete" or "options" or "head" or "patch" or "trace")
                            Add(operation, method, "operation");
                    break;
                case "operation":
                    Map("responses", "response");
                    Map("callbacks", "callback");
                    break;
                case "response":
                    Add(node["links"], "links", "links");
                    break;
                case "callback":
                    foreach ((string key, JsonNode? child) in node)
                        if (key != "$ref" && !key.StartsWith("x-", StringComparison.Ordinal))
                            Add(child, key, "path");
                    break;
                default:
                    if (kind.StartsWith("map:", StringComparison.Ordinal))
                        foreach ((string key, JsonNode? child) in node)
                            if (kind != "map:path" && kind != "map:response" || !key.StartsWith("x-", StringComparison.Ordinal))
                                Add(child, key, kind[4..]);
                    break;
            }
        }
    }
}
