using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static string NormalizeClientSchemaCleanup(string json, OpenApiFixerOptions options)
    {
        JsonNode root = JsonNode.Parse(json)!;
        bool changed = false;
        if (options.SelectPrimaryServer && (root["servers"] is null or JsonArray { Count: 0 }) && root["paths"] is JsonObject paths)
        {
            JsonObject? primary = null;
            bool fullyScoped = true;
            foreach (JsonObject item in paths.Select(p => p.Value).OfType<JsonObject>())
                foreach ((string verb, JsonNode? node) in item)
                {
                    if (verb is not ("get" or "put" or "post" or "delete" or "options" or "head" or "patch" or "trace") || node is not JsonObject operation) continue;
                    JsonArray? scoped = (operation["servers"] as JsonArray is { Count: > 0 } operationServers ? operationServers : item["servers"] as JsonArray);
                    if (scoped is not { Count: > 0 } || scoped[0] is not JsonObject server || server["url"] is not JsonValue)
                    {
                        fullyScoped = false;
                        continue;
                    }
                    primary ??= server;
                }
            // A root default cannot change these operations: each already overrides it. Generated
            // service routing retains their individual hosts and expands server variable defaults.
            if (fullyScoped && primary != null)
            {
                root["servers"] = new JsonArray(primary.DeepClone());
                changed = true;
            }
        }
        if (options.SelectPrimaryServer && root["servers"] is JsonArray { Count: > 1 } servers)
        {
            root["x-client-original-servers"] ??= servers.DeepClone();
            root["servers"] = new JsonArray(servers[0]!.DeepClone());
            changed = true;
        }
        if (!options.ArchiveUnusedSchemas || root["components"]?["schemas"] is not JsonObject schemas ||
            root["paths"] is not JsonObject { Count: > 0 } && root["webhooks"] is not JsonObject { Count: > 0 })
            return changed ? root.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) : json;

        var used = new HashSet<string>(StringComparer.Ordinal);
        var pending = new Queue<JsonNode>();
        bool hasAnchorReference = false;
        void Reference(string? reference)
        {
            const string prefix = "#/components/schemas/";
            if (reference == null) return;
            if (reference.StartsWith('#') && !reference.StartsWith("#/", StringComparison.Ordinal))
            {
                // Resolving dynamic/recursive anchors needs JSON Schema scope tracking. Keep the catalog
                // intact rather than archive a schema that might supply an anchor in another scope.
                hasAnchorReference = true;
                return;
            }
            string key = reference.StartsWith(prefix, StringComparison.Ordinal)
                ? reference[prefix.Length..].Split('/')[0].Replace("~1", "/").Replace("~0", "~") : reference;
            if (schemas[key] is JsonNode schema && used.Add(key)) pending.Enqueue(schema);
        }
        void Visit(JsonNode? node)
        {
            if (ReferenceEquals(node, schemas)) return;
            if (node is JsonObject obj)
            {
                foreach (string keyword in new[] { "$ref", "$dynamicRef", "$recursiveRef" })
                    if (obj[keyword] is JsonValue reference && reference.TryGetValue(out string? id)) Reference(id);
                if (obj["discriminator"] is JsonObject discriminator && discriminator["mapping"] is JsonObject mapping)
                    foreach (JsonValue value in mapping.Select(p => p.Value).OfType<JsonValue>())
                        if (value.TryGetValue(out string? target)) Reference(target);
                foreach ((string key, JsonNode? child) in obj)
                    if (!key.StartsWith("x-client-original-", StringComparison.Ordinal) && key != "x-client-unused-schemas") Visit(child);
            }
            else if (node is JsonArray array)
                foreach (JsonNode? child in array) Visit(child);
        }
        Visit(root);
        while (pending.TryDequeue(out JsonNode? schema)) Visit(schema);
        if (hasAnchorReference) return changed ? root.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) : json;
        JsonObject? archive = root["x-client-unused-schemas"] as JsonObject;
        foreach ((string name, JsonNode? schema) in schemas.ToArray())
        {
            if (used.Contains(name)) continue;
            if (archive == null) root["x-client-unused-schemas"] = archive = new JsonObject();
            archive[name] = schema?.DeepClone();
            schemas.Remove(name);
            changed = true;
        }
        return changed ? root.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) : json;
    }
}
