using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static string HoistNegationOnlyAllOfBranches(string json)
    {
        JsonNode root = JsonNode.Parse(json)!;
        bool changed = OpenApiJsonSchemaWalker.Visit(root, (schema, _) =>
        {
            if (schema["type"]?.ToString() != "object" || schema["allOf"] is not JsonArray branches)
                return false;

            bool hoisted = false;
            foreach (JsonObject branch in branches.OfType<JsonObject>().ToList())
            {
                // Kiota interprets allOf entries as base models. A negation-only entry has
                // no model shape; the same constraint can live directly on the object.
                // Do not overwrite an existing constraint or discard branch annotations.
                if (!branch.ContainsKey("not") || !branch.All(p => p.Key is "not" or "title" or "description" or
                        "deprecated" or "readOnly" or "writeOnly" or "example" or "examples" or "default" or "xml" or "externalDocs" ||
                        p.Key.StartsWith("x-", StringComparison.Ordinal)) ||
                    branch.Any(p => schema.ContainsKey(p.Key) && !JsonNode.DeepEquals(schema[p.Key], p.Value)))
                    continue;

                foreach ((string key, JsonNode? value) in branch)
                    schema[key] = value?.DeepClone();
                branches.Remove(branch);
                hoisted = true;
            }

            if (branches.Count == 0)
                schema.Remove("allOf");
            return hoisted;
        });
        return changed ? root.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) : json;
    }

    private static string CollapseAnnotationOnlyAllOfWrappers(string json)
    {
        JsonNode root = JsonNode.Parse(json)!;
        JsonObject? components = root["components"]?["schemas"] as JsonObject;
        var cycleCache = new Dictionary<string, bool>(StringComparer.Ordinal);
        bool HasReferenceCycle(JsonNode? node, HashSet<string> active)
        {
            if (node is JsonObject obj)
            {
                if (obj["$ref"] is JsonValue reference && reference.TryGetValue(out string? id) &&
                    id.StartsWith("#/components/schemas/", StringComparison.Ordinal))
                {
                    if (cycleCache.TryGetValue(id, out bool cached))
                    {
                        if (cached) return true;
                    }
                    else
                    {
                        if (!active.Add(id)) return true;
                        bool cycle = HasReferenceCycle(components?[id[21..].Replace("~1", "/").Replace("~0", "~")], active);
                        active.Remove(id);
                        cycleCache[id] = cycle;
                        if (cycle) return true;
                    }
                }
                return obj.Any(p => HasReferenceCycle(p.Value, active));
            }
            return node is JsonArray array && array.Any(child => HasReferenceCycle(child, active));
        }
        bool changed = OpenApiJsonSchemaWalker.Visit(root, (schema, _) =>
        {
            var seen = new HashSet<string>(StringComparer.Ordinal);
            bool collapsed = false;
            while (schema["allOf"] is JsonArray { Count: 1 } branches && branches[0] is JsonObject branch)
            {
                if (!schema.All(p => p.Key is "allOf" or "unevaluatedProperties" or "type" or "title" or "description" or "default" or "example" or "examples" or
                        "deprecated" or "readOnly" or "writeOnly" or "xml" or "externalDocs" || p.Key.StartsWith("x-", StringComparison.Ordinal)))
                    break;
                JsonObject? target = branch;
                if (branch["$ref"] is JsonValue reference)
                {
                    string id = reference.GetValue<string>();
                    const string prefix = "#/components/schemas/";
                    if (branch.Count != 1 || !id.StartsWith(prefix, StringComparison.Ordinal) || !seen.Add(id))
                        break;
                    target = components?[id[prefix.Length..].Replace("~1", "/").Replace("~0", "~")] as JsonObject;
                }
                if (target == null || ReferenceEquals(target, schema) || HasReferenceCycle(target, new HashSet<string>(StringComparer.Ordinal)) ||
                    (schema["type"] != null && !JsonNode.DeepEquals(schema["type"], target["type"]) && !(target["type"] is JsonArray types && types.Any(t => JsonNode.DeepEquals(t, schema["type"])))))
                    break;
                if (schema["unevaluatedProperties"] is JsonNode unevaluated)
                {
                    bool closed = target["additionalProperties"] is JsonValue allowed && allowed.TryGetValue(out bool permits) && !permits ||
                                  target["additionalProperties"] is JsonObject closedSchema && closedSchema["not"] is JsonObject { Count: 0 };
                    bool rejects = unevaluated is JsonValue value && value.TryGetValue(out bool accepts) && !accepts ||
                                   unevaluated is JsonObject constraint && constraint["not"] is JsonObject { Count: 0 };
                    if (!closed || !rejects) break;
                }
                JsonObject copy = target.DeepClone().AsObject();
                if (schema["type"] != null) copy.Remove("nullable");
                foreach ((string key, JsonNode? value) in schema)
                    if (key != "allOf") copy[key] = value?.DeepClone();
                schema.Clear();
                foreach ((string key, JsonNode? value) in copy)
                    schema[key] = value?.DeepClone();
                collapsed = true;
            }
            return collapsed;
        });
        return changed ? root.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) : json;
    }
}
