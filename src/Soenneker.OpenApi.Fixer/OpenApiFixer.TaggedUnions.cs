using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static string ProjectTaggedObjectUnions(string json)
    {
        JsonNode root = JsonNode.Parse(json)!;
        if (root["components"]?["schemas"] is not JsonObject schemas) return json;
        var owned = new HashSet<string>(StringComparer.Ordinal);
        bool changed = false;
        JsonObject? Resolve(JsonNode? node)
        {
            var seen = new HashSet<string>(StringComparer.Ordinal);
            while (node is JsonObject reference && reference["$ref"] is JsonValue id)
            {
                if (!reference.All(p => p.Key is "$ref" or "title" or "description" || p.Key.StartsWith("x-", StringComparison.Ordinal)) ||
                    !id.TryGetValue(out string? path) || !path.StartsWith("#/components/schemas/", StringComparison.Ordinal) || !seen.Add(path)) return null;
                node = schemas[path[21..].Replace("~1", "/").Replace("~0", "~")];
            }
            return node as JsonObject;
        }
        foreach ((string name, JsonNode? node) in schemas.ToArray())
        {
            if (node is not JsonObject union || (union["oneOf"] ?? union["anyOf"]) is not JsonArray { Count: > 1 } alternatives ||
                union["oneOf"] != null && union["anyOf"] != null ||
                !union.All(p => p.Key is "oneOf" or "anyOf" or "discriminator" or "description" or "title" or "deprecated" || p.Key.StartsWith("x-", StringComparison.Ordinal))) continue;
            var branches = new List<(string Reference, JsonObject Schema)>();
            foreach (JsonNode? alternative in alternatives)
            {
                if (alternative is not JsonObject reference || reference["$ref"] is not JsonValue id || !id.TryGetValue(out string? path) ||
                    owned.Contains(path) || Resolve(reference) is not JsonObject branch || branch["type"]?.ToJsonString() != "\"object\"" ||
                    branch["nullable"]?.ToJsonString() == "true" || branch["properties"] is not JsonObject ||
                    branch["allOf"] != null || branch["oneOf"] != null || branch["anyOf"] != null || branch["discriminator"] != null) break;
                branches.Add((path, branch));
            }
            if (branches.Count != alternatives.Count || branches.Select(b => b.Reference).Distinct(StringComparer.Ordinal).Count() != branches.Count) continue;
            foreach (string tag in branches[0].Schema["properties"]!.AsObject().Select(p => p.Key)
                         .OrderBy(p => p == union["discriminator"]?["propertyName"]?.ToString() ? 0 : p is "type" or "object" or "kind" ? 1 : 2))
            {
                var mapping = new JsonObject();
                var labels = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
                bool valid = true;
                foreach ((string reference, JsonObject branch) in branches)
                {
                    JsonObject? field = Resolve(branch["properties"]?[tag]);
                    JsonArray? values = field?["enum"] as JsonArray;
                    if ((field?["const"] ?? field?["x-client-original-const"]) is JsonValue literal) values = new JsonArray(literal.DeepClone());
                    if (branch["required"] is not JsonArray required || !required.Any(v => v?.ToJsonString() == JsonValue.Create(tag)!.ToJsonString()) ||
                        field?["type"]?.ToJsonString() != "\"string\"" || field["nullable"]?.ToJsonString() == "true" ||
                        field["readOnly"]?.ToJsonString() == "true" || field["writeOnly"]?.ToJsonString() == "true" || values is not { Count: > 0 }) { valid = false; break; }
                    foreach (JsonNode? value in values)
                    {
                        if (value is not JsonValue scalar || !scalar.TryGetValue(out string? label) || string.IsNullOrEmpty(label) || !labels.Add(label)) { valid = false; break; }
                        mapping[label] = reference;
                    }
                    if (!valid) break;
                }
                if (!valid) continue;
                // This is a generation view: the original union remains available for validation.
                // Every child already requires this string field, so inheriting the base adds no child constraint.
                // Dedicated children avoid Kiota reusing a non-inherited model created through an array union.
                union["x-client-original-tagged-union"] = union.DeepClone();
                union.Remove("oneOf");
                union.Remove("anyOf");
                union["type"] = "object";
                union["properties"] = new JsonObject { [tag] = new JsonObject { ["type"] = "string" } };
                union["required"] = new JsonArray(JsonValue.Create(tag));
                union["discriminator"] = new JsonObject { ["propertyName"] = tag, ["mapping"] = mapping };
                string parentReference = "#/components/schemas/" + name.Replace("~", "~0").Replace("/", "~1");
                foreach ((string reference, JsonObject branch) in branches)
                {
                    string branchName = reference[21..].Replace("~1", "/").Replace("~0", "~");
                    string projectedName = name + branchName;
                    int suffix = 2;
                    while (schemas.ContainsKey(projectedName)) projectedName = name + branchName + suffix++;
                    schemas[projectedName] = new JsonObject { ["allOf"] = new JsonArray(new JsonObject { ["$ref"] = parentReference }, branch.DeepClone()) };
                    string projectedReference = "#/components/schemas/" + projectedName.Replace("~", "~0").Replace("/", "~1");
                    foreach (string label in mapping.Where(p => p.Value?.ToString() == reference).Select(p => p.Key).ToArray()) mapping[label] = projectedReference;
                    owned.Add(reference);
                }
                changed = true;
                break;
            }
        }
        changed |= OpenApiJsonSchemaWalker.Visit(root, (obj, _) =>
        {
            bool arrayChanged = false;
            if ((obj["oneOf"] ?? obj["anyOf"]) is JsonArray alternatives)
                foreach (JsonObject array in alternatives.OfType<JsonObject>())
                {
                    if (array["type"]?.ToJsonString() != "\"array\"") continue;
                    JsonObject? item = Resolve(array["items"]);
                    var visited = new HashSet<JsonObject>(ReferenceEqualityComparer.Instance);
                    while (item?["type"]?.ToJsonString() == "\"array\"" && visited.Add(item)) item = Resolve(item["items"]);
                    if (item?["x-client-original-tagged-union"] != null && array["properties"] == null && array["discriminator"] == null)
                    {
                        // Kiota builds array-union item models from the array schema. Object properties
                        // do not constrain arrays, but make the item fields and factory visible to Kiota.
                        array["properties"] = item["properties"]!.DeepClone();
                        array["required"] = item["required"]!.DeepClone();
                        array["discriminator"] = item["discriminator"]!.DeepClone();
                        array["x-client-array-item-dispatch"] = true;
                        arrayChanged = true;
                    }
                }
            return arrayChanged;
        });
        return changed ? root.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) : json;
    }
}
