using System;
using System.Linq;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static string PreserveGeneratorUnionBranches(string json)
    {
        JsonNode root = JsonNode.Parse(json)!;
        if (root["components"]?["schemas"] is not JsonObject schemas) return json;
        bool changed = OpenApiJsonSchemaWalker.Visit(root, (schema, name) =>
        {
            bool updated = false;
            foreach (string keyword in new[] { "anyOf", "oneOf" })
            {
                if (schema[keyword] is not JsonArray branches) continue;
                for (int index = 0; index < branches.Count; index++)
                {
                    if (branches[index] is not JsonObject branch) continue;
                    if (branch.Count == 1 && branch["$ref"] is JsonValue reference && reference.TryGetValue(out string? id) &&
                        id.StartsWith("#/components/schemas/", StringComparison.Ordinal) &&
                        schemas[id[21..].Replace("~1", "/").Replace("~0", "~")] is JsonObject target &&
                        target["type"] is JsonValue type && type.TryGetValue(out string? typeName) &&
                        typeName is "string" or "integer" or "number" or "boolean" && target["enum"] is JsonArray values &&
                        !values.Any(v => v is JsonValue value && value.TryGetValue(out string? text) && !string.IsNullOrEmpty(text)))
                    {
                        // Kiota treats referenced non-string enums and the empty-string enum as object models.
                        branches[index] = target.DeepClone();
                        updated = true;
                    }
                    else if (branch["type"] is JsonValue mapType && mapType.TryGetValue(out string? mapTypeName) && mapTypeName == "object" && branch["additionalProperties"] is JsonObject &&
                             branch["properties"] == null)
                    {
                        // A referenced map remains a meaningful union branch to Kiota; an inline map is silently ignored.
                        string key = OpenApiNameNormalizer.ReserveComponentName(schemas.Select(p => p.Key), $"{name ?? "Union"} {keyword} {index + 1}", "Map");
                        schemas[key] = branch.DeepClone();
                        branches[index] = new JsonObject { ["$ref"] = $"#/components/schemas/{key}" };
                        updated = true;
                    }
                }
            }
            bool RequiresObject(JsonNode? node, HashSet<string> seen)
            {
                if (node is not JsonObject obj) return false;
                if (obj["type"] is JsonValue type && type.TryGetValue(out string? kind) && kind == "object" && obj["nullable"]?.GetValue<bool>() != true) return true;
                if (obj.Count == 1 && obj["$ref"] is JsonValue reference)
                {
                    string id = reference.GetValue<string>();
                    if (!id.StartsWith("#/components/schemas/", StringComparison.Ordinal) || !seen.Add(id)) return false;
                    bool result = RequiresObject(schemas[id[21..].Replace("~1", "/").Replace("~0", "~")], seen);
                    seen.Remove(id);
                    return result;
                }
                return (obj["oneOf"] ?? obj["anyOf"]) is JsonArray { Count: > 0 } union && union.All(branch => RequiresObject(branch, seen));
            }
            if (schema["type"] is JsonValue objectType && objectType.TryGetValue(out string? objectKind) && objectKind == "object" &&
                schema["properties"] == null && (schema["oneOf"] ?? schema["anyOf"]) is JsonArray { Count: > 0 } objectAlternatives &&
                objectAlternatives.All(branch => RequiresObject(branch, new HashSet<string>(StringComparer.Ordinal))))
            {
                // The branches already require objects. Keeping the redundant type makes Kiota collapse their models together.
                schema.Remove("type");
                updated = true;
            }
            if (schema["discriminator"] == null)
            {
                JsonObject? Resolve(JsonNode? node)
                {
                    var seen = new HashSet<string>(StringComparer.Ordinal);
                    while (node is JsonObject obj && obj.Count == 1 && obj["$ref"] is JsonValue reference)
                    {
                        string id = reference.GetValue<string>();
                        if (!id.StartsWith("#/components/schemas/", StringComparison.Ordinal) || !seen.Add(id)) return null;
                        node = schemas[id[21..].Replace("~1", "/").Replace("~0", "~")];
                    }
                    return node as JsonObject;
                }
                foreach (string keyword in new[] { "oneOf", "anyOf" })
                {
                    if (schema[keyword] is not JsonArray { Count: > 1 } alternatives) continue;
                    JsonArray originalAlternatives = alternatives;
                    JsonObject?[] nested = alternatives.Select(Resolve).ToArray();
                    bool PureUnion(JsonObject? branch) => branch != null &&
                        branch.All(p => p.Key is "oneOf" or "anyOf" or "discriminator" or "title" or "description" || p.Key.StartsWith("x-", StringComparison.Ordinal)) &&
                        (branch["oneOf"] is JsonArray || branch["anyOf"] is JsonArray);
                    if (nested.Any(PureUnion) && nested.All(branch => PureUnion(branch) || branch?["properties"] is JsonObject))
                    {
                        // Flatten only after the required literal tags below prove every leaf is disjoint.
                        alternatives = new JsonArray(nested.SelectMany(branch => PureUnion(branch)
                            ? (IEnumerable<JsonNode?>)(branch!["oneOf"] ?? branch["anyOf"])!.AsArray()
                            : new JsonNode?[] { branch }).Select(branch => branch?.DeepClone()).ToArray());
                    }
                    JsonObject?[] targets = alternatives.Select(Resolve).ToArray();
                    if (targets.Any(t => t?["properties"] is not JsonObject || !RequiresObject(t, new HashSet<string>(StringComparer.Ordinal)))) continue;
                    foreach (string property in targets[0]!["properties"]!.AsObject().Select(p => p.Key))
                    {
                        bool requiredTag = targets.All(target => target!["required"] is JsonArray required && required.Any(n => n?.GetValue<string>() == property));
                        var labels = new List<string>();
                        bool valid = true;
                        foreach (JsonObject target in targets!)
                        {
                            JsonObject? field = Resolve(target["properties"]?[property]);
                            if (field?["enum"] is not JsonArray { Count: 1 } values || values[0] is not JsonValue value ||
                                !value.TryGetValue(out string? label) || string.IsNullOrEmpty(label) || labels.Contains(label))
                            { valid = false; break; }
                            labels.Add(label);
                        }
                        if (!valid || (!requiredTag && (keyword == "anyOf" || !ReferenceEquals(originalAlternatives, alternatives)))) continue;
                        var mapping = new JsonObject();
                        for (int index = 0; index < targets.Length; index++)
                        {
                            string? reference = alternatives[index]?["$ref"]?.GetValue<string>();
                            if (reference == null)
                            {
                                string key = OpenApiNameNormalizer.ReserveComponentName(schemas.Select(p => p.Key), $"{name ?? "Union"} {labels[index]}", "Variant");
                                schemas[key] = targets[index]!.DeepClone();
                                reference = "#/components/schemas/" + key;
                                alternatives[index] = new JsonObject { ["$ref"] = reference };
                            }
                            mapping[labels[index]] = reference;
                        }
                        if (keyword == "anyOf" && schema["oneOf"] == null)
                        {
                            // Distinct required tags make the alternatives exclusive. Kiota's anyOf wrapper otherwise merges every object.
                            schema.Remove("anyOf");
                            schema["oneOf"] = alternatives;
                        }
                        else if (!ReferenceEquals(originalAlternatives, alternatives)) schema[keyword] = alternatives;
                        schema["discriminator"] = new JsonObject { ["propertyName"] = property, ["mapping"] = mapping };
                        updated = true;
                        break;
                    }
                }
            }
            return updated;
        });
        return changed ? root.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) : json;
    }
}
