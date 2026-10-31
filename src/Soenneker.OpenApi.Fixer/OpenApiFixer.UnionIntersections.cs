using System;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static string NormalizeGeneratorUnionIntersections(string json)
    {
        for (int pass = 0; pass < 16; pass++)
        {
            string normalized = NormalizeGeneratorUnionIntersectionsPass(json);
            if (normalized == json) return json;
            json = normalized;
        }
        throw new InvalidOperationException("Union intersection normalization did not converge.");
    }

    private static string NormalizeGeneratorUnionIntersectionsPass(string json)
    {
        JsonNode root = JsonNode.Parse(json)!;
        if (root["components"]?["schemas"] is not JsonObject schemas) return json;
        bool changed = false;
        foreach ((string name, JsonNode? node) in schemas.ToArray())
        {
            if (node is not JsonObject schema || schema["properties"] is not JsonObject) continue;
            string? keyword = schema["oneOf"] is JsonArray ? "oneOf" : schema["anyOf"] is JsonArray ? "anyOf" : null;
            if (keyword == null || schema[keyword] is not JsonArray { Count: > 0 } branches) continue;
            JsonObject? Resolve(JsonNode? branch)
            {
                if (branch is not JsonObject obj) return null;
                if (obj["$ref"] is not JsonValue reference) return obj;
                string id = reference.GetValue<string>();
                return id.StartsWith("#/components/schemas/", StringComparison.Ordinal) ? schemas[id[21..].Replace("~1", "/").Replace("~0", "~")] as JsonObject : null;
            }
            int remainingBranches = 128;
            bool IsObject(JsonObject target, int depth = 0)
            {
                if (--remainingBranches < 0 || depth > 16 || target["nullable"]?.ToJsonString() == "true" || target["allOf"] != null) return false;
                if ((target["oneOf"] ?? target["anyOf"]) is JsonArray nested)
                    return nested.Count > 0 && nested.All(b => Resolve(b) is JsonObject child && IsObject(child, depth + 1));
                return target["type"]?.ToJsonString() == "\"object\"";
            }
            if (!branches.All(b => Resolve(b) is JsonObject target && IsObject(target))) continue;
            if (schema.Any(p => p.Key is not ("oneOf" or "anyOf" or "type" or "properties" or "required" or "discriminator" or
                    "nullable" or "additionalProperties" or "title" or "description" or "example" or "examples" or "deprecated") && !p.Key.StartsWith("x-", StringComparison.Ordinal))) continue;
            if (schema["additionalProperties"] is JsonObject) continue;
            bool parentClosed = schema["additionalProperties"]?.ToJsonString() == "false";
            var common = new JsonObject { ["type"] = "object", ["properties"] = schema["properties"]!.DeepClone() };
            if (schema["required"] != null) common["required"] = schema["required"]!.DeepClone();
            bool Redundant(JsonObject target) => common["properties"]!.AsObject().All(p =>
                p.Value is JsonObject { Count: 1 } field && field["type"]?.ToJsonString() == "\"string\"" &&
                Resolve(target["properties"]?[p.Key]) is JsonObject actual && actual["type"]?.ToJsonString() == "\"string\"" && actual["nullable"]?.ToJsonString() != "true") &&
                (common["required"] is not JsonArray required || required.All(r =>
                    target["required"] is JsonArray targetRequired && targetRequired.Any(t => JsonNode.DeepEquals(r, t))));
            bool CanDistribute(JsonObject target)
            {
                if ((target["oneOf"] ?? target["anyOf"]) is JsonArray nested)
                    return target["properties"] == null && target["additionalProperties"] == null && nested.All(b => CanDistribute(Resolve(b)!));
                JsonObject parentFields = common["properties"]!.AsObject();
                JsonObject targetFields = target["properties"] as JsonObject ?? new();
                if (parentClosed && targetFields.Any(p => !parentFields.ContainsKey(p.Key))) return false;
                return target["additionalProperties"]?.ToJsonString() != "false" || parentFields.All(p => targetFields.ContainsKey(p.Key));
            }
            if (branches.Any(b => !CanDistribute(Resolve(b)!))) continue;
            JsonNode Distribute(JsonNode branch)
            {
                JsonObject target = Resolve(branch)!;
                if ((target["oneOf"] ?? target["anyOf"]) is JsonArray nested)
                {
                    JsonObject union = target.DeepClone().AsObject();
                    if (union["discriminator"] != null)
                    {
                        union["x-client-original-discriminator"] = union["discriminator"]!.DeepClone();
                        union.Remove("discriminator");
                    }
                    union[target["oneOf"] != null ? "oneOf" : "anyOf"] = new JsonArray(nested.Select(b => Distribute(b!)).ToArray());
                    return union;
                }
                if (!parentClosed && Redundant(target)) return branch.DeepClone();
                JsonObject copy = target.DeepClone().AsObject();
                bool closed = parentClosed || copy["additionalProperties"]?.ToJsonString() == "false";
                JsonNode? map = copy["additionalProperties"]?.DeepClone();
                copy.Remove("additionalProperties");
                JsonObject fields = common.DeepClone().AsObject();
                if (map is JsonObject)
                    foreach ((string field, JsonNode? value) in fields["properties"]!.AsObject().ToArray())
                        if (copy["properties"]?[field] == null)
                            fields["properties"]![field] = new JsonObject { ["allOf"] = new JsonArray(value!.DeepClone(), map.DeepClone()) };
                var intersection = new JsonObject { ["allOf"] = new JsonArray(fields, copy) };
                if (closed) intersection["x-client-close-after-intersection"] = true;
                else if (map is JsonObject) intersection["x-client-map-after-intersection"] = map;
                return intersection;
            }
            var alternatives = new JsonArray(branches.Select(b => Distribute(b!)).ToArray());
            if (schema["discriminator"] != null)
                schema["x-client-original-discriminator"] = schema["discriminator"]!.DeepClone();
            schema.Remove("discriminator");
            schema.Remove("properties");
            schema.Remove("required");
            schema.Remove("additionalProperties");
            schema[keyword] = alternatives;
            changed = true;
        }
        if (!changed) return json;
        root = JsonNode.Parse(OpenApiJsonCompositionNormalizer.Normalize(root.ToJsonString()))!;
        schemas = root["components"]!["schemas"]!.AsObject();
        void RestoreConstraints(JsonObject schema)
        {
            if (schema["x-client-close-after-intersection"]?.ToJsonString() == "true")
            {
                schema.Remove("x-client-close-after-intersection");
                schema["additionalProperties"] = false;
            }
            if (schema["x-client-map-after-intersection"] is JsonNode map)
            {
                schema.Remove("x-client-map-after-intersection");
                schema["additionalProperties"] = map;
            }
        }
        OpenApiJsonSchemaWalker.Visit(root, (schema, _) => { RestoreConstraints(schema); return false; });
        OpenApiJsonSchemaWalker.Visit(root, (schema, name) =>
        {
            foreach (string keyword in new[] { "oneOf", "anyOf" })
                if (schema[keyword] is JsonArray branches)
                    for (int index = 0; index < branches.Count; index++)
                        if (branches[index] is JsonObject branch && branch["$ref"] == null &&
                            (branch["type"]?.ToJsonString() == "\"object\"" && branch["properties"] is JsonObject || branch["oneOf"] is JsonArray || branch["anyOf"] is JsonArray))
                        {
                            string key = OpenApiNameNormalizer.ReserveComponentName(schemas.Select(p => p.Key), $"{name ?? "Union"} {keyword} {index + 1}", "Branch");
                            schemas[key] = branch.DeepClone();
                            branches[index] = new JsonObject { ["$ref"] = "#/components/schemas/" + key };
                        }
            return false;
        });
        return root.ToJsonString(new JsonSerializerOptions { WriteIndented = true });
    }
}
