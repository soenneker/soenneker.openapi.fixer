using System;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static string NormalizeGeneratorUnionIntersections(string json)
    {
        JsonNode root = JsonNode.Parse(json)!;
        if (root["components"]?["schemas"] is not JsonObject schemas) return json;
        bool changed = false;
        foreach ((string name, JsonNode? node) in schemas.ToArray())
        {
            if (node is not JsonObject schema || schema["discriminator"] is not JsonObject || schema["properties"] is not JsonObject) continue;
            string? keyword = schema["oneOf"] is JsonArray ? "oneOf" : schema["anyOf"] is JsonArray ? "anyOf" : null;
            if (keyword == null || schema[keyword] is not JsonArray { Count: > 0 } branches) continue;
            JsonObject? Resolve(JsonNode? branch)
            {
                if (branch is not JsonObject obj) return null;
                if (obj["$ref"] is not JsonValue reference) return obj;
                string id = reference.GetValue<string>();
                return id.StartsWith("#/components/schemas/", StringComparison.Ordinal) ? schemas[id[21..].Replace("~1", "/").Replace("~0", "~")] as JsonObject : null;
            }
            // This distribution is safe for concrete object alternatives. Recursive inheritance and
            // closed object constraints need a separate projection and are deliberately left intact.
            if (!branches.All(b => Resolve(b) is JsonObject target && target["type"]?.ToJsonString() == "\"object\"" &&
                    target["nullable"]?.ToJsonString() != "true" && target["allOf"] == null && target["oneOf"] == null && target["anyOf"] == null)) continue;
            if (schema.Any(p => p.Key is not ("oneOf" or "anyOf" or "type" or "properties" or "required" or "discriminator" or
                    "title" or "description" or "example" or "examples" or "deprecated") && !p.Key.StartsWith("x-", StringComparison.Ordinal))) continue;
            var common = new JsonObject { ["type"] = "object", ["properties"] = schema["properties"]!.DeepClone() };
            if (schema["required"] != null) common["required"] = schema["required"]!.DeepClone();
            bool Redundant(JsonObject target) => common["properties"]!.AsObject().All(p =>
                p.Value is JsonObject { Count: 1 } field && field["type"]?.ToJsonString() == "\"string\"" &&
                Resolve(target["properties"]?[p.Key]) is JsonObject actual && actual["type"]?.ToJsonString() == "\"string\"" && actual["nullable"]?.ToJsonString() != "true") &&
                (common["required"] is not JsonArray required || required.All(r =>
                    target["required"] is JsonArray targetRequired && targetRequired.Any(t => JsonNode.DeepEquals(r, t))));
            if (branches.Any(b => Resolve(b)!["additionalProperties"] != null && !Redundant(Resolve(b)!))) continue;
            var alternatives = new JsonArray();
            foreach (JsonNode? branch in branches)
                alternatives.Add(Redundant(Resolve(branch)!) ? branch!.DeepClone() : (JsonNode)new JsonObject { ["allOf"] = new JsonArray(common.DeepClone(), Resolve(branch)!.DeepClone()) });
            schema["x-client-original-discriminator"] = schema["discriminator"]!.DeepClone();
            schema.Remove("discriminator");
            schema.Remove("properties");
            schema.Remove("required");
            schema[keyword] = alternatives;
            changed = true;
        }
        if (!changed) return json;
        root = JsonNode.Parse(OpenApiJsonCompositionNormalizer.Normalize(root.ToJsonString()))!;
        schemas = root["components"]!["schemas"]!.AsObject();
        OpenApiJsonSchemaWalker.Visit(root, (schema, name) =>
        {
            foreach (string keyword in new[] { "oneOf", "anyOf" })
                if (schema[keyword] is JsonArray branches)
                    for (int index = 0; index < branches.Count; index++)
                        if (branches[index] is JsonObject branch && branch["$ref"] == null && branch["type"]?.ToJsonString() == "\"object\"" && branch["properties"] is JsonObject)
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
