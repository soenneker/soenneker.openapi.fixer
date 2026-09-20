using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Numerics;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Soenneker.OpenApi.Fixer.Fixers;

public sealed partial class OpenApiPreprocessingFixer
{
    private static bool RecoverSchemasFromExamples(JsonObject document, bool is31)
    {
        return OpenApiJsonSchemaWalker.Visit(document, (_, _) => false, content =>
        {
            bool changed = false;
            foreach ((string key, JsonNode? node) in content)
            {
                string mediaType = key.Split(';')[0].Trim();
                if (node is not JsonObject media ||
                    !(mediaType.Equals("application/json", StringComparison.OrdinalIgnoreCase) || mediaType.EndsWith("+json", StringComparison.OrdinalIgnoreCase)))
                    continue;
                var samples = new List<JsonNode?>();
                if (media.ContainsKey("example")) samples.Add(media["example"]);
                if (media["examples"] is JsonObject examples)
                    foreach (JsonObject example in examples.Select(pair => pair.Value).OfType<JsonObject>())
                    {
                        JsonObject resolved = ResolveLocalExampleSchema(document, example);
                        if (resolved.ContainsKey("value")) samples.Add(resolved["value"]);
                    }
                if (samples.Count == 0) continue;
                // Explicit schemas, including unconstrained and boolean schemas, remain authoritative.
                if (!media.ContainsKey("schema"))
                {
                    media["schema"] = InferExampleSchema(samples, is31, 0);
                    media["x-schema-inferred-from-examples"] = true;
                    changed = true;
                }
                else if (media["schema"] is JsonObject schema)
                    foreach (JsonNode? sample in samples)
                        changed |= WidenExampleIntegers(document, schema, sample, new HashSet<(JsonObject, JsonNode)>(), 0);
            }
            return changed;
        });
    }

    private static JsonObject ResolveLocalExampleSchema(JsonObject document, JsonObject value)
    {
        var visited = new HashSet<JsonObject>();
        while (visited.Add(value) && value["$ref"] is JsonValue reference && reference.TryGetValue(out string? pointer) && pointer.StartsWith("#/", StringComparison.Ordinal))
        {
            JsonNode? target = document;
            foreach (string part in Uri.UnescapeDataString(pointer[2..]).Split('/'))
                target = target is JsonObject obj ? obj[part.Replace("~1", "/", StringComparison.Ordinal).Replace("~0", "~", StringComparison.Ordinal)] : null;
            if (target is not JsonObject resolved) break;
            value = resolved;
        }
        return value;
    }

    private static JsonObject InferExampleSchema(List<JsonNode?> samples, bool is31, int depth)
    {
        if (depth > 64 || samples.Count == 0) return new JsonObject();
        bool nullable = samples.Any(sample => sample == null);
        var branches = new JsonArray();
        foreach (var group in samples.Where(sample => sample != null).GroupBy(sample => sample!.GetValueKind() switch
                 {
                     JsonValueKind.True or JsonValueKind.False => "boolean",
                     JsonValueKind.Number => "number",
                     JsonValueKind.Object => "object",
                     JsonValueKind.Array => "array",
                     _ => "string"
                 }))
        {
            var schema = new JsonObject { ["type"] = group.Key };
            if (group.Key == "object")
            {
                var properties = new JsonObject();
                foreach (var property in group.Cast<JsonObject>().SelectMany(obj => obj).GroupBy(pair => pair.Key, StringComparer.Ordinal))
                    properties[property.Key] = InferExampleSchema(property.Select(pair => pair.Value).ToList(), is31, depth + 1);
                schema["properties"] = properties;
            }
            else if (group.Key == "array")
                schema["items"] = InferExampleSchema(group.Cast<JsonArray>().SelectMany(array => array).ToList(), is31, depth + 1);
            else if (group.Key == "number")
            {
                BigInteger[] integers = group.Select(sample => BigInteger.TryParse(sample!.ToJsonString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var number)
                    ? (BigInteger?)number : null).OfType<BigInteger>().ToArray();
                if (integers.Length == group.Count())
                {
                    schema["type"] = "integer";
                    if (integers.All(number => number >= long.MinValue && number <= long.MaxValue)) schema["format"] = "int64";
                }
            }
            if (nullable && !is31) schema["nullable"] = true;
            branches.Add(schema);
        }
        if (nullable && is31) branches.Add(new JsonObject { ["type"] = "null" });
        // A null-only 3.0 sample does not establish a non-null type.
        return branches.Count switch
        {
            0 => new JsonObject(),
            1 => (JsonObject)branches[0]!.DeepClone(),
            _ => new JsonObject { ["anyOf"] = branches }
        };
    }

    private static bool WidenExampleIntegers(JsonObject document, JsonObject schema, JsonNode? sample, HashSet<(JsonObject, JsonNode)> active, int depth)
    {
        if (sample == null || depth > 64 || !active.Add((schema, sample))) return false;
        bool changed = false;
        try
        {
            JsonObject resolved = ResolveLocalExampleSchema(document, schema);
            if (!ReferenceEquals(resolved, schema))
                changed |= WidenExampleIntegers(document, resolved, sample, active, depth + 1);
            if (schema["type"]?.ToString() == "integer" && schema["format"]?.ToString() is null or "int32" &&
                sample.GetValueKind() == JsonValueKind.Number && long.TryParse(sample.ToJsonString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out long integer) &&
                (integer > int.MaxValue || integer < int.MinValue))
            {
                schema["format"] = "int64";
                changed = true;
            }
            if (sample is JsonObject obj)
                foreach ((string key, JsonNode? value) in obj)
                {
                    if (schema["properties"] is JsonObject properties && properties[key] is JsonObject property)
                        changed |= WidenExampleIntegers(document, property, value, active, depth + 1);
                    else if (schema["additionalProperties"] is JsonObject additional)
                        changed |= WidenExampleIntegers(document, additional, value, active, depth + 1);
                }
            if (sample is JsonArray array && schema["items"] is JsonObject items)
                foreach (JsonNode? item in array)
                    changed |= WidenExampleIntegers(document, items, item, active, depth + 1);
            foreach (string keyword in new[] { "allOf", "anyOf", "oneOf" })
                if (schema[keyword] is JsonArray branches)
                    foreach (JsonObject branch in branches.OfType<JsonObject>())
                        changed |= WidenExampleIntegers(document, branch, sample, active, depth + 1);
            return changed;
        }
        finally { active.Remove((schema, sample)); }
    }
}
