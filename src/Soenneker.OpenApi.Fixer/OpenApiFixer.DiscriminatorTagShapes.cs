using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static string NormalizeMalformedDiscriminatorTags(string json)
    {
        JsonNode root = JsonNode.Parse(json)!;
        if (root["components"]?["schemas"] is not JsonObject schemas) return json;
        JsonObject? Resolve(JsonNode? node)
        {
            var seen = new HashSet<string>(StringComparer.Ordinal);
            while (node is JsonObject { Count: 1 } reference && reference["$ref"] is JsonValue id)
            {
                if (!id.TryGetValue(out string? path)) return null;
                if (!path.StartsWith("#/components/schemas/", StringComparison.Ordinal) || !seen.Add(path)) return null;
                node = schemas[path[21..].Replace("~1", "/").Replace("~0", "~")];
            }
            return node as JsonObject;
        }
        bool changed = false;
        foreach ((string name, JsonNode? node) in schemas)
        {
            if (node is not JsonObject schema || schema["discriminator"] is not JsonObject discriminator ||
                discriminator["propertyName"] is not JsonValue property || discriminator["mapping"] is not JsonObject { Count: > 0 } mapping)
                continue;
            string wire = property.GetValue<string>();
            if (schema["properties"]?[wire] is not JsonObject field || field["type"]?.ToJsonString() != "\"array\"" ||
                !field.All(p => p.Key is "type" or "items" or "description" or "title" || p.Key.StartsWith("x-", StringComparison.Ordinal)) ||
                Resolve(field["items"]) is not JsonObject item || item["type"]?.ToJsonString() != "\"string\"" || item["enum"] is not JsonArray values)
                continue;
            var labels = values.OfType<JsonValue>().Where(v => v.TryGetValue<string>(out _)).Select(v => v.GetValue<string>()).ToHashSet(StringComparer.Ordinal);
            if (labels.Count != values.Count || !labels.SetEquals(mapping.Select(p => p.Key))) continue;
            bool proven = mapping.All(pair =>
            {
                JsonObject? derived = Resolve(new JsonObject { ["$ref"] = pair.Value?.DeepClone() });
                JsonObject? tag = Resolve(derived?["properties"]?[wire]);
                return derived?["allOf"] is JsonArray parents && parents.Any(p => p?["$ref"]?.GetValue<string>() == "#/components/schemas/" + name) &&
                    tag?["type"]?.ToJsonString() == "\"string\"" && tag["enum"] is JsonArray { Count: 1 } allowed &&
                    allowed[0]?.ToJsonString() == JsonValue.Create(pair.Key)!.ToJsonString() &&
                    derived["example"] is JsonObject example && example[wire]?.ToJsonString() == allowed[0]?.ToJsonString();
            });
            if (!proven) continue;
            // A discriminator cannot be an array. Only repair when every mapped subtype's literal
            // and documented payload agree on the scalar tag; retain the contradictory source.
            JsonObject replacement = item.DeepClone().AsObject();
            foreach ((string key, JsonNode? value) in field.Where(p => p.Key is not ("type" or "items"))) replacement[key] = value?.DeepClone();
            replacement["x-client-original-discriminator-property"] = field.DeepClone();
            schema["properties"]![wire] = replacement;
            changed = true;
        }
        return changed ? root.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) : json;
    }
}
