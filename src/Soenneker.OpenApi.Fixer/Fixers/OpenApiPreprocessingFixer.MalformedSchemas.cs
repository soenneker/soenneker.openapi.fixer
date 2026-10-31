using System;
using System.Linq;
using System.Text.Json.Nodes;

namespace Soenneker.OpenApi.Fixer.Fixers;

public sealed partial class OpenApiPreprocessingFixer
{
    private static bool NormalizeMalformedSchemaMembers(JsonObject schema)
    {
        bool changed = false;
        void Remember(string key, JsonNode value)
        {
            if (schema["x-client-original-malformed-schema"] is not JsonObject original)
                schema["x-client-original-malformed-schema"] = original = new JsonObject();
            if (!original.ContainsKey(key)) original[key] = value.DeepClone();
        }
        // A property schema cannot be an array or a primitive type name. Recover misplaced
        // schema keywords without touching legitimate properties named "type" or "allOf".
        if (schema["properties"] is JsonObject properties)
        {
            foreach ((string key, JsonNode? value) in properties.ToArray())
            {
                bool composition = key is "allOf" or "oneOf" or "anyOf" && value is JsonArray;
                bool type = key == "type" && value is JsonValue scalar && scalar.TryGetValue<string>(out string? name) &&
                    name is "object" or "array" or "string" or "integer" or "number" or "boolean" or "null";
                if ((!composition && !type) || schema[key] != null && !JsonNode.DeepEquals(schema[key], value)) continue;
                Remember("properties." + key, value!);
                schema[key] = value!.DeepClone();
                properties.Remove(key);
                changed = true;
            }
            if (properties.Count == 0) schema.Remove("properties");
        }
        foreach (string keyword in new[] { "allOf", "oneOf", "anyOf" })
        {
            if (schema[keyword] is not JsonArray branches) continue;
            for (int index = 0; index < branches.Count; index++)
            {
                if (branches[index] is not JsonValue value || !value.TryGetValue<string>(out string? text)) continue;
                text = text.Trim();
                if (!text.StartsWith("$ref:", StringComparison.Ordinal)) continue;
                string reference = text[5..].Trim().Trim('\'', '"');
                if (!reference.StartsWith("#/", StringComparison.Ordinal) || reference.Any(char.IsWhiteSpace)) continue;
                Remember(keyword, branches);
                branches[index] = new JsonObject { ["$ref"] = reference };
                changed = true;
            }
        }
        return changed;
    }
}
