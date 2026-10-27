using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static string NormalizeDisjointObjectArrayUnions(string json)
    {
        JsonNode root = JsonNode.Parse(json)!;
        JsonObject? schemas = root["components"]?["schemas"] as JsonObject;

        string? GetNonNullableType(JsonNode? node)
        {
            var seen = new HashSet<string>(StringComparer.Ordinal);
            while (node is JsonObject { Count: 1 } reference && reference["$ref"] is JsonValue value)
            {
                string id = value.GetValue<string>();
                if (!id.StartsWith("#/components/schemas/", StringComparison.Ordinal) || !seen.Add(id)) return null;
                node = schemas?[id[21..].Replace("~1", "/").Replace("~0", "~")];
            }
            if (node is not JsonObject schema || schema["nullable"]?.ToJsonString() == "true") return null;
            return schema["type"] is JsonValue type && type.TryGetValue(out string? name) ? name : null;
        }

        bool changed = OpenApiJsonSchemaWalker.Visit(root, (schema, _) =>
        {
            if (schema["discriminator"] != null || schema["anyOf"] != null || schema["oneOf"] is not JsonArray { Count: 2 } branches)
                return false;
            string? first = GetNonNullableType(branches[0]);
            string? second = GetNonNullableType(branches[1]);
            if (!((first == "object" && second == "array") || (first == "array" && second == "object"))) return false;

            // Explicit, non-null object and array types cannot overlap, so anyOf is equivalent here.
            // Kiota's oneOf factory requires a discriminator for the object branch, whereas its anyOf
            // factory checks the collection first and falls back to the sole object model.
            schema.Remove("oneOf");
            schema["anyOf"] = branches;
            return true;
        });
        return changed ? root.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) : json;
    }
}
