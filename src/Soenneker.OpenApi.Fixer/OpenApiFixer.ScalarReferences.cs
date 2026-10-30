using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static string InlineScalarReferences(string json)
    {
        JsonNode root = JsonNode.Parse(json)!;
        if (root["components"]?["schemas"] is not JsonObject schemas) return json;
        bool changed = OpenApiJsonSchemaWalker.Visit(root, (schema, _) =>
        {
            JsonObject target = schema;
            var seen = new HashSet<string>(StringComparer.Ordinal);
            while (target["$ref"] is JsonValue reference)
            {
                string id = reference.GetValue<string>();
                if (target.Count != 1 || !id.StartsWith("#/components/schemas/", StringComparison.Ordinal) || !seen.Add(id) ||
                    schemas[id[21..].Replace("~1", "/").Replace("~0", "~")] is not JsonObject resolved) return false;
                target = resolved;
            }
            if (ReferenceEquals(target, schema) || target["x-client-inline-scalar"]?.ToJsonString() != "true" || target["enum"] != null || target["anyOf"] != null || target["oneOf"] != null || target["allOf"] != null ||
                target["type"] is not JsonValue type || !type.TryGetValue<string>(out string? typeName) || typeName is not ("string" or "boolean" or "integer" or "number")) return false;
            JsonObject replacement = target.DeepClone().AsObject();
            schema.Clear();
            foreach ((string key, JsonNode? value) in replacement) schema[key] = value?.DeepClone();
            return true;
        });
        return changed ? root.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) : json;
    }
}
