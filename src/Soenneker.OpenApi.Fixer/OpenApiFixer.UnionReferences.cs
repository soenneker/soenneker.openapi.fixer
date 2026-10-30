using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static string ShareEquivalentUnionReferences(string json)
    {
        JsonNode root = JsonNode.Parse(json)!;
        if (root["components"]?["schemas"] is not JsonObject schemas) return json;
        var canonical = new Dictionary<string, string>(StringComparer.Ordinal);
        var aliases = new Dictionary<string, string>(StringComparer.Ordinal);
        string Reference(string name) => "#/components/schemas/" + name.Replace("~", "~0").Replace("/", "~1");
        foreach ((string name, JsonNode? value) in schemas.OrderBy(entry => entry.Key, StringComparer.Ordinal))
        {
            if (value is not JsonObject schema || (schema["oneOf"] ?? schema["anyOf"]) is not JsonArray { Count: > 1 }) continue;
            // Compare complete definitions, including annotations and constraints. Keep every
            // original component available; only references to identical unions are shared.
            string signature = schema.ToJsonString();
            string reference = Reference(name);
            if (canonical.TryGetValue(signature, out string? existing)) aliases[reference] = existing;
            else canonical[signature] = reference;
        }
        if (aliases.Count == 0) return json;
        bool changed = OpenApiJsonSchemaWalker.Visit(root, (schema, _) =>
        {
            if (schema["$ref"] is not JsonValue value || !value.TryGetValue<string>(out string? reference) || !aliases.TryGetValue(reference, out string? target)) return false;
            schema["$ref"] = target;
            return true;
        });
        return changed ? root.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) : json;
    }
}
