using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static string ProjectCompatibleObjectAlternatives(string json)
    {
        JsonNode root = JsonNode.Parse(json)!;
        JsonObject? schemas = root["components"]?["schemas"] as JsonObject;

        JsonObject? Resolve(JsonNode? node)
        {
            var seen = new HashSet<string>(StringComparer.Ordinal);
            while (node is JsonObject { Count: 1 } reference && reference["$ref"] is JsonValue value)
            {
                string id = value.GetValue<string>();
                if (!id.StartsWith("#/components/schemas/", StringComparison.Ordinal) || !seen.Add(id)) return null;
                node = schemas?[id[21..].Replace("~1", "/").Replace("~0", "~")];
            }
            return node as JsonObject;
        }

        static bool Annotation(string key) => key is "title" or "description" or "default" or "example" or "examples" or
            "deprecated" or "readOnly" or "writeOnly" or "xml" or "externalDocs" || key.StartsWith("x-", StringComparison.Ordinal);

        bool changed = OpenApiJsonSchemaWalker.Visit(root, (schema, _) =>
        {
            // Never project oneOf, closed shapes, discriminators, branch-specific requirements, or other constraints.
            if (schema["anyOf"] is not JsonArray { Count: > 1 } alternatives ||
                schema.ContainsKey("x-client-original-anyOf") ||
                !schema.All(p => Annotation(p.Key) || p.Key is "anyOf" or "type") ||
                (schema["type"] != null && schema["type"]!.ToJsonString() != "\"object\"")) return false;

            var properties = new JsonObject();
            HashSet<string>? required = null;
            foreach (JsonNode? alternative in alternatives)
            {
                JsonObject? branch = Resolve(alternative);
                if (branch == null || branch["type"]?.ToJsonString() != "\"object\"" ||
                    branch["properties"] is not JsonObject fields ||
                    !branch.All(p => Annotation(p.Key) || p.Key is "type" or "properties" or "required" ||
                        (p.Key == "additionalProperties" && p.Value?.ToJsonString() == "true"))) return false;

                var branchRequired = new HashSet<string>(branch["required"] is JsonArray names
                    ? names.Select(n => n!.GetValue<string>()) : [], StringComparer.Ordinal);
                if (required != null && !required.SetEquals(branchRequired)) return false;
                required ??= branchRequired;
                if (branchRequired.Any(name => !fields.ContainsKey(name))) return false;

                foreach ((string name, JsonNode? field) in fields)
                {
                    if (properties.TryGetPropertyValue(name, out JsonNode? existing))
                    {
                        if (!JsonNode.DeepEquals(existing, field)) return false;
                    }
                    else properties[name] = field?.DeepClone();
                }
            }

            // Preserve the source alternatives for consumers that need the original validation contract.
            schema["x-client-original-anyOf"] = alternatives.DeepClone();
            schema.Remove("anyOf");
            schema["type"] = "object";
            schema["properties"] = properties;
            if (required is { Count: > 0 })
                schema["required"] = new JsonArray(required.Order(StringComparer.Ordinal).Select(n => (JsonNode?)JsonValue.Create(n)).ToArray());
            return true;
        });
        return changed ? root.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) : json;
    }
}
