using System;
using System.Collections.Generic;
using System.Globalization;
using System.Text.Json.Nodes;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static string ApplySchemaTypeOverrides(string json, OpenApiFixerOptions? options)
    {
        if (options?.SchemaTypeOverrides is not { Count: > 0 } overrides)
            return json;

        JsonNode root = JsonNode.Parse(json)!;
        var schemas = new HashSet<JsonObject>();
        OpenApiJsonSchemaWalker.Visit(root, (schema, _) =>
        {
            schemas.Add(schema);
            return false;
        });

        foreach ((string pointer, OpenApiSchemaTypeOverride replacement) in overrides)
        {
            if (replacement == null || replacement.Type is not ("string" or "number" or "integer" or "boolean" or "object" or "array"))
                throw new ArgumentException($"Schema type override '{pointer}' must specify a supported JSON Schema type.", nameof(options));

            JsonNode? node = root;
            if (!pointer.StartsWith("/", StringComparison.Ordinal))
                throw new ArgumentException($"Schema type override '{pointer}' must be an absolute JSON Pointer.", nameof(options));

            foreach (string segment in pointer.Substring(1).Split('/'))
            {
                for (int i = 0; i < segment.Length; i++)
                {
                    if (segment[i] == '~' && (++i >= segment.Length || segment[i] is not ('0' or '1')))
                        throw new ArgumentException($"Schema type override '{pointer}' contains an invalid JSON Pointer escape.", nameof(options));
                }

                string key = segment.Replace("~1", "/", StringComparison.Ordinal).Replace("~0", "~", StringComparison.Ordinal);
                node = node switch
                {
                    JsonObject obj => obj[key],
                    JsonArray array when int.TryParse(key, NumberStyles.None, CultureInfo.InvariantCulture, out int index) &&
                                         (key == "0" || !key.StartsWith('0')) && index < array.Count => array[index],
                    _ => null
                };
            }

            if (node is not JsonObject target || !schemas.Contains(target) || target.ContainsKey("$ref"))
                throw new ArgumentException($"Schema type override '{pointer}' must target an existing schema object without a $ref.", nameof(options));

            bool nullable = false;
            if (target["type"] is JsonArray types)
                foreach (JsonNode? type in types)
                    nullable |= type is JsonValue value && value.TryGetValue(out string? name) && name == "null";

            target["type"] = nullable ? new JsonArray(replacement.Type, "null") : JsonValue.Create(replacement.Type);
            if (replacement.Format == null)
                target.Remove("format");
            else
                target["format"] = replacement.Format;
        }

        return root.ToJsonString();
    }
}
