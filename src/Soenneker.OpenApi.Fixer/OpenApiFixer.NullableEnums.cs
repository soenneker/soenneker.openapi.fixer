using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static string NormalizeNullableEnumValues(string json)
    {
        JsonNode root = JsonNode.Parse(json)!;
        bool version30 = root["openapi"]?.GetValue<string>().StartsWith("3.0", System.StringComparison.Ordinal) == true;
        string? nullSchemaName = null;
        bool changed = OpenApiJsonSchemaWalker.Visit(root, (schema, _) =>
        {
            // Null is not a permitted enum value when the declared type excludes it.
            // Removing that impossible alternative preserves the schema's accepted values.
            if (schema["enum"] is JsonArray nonNullableValues && nonNullableValues.Any(v => v == null) &&
                nonNullableValues.Any(v => v != null) && schema["nullable"]?.ToJsonString() != "true" &&
                (schema["type"]?.ToJsonString() is "\"string\"" or "\"integer\"" or "\"number\"" or "\"boolean\"" ||
                 schema["type"] is JsonArray { Count: > 0 } nonNullableTypes && nonNullableTypes.All(t => t != null && t.ToJsonString() != "\"null\"")))
            {
                schema["x-client-original-enum"] ??= nonNullableValues.DeepClone();
                schema["enum"] = new JsonArray(nonNullableValues.Where(v => v != null).Select(v => v!.DeepClone()).ToArray());
                return true;
            }
            if (schema["enum"] is JsonArray { Count: > 0 } onlyNull && onlyNull.All(v => v == null) &&
                (schema["type"] == null || schema["type"]?.ToJsonString() == "\"null\"" || schema["nullable"]?.ToJsonString() == "true" ||
                 schema["type"] is JsonArray nullTypes && nullTypes.Any(t => t?.ToJsonString() == "\"null\"")))
            {
                schema["x-client-original-enum"] ??= onlyNull.DeepClone();
                schema["x-client-untyped-value"] = true;
                schema.Remove("enum");
                if (version30)
                {
                    if (schema["not"] is JsonNode excluded)
                    {
                        JsonArray all = schema["allOf"] as JsonArray ?? new JsonArray();
                        all.Add((JsonNode)new JsonObject { ["not"] = excluded.DeepClone() });
                        if (schema["allOf"] == null) schema["allOf"] = all;
                    }
                    schema["type"] = "object";
                    schema["nullable"] = true;
                    schema["not"] = new JsonObject { ["type"] = "object" };
                }
                else
                {
                    schema["type"] = "null";
                    schema.Remove("nullable");
                }
                return true;
            }
            if (schema["enum"] is not JsonArray values || !values.Any(v => v == null) || !values.Any(v => v != null) ||
                !(schema["nullable"]?.ToJsonString() == "true" || schema["type"] is JsonArray nullableTypes && nullableTypes.Any(t => t?.ToJsonString() == "\"null\"")) ||
                schema["oneOf"] != null || schema["anyOf"] != null || schema["allOf"] != null) return false;
            JsonObject nonNull = schema.DeepClone().AsObject();
            JsonNode?[] nonNullTypes = schema["type"] is JsonArray types
                ? types.Where(t => t?.ToJsonString() != "\"null\"").Select(t => t?.DeepClone()).ToArray()
                : [schema["type"]?.DeepClone()];
            if (nonNullTypes.Length != 1 || nonNullTypes[0]?.ToJsonString() is not ("\"string\"" or "\"integer\"" or "\"number\"" or "\"boolean\"")) return false;
            nonNull["type"] = nonNullTypes[0];
            nonNull["enum"] = new JsonArray(values.Where(v => v != null).Select(v => v!.DeepClone()).ToArray());
            nonNull.Remove("nullable");
            schema.Remove("type");
            schema.Remove("enum");
            schema.Remove("nullable");
            // Explicit alternatives preserve the enum's null value without exposing OpenAPI.NET's
            // null sentinel as a string member or making a numeric enum use the string codec.
            JsonObject nullBranch;
            if (version30)
            {
                // Kiota cannot name an inline null-only object in a referenced 3.0 enum union.
                // A named reference preserves the null-only constraint and avoids that generator crash.
                if (nullSchemaName == null)
                {
                    nullSchemaName = "OpenApiNullValue";
                    while (root["components"]?["schemas"]?[nullSchemaName] != null) nullSchemaName += "Value";
                }
                nullBranch = new JsonObject { ["$ref"] = "#/components/schemas/" + nullSchemaName };
            }
            else nullBranch = new JsonObject { ["type"] = "null" };
            schema["anyOf"] = new JsonArray(nonNull, nullBranch);
            return true;
        });
        if (nullSchemaName != null)
        {
            root["components"] ??= new JsonObject();
            root["components"]!["schemas"] ??= new JsonObject();
            root["components"]!["schemas"]![nullSchemaName] = new JsonObject
            {
                ["type"] = "object", ["nullable"] = true, ["not"] = new JsonObject { ["type"] = "object" },
                ["x-client-untyped-value"] = true
            };
        }
        return changed ? root.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) : json;
    }
}
