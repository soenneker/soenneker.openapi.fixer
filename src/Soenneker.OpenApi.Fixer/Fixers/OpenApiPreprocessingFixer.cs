using Microsoft.Extensions.Logging;
using Soenneker.OpenApi.Fixer.Fixers.Abstract;
using System;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Soenneker.OpenApi.Fixer.Fixers;

/// <inheritdoc cref="IOpenApiPreprocessingFixer"/>
public sealed class OpenApiPreprocessingFixer : IOpenApiPreprocessingFixer
{
    private readonly ILogger<OpenApiPreprocessingFixer> _logger;

    public OpenApiPreprocessingFixer(ILogger<OpenApiPreprocessingFixer> logger)
    {
        _logger = logger;
    }

    public string Fix(string json)
    {
        if (string.IsNullOrWhiteSpace(json))
            return json;

        JsonNode? root;

        try
        {
            root = JsonNode.Parse(json);
        }
        catch (JsonException ex)
        {
            _logger.LogDebug(ex, "Unable to parse OpenAPI JSON during preprocessing");
            return json;
        }

        if (root is null)
            return json;

        bool normalizeLegacyNullable = root is JsonObject rootObject && IsOpenApi31OrLater(rootObject);
        bool changed = NormalizeLooseSchemaBooleanFields(root, false, false, normalizeLegacyNullable);

        return changed ? root.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) : json;
    }

    private static bool NormalizeLooseSchemaBooleanFields(JsonNode? node, bool isSchema, bool childrenAreSchemas, bool normalizeLegacyNullable)
    {
        switch (node)
        {
            case JsonObject obj:
            {
                bool changed = isSchema && NormalizeSchemaBooleanFields(obj, normalizeLegacyNullable);

                foreach ((string key, JsonNode? child) in obj.ToList())
                {
                    if (key.StartsWith("x-", StringComparison.Ordinal) || key is "example" or "examples")
                        continue;

                    bool childIsSchema = childrenAreSchemas || IsSchemaChild(key, isSchema);
                    bool childChildrenAreSchemas = key is "schemas" || (isSchema && key is "properties");

                    changed |= NormalizeLooseSchemaBooleanFields(child, childIsSchema, childChildrenAreSchemas, normalizeLegacyNullable);
                }

                return changed;
            }
            case JsonArray array:
            {
                bool changed = false;

                foreach (JsonNode? child in array)
                {
                    changed |= NormalizeLooseSchemaBooleanFields(child, isSchema, false, normalizeLegacyNullable);
                }

                return changed;
            }
            default:
                return false;
        }
    }

    private static bool NormalizeSchemaBooleanFields(JsonObject obj, bool normalizeLegacyNullable)
    {
        bool changed = false;

        changed |= TryCoerceBooleanField(obj, "nullable");
        changed |= TryCoerceBooleanField(obj, "readOnly");
        changed |= TryCoerceBooleanField(obj, "writeOnly");
        changed |= TryCoerceBooleanField(obj, "deprecated");
        changed |= TryCoerceBooleanField(obj, "uniqueItems");
        changed |= TryCoerceBooleanField(obj, "exclusiveMaximum");
        changed |= TryCoerceBooleanField(obj, "exclusiveMinimum");

        if (normalizeLegacyNullable)
            changed |= NormalizeLegacyNullable(obj);

        return changed;
    }

    private static bool IsOpenApi31OrLater(JsonObject root)
    {
        string? version = root["openapi"]?.GetValue<string>();

        return Version.TryParse(version, out Version? parsed) && parsed.Major == 3 && parsed.Minor >= 1;
    }

    private static bool NormalizeLegacyNullable(JsonObject schema)
    {
        if (!schema.TryGetPropertyValue("nullable", out JsonNode? nullableNode) || nullableNode is not JsonValue nullableValue ||
            !nullableValue.TryGetValue(out bool nullable))
            return false;

        schema.Remove("nullable");

        if (!nullable)
            return true;

        if (schema["type"] is JsonValue typeValue && typeValue.TryGetValue(out string? type))
        {
            schema["type"] = new JsonArray(type, "null");
            return true;
        }

        if (schema["type"] is JsonArray types)
        {
            bool hasNull = types.Any(node => node is JsonValue value && value.TryGetValue(out string? itemType) && itemType == "null");

            if (!hasNull)
                types.Add("null");

            return true;
        }

        var nonNullSchema = (JsonObject) schema.DeepClone();
        schema.Clear();
        schema["anyOf"] = new JsonArray(nonNullSchema, new JsonObject { ["type"] = "null" });
        return true;
    }

    private static bool IsSchemaChild(string key, bool parentIsSchema)
    {
        if (key is "schema")
            return true;

        if (!parentIsSchema)
            return false;

        return key is "properties" or "items" or "additionalProperties" or "propertyNames" or "not" or "allOf" or "anyOf" or "oneOf";
    }

    private static bool TryCoerceBooleanField(JsonObject obj, string key)
    {
        if (!obj.TryGetPropertyValue(key, out JsonNode? node) || !TryCoerceBoolean(node, out bool value))
            return false;

        obj[key] = value;
        return true;
    }

    private static bool TryCoerceBoolean(JsonNode? node, out bool value)
    {
        value = false;

        if (node is not JsonValue jsonValue)
            return false;

        if (jsonValue.TryGetValue(out bool booleanValue))
        {
            value = booleanValue;
            return false;
        }

        if (jsonValue.TryGetValue(out string? stringValue))
        {
            switch (stringValue?.Trim().ToLowerInvariant())
            {
                case "0":
                    value = false;
                    return true;
                case "1":
                    value = true;
                    return true;
                case "false":
                    value = false;
                    return true;
                case "true":
                    value = true;
                    return true;
                default:
                    return false;
            }
        }

        if (jsonValue.TryGetValue(out int intValue) && intValue is 0 or 1)
        {
            value = intValue == 1;
            return true;
        }

        if (jsonValue.TryGetValue(out long longValue) && longValue is 0 or 1)
        {
            value = longValue == 1;
            return true;
        }

        return false;
    }
}
