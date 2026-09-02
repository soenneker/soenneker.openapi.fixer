using Microsoft.Extensions.Logging;
using Soenneker.OpenApi.Fixer.Fixers.Abstract;
using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Numerics;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;

namespace Soenneker.OpenApi.Fixer.Fixers;

/// <inheritdoc cref="IOpenApiPreprocessingFixer"/>
public sealed class OpenApiPreprocessingFixer : IOpenApiPreprocessingFixer
{
    private const string Redacted = "[REDACTED]";

    private static readonly HashSet<string> CredentialNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "access_token", "accesstoken", "refresh_token", "refreshtoken", "auth_token", "authtoken", "token", "api_key", "apikey",
        "client_secret", "clientsecret", "secret", "password", "passwd", "webhook", "webhook_url", "webhookurl"
    };

    private static readonly Regex WebhookUrlRegex = new(
        @"https?://[^\s\""'`<>]*(?:hooks\.slack\.com/services|discord(?:app)?\.com/api/webhooks|/webhooks?/)[^\s\""'`<>]*",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);

    private static readonly Regex BearerTokenRegex = new(@"(?<prefix>\bBearer\s+)[A-Za-z0-9._~+/=-]{12,}",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);

    private static readonly Regex AssignedCredentialRegex = new(
        @"(?<prefix>[\""']?(?:access[_-]?token|refresh[_-]?token|auth[_-]?token|api[_-]?key|client[_-]?secret|secret|password)[\""']?\s*[:=]\s*[\""']?)(?<value>[A-Za-z0-9._~+/=-]{8,})",
        RegexOptions.Compiled | RegexOptions.CultureInvariant | RegexOptions.IgnoreCase);

    private readonly ILogger<OpenApiPreprocessingFixer> _logger;

    public OpenApiPreprocessingFixer(ILogger<OpenApiPreprocessingFixer> logger)
    {
        _logger = logger;
    }

    public string Fix(string json, OpenApiFixerOptions? options = null)
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
        bool changed = NormalizeLooseSchemaFields(root, false, false, normalizeLegacyNullable);
        changed |= NormalizePathParameterRequirements(root);

        if (options?.RedactCredentialLikeValues == true)
            changed |= RedactCredentialLikeContent(root, null);

        return changed ? root.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) : json;
    }

    private static bool NormalizePathParameterRequirements(JsonNode root)
    {
        if (root is not JsonObject rootObject)
            return false;

        bool changed = false;

        if (rootObject["paths"] is JsonObject paths)
        {
            foreach (JsonNode? pathNode in paths.Select(path => path.Value))
            {
                if (pathNode is not JsonObject pathItem)
                    continue;

                changed |= NormalizePathParameterArray(pathItem["parameters"]);

                foreach ((string key, JsonNode? operationNode) in pathItem)
                {
                    if (key is not ("get" or "put" or "post" or "delete" or "options" or "head" or "patch" or "trace") ||
                        operationNode is not JsonObject operation)
                        continue;

                    changed |= NormalizePathParameterArray(operation["parameters"]);
                }
            }
        }

        if (rootObject["components"]?["parameters"] is JsonObject componentParameters)
        {
            foreach (JsonNode? parameter in componentParameters.Select(entry => entry.Value))
                changed |= NormalizePathParameterRequirement(parameter);
        }

        return changed;
    }

    private static bool NormalizePathParameterArray(JsonNode? node)
    {
        if (node is not JsonArray parameters)
            return false;

        bool changed = false;

        foreach (JsonNode? parameter in parameters)
            changed |= NormalizePathParameterRequirement(parameter);

        return changed;
    }

    private static bool NormalizePathParameterRequirement(JsonNode? node)
    {
        if (node is not JsonObject parameter || parameter["in"] is not JsonValue locationValue ||
            !locationValue.TryGetValue(out string? location) || !string.Equals(location, "path", StringComparison.Ordinal))
            return false;

        if (parameter["required"] is JsonValue requiredValue && requiredValue.TryGetValue(out bool required) && required)
            return false;

        parameter["required"] = true;
        return true;
    }

    private static bool NormalizeLooseSchemaFields(JsonNode? node, bool isSchema, bool childrenAreSchemas, bool normalizeLegacyNullable)
    {
        switch (node)
        {
            case JsonObject obj:
            {
                bool changed = isSchema && NormalizeSchemaFields(obj, normalizeLegacyNullable);

                foreach ((string key, JsonNode? child) in obj.ToList())
                {
                    if (key.StartsWith("x-", StringComparison.Ordinal) || key is "example" or "examples")
                        continue;

                    bool childIsSchema = childrenAreSchemas || IsSchemaChild(key, isSchema);
                    bool childChildrenAreSchemas = key is "schemas" || (isSchema && key is "properties");

                    changed |= NormalizeLooseSchemaFields(child, childIsSchema, childChildrenAreSchemas, normalizeLegacyNullable);
                }

                return changed;
            }
            case JsonArray array:
            {
                bool changed = false;

                foreach (JsonNode? child in array)
                {
                    changed |= NormalizeLooseSchemaFields(child, isSchema, false, normalizeLegacyNullable);
                }

                return changed;
            }
            default:
                return false;
        }
    }

    private static bool NormalizeSchemaFields(JsonObject obj, bool normalizeLegacyNullable)
    {
        bool changed = NormalizeIntegerValues(obj);

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

    private static bool NormalizeIntegerValues(JsonObject schema)
    {
        if (!IsIntegerSchema(schema))
            return false;

        bool changed = false;
        bool requiresInt64 = false;

        changed |= NormalizeIntegerValue(schema, "default", ref requiresInt64);
        changed |= NormalizeIntegerValue(schema, "example", ref requiresInt64);
        changed |= NormalizeIntegerExamples(schema, ref requiresInt64);

        bool alreadyInt64 = schema["format"] is JsonValue formatValue && formatValue.TryGetValue(out string? format) &&
                            string.Equals(format, "int64", StringComparison.OrdinalIgnoreCase);

        if (requiresInt64 && !alreadyInt64)
        {
            schema["format"] = "int64";
            changed = true;
        }

        return changed;
    }

    private static bool NormalizeIntegerValue(JsonObject schema, string key, ref bool requiresInt64)
    {
        if (!schema.TryGetPropertyValue(key, out JsonNode? node) || node is null || !TryReadInteger(node, out BigInteger value))
            return false;

        if (value < int.MinValue || value > int.MaxValue)
            requiresInt64 = true;

        if (value >= long.MinValue && value <= long.MaxValue)
            return false;

        schema.Remove(key);
        return true;
    }

    private static bool NormalizeIntegerExamples(JsonObject schema, ref bool requiresInt64)
    {
        if (schema["examples"] is not JsonArray examples)
            return false;

        bool changed = false;

        for (int i = examples.Count - 1; i >= 0; i--)
        {
            JsonNode? node = examples[i];

            if (node is null || !TryReadInteger(node, out BigInteger value))
                continue;

            if (value < int.MinValue || value > int.MaxValue)
                requiresInt64 = true;

            if (value >= long.MinValue && value <= long.MaxValue)
                continue;

            examples.RemoveAt(i);
            changed = true;
        }

        if (examples.Count == 0)
            schema.Remove("examples");

        return changed;
    }

    private static bool IsIntegerSchema(JsonObject schema)
    {
        if (schema["type"] is JsonValue value && value.TryGetValue(out string? type))
            return string.Equals(type, "integer", StringComparison.Ordinal);

        return schema["type"] is JsonArray types && types.Any(node =>
            node is JsonValue typeValue && typeValue.TryGetValue(out string? type) && string.Equals(type, "integer", StringComparison.Ordinal));
    }

    private static bool TryReadInteger(JsonNode node, out BigInteger value)
    {
        string raw = node.ToJsonString();
        return BigInteger.TryParse(raw, NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture, out value);
    }

    private static bool RedactCredentialLikeContent(JsonNode? node, string? contextName)
    {
        switch (node)
        {
            case JsonObject obj:
            {
                bool changed = false;

                foreach ((string key, JsonNode? child) in obj.ToList())
                {
                    if (key == "description" && child is JsonValue descriptionValue && descriptionValue.TryGetValue(out string? description))
                    {
                        string redactedDescription = RedactCredentialText(description);

                        if (!string.Equals(description, redactedDescription, StringComparison.Ordinal))
                        {
                            obj[key] = redactedDescription;
                            changed = true;
                        }

                        continue;
                    }

                    if (key is "example" or "examples")
                    {
                        changed |= RedactExample(child, contextName);
                        continue;
                    }

                    changed |= RedactCredentialLikeContent(child, key);
                }

                return changed;
            }
            case JsonArray array:
            {
                bool changed = false;

                foreach (JsonNode? child in array)
                    changed |= RedactCredentialLikeContent(child, contextName);

                return changed;
            }
            default:
                return false;
        }
    }

    private static bool RedactExample(JsonNode? node, string? contextName)
    {
        switch (node)
        {
            case JsonObject obj:
            {
                bool changed = false;

                foreach ((string key, JsonNode? child) in obj.ToList())
                {
                    if (IsCredentialName(key) && child is JsonValue)
                    {
                        obj[key] = Redacted;
                        changed = true;
                        continue;
                    }

                    changed |= RedactExample(child, key);
                }

                return changed;
            }
            case JsonArray array:
            {
                bool changed = false;

                for (int i = 0; i < array.Count; i++)
                {
                    JsonNode? child = array[i];

                    if (child is JsonValue value && value.TryGetValue(out string? text) && ShouldRedactExampleValue(contextName, text))
                    {
                        array[i] = Redacted;
                        changed = true;
                    }
                    else
                    {
                        changed |= RedactExample(child, contextName);
                    }
                }

                return changed;
            }
            case JsonValue value when value.TryGetValue(out string? text) && ShouldRedactExampleValue(contextName, text):
                node!.ReplaceWith(JsonValue.Create(Redacted));
                return true;
            default:
                return false;
        }
    }

    private static bool ShouldRedactExampleValue(string? contextName, string value) =>
        IsCredentialName(contextName) || WebhookUrlRegex.IsMatch(value) || BearerTokenRegex.IsMatch(value) || AssignedCredentialRegex.IsMatch(value);

    private static bool IsCredentialName(string? name)
    {
        if (string.IsNullOrWhiteSpace(name))
            return false;

        string normalized = name.Replace('-', '_');
        return CredentialNames.Contains(normalized);
    }

    private static string RedactCredentialText(string value)
    {
        string result = WebhookUrlRegex.Replace(value, Redacted);
        result = BearerTokenRegex.Replace(result, match => match.Groups["prefix"].Value + Redacted);
        return AssignedCredentialRegex.Replace(result, match => match.Groups["prefix"].Value + Redacted);
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
