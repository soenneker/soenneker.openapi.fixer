using Microsoft.Extensions.Logging;
using Soenneker.OpenApi.Fixer.Fixers.Abstract;
using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;

namespace Soenneker.OpenApi.Fixer.Fixers;

public sealed partial class OpenApiPreprocessingFixer : IOpenApiPreprocessingFixer
{
    private const string Redacted = "[REDACTED]";

    private static readonly (string Token, string Canonical)[] LooseJsonLiterals =
        [("true", "true"), ("false", "false"), ("null", "null"), ("None", "null")];

    private static readonly FrozenSet<string> CredentialNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        "access_token", "accesstoken", "refresh_token", "refreshtoken", "auth_token", "authtoken", "token", "api_key", "apikey",
        "client_secret", "clientsecret", "secret", "password", "passwd", "webhook", "webhook_url", "webhookurl"
    }.ToFrozenSet(StringComparer.OrdinalIgnoreCase);

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
        using IDisposable? loggingScope = OpenApiFixerLogging.BeginIfNeeded(options?.VerboseLogging ?? false);
        if (string.IsNullOrWhiteSpace(json))
            return json;

        JsonNode? root;
        bool requiresCanonicalization = false;
        try
        {
            // Reject duplicate names up front: JsonNode otherwise throws later when a lazy object
            // is first accessed, outside the parsing recovery path.
            root = JsonNode.Parse(json, documentOptions: new JsonDocumentOptions { AllowDuplicateProperties = false });
        }
        catch (JsonException)
        {
            string sanitized = NormalizeLooseJsonSyntax(json);
            var documentOptions = new JsonDocumentOptions
            {
                AllowTrailingCommas = true,
                CommentHandling = JsonCommentHandling.Skip,
                AllowDuplicateProperties = false
            };
            try
            {
                root = JsonNode.Parse(sanitized, documentOptions: documentOptions);
            }
            catch (JsonException)
            {
                try
                {
                    documentOptions.AllowDuplicateProperties = true;
                    using JsonDocument document = JsonDocument.Parse(sanitized, documentOptions);
                    int duplicates = 0;
                    root = ReadDuplicateTolerantNode(document.RootElement, ref duplicates);
                    if (duplicates > 0)
                        _logger.LogWarning("Recovered {Count} duplicate JSON properties using the last occurrence of each property", duplicates);
                }
                catch (JsonException ex)
                {
                    _logger.LogVerbose(ex, "Unable to parse OpenAPI JSON after syntax recovery");
                    return json;
                }
            }
            requiresCanonicalization = true;
        }

        if (root is null)
            return json;

        bool changed = root is JsonObject metadata && NormalizeDocumentMetadata(metadata);
        bool normalizeLegacyNullable = root is JsonObject rootObject && IsOpenApi31OrLater(rootObject);
        changed |= NormalizePathParameterRequirements(root);
        changed |= OpenApiJsonSchemaWalker.Visit(root, (schema, _) => NormalizeSchemaFields(schema, normalizeLegacyNullable), NormalizeMediaTypeKeys);
        if (root is JsonObject responseDocument && options?.InferSchemasFromExamples != false)
            changed |= RecoverSchemasFromExamples(responseDocument, normalizeLegacyNullable);
        changed |= requiresCanonicalization;

        if (options?.RedactCredentialLikeValues == true)
            changed |= RedactCredentialLikeContent(root, null);

        return changed ? root.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) : json;
    }

    private static bool NormalizeMediaTypeKeys(JsonObject content)
    {
        bool changed = false;
        foreach ((string mediaType, JsonNode? value) in content.ToList())
        {
            string normalized = mediaType.Trim();
            while (normalized.EndsWith(';'))
                normalized = normalized[..^1].TrimEnd();

            if (normalized.Length == 0 || normalized == mediaType)
                continue;

            // Keep an explicitly supplied canonical entry when both spellings exist.
            content.Remove(mediaType);
            if (!content.ContainsKey(normalized))
                content[normalized] = value;
            changed = true;
        }
        return changed;
    }

    private static JsonNode? ReadDuplicateTolerantNode(JsonElement element, ref int duplicates)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                var obj = new JsonObject();
                foreach (JsonProperty property in element.EnumerateObject())
                {
                    if (obj.ContainsKey(property.Name))
                        duplicates++;
                    obj[property.Name] = ReadDuplicateTolerantNode(property.Value, ref duplicates);
                }
                return obj;
            case JsonValueKind.Array:
                var array = new JsonArray();
                foreach (JsonElement item in element.EnumerateArray())
                    array.Add(ReadDuplicateTolerantNode(item, ref duplicates));
                return array;
            case JsonValueKind.Null:
                return null;
            default:
                return JsonValue.Create(element.Clone());
        }
    }

    private static bool NormalizePathParameterRequirements(JsonNode root)
    {
        if (root is not JsonObject document)
            return false;

        bool changed = false;
        bool swagger = document.ContainsKey("swagger");
        var pending = new Queue<(JsonObject Path, string? Template)>();
        AddPaths(document["paths"], hasTemplate: true);
        AddPaths(document["webhooks"], hasTemplate: false);

        if (document["components"] is JsonObject components)
        {
            if (components["parameters"] is JsonObject parameters)
                foreach (JsonNode? parameter in parameters.Select(entry => entry.Value))
                    changed |= NormalizeParameterFields(parameter);
            AddPaths(components["pathItems"], hasTemplate: false);
            if (components["callbacks"] is JsonObject callbacks)
                foreach (JsonNode? callback in callbacks.Select(entry => entry.Value))
                    AddPaths(callback, hasTemplate: false, skipExtensions: true);
        }
        if (document["parameters"] is JsonObject legacyParameters)
            foreach (JsonNode? parameter in legacyParameters.Select(entry => entry.Value))
                changed |= NormalizeParameterFields(parameter);

        while (pending.TryDequeue(out var entry))
        {
            (JsonObject pathItem, string? template) = entry;
            HashSet<string>? names = template is null ? null : Regex.Matches(template, @"\{([^/{}]+)\}", RegexOptions.CultureInvariant)
                .Select(match => match.Groups[1].Value).ToHashSet(StringComparer.Ordinal);
            NormalizeParameters(pathItem, names);
            var operations = pathItem.Where(entry => OpenApiJsonSchemaWalker.IsOperation(entry.Key)).Select(entry => entry.Value).OfType<JsonObject>().ToList();
            if (pathItem["additionalOperations"] is JsonObject additionalOperations)
                operations.AddRange(additionalOperations.Select(entry => entry.Value).OfType<JsonObject>());
            foreach (JsonObject operation in operations)
            {
                NormalizeParameters(operation, names);
                if (names != null)
                {
                    var declared = new HashSet<string>(StringComparer.Ordinal);
                    CollectDeclared(pathItem["parameters"], declared);
                    CollectDeclared(operation["parameters"], declared);
                    foreach (string name in names)
                    {
                        if (declared.Contains(name))
                            continue;
                        var parameter = new JsonObject { ["name"] = name, ["in"] = "path", ["required"] = true };
                        if (swagger)
                            parameter["type"] = "string";
                        else
                            parameter["schema"] = new JsonObject { ["type"] = "string" };
                        if (operation["parameters"] is not JsonArray)
                            operation["parameters"] = new JsonArray();
                        operation["parameters"]!.AsArray().Add((JsonNode?)parameter);
                        changed = true;
                    }
                }
                if (operation["callbacks"] is JsonObject callbacks)
                    foreach (JsonNode? callback in callbacks.Select(entry => entry.Value))
                        AddPaths(callback, hasTemplate: false, skipExtensions: true);
            }
        }
        return changed;

        void AddPaths(JsonNode? node, bool hasTemplate, bool skipExtensions = false)
        {
            if (node is not JsonObject paths)
                return;
            foreach ((string key, JsonNode? path) in paths)
                if (key != "$ref" && (!(hasTemplate || skipExtensions) || !key.StartsWith("x-", StringComparison.Ordinal)) && path is JsonObject item)
                    pending.Enqueue((item, hasTemplate ? key : null));
        }

        void NormalizeParameters(JsonObject owner, IReadOnlySet<string>? names)
        {
            if (owner["parameters"] is JsonObject singleParameter)
            {
                owner["parameters"] = new JsonArray(singleParameter.DeepClone());
                changed = true;
            }
            if (owner["parameters"] is not JsonArray parameters)
                return;
            for (int i = parameters.Count - 1; i >= 0; i--)
            {
                JsonObject? parameter = ResolveParameter(parameters[i]);
                if (names != null && IsExtraneousPathParameter(parameter, names))
                {
                    parameters.RemoveAt(i);
                    changed = true;
                    continue;
                }
                changed |= NormalizeParameterFields(parameter);
            }
        }

        void CollectDeclared(JsonNode? node, ISet<string> declared)
        {
            if (node is not JsonArray parameters)
                return;
            foreach (JsonNode? entry in parameters)
            {
                JsonObject? parameter = ResolveParameter(entry);
                if (parameter?["in"] is JsonValue location && location.TryGetValue(out string? where) && where == "path" &&
                    parameter["name"] is JsonValue name && name.TryGetValue(out string? text) && text != null)
                    declared.Add(text);
            }
        }

        JsonObject? ResolveParameter(JsonNode? node)
        {
            var visited = new HashSet<string>(StringComparer.Ordinal);
            while (node is JsonObject parameter)
            {
                if (parameter["$ref"] is not JsonValue reference || !reference.TryGetValue(out string? text))
                    return parameter;
                if (text is null || !text.StartsWith("#/", StringComparison.Ordinal) || !visited.Add(text))
                    return null;
                node = document;
                foreach (string token in text[2..].Split('/'))
                {
                    string key = Uri.UnescapeDataString(token).Replace("~1", "/", StringComparison.Ordinal).Replace("~0", "~", StringComparison.Ordinal);
                    node = node is JsonObject obj ? obj[key] : null;
                }
            }
            return null;
        }
    }

    private static bool IsExtraneousPathParameter(JsonNode? node, IReadOnlySet<string> pathParameterNames)
    {
        return node is JsonObject parameter &&
               parameter["in"] is JsonValue locationValue && locationValue.TryGetValue(out string? location) &&
               string.Equals(location, "path", StringComparison.Ordinal) &&
               parameter["name"] is JsonValue nameValue && nameValue.TryGetValue(out string? name) &&
               !string.IsNullOrWhiteSpace(name) && !pathParameterNames.Contains(name);
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

    private static bool NormalizeParameterFields(JsonNode? node)
    {
        if (node is not JsonObject parameter)
            return false;
        bool changed = NormalizePathParameterRequirement(parameter);
        foreach (string key in new[] { "required", "deprecated", "explode", "allowEmptyValue", "allowReserved" })
            changed |= TryCoerceBooleanField(parameter, key);
        return changed;
    }

    private static bool NormalizeSchemaFields(JsonObject obj, bool normalizeLegacyNullable)
    {
        bool changed = NormalizeIntegerValues(obj);

        foreach (string key in new[] { "allOf", "anyOf", "oneOf" })
        {
            if (obj[key] is not JsonObject branch)
                continue;
            obj[key] = new JsonArray(branch.DeepClone());
            changed = true;
        }

        // Some 3.0 publishers wrap a homogeneous item schema in a one-element array.
        if (!normalizeLegacyNullable && obj["items"] is JsonArray { Count: 1 } items && items[0] is JsonObject item)
        {
            obj["items"] = item.DeepClone();
            changed = true;
        }

        // Schema examples are an array of payloads, unlike media-type named examples.
        if (obj["examples"] is JsonNode examples && examples is not JsonArray)
        {
            obj["examples"] = new JsonArray(examples.DeepClone());
            changed = true;
        }

        // OpenAPI 3.0 readers require a scalar type. Keep multi-type constraints as a union.
        if (!normalizeLegacyNullable && obj["type"] is JsonArray types && types.Count > 0 &&
            types.All(static node => node is JsonValue value && value.TryGetValue(out string? type) &&
                type is "string" or "number" or "integer" or "boolean" or "object" or "array" or "null"))
        {
            var branches = new JsonArray();
            foreach (string type in types.Select(static node => node!.GetValue<string>()).Distinct(StringComparer.Ordinal))
                branches.Add(type == "null"
                    ? new JsonObject { ["type"] = "string", ["nullable"] = true, ["enum"] = new JsonArray((JsonNode?)null) }
                    : (JsonNode)new JsonObject { ["type"] = type });
            obj.Remove("type");
            if (obj.ContainsKey("anyOf"))
                (obj["allOf"] ??= new JsonArray()).AsArray().Add((JsonNode?)new JsonObject { ["anyOf"] = branches });
            else
                obj["anyOf"] = branches;
            changed = true;
        }

        // These shapes occur in hand-authored and loosely generated specs. Preserve the supplied
        // value when its intended collection shape is unambiguous.
        if (obj["required"] is JsonValue required && required.TryGetValue(out string? propertyName))
        {
            obj["required"] = new JsonArray(propertyName);
            changed = true;
        }

        if (obj["enum"] is JsonValue scalarEnum)
        {
            obj["enum"] = new JsonArray(scalarEnum.DeepClone());
            changed = true;
        }

        foreach (string key in new[] { "minimum", "maximum", "multipleOf", "exclusiveMinimum", "exclusiveMaximum" })
        {
            if (key is "exclusiveMinimum" or "exclusiveMaximum" && !normalizeLegacyNullable)
                continue;
            changed |= TryCoerceNumericField(obj, key, integerOnly: false);
        }
        foreach (string key in new[] { "minLength", "maxLength", "minItems", "maxItems", "minProperties", "maxProperties", "minContains", "maxContains" })
            changed |= TryCoerceNumericField(obj, key, integerOnly: true);

        changed |= TryCoerceBooleanField(obj, "nullable");
        changed |= TryCoerceBooleanField(obj, "readOnly");
        changed |= TryCoerceBooleanField(obj, "writeOnly");
        changed |= TryCoerceBooleanField(obj, "deprecated");
        changed |= TryCoerceBooleanField(obj, "uniqueItems");
        // In 3.1+, these are numeric bounds. In particular, 0 and 1 are not boolean flags.
        if (!normalizeLegacyNullable)
        {
            changed |= TryCoerceBooleanField(obj, "exclusiveMaximum");
            changed |= TryCoerceBooleanField(obj, "exclusiveMinimum");
        }
        else
        {
            changed |= NormalizeExclusiveBound(obj, "exclusiveMinimum", "minimum");
            changed |= NormalizeExclusiveBound(obj, "exclusiveMaximum", "maximum");
        }

        if (normalizeLegacyNullable)
            changed |= NormalizeLegacyNullable(obj);

        return changed;
    }

    private static bool TryCoerceNumericField(JsonObject schema, string key, bool integerOnly)
    {
        if (schema[key] is not JsonValue value || !value.TryGetValue(out string? text) || text is null)
            return false;

        if (integerOnly)
        {
            if (!int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out int count) || count < 0)
                return false;
            schema[key] = count;
            return true;
        }

        try
        {
            // Keep the original numeric token, including precision beyond decimal/double.
            // Recovery must not round a constraint or turn a tiny positive limit into zero.
            if (JsonNode.Parse(text) is not JsonValue number || number.GetValueKind() != JsonValueKind.Number)
                return false;
            schema[key] = number;
            return true;
        }
        catch (JsonException)
        {
            return false;
        }
    }

    private static bool NormalizeDocumentMetadata(JsonObject root)
    {
        bool changed = false;
        foreach (string key in new[] { "openapi", "swagger" })
        {
            if (root[key] is not JsonValue value)
                continue;
            string? text = value.TryGetValue(out string? version) ? version :
                value.GetValueKind() == JsonValueKind.Number ? value.ToJsonString() : null;
            if (text is null)
                continue;
            string normalized = text.Trim();
            if (key == "openapi" && normalized is "3.0" or "3.1" or "3.2")
                normalized += ".0";
            if (key == "swagger" && normalized == "2")
                normalized = "2.0";
            if (version != normalized)
            {
                root[key] = normalized;
                changed = true;
            }
        }

        if (root["info"] is JsonObject info)
        {
            foreach (string key in new[] { "title", "version" })
            {
                if (info[key] is not JsonValue value || value.GetValueKind() != JsonValueKind.Number)
                    continue;
                info[key] = value.ToJsonString();
                changed = true;
            }
        }
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
                if (node.Parent is JsonObject parentObject)
                    parentObject[node.GetPropertyName()] = Redacted;
                else if (node.Parent is JsonArray parentArray)
                    parentArray[parentArray.IndexOf(node)] = Redacted;
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
        string? version = root["openapi"] is JsonValue value && value.TryGetValue(out string? text) ? text : null;

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
                types.Add((JsonNode?)"null");

            return true;
        }

        var nonNullSchema = (JsonObject) schema.DeepClone();
        schema.Clear();
        schema["anyOf"] = new JsonArray(nonNullSchema, new JsonObject { ["type"] = "null" });
        return true;
    }

    private static bool NormalizeExclusiveBound(JsonObject schema, string exclusiveKey, string inclusiveKey)
    {
        if (schema[exclusiveKey] is not JsonValue value)
            return false;

        bool exclusive;
        if (!value.TryGetValue(out exclusive))
        {
            if (!value.TryGetValue(out string? text) || !bool.TryParse(text, out exclusive))
                return false;
        }

        // Convert a legacy flag only when its corresponding numeric limit supplies the bound.
        // A false flag imposes no extra constraint; the inclusive limit stays in place.
        if (exclusive && schema[inclusiveKey] is JsonValue bound && bound.GetValueKind() == JsonValueKind.Number)
        {
            schema[exclusiveKey] = bound.DeepClone();
            schema.Remove(inclusiveKey);
        }
        else
            schema.Remove(exclusiveKey);

        return true;
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

    private static string NormalizeLooseJsonSyntax(string json)
    {
        var builder = new StringBuilder(json.Length + 16);
        bool inString = false;
        bool escaped = false;
        bool lineComment = false;
        bool blockComment = false;
        char previous = '\0';

        for (int index = 0; index < json.Length; index++)
        {
            char value = json[index];
            if (index == 0 && value == '\uFEFF')
                continue;

            if (lineComment || blockComment)
            {
                builder.Append(value);
                if (lineComment && value is '\r' or '\n')
                    lineComment = false;
                else if (blockComment && value == '*' && index + 1 < json.Length && json[index + 1] == '/')
                {
                    builder.Append(json[++index]);
                    blockComment = false;
                }
                continue;
            }

            if (inString)
            {
                if (value < ' ')
                {
                    builder.Append(escaped ? "u" : "\\u").Append(((int)value).ToString("X4", CultureInfo.InvariantCulture));
                    escaped = false;
                    continue;
                }

                builder.Append(value);
                if (escaped)
                    escaped = false;
                else if (value == '\\')
                    escaped = true;
                else if (value == '"')
                {
                    inString = false;
                    previous = '"';
                }
                continue;
            }

            if (value == '/' && index + 1 < json.Length && json[index + 1] is '/' or '*')
            {
                lineComment = json[index + 1] == '/';
                blockComment = !lineComment;
                builder.Append(value).Append(json[++index]);
                continue;
            }

            if (value == '"')
                inString = true;
            else if (previous is ':' or '[' or ',')
            {
                string? replacement = null;
                int length = 0;
                foreach ((string token, string canonical) in LooseJsonLiterals)
                {
                    if (!json.AsSpan(index).StartsWith(token, StringComparison.OrdinalIgnoreCase))
                        continue;
                    int end = index + token.Length;
                    if (end < json.Length && !char.IsWhiteSpace(json[end]) && json[end] is not (',' or ']' or '}' or '/'))
                        continue;
                    replacement = canonical;
                    length = token.Length;
                    break;
                }
                if (replacement != null)
                {
                    builder.Append(replacement);
                    index += length - 1;
                    previous = replacement[^1];
                    continue;
                }
            }

            builder.Append(value);
            if (!char.IsWhiteSpace(value))
                previous = value;
        }

        string result = builder.ToString();
        return string.Equals(result, json, StringComparison.Ordinal) ? json : result;
    }
}
