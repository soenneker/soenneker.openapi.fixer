using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static readonly HashSet<string> SupportedKiotaStringFormats = new(StringComparer.OrdinalIgnoreCase)
    {
        // Kiota's string codecs and accepted annotation formats. Unknown formats remain strings;
        // preserve their annotation separately instead of asking Kiota to interpret it as a type.
        "commonmark", "html", "date", "date-time", "duration", "time", "base64url", "uuid", "binary", "byte"
    };

    private static string NormalizeKiotaResponseCompatibility(string json, OpenApiFixerOptions options)
    {
        if (!options.ProjectCompatibleErrorResponses && !options.ProjectRawErrorResponses && !options.NormalizeUnsupportedStringFormats && !options.NormalizeUnsignedIntegerFormats &&
            !options.NormalizeUnsupportedNumericFormats && !options.NormalizeUnsupportedContainerFormats)
            return json;

        JsonNode root = JsonNode.Parse(json)!;
        JsonObject? schemas = root["components"]?["schemas"] as JsonObject;
        var projected = new Dictionary<string, string>(StringComparer.Ordinal);

        JsonObject? Resolve(JsonNode? node, string section)
        {
            var seen = new HashSet<string>(StringComparer.Ordinal);
            string prefix = "#/components/" + section + "/";
            JsonObject? annotations = null;
            while (node is JsonObject reference && reference["$ref"] is JsonValue value)
            {
                if (!reference.All(p => p.Key == "$ref" || Annotation(p.Key) || p.Key == "summary") ||
                    !value.TryGetValue(out string? id) || !id.StartsWith(prefix, StringComparison.Ordinal) || !seen.Add(id))
                    return null;
                foreach ((string key, JsonNode? annotation) in reference.Where(p => p.Key != "$ref"))
                {
                    annotations ??= new JsonObject();
                    if (!annotations.ContainsKey(key)) annotations[key] = annotation?.DeepClone();
                }
                node = root["components"]?[section]?[id[prefix.Length..].Replace("~1", "/").Replace("~0", "~")];
            }
            if (node is JsonObject resolved && annotations != null)
            {
                JsonObject copy = resolved.DeepClone().AsObject();
                foreach ((string key, JsonNode? annotation) in annotations) copy[key] = annotation?.DeepClone();
                return copy;
            }
            return node as JsonObject;
        }

        static bool Annotation(string key) => key is "$schema" or "title" or "description" or "default" or "example" or "examples" or
            "deprecated" or "readOnly" or "writeOnly" or "xml" or "externalDocs" || key.StartsWith("x-", StringComparison.Ordinal);

        JsonNode MergeField(JsonNode? left, JsonNode? right)
        {
            if (JsonNode.DeepEquals(left, right)) return left?.DeepClone() ?? new JsonObject();
            JsonObject? a = Resolve(left, "schemas"), b = Resolve(right, "schemas");
            JsonObject Shape(JsonObject value) => new(value.Where(p => !Annotation(p.Key) || p.Key is "readOnly" or "writeOnly").Select(p => new KeyValuePair<string, JsonNode?>(p.Key, p.Value?.DeepClone())));
            if (a != null && b != null)
            {
                JsonObject shapeA = Shape(a), shapeB = Shape(b);
                if (JsonNode.DeepEquals(shapeA, shapeB)) return left!.DeepClone();
                // Enum alternatives share a CLR shape. Unite their values without inventing a tag.
                JsonNode? enumA = shapeA["enum"]?.DeepClone(), enumB = shapeB["enum"]?.DeepClone();
                shapeA.Remove("enum");
                shapeB.Remove("enum");
                if (shapeA["type"]?.ToJsonString() == "\"string\"" && JsonNode.DeepEquals(shapeA, shapeB))
                {
                    if (enumA is JsonArray valuesA && enumB is JsonArray valuesB)
                    {
                        var values = new JsonArray();
                        foreach (JsonNode? value in valuesA.Concat(valuesB))
                            if (!values.Any(v => JsonNode.DeepEquals(v, value))) values.Add(value?.DeepClone());
                        shapeA["enum"] = values;
                    }
                    return shapeA;
                }
            }
            // Only the incompatible field becomes untyped; the surrounding exception stays typed.
            return new JsonObject
            {
                ["x-client-untyped-value"] = true,
                ["x-client-original-field-schemas"] = new JsonArray(left?.DeepClone(), right?.DeepClone())
            };
        }

        JsonObject? Project(JsonObject schema, int depth = 0)
        {
            if (depth > 16 || (schema["oneOf"] ?? schema["anyOf"]) is not JsonArray { Count: > 1 } alternatives ||
                !schema.All(p => Annotation(p.Key) || p.Key is "oneOf" or "anyOf" or "type" or "discriminator") ||
                (schema.ContainsKey("oneOf") && schema.ContainsKey("anyOf")) ||
                (schema["type"] != null && schema["type"]!.ToJsonString() != "\"object\""))
                return null;

            var properties = new JsonObject();
            HashSet<string>? required = null;
            foreach (JsonNode? alternative in alternatives)
            {
                JsonObject? branch = Resolve(alternative, "schemas");
                if (branch != null && (branch["oneOf"] ?? branch["anyOf"]) is JsonArray)
                    branch = Project(branch, depth + 1);
                if (branch?["type"]?.ToJsonString() != "\"object\"" || branch["properties"] is not JsonObject fields ||
                    !branch.All(p => Annotation(p.Key) || p.Key is "type" or "properties" or "required" ||
                        (p.Key == "additionalProperties" && p.Value?.ToJsonString() == "true")))
                    return null;

                var names = new HashSet<string>(branch["required"] is JsonArray list
                    ? list.Select(n => n!.GetValue<string>()) : [], StringComparer.Ordinal);
                if (names.Any(name => !fields.ContainsKey(name))) return null;
                if (required == null) required = names;
                else required.IntersectWith(names);
                foreach ((string name, JsonNode? field) in fields)
                {
                    if (properties.TryGetPropertyValue(name, out JsonNode? existing))
                    {
                        properties[name] = MergeField(existing, field);
                    }
                    else properties[name] = field?.DeepClone();
                }
            }

            var result = new JsonObject { ["type"] = "object", ["properties"] = properties };
            foreach ((string name, JsonNode? value) in schema.Where(p => Annotation(p.Key)))
                result[name] = value?.DeepClone();
            if (required is { Count: > 0 })
                result["required"] = new JsonArray(required.Order(StringComparer.Ordinal).Select(n => (JsonNode?)JsonValue.Create(n)).ToArray());
            return result;
        }

        bool changed = OpenApiJsonSchemaWalker.Visit(root, (_, _) => false, responseVisitor: (response, status) =>
        {
            if ((!options.ProjectCompatibleErrorResponses && !options.ProjectRawErrorResponses) || schemas == null ||
                !(status is "default" or "4XX" or "5XX" or "4xx" or "5xx" ||
                  status is { Length: 3 } && int.TryParse(status, out int code) && code is >= 400 and <= 599))
                return false;

            JsonObject? resolved = Resolve(response, "responses");
            if (resolved?["content"] is not JsonObject) return false;
            // Work on a copy so a reusable response can also be used by a successful operation.
            JsonObject copy = resolved.DeepClone().AsObject();
            bool updated = false;
            foreach ((string mediaType, JsonNode? mediaNode) in copy["content"]!.AsObject())
            {
                if (mediaNode is not JsonObject media) continue;
                JsonObject? schema = Resolve(media["schema"], "schemas");
                if (schema == null) continue;
                string identity = mediaType + "|" + media["schema"]!.ToJsonString();
                if (!projected.TryGetValue(identity, out string? reference))
                {
                    JsonObject? model = options.ProjectCompatibleErrorResponses ? Project(schema) : null;
                    string mimeType = mediaType.Split(';')[0].Trim();
                    if (model == null && options.ProjectRawErrorResponses &&
                        (mimeType.Equals("application/json", StringComparison.OrdinalIgnoreCase) || mimeType.EndsWith("+json", StringComparison.OrdinalIgnoreCase) ||
                         mimeType.Equals("text/plain", StringComparison.OrdinalIgnoreCase)) &&
                        (schema["type"]?.ToJsonString() is "\"string\"" or "\"integer\"" or "\"number\"" or "\"boolean\"" or "\"array\"" ||
                         !schema.ContainsKey("properties") && (schema["oneOf"] is JsonArray { Count: > 0 } || schema["anyOf"] is JsonArray { Count: > 0 }) ||
                         schema.All(p => Annotation(p.Key) || p.Key == "type" && p.Value?.ToJsonString() == "\"object\"") ||
                         schema["content"] is JsonObject && schema.All(p => Annotation(p.Key) || p.Key == "content")))
                    {
                        // Kiota needs an object model to create an exception mapping. The generated-code
                        // repair replaces this placeholder's serializer with a raw JSON payload serializer.
                        model = new JsonObject
                        {
                            ["type"] = "object",
                            ["properties"] = new JsonObject { ["payload"] = new JsonObject { ["x-client-untyped-value"] = true } },
                            ["x-client-raw-error-media-type"] = mimeType,
                            ["x-client-raw-error-schema"] = media["schema"]!.DeepClone()
                        };
                    }
                    if (model == null) continue;
                    string hint = media["schema"]?["$ref"]?.GetValue<string>()?.Split('/').Last() ?? "Response";
                    string key = OpenApiNameNormalizer.ReserveComponentName(schemas.Select(p => p.Key), hint + " Error", "Model");
                    model["x-client-original-error-schema"] = media["schema"]!.DeepClone();
                    schemas[key] = model;
                    reference = "#/components/schemas/" + key;
                    projected[identity] = reference;
                }
                media["schema"] = new JsonObject { ["$ref"] = reference };
                updated = true;
            }
            if (!updated) return false;
            response.Clear();
            foreach ((string key, JsonNode? value) in copy)
                response[key] = value?.DeepClone();
            return true;
        });
        changed |= OpenApiJsonSchemaWalker.Visit(root, (schema, _) =>
        {
            if (options.NormalizeUnsupportedNumericFormats && schema["format"] is JsonValue numericFormat &&
                numericFormat.TryGetValue(out string? numericName) &&
                ((schema["type"]?.ToJsonString() == "\"integer\"" && numericName is "unix-time" or "timestamp") ||
                 (schema["type"]?.ToJsonString() == "\"number\"" && numericName == "integer")))
            {
                schema["x-client-original-format"] ??= numericFormat.DeepClone();
                if (numericName is "unix-time" or "timestamp") schema["format"] = "int64";
                else schema.Remove("format");
                return true;
            }
            if (options.NormalizeUnsignedIntegerFormats && schema["format"] is JsonValue unsignedFormat &&
                unsignedFormat.TryGetValue(out string? unsignedName) && unsignedName is "uint32" or "uint64" &&
                (schema["type"]?.ToJsonString() == "\"integer\"" || schema["type"] is JsonArray integerTypes &&
                    integerTypes.Any(t => t?.ToJsonString() == "\"integer\"") &&
                    integerTypes.All(t => t?.ToJsonString() is "\"integer\"" or "\"null\"")))
            {
                if (unsignedName == "uint64" && schema["multipleOf"] is JsonValue step &&
                    (!step.TryGetValue<decimal>(out decimal stepValue) || stepValue <= 0))
                    return false;
                schema["x-client-original-format"] ??= unsignedFormat.DeepClone();
                schema["format"] = unsignedName == "uint32" ? "int64" : "decimal";
                if (unsignedName == "uint64")
                {
                    schema["x-client-original-type"] ??= schema["type"]!.DeepClone();
                    if (schema["type"] is JsonArray nullableTypes)
                        for (int i = 0; i < nullableTypes.Count; i++)
                        {
                            if (nullableTypes[i]?.ToJsonString() == "\"integer\"") nullableTypes[i] = "number";
                        }
                    else schema["type"] = "number";
                    if (schema["multipleOf"] is JsonValue multiple && multiple.TryGetValue<decimal>(out decimal numerator))
                    {
                        // Integer multiples of p/q are multiples of p/gcd(p,q). Preserve that constraint
                        // when the generator-facing representation becomes a decimal number.
                        decimal denominator = 1;
                        while (numerator != decimal.Truncate(numerator)) { numerator *= 10; denominator *= 10; }
                        decimal a = numerator, b = denominator;
                        while (b != 0) { decimal remainder = a % b; a = b; b = remainder; }
                        schema["x-client-original-multipleOf"] ??= multiple.DeepClone();
                        schema["multipleOf"] = numerator / a;
                    }
                    else schema["multipleOf"] = 1;
                }
                return true;
            }
            if (options.NormalizeUnsupportedNumericFormats && schema["format"] is JsonValue otherFormat &&
                otherFormat.TryGetValue(out string? otherName) && !string.IsNullOrEmpty(otherName))
            {
                string? numericType = schema["type"] is JsonArray nullable
                    ? nullable.Count(t => t?.ToJsonString() != "\"null\"") == 1
                        ? nullable.OfType<JsonValue>().Select(t => t.GetValue<string>()).FirstOrDefault(t => t != "null") : null
                    : schema["type"] is JsonValue scalarType && scalarType.TryGetValue(out string? valueType) ? valueType : null;
                bool integralFormat = otherName is "int8" or "uint8" or "int16" or "uint16" or "int32" or "int64";
                if (numericType is "integer" or "number" && !integralFormat && otherName is not ("uint32" or "uint64") &&
                    !(numericType == "number" && otherName is "float" or "double" or "decimal"))
                {
                    schema["x-client-original-format"] ??= otherFormat.DeepClone();
                    if (numericType == "integer") schema["format"] = otherName == "byte" ? "int32" : "int64";
                    else schema.Remove("format");
                    return true;
                }
            }
            if (options.NormalizeUnsupportedContainerFormats && schema["format"] is JsonValue containerFormat &&
                (schema["type"]?.ToJsonString() is "\"object\"" or "\"array\"" or "\"boolean\"" or "\"null\"" ||
                 schema["type"] is JsonArray { Count: > 0 } containerTypes &&
                 containerTypes.All(t => t?.ToJsonString() is "\"object\"" or "\"array\"" or "\"boolean\"" or "\"null\"")))
            {
                schema["x-client-original-format"] ??= containerFormat.DeepClone();
                schema.Remove("format");
                return true;
            }
            if (!options.NormalizeUnsupportedStringFormats || schema["format"] is not JsonValue format ||
                !format.TryGetValue(out string? name) || string.IsNullOrEmpty(name) || SupportedKiotaStringFormats.Contains(name) ||
                !(schema["type"] == null || schema["type"]?.ToJsonString() == "\"string\"" || schema["type"] is JsonArray types &&
                    types.Any(t => t?.ToJsonString() == "\"string\"") && types.All(t => t?.ToJsonString() is "\"string\"" or "\"null\"")))
                return false;
            schema["x-client-original-format"] ??= format.DeepClone();
            schema.Remove("format");
            return true;
        });
        return changed ? root.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) : json;
    }
}

