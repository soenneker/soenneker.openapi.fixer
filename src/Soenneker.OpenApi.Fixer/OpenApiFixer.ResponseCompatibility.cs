using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static readonly HashSet<string> UnsupportedKiotaStringFormats = new(StringComparer.OrdinalIgnoreCase)
    {
        "email", "idn-email", "hostname", "idn-hostname", "ipv4", "ipv6", "uri", "uri-reference",
        "iri", "iri-reference", "uri-template", "json-pointer", "relative-json-pointer", "regex"
    };

    private static string NormalizeKiotaResponseCompatibility(string json, OpenApiFixerOptions options)
    {
        if (!options.ProjectCompatibleErrorResponses && !options.NormalizeUnsupportedStringFormats)
            return json;

        JsonNode root = JsonNode.Parse(json)!;
        JsonObject? schemas = root["components"]?["schemas"] as JsonObject;
        var projected = new Dictionary<string, string>(StringComparer.Ordinal);

        JsonObject? Resolve(JsonNode? node, string section)
        {
            var seen = new HashSet<string>(StringComparer.Ordinal);
            string prefix = "#/components/" + section + "/";
            while (node is JsonObject { Count: 1 } reference && reference["$ref"] is JsonValue value)
            {
                if (!value.TryGetValue(out string? id) || !id.StartsWith(prefix, StringComparison.Ordinal) || !seen.Add(id))
                    return null;
                node = root["components"]?[section]?[id[prefix.Length..].Replace("~1", "/").Replace("~0", "~")];
            }
            return node as JsonObject;
        }

        static bool Annotation(string key) => key is "title" or "description" or "default" or "example" or "examples" or
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
            if (!options.ProjectCompatibleErrorResponses || schemas == null ||
                !(status is "default" or "4XX" or "5XX" or "4xx" or "5xx" ||
                  status is { Length: 3 } && int.TryParse(status, out int code) && code is >= 400 and <= 599))
                return false;

            JsonObject? resolved = Resolve(response, "responses");
            if (resolved?["content"] is not JsonObject) return false;
            // Work on a copy so a reusable response can also be used by a successful operation.
            JsonObject copy = resolved.DeepClone().AsObject();
            bool updated = false;
            foreach (JsonObject media in copy["content"]!.AsObject().Select(p => p.Value).OfType<JsonObject>())
            {
                JsonObject? schema = Resolve(media["schema"], "schemas");
                if (schema == null) continue;
                string identity = media["schema"]!.ToJsonString();
                if (!projected.TryGetValue(identity, out string? reference))
                {
                    JsonObject? model = Project(schema);
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
            if (!options.NormalizeUnsupportedStringFormats || schema["format"] is not JsonValue format ||
                !format.TryGetValue(out string? name) || !UnsupportedKiotaStringFormats.Contains(name) ||
                !(schema["type"]?.ToJsonString() == "\"string\"" || schema["type"] is JsonArray types &&
                    types.Any(t => t?.ToJsonString() == "\"string\"") && types.All(t => t?.ToJsonString() is "\"string\"" or "\"null\"")) ||
                schema.ContainsKey("x-client-original-format"))
                return false;
            schema["x-client-original-format"] = format.DeepClone();
            schema.Remove("format");
            return true;
        });
        return changed ? root.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) : json;
    }
}

