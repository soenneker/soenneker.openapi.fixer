using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static string ReconcileSchemaExamples(string json)
    {
        JsonNode root = JsonNode.Parse(json)!;
        JsonObject? schemas = root["components"]?["schemas"] as JsonObject;
        JsonObject? Resolve(JsonNode? node)
        {
            var seen = new HashSet<string>(StringComparer.Ordinal);
            while (node is JsonObject obj && obj["$ref"] is JsonValue reference)
            {
                string id = reference.GetValue<string>();
                if (!id.StartsWith("#/components/schemas/", StringComparison.Ordinal) || !seen.Add(id)) return null;
                node = schemas?[id[21..].Replace("~1", "/").Replace("~0", "~")];
            }
            return node as JsonObject;
        }
        bool Matches(JsonObject schema, JsonNode? value, int depth = 0)
        {
            if (depth > 32) return false;
            if (schema["$ref"] != null) return Resolve(schema) is JsonObject resolved && Matches(resolved, value, depth + 1);
            if (schema["enum"] is JsonArray allowed && !allowed.Any(v => JsonNode.DeepEquals(v, value))) return false;
            if (schema["type"] is JsonValue kind && kind.TryGetValue<string>(out string? type))
            {
                bool shape = type switch
                {
                    "object" => value is JsonObject,
                    "array" => value is JsonArray,
                    "string" => value is JsonValue v && v.TryGetValue<string>(out _),
                    "boolean" => value is JsonValue v && v.TryGetValue<bool>(out _),
                    "integer" => value is JsonValue v && v.TryGetValue<decimal>(out decimal n) && decimal.Truncate(n) == n,
                    "number" => value is JsonValue v && v.TryGetValue<double>(out _),
                    _ => false
                };
                if (!shape && !(value == null && schema["nullable"]?.ToJsonString() == "true")) return false;
            }
            foreach (string key in new[] { "oneOf", "anyOf" })
                if (schema[key] is JsonArray branches && !branches.Any(b => Resolve(b) is JsonObject child && Matches(child, value, depth + 1))) return false;
            if (schema["allOf"] is JsonArray all && !all.All(b => Resolve(b) is JsonObject child && Matches(child, value, depth + 1))) return false;
            if (value is JsonObject data)
            {
                if (schema["required"] is JsonArray required && required.Any(r => !data.ContainsKey(r!.GetValue<string>()))) return false;
                if (schema["properties"] is JsonObject fields)
                    foreach ((string key, JsonNode? fieldValue) in data)
                    {
                        if (Resolve(fields[key]) is JsonObject field && !Matches(field, fieldValue, depth + 1)) return false;
                        if (!fields.ContainsKey(key) && schema["additionalProperties"]?.ToJsonString() == "false") return false;
                    }
            }
            if (value is JsonArray entries && Resolve(schema["items"]) is JsonObject item && entries.Any(v => !Matches(item, v, depth + 1))) return false;
            return true;
        }
        bool changed = OpenApiJsonSchemaWalker.Visit(root, (schema, schemaPath) =>
        {
            if (schema["x-stainless-any"]?.ToJsonString() == "true" && schema["type"]?.ToJsonString() == "\"object\"" &&
                schema.All(p => p.Key is "type" or "description" or "title" or "example" or "examples" or "readOnly" or "writeOnly" || p.Key.StartsWith("x-", StringComparison.Ordinal)))
            {
                schema["x-client-original-type"] = "object";
                schema["x-client-untyped-value"] = true;
                schema.Remove("type");
                return true;
            }
            if (schema["example"] is not JsonValue example || schema["type"]?.ToJsonString() != "\"object\"") return false;
            JsonNode? replacement = null;
            string? repair = null;
            if (example.TryGetValue<string>(out string? text))
            {
                try
                {
                    JsonNode? parsed = JsonNode.Parse(text);
                    if (parsed is JsonObject && Matches(schema, parsed)) { replacement = parsed; repair = "decoded-json-object"; }
                }
                catch (JsonException) { }
                if (replacement == null && text.StartsWith("/", StringComparison.Ordinal) && schema["properties"] is JsonObject { Count: 1 } pointerFields &&
                    Resolve(pointerFields["pointer"])?["type"]?.ToJsonString() == "\"string\"")
                {
                    var pointer = new JsonObject { ["pointer"] = text };
                    if (Matches(schema, pointer)) { replacement = pointer; repair = "wrapped-json-pointer"; }
                }
            }
            if (replacement == null && schema.All(p => p.Key is "type" or "example" or "description" or "title" or "readOnly" or "writeOnly" or "deprecated" || p.Key.StartsWith("x-", StringComparison.Ordinal)))
            {
                // Empty object declarations are sometimes used as placeholders for unknown values.
                // Preserve the declared object alternative as well as the observed scalar shape.
                string scalar = example.TryGetValue<string>(out _) ? "string" : example.TryGetValue<bool>(out _) ? "boolean" : "number";
                schema["x-client-original-type"] = "object";
                schema.Remove("type");
                schema["anyOf"] = new JsonArray(new JsonObject { ["type"] = "object" }, new JsonObject { ["type"] = scalar });
                schema["x-client-example-repair"] = "preserved-object-and-scalar-shapes";
                return true;
            }
            if (replacement == null && schema["default"] is JsonObject value && Matches(schema, value))
            { replacement = value.DeepClone(); repair = "used-compatible-default"; }
            if (replacement == null && schema["properties"] is JsonObject properties)
            {
                var candidate = new JsonObject();
                foreach ((string name, JsonNode? fieldNode) in properties)
                    if (Resolve(fieldNode) is JsonObject field && (field["example"] ?? field["default"]) is JsonNode sample && Matches(field, sample))
                        candidate[name] = sample.DeepClone();
                if (candidate.Count > 0 && Matches(schema, candidate)) { replacement = candidate; repair = "used-property-examples"; }
            }
            if (replacement == null) return false;
            schema["x-client-original-example"] = example.DeepClone();
            schema["example"] = replacement;
            schema["x-client-example-repair"] = repair;
            return true;
        });
        return changed ? root.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) : json;
    }
}

