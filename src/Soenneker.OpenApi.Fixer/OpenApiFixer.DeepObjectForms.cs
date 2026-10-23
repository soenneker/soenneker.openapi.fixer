using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static string FlattenFixedDeepObjectForms(string json)
    {
        JsonNode root = JsonNode.Parse(json)!;
        bool changed = false;
        foreach (string section in new[] { "paths", "webhooks" })
        {
            if (root[section] is not JsonObject paths) continue;
            foreach (JsonNode? path in paths.Select(p => p.Value).ToArray())
            {
                if (path is not JsonObject item) continue;
                foreach (string verb in new[] { "get", "put", "post", "delete", "options", "head", "patch", "trace" })
                {
                    if (item[verb] is not JsonObject operation || ResolveFormNode(root, operation["requestBody"]) is not JsonObject originalBody) continue;
                    var body = (JsonObject)originalBody.DeepClone();
                    if (body["content"]?["application/x-www-form-urlencoded"] is not JsonObject media ||
                        media["encoding"] is not JsonObject encoding || media.ContainsKey("example") || media.ContainsKey("examples") ||
                        ResolveFormNode(root, media["schema"]) is not JsonObject originalSchema ||
                        !HasFormObjectShape(originalSchema) || originalSchema["properties"] is not JsonObject originalProperties) continue;
                    // Work on an operation-local copy: response schemas and other media types can share the same component.
                    var schema = (JsonObject)originalSchema.DeepClone();
                    var properties = (JsonObject)schema["properties"]!;
                    var required = schema["required"] as JsonArray;
                    bool updated = false;
                    foreach ((string name, JsonNode? value) in encoding.ToArray())
                    {
                        if (value is not JsonObject rule || rule["style"]?.ToString() != "deepObject" ||
                            rule["explode"]?.ToString() != "true" || rule.Any(p => p.Key is not ("style" or "explode")) ||
                            name.IndexOfAny(['[', ']']) >= 0 ||
                            ResolveFormNode(root, originalProperties[name]) is not JsonObject obj || !HasFormObjectShape(obj) ||
                            !IsClosedFormObject(obj) || obj["properties"] is not JsonObject { Count: > 0 } children) continue;
                        bool parentRequired = required?.Any(n => n?.ToString() == name) == true;
                        string[] childRequired = (obj["required"] as JsonArray)?.Select(n => n!.GetValue<string>()).ToArray() ?? [];
                        // Optional groups with required children need conditional validation; required empty groups have no form representation.
                        if ((!parentRequired && childRequired.Length != 0) || (parentRequired && childRequired.Length == 0) ||
                            childRequired.Any(n => !children.ContainsKey(n))) continue;
                        var fields = new Dictionary<string, JsonNode>(StringComparer.Ordinal);
                        bool supported = true;
                        foreach ((string childName, JsonNode? child) in children)
                        {
                            string key = name + "[" + childName + "]";
                            if (childName.Length == 0 || childName.IndexOfAny(['[', ']']) >= 0 || properties.ContainsKey(key) || encoding.ContainsKey(key) ||
                                ResolveFormNode(root, child) is not JsonObject scalar || !IsFormScalar(scalar))
                            { supported = false; break; }
                            fields.Add(key, scalar.DeepClone());
                        }
                        if (!supported) continue;
                        properties.Remove(name);
                        foreach ((string key, JsonNode scalar) in fields) properties.Add(key, scalar);
                        if (parentRequired)
                        {
                            for (int i = required!.Count - 1; i >= 0; i--) if (required[i]?.ToString() == name) required.RemoveAt(i);
                            foreach (string child in childRequired) required.Add(JsonValue.Create(name + "[" + child + "]"));
                        }
                        encoding.Remove(name);
                        updated = true;
                    }
                    if (!updated) continue;
                    media["schema"] = schema;
                    operation["requestBody"] = body;
                    changed = true;
                }
            }
        }
        return changed ? root.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) : json;
    }

    private static JsonObject? ResolveFormNode(JsonNode root, JsonNode? node)
    {
        var visited = new HashSet<string>(StringComparer.Ordinal);
        while (node is JsonObject obj && obj["$ref"] is JsonValue reference)
        {
            if (obj.Count != 1 || !reference.TryGetValue(out string? pointer) || !pointer.StartsWith("#/", StringComparison.Ordinal) || !visited.Add(pointer)) return null;
            node = root;
            foreach (string token in pointer[2..].Split('/'))
            {
                if (node is not JsonObject container) return null;
                node = container[token.Replace("~1", "/").Replace("~0", "~")];
            }
        }
        return node as JsonObject;
    }

    private static bool HasFormObjectShape(JsonObject schema) => schema["type"]?.ToString() == "object" && schema.All(p =>
        p.Key is "type" or "properties" or "required" or "additionalProperties" or "title" or "description" or "$comment");

    private static bool IsClosedFormObject(JsonObject schema) => schema["additionalProperties"] is JsonValue flag && flag.TryGetValue(out bool allowed) && !allowed ||
        schema["additionalProperties"] is JsonObject { Count: 1 } blocked && blocked["not"] is JsonObject { Count: 0 };

    private static bool IsFormScalar(JsonObject schema)
    {
        if (schema["type"]?.ToString() is not ("string" or "integer" or "number" or "boolean")) return false;
        // Retain scalar validation and documentation, but do not flatten nullability, compositions, or read/write-only projections.
        return schema.All(p => p.Key is "type" or "format" or "enum" or "const" or "default" or "title" or "description" or "$comment" or
            "minimum" or "maximum" or "exclusiveMinimum" or "exclusiveMaximum" or "multipleOf" or "minLength" or "maxLength" or "pattern" || p.Key.StartsWith("x-", StringComparison.Ordinal));
    }
}
