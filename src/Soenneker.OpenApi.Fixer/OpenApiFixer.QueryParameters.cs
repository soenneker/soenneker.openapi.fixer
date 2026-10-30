using System;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static string NormalizeKiotaQueryParameters(string json)
    {
        JsonNode root = JsonNode.Parse(json)!;
        bool changed = false;
        foreach (string section in new[] { "paths", "webhooks" })
        {
            if (root[section] is not JsonObject paths) continue;
            foreach (JsonObject path in paths.Select(p => p.Value).OfType<JsonObject>())
            {
                NormalizeParameters(path["parameters"] as JsonArray);
                foreach (string verb in new[] { "get", "put", "post", "delete", "options", "head", "patch", "trace" })
                {
                    if (path[verb] is not JsonObject operation) continue;
                    NormalizeParameters(operation["parameters"] as JsonArray);
                    if (verb != "get" || ResolveFormNode(root, operation["requestBody"]) is not JsonObject body ||
                        body["required"]?.ToJsonString() == "true" || body["content"] is not JsonObject { Count: > 0 } content) continue;
                    if (!content.All(p => p.Value is JsonObject media && ResolveFormNode(root, media["schema"]) is JsonObject schema &&
                        HasFormObjectShape(schema) && IsClosedFormObject(schema) && (schema["properties"] == null || schema["properties"] is JsonObject { Count: 0 }) &&
                        schema["required"] is not JsonArray { Count: > 0 })) continue;
                    operation["x-client-original-empty-request-body"] = operation["requestBody"]!.DeepClone();
                    operation.Remove("requestBody");
                    changed = true;
                }
            }
        }
        return changed ? root.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) : json;

        void NormalizeParameters(JsonArray? parameters)
        {
            if (parameters == null) return;
            foreach (JsonNode? original in parameters.ToArray())
            {
                if (ResolveFormNode(root, original) is not JsonObject parameter || parameter["in"]?.ToString() != "query" ||
                    parameter["style"]?.ToString() != "deepObject" || parameter["required"]?.ToJsonString() == "true" ||
                    parameter.ContainsKey("x-client-original-query-schema") || parameter["name"] is not JsonValue nameValue ||
                    !nameValue.TryGetValue(out string? name) || name.IndexOfAny(['[', ']']) >= 0 ||
                    ResolveFormNode(root, parameter["schema"]) is not JsonObject schema) continue;
                JsonObject? obj = null;
                JsonObject? scalar = null;
                if ((schema["anyOf"] ?? schema["oneOf"]) is JsonArray { Count: 2 } alternatives)
                {
                    foreach (JsonNode? alternative in alternatives)
                    {
                        JsonObject? branch = ResolveFormNode(root, alternative);
                        if (branch != null && IsFormScalar(branch)) scalar = branch;
                        else if (branch != null && HasFormObjectShape(branch)) obj = branch;
                    }
                    if (scalar == null) continue;
                }
                else if (HasFormObjectShape(schema) && IsClosedFormObject(schema)) obj = schema;
                if (obj?["properties"] is not JsonObject { Count: > 0 } fields || obj["required"] is JsonArray { Count: > 0 } ||
                    !fields.All(p => p.Key.Length > 0 && p.Key.IndexOfAny(['[', ']']) < 0 && ResolveFormNode(root, p.Value) is JsonObject field && IsFormScalar(field))) continue;
                if (fields.Any(p => parameters.Any(existing => ResolveFormNode(root, existing)?["name"]?.ToString() == name + "[" + p.Key + "]"))) continue;
                foreach ((string fieldName, JsonNode? field) in fields)
                {
                    parameters.Add((JsonNode)new JsonObject
                    {
                        ["name"] = name + "[" + fieldName + "]", ["in"] = "query", ["required"] = false,
                        ["style"] = "form", ["explode"] = false, ["schema"] = ResolveFormNode(root, field)!.DeepClone(),
                        ["x-client-original-query-schema"] = parameter["schema"]!.DeepClone()
                    });
                }
                if (scalar != null)
                {
                    var replacement = parameter.DeepClone().AsObject();
                    replacement["x-client-original-query-schema"] = parameter["schema"]!.DeepClone();
                    replacement["schema"] = scalar.DeepClone();
                    replacement["style"] = "form";
                    replacement["explode"] = false;
                    parameters[parameters.IndexOf(original)] = replacement;
                }
                else parameters.Remove(original);
                changed = true;
            }
        }
    }
}
