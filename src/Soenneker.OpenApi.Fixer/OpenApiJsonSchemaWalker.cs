using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;

namespace Soenneker.OpenApi.Fixer;

// Follow OpenAPI and JSON Schema structure, never arbitrary payloads. Map keys are user-defined:
// a property named "example", "type", or "x-value" is still a schema, not a keyword or extension.
internal static class OpenApiJsonSchemaWalker
{
    internal static bool Visit(JsonNode root, Func<JsonObject, string?, bool> visitor)
    {
        if (root is not JsonObject document)
            return false;

        var pending = new Stack<(JsonObject Node, string? Name, string Kind)>();
        bool changed = false;
        bool swagger = document.ContainsKey("swagger");

        AddMap(document["definitions"], "schema");
        AddMap(document["parameters"], "parameter");
        AddMap(document["responses"], "response");
        AddMap(document["paths"], "path", skipExtensions: true);
        AddMap(document["webhooks"], "path");

        if (document["components"] is JsonObject components)
        {
            foreach ((string key, string kind) in new[]
                     {
                         ("schemas", "schema"), ("parameters", "parameter"), ("headers", "parameter"),
                         ("responses", "response"), ("requestBodies", "body"), ("callbacks", "callback"),
                         ("pathItems", "path"), ("mediaTypes", "media")
                     })
                AddMap(components[key], kind);
        }

        while (pending.TryPop(out var entry))
        {
            (JsonObject node, string? name, string kind) = entry;
            switch (kind)
            {
                case "schema":
                    changed |= visitor(node, name);
                    AddSchemaChildren(node, name);
                    break;
                case "path":
                    AddArray(node["parameters"], "parameter");
                    foreach ((string method, JsonNode? operation) in node)
                        if (IsOperation(method))
                            Add(operation, "operation", name);
                    AddMap(node["additionalOperations"], "operation");
                    break;
                case "operation":
                    AddArray(node["parameters"], "parameter");
                    Add(node["requestBody"], "body", name);
                    AddMap(node["responses"], "response", skipExtensions: true);
                    AddMap(node["callbacks"], "callback");
                    break;
                case "callback":
                    foreach ((string expression, JsonNode? path) in node)
                        if (expression != "$ref" && !expression.StartsWith("x-", StringComparison.Ordinal))
                            Add(path, "path", name);
                    break;
                case "parameter":
                    name = node["name"] is JsonValue value && value.TryGetValue(out string? parameterName) ? parameterName : name;
                    if (swagger && node.ContainsKey("type"))
                    {
                        changed |= visitor(node, name);
                        AddSchemaChildren(node, name);
                    }
                    Add(node["schema"], "schema", name);
                    AddMap(node["content"], "media");
                    break;
                case "response":
                    Add(node["schema"], "schema", name);
                    AddMap(node["headers"], "parameter");
                    AddMap(node["content"], "media");
                    break;
                case "body":
                    AddMap(node["content"], "media");
                    break;
                case "media":
                    Add(node["schema"], "schema", name);
                    Add(node["itemSchema"], "schema", name);
                    AddMap(node["encoding"], "encoding");
                    Add(node["itemEncoding"], "encoding", name);
                    AddArray(node["prefixEncoding"], "encoding");
                    break;
                case "encoding":
                    AddMap(node["headers"], "parameter");
                    AddMap(node["encoding"], "encoding");
                    break;
            }
        }

        return changed;

        void Add(JsonNode? node, string kind, string? name)
        {
            if (node is JsonObject obj)
                pending.Push((obj, name, kind));
        }

        void AddMap(JsonNode? node, string kind, bool skipExtensions = false)
        {
            if (node is not JsonObject map)
                return;
            foreach ((string key, JsonNode? value) in map.Reverse())
                if (!skipExtensions || !key.StartsWith("x-", StringComparison.Ordinal))
                    Add(value, kind, key);
        }

        void AddArray(JsonNode? node, string kind, string? name = null)
        {
            if (node is not JsonArray array)
                return;
            for (int i = array.Count - 1; i >= 0; i--)
                Add(array[i], kind, name);
        }

        void AddSchemaChildren(JsonObject schema, string? name)
        {
            foreach ((string key, JsonNode? child) in schema)
            {
                switch (key)
                {
                    case "properties":
                    case "patternProperties":
                    case "dependentSchemas":
                    case "$defs":
                    case "definitions":
                        AddMap(child, "schema");
                        break;
                    case "allOf":
                    case "anyOf":
                    case "oneOf":
                    case "prefixItems":
                        AddArray(child, "schema", name);
                        break;
                    case "items":
                        Add(child, "schema", $"{name ?? "Item"}Item");
                        AddArray(child, "schema", name);
                        break;
                    case "additionalProperties":
                        Add(child, "schema", $"{name ?? "AdditionalProperty"}AdditionalProperty");
                        break;
                    case "additionalItems":
                    case "unevaluatedItems":
                    case "unevaluatedProperties":
                    case "propertyNames":
                    case "contains":
                    case "not":
                    case "if":
                    case "then":
                    case "else":
                    case "contentSchema":
                        Add(child, "schema", name);
                        break;
                }
            }
        }
    }

    internal static bool IsOperation(string key) =>
        key is "get" or "put" or "post" or "delete" or "options" or "head" or "patch" or "trace" or "query";
}
