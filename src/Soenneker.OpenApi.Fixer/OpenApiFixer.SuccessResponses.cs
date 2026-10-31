using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static string ProjectDivergentSuccessResponses(string json)
    {
        JsonNode root = JsonNode.Parse(json)!;
        static bool IsJson(string mediaType)
        {
            string type = mediaType.Split(';')[0].Trim();
            return type.Equals("application/json", StringComparison.OrdinalIgnoreCase) ||
                type.StartsWith("application/", StringComparison.OrdinalIgnoreCase) && type.EndsWith("+json", StringComparison.OrdinalIgnoreCase);
        }
        JsonObject? ResolveResponse(JsonNode? node)
        {
            var seen = new HashSet<string>(StringComparer.Ordinal);
            while (node is JsonObject response && response["$ref"] is JsonValue reference)
            {
                string id = reference.GetValue<string>();
                const string prefix = "#/components/responses/";
                if (!id.StartsWith(prefix, StringComparison.Ordinal) || !seen.Add(id)) return null;
                node = root["components"]?["responses"]?[id[prefix.Length..].Replace("~1", "/").Replace("~0", "~")];
            }
            return node as JsonObject;
        }
        bool changed = false;
        foreach (string section in new[] { "paths", "webhooks" })
        {
            if (root[section] is not JsonObject paths) continue;
            foreach ((string path, JsonNode? item) in paths)
            {
                if (item is not JsonObject pathItem) continue;
                foreach ((string method, JsonNode? node) in pathItem)
                {
                    if (node is not JsonObject operation || operation["responses"] is not JsonObject responses) continue;
                    var success = responses.Where(p => p.Key.Equals("2XX", StringComparison.OrdinalIgnoreCase) ||
                        int.TryParse(p.Key, out int status) && status is >= 200 and <= 299)
                        .Select(p => (p.Key, Response: ResolveResponse(p.Value)?.DeepClone() as JsonObject)).Where(p => p.Response?["content"] is JsonObject).ToArray();
                    var bodies = success.Where(p => p.Response!["content"]!.AsObject().Count > 0).ToArray();
                    if (bodies.Length > 0 && bodies.All(p => p.Response!["content"]!.AsObject()
                        .Any(m => IsJson(m.Key) && m.Value is JsonObject obj && obj["schema"] != null)))
                    {
                        foreach (var response in bodies)
                        {
                            JsonObject content = response.Response!["content"]!.AsObject();
                            var excluded = content.Where(p => !IsJson(p.Key)).ToArray();
                            if (excluded.Length == 0) continue;
                            var originals = new JsonObject();
                            foreach (var entry in excluded)
                            {
                                originals[entry.Key] = entry.Value?.DeepClone();
                                content.Remove(entry.Key);
                            }
                            response.Response["x-client-original-non-json-content"] = originals;
                            responses[response.Key] = response.Response.DeepClone();
                            changed = true;
                        }
                    }
                    var media = success.SelectMany(p => p.Response!["content"]!.AsObject()).Where(p => p.Value is JsonObject obj && obj["schema"] != null).ToArray();
                    var alternatives = media.Select(p => p.Value!["schema"]!).GroupBy(s => s.ToJsonString(), StringComparer.Ordinal).Select(g => g.First()).ToArray();
                    void PublishSuccessAlias()
                    {
                        if (ResolveResponse(responses["200"])?["content"] is JsonObject existing && existing.Any(p => p.Value?["schema"] != null)) return;
                        var source = success.FirstOrDefault(p => p.Response!["content"]!.AsObject().Any(m => m.Value?["schema"] != null));
                        if (source.Response == null) return;
                        JsonObject alias = source.Response.DeepClone().AsObject();
                        alias["x-client-success-response-status"] = source.Key;
                        if (responses["200"] != null) alias["x-client-original-response"] = responses["200"]!.DeepClone();
                        responses["200"] = alias;
                        changed = true;
                    }
                    if (alternatives.Length < 2) { PublishSuccessAlias(); continue; }
                    bool jsonOnly = media.All(p => IsJson(p.Key));
                    JsonObject projection;
                    if (jsonOnly)
                    {
                        if (root["components"] is not JsonObject components)
                            root["components"] = components = new JsonObject();
                        if (components["schemas"] is not JsonObject schemas)
                            components["schemas"] = schemas = new JsonObject();
                        string name = OpenApiNameNormalizer.ReserveComponentName(schemas.Select(p => p.Key),
                            (operation["operationId"]?.GetValue<string>() ?? method + " " + path) + " SuccessResponse", "Response");
                        schemas[name] = new JsonObject { ["anyOf"] = new JsonArray(alternatives.Select(s => s.DeepClone()).ToArray()) };
                        projection = new JsonObject { ["$ref"] = "#/components/schemas/" + name };
                    }
                    else
                    {
                        // A single C# return type cannot parse both JSON and binary/text responses.
                        // Return the response stream so every representation remains available intact.
                        projection = new JsonObject { ["type"] = "string", ["format"] = "binary" };
                    }
                    foreach (var response in success)
                    {
                        foreach (JsonObject content in response.Response!["content"]!.AsObject().Select(p => p.Value).OfType<JsonObject>())
                        {
                            if (content["schema"] == null) continue;
                            content["x-client-original-success-schema"] = content["schema"]!.DeepClone();
                            content["schema"] = projection.DeepClone();
                        }
                        responses[response.Key] = response.Response;
                    }
                    changed = true;
                    PublishSuccessAlias();
                }
            }
        }
        return changed ? root.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) : json;
    }
}
