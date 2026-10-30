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
        if (root["components"]?["schemas"] is not JsonObject schemas) return json;
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
                    var media = success.SelectMany(p => p.Response!["content"]!.AsObject()).Where(p => p.Value is JsonObject obj && obj["schema"] != null).ToArray();
                    var alternatives = media.Select(p => p.Value!["schema"]!).GroupBy(s => s.ToJsonString(), StringComparer.Ordinal).Select(g => g.First()).ToArray();
                    if (alternatives.Length < 2) continue;
                    bool jsonOnly = media.All(p => p.Key.Equals("application/json", StringComparison.OrdinalIgnoreCase) || p.Key.EndsWith("+json", StringComparison.OrdinalIgnoreCase));
                    JsonObject projection;
                    if (jsonOnly)
                    {
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
                }
            }
        }
        return changed ? root.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) : json;
    }
}
