using System;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static string NormalizeUnresolvedServerVariables(string json)
    {
        JsonNode root = JsonNode.Parse(json)!;
        bool changed = false;
        void Servers(JsonNode? node)
        {
            if (node is not JsonArray servers) return;
            foreach (JsonObject server in servers.OfType<JsonObject>())
            {
                if (server["variables"] is not JsonObject variables || server["url"] is not JsonValue urlValue ||
                    !urlValue.TryGetValue(out string? url)) continue;
                foreach ((string name, JsonNode? value) in variables)
                {
                    if (value is not JsonObject variable || !url.Contains("{" + name + "}", StringComparison.Ordinal) ||
                        variable["enum"] is JsonArray { Count: > 0 } ||
                        variable["default"] is JsonValue existing && (!existing.TryGetValue(out string? text) || !string.IsNullOrEmpty(text))) continue;
                    // OpenAPI.NET rejects missing/empty defaults. Keep an explicitly unresolved
                    // variable rather than invent a service URL. Generated routing requires an override.
                    variable["x-client-original-default"] = variable["default"]?.DeepClone();
                    variable["default"] = "{" + name + "}";
                    changed = true;
                }
            }
        }
        Servers(root["servers"]);
        foreach (string section in new[] { "paths", "webhooks" })
            if (root[section] is JsonObject paths)
                foreach (JsonObject item in paths.Select(p => p.Value).OfType<JsonObject>())
                {
                    Servers(item["servers"]);
                    foreach (string verb in new[] { "get", "post", "put", "patch", "delete", "head", "options", "trace" })
                        if (item[verb] is JsonObject operation) Servers(operation["servers"]);
                }
        return changed ? root.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) : json;
    }
}
