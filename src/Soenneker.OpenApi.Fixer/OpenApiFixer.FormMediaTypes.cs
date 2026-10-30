using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static string NormalizeExplicitFormMediaTypes(string json)
    {
        JsonNode root = JsonNode.Parse(json)!;
        bool changed = false;
        void VisitBody(JsonNode? body)
        {
            if (body?["content"] is not JsonObject content || content.ContainsKey("application/x-www-form-urlencoded") ||
                content["text/plain"] is not JsonObject media || media["encoding"] is not JsonObject { Count: > 0 } encoding ||
                !encoding.All(p => p.Value?["style"]?.ToJsonString() == "\"form\"")) return;
            // Explicit field encodings describe a form; the text writer cannot serialize its object DTO.
            JsonNode replacement = media.DeepClone();
            replacement["x-client-original-media-type"] = "text/plain";
            content.Remove("text/plain");
            content["application/x-www-form-urlencoded"] = replacement;
            changed = true;
        }
        if (root["components"]?["requestBodies"] is JsonObject bodies)
            foreach (JsonNode? body in bodies.Select(p => p.Value)) VisitBody(body);
        foreach (string section in new[] { "paths", "webhooks" })
            if (root[section] is JsonObject paths)
                foreach (JsonObject item in paths.Select(p => p.Value).OfType<JsonObject>())
                    foreach (JsonObject operation in item.Select(p => p.Value).OfType<JsonObject>()) VisitBody(operation["requestBody"]);
        return changed ? root.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) : json;
    }
}
