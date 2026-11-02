using System;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static string CorrectKnownProviderOperations(string json)
    {
        JsonNode root = JsonNode.Parse(json)!;
        if (root["paths"] is not JsonObject paths)
            return json;
        bool HasScopedHost(JsonNode? servers, string host) => servers is JsonArray list && list.Any(server =>
            Uri.TryCreate(server?["url"]?.GetValue<string>(), UriKind.Absolute, out Uri? uri) &&
            uri.Host.Equals(host, StringComparison.OrdinalIgnoreCase));
        bool HasHost(string host) => HasScopedHost(root["servers"], host);
        bool cloudflare = HasHost("api.cloudflare.com"), telnyx = HasHost("api.telnyx.com"), github = HasHost("api.github.com");

        bool changed = false;
        foreach ((string path, JsonNode? item) in paths)
        {
            // Cloudflare's GET contract has only path parameters. The source specification accidentally
            // copies the PATCH body here: https://developers.cloudflare.com/api/resources/zero_trust/subresources/networks/subresources/virtual_networks/methods/get/
            if (item?["get"] is not JsonObject operation || operation["requestBody"] is not JsonObject body)
                continue;
            bool virtualNetwork = cloudflare && Regex.IsMatch(path, @"^/accounts/\{[^{}]+\}/teamnet/virtual_networks/\{[^{}]+\}$");
            // These are binary WebSocket messages after the upgrade, not a body on the HTTP handshake.
            bool audioFrames = telnyx && path == "/speech-to-text/transcription" &&
                operation["description"]?.GetValue<string>().Contains("WebSocket", StringComparison.Ordinal) == true &&
                body["content"] is JsonObject { Count: 1 } content && content.ContainsKey("application/octet-stream");
            // GitHub's get-content request body contains response example summaries, not input.
            // https://docs.github.com/en/rest/repos/contents#get-repository-content
            bool contentExamples = github && path == "/repos/{owner}/{repo}/contents/{path}" &&
                body["content"] is JsonObject { Count: 1 } githubContent &&
                githubContent["application/json"] is JsonObject media && media.All(p => p.Key is "examples" or "schema") &&
                (media["schema"] == null || media["schema"] is JsonObject { Count: 1 } fallback && fallback["description"]?.ToString() == "Fallback media schema");
            // Postman exports can represent an explicitly empty raw GET body as a string schema.
            bool emptyLinkedInExport = (HasHost("api.linkedin.com") || HasScopedHost(operation["servers"], "api.linkedin.com") || HasScopedHost(item?["servers"], "api.linkedin.com") ||
                root["x-postman-warnings"] != null && root["info"]?["title"]?.ToString() == "LinkedIn Live Events" ||
                root["x-merged-postman-collections"] != null && root["info"]?["title"]?.ToString() == "LinkedIn API") &&
                body["required"]?.ToJsonString() != "true" && body["content"] is JsonObject { Count: > 0 } linkedInContent &&
                linkedInContent.All(p => p.Value is JsonObject linkedInMedia &&
                    (linkedInMedia["x-postman-raw-body"]?.ToJsonString() == "\"\"" ||
                     linkedInMedia["examples"] is JsonObject { Count: > 0 } examples &&
                     examples.All(e => e.Value is JsonObject example && example["value"]?.ToJsonString() == "\"\"") &&
                     ResolveFormNode(root, linkedInMedia["schema"])?["type"]?.ToJsonString() == "\"string\""));
            if (!virtualNetwork && !audioFrames && !contentExamples && !emptyLinkedInExport) continue;
            operation["x-client-original-request-body"] ??= body.DeepClone();
            operation.Remove("requestBody");
            changed = true;
        }
        return changed ? root.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) : json;
    }
}
