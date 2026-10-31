using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static async ValueTask<OpenApiGeneratedRouteMetadata> ReadGeneratedServiceRoutes(string root, CancellationToken ct)
    {
        var result = new OpenApiGeneratedRouteMetadata();
        string lockFile = Path.Combine(root, "kiota-lock.json");
        if (!File.Exists(lockFile)) return result;
        string? location = JsonNode.Parse(await File.ReadAllTextAsync(lockFile, ct))?["descriptionLocation"]?.GetValue<string>();
        if (string.IsNullOrWhiteSpace(location)) return result;
        if (Uri.TryCreate(location, UriKind.Absolute, out Uri? uri) && !uri.IsFile) return result;
        string path = uri?.IsFile == true ? uri.LocalPath : Path.GetFullPath(Path.Combine(root, location));
        if (!File.Exists(path) || !Path.GetExtension(path).Equals(".json", StringComparison.OrdinalIgnoreCase)) return result;
        JsonNode document = JsonNode.Parse(await File.ReadAllTextAsync(path, ct))!;
        string? Server(JsonNode? servers)
        {
            if (servers is not JsonArray array) return null;
            string? first = null;
            foreach (JsonObject server in array.OfType<JsonObject>())
            {
                if (server["url"] is not JsonValue value || !value.TryGetValue<string>(out string? url)) continue;
                url = Regex.Replace(url, @"\{(?<name>[^}]+)\}", match => server["variables"]?[match.Groups["name"].Value]?["default"]?.GetValue<string>() ?? match.Value);
                if (url.Contains('{'))
                {
                    first ??= url.TrimEnd('/');
                    continue;
                }
                if (!Uri.TryCreate(url, UriKind.Absolute, out Uri? address) || address.Scheme is not ("https" or "http")) continue;
                url = url.TrimEnd('/');
                first ??= url;
                result.Servers.Add(url);
                result.Servers.Add(address.GetLeftPart(UriPartial.Authority));
            }
            return first;
        }
        string? rootServer = Server(document["servers"]);
        result.DefaultServer = rootServer?.Contains('{') == false ? rootServer : null;
        if (document["paths"] is not JsonObject paths) return result;
        foreach ((string route, JsonNode? pathNode) in paths)
        {
            if (pathNode is not JsonObject item) continue;
            string? pathServer = Server(item["servers"]) ?? rootServer;
            foreach ((string method, JsonNode? operation) in item)
            {
                if (method is not ("get" or "put" or "post" or "delete" or "patch" or "head" or "options" or "trace") || operation is not JsonObject op) continue;
                string? server = Server(op["servers"]) ?? pathServer;
                string original = (op["x-client-original-path"] ?? item["x-client-original-path"])?.GetValue<string>() ?? route;
                if (!original.EndsWith('/') && op["x-original-trailing-slashes"] is JsonValue trailing && trailing.TryGetValue<int>(out int count) && count > 0) original += new string('/', count);
                if (server == null && original == route) continue;
                if (server?.Contains('{') == false) result.DefaultServer ??= server;
                string signature = Regex.Replace(route, @"\{[^}]+\}", "{}");
                if (!result.Routes.TryGetValue(signature, out var methods)) result.Routes[signature] = methods = new(StringComparer.Ordinal);
                methods[method.ToUpperInvariant()] = (original, server ?? "");
            }
        }
        return result;
    }

    private static string RepairGeneratedServiceRoutes(string source, OpenApiGeneratedRouteMetadata metadata)
    {
        const string helper = "global::Soenneker.OpenApi.Generated.OpenApiServiceRoutes";
        if (metadata.Routes.Count == 0 || source.Contains(helper, StringComparison.Ordinal)) return source;
        // Kiota leaves merged clients without a root URL even when operations declare servers.
        const string rootMarker = "// Preserve declared operation service roots.";
        if (metadata.DefaultServer != null && source.Contains("RegisterDefaultSerializer", StringComparison.Ordinal) && !source.Contains(rootMarker, StringComparison.Ordinal) && !source.Contains("RequestAdapter.BaseUrl =", StringComparison.Ordinal))
            source = Regex.Replace(source, @"(public \w+\(IRequestAdapter requestAdapter\)[^\r\n]*\r?\n\s*\{)",
                "$1\n            " + rootMarker + "\n            if (string.IsNullOrEmpty(RequestAdapter.BaseUrl)) RequestAdapter.BaseUrl = " + JsonSerializer.Serialize(metadata.DefaultServer) + ";\n            PathParameters[\"baseurl\"] = RequestAdapter.BaseUrl;");
        Match template = Regex.Match(source, "base\\(requestAdapter, \"\\{\\+baseurl\\}(?<path>[^\"]*)\"");
        if (!template.Success) return source;
        string route = template.Groups["path"].Value;
        Match queryMatch = Regex.Match(route, @"\{[?&]|\?");
        int query = queryMatch.Success ? queryMatch.Index : -1;
        if (query >= 0) route = route[..query];
        string signature = Regex.Replace(route, @"\{[^}]+\}", "{}");
        if (!metadata.Routes.TryGetValue(signature, out var methods))
        {
            // Kiota drops literal suffixes on parameter-only path segments (for example {sid}.json).
            var matches = metadata.Routes.Where(entry => Regex.Replace(entry.Key, @"(\{\})\.[^/{}]+(?=/|$)", "$1") == signature).ToArray();
            if (matches.Length != 1) return source;
            methods = matches[0].Value;
        }
        string[] parameters = Regex.Matches(route, @"\{[^}]+\}").Select(m => m.Value).ToArray();
        foreach ((string method, (string original, string server)) in methods)
        {
            int index = 0;
            if (Regex.Matches(original, @"\{[^}]+\}").Count != parameters.Length)
                throw new InvalidOperationException("Original route parameter count differs from the generated route: " + original);
            string wirePath = Regex.Replace(original, @"\{[^}]+\}", _ => parameters[index++]);
            bool useConfiguredBase = server.Length == 0 || string.Equals(server, metadata.DefaultServer, StringComparison.Ordinal);
            if (useConfiguredBase && wirePath == route) continue;
            source = Regex.Replace(source, @"new RequestInformation\(Method\." + method + @", (?<template>[^\r\n]+?), PathParameters\)", match =>
            {
                string expression = match.Groups["template"].Value;
                if (useConfiguredBase)
                {
                    // Paths and query suffixes are known at generation time. Keep baseurl dynamic.
                    string? existing = expression == "UrlTemplate" ? "{+baseurl}" + template.Groups["path"].Value
                        : expression.StartsWith('"') && expression.EndsWith('"') ? JsonSerializer.Deserialize<string>(expression) : null;
                    if (existing != null)
                    {
                        Match suffix = Regex.Match(existing, @"\{[?&]|\?");
                        string corrected = "{+baseurl}" + wirePath + (suffix.Success ? existing[suffix.Index..] : "");
                        return "new RequestInformation(Method." + method + ", " + JsonSerializer.Serialize(corrected) + ", PathParameters)";
                    }
                }
                return "new RequestInformation(Method." + method + ", " + helper + ".Resolve(" + expression +
                    ", RequestAdapter.BaseUrl, " + JsonSerializer.Serialize(server) + ", " + JsonSerializer.Serialize(wirePath) + ", PathParameters.ContainsKey(\"request-raw-url\")), PathParameters)";
            });
        }
        return source;
    }

    private static async ValueTask WriteGeneratedServiceRoutes(string root, OpenApiGeneratedRouteMetadata metadata, CancellationToken ct)
    {
        if (metadata.Routes.Count == 0) return;
        string known = string.Join(", ", metadata.Servers.Order(StringComparer.Ordinal).Select(value => JsonSerializer.Serialize(value)));
        string source = """
            // <auto-generated/>
            #nullable enable
            namespace Soenneker.OpenApi.Generated;
            internal static class OpenApiServiceRoutes
            {
                private static readonly global::System.Collections.Generic.HashSet<string> KnownServers = new(global::System.StringComparer.OrdinalIgnoreCase) { KNOWN_SERVERS };
                internal static string Resolve(string template, string? configuredBaseUrl, string server, string path, bool rawUrl)
                {
                    int query = template.IndexOf('?');
                    if (query > 0 && template[query - 1] == '{') query--;
                    int continuation = template.IndexOf("{&", global::System.StringComparison.Ordinal);
                    if (continuation >= 0 && (query < 0 || continuation < query)) query = continuation;
                    string suffix = query >= 0 ? template.Substring(query) : "";
                    bool useService = server.Length > 0 && (string.IsNullOrEmpty(configuredBaseUrl) || KnownServers.Contains(configuredBaseUrl.TrimEnd('/')));
                    if (useService && !rawUrl && server.Contains('{'))
                        throw new global::System.InvalidOperationException("This operation requires a service URL with tenant/device variables resolved. Set RequestAdapter.BaseUrl to that service URL or use WithUrl.");
                    return (useService ? server : "{+baseurl}") + path + suffix;
                }
            }
            """;
        await File.WriteAllTextAsync(Path.Combine(root, "OpenApiServiceRoutes.cs"), source.Replace("KNOWN_SERVERS", known, StringComparison.Ordinal), ct);
    }
}
