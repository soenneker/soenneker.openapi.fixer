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
    private static async ValueTask RepairGeneratedFormSerialization(string root, CancellationToken ct)
    {
        string lockPath = Path.Combine(root, "kiota-lock.json");
        if (!File.Exists(lockPath)) return;
        JsonNode? configuration = JsonNode.Parse(await File.ReadAllTextAsync(lockPath, ct));
        string? location = configuration?["descriptionLocation"]?.GetValue<string>();
        if (string.IsNullOrWhiteSpace(location)) return;
        string path;
        if (Uri.TryCreate(location, UriKind.Absolute, out Uri? uri))
        {
            if (!uri.IsFile) return;
            path = uri.LocalPath;
        }
        else path = Path.GetFullPath(Path.Combine(root, location));
        if (!File.Exists(path) || Path.GetExtension(path).Equals(".yaml", StringComparison.OrdinalIgnoreCase) || Path.GetExtension(path).Equals(".yml", StringComparison.OrdinalIgnoreCase)) return;
        JsonNode document = JsonNode.Parse(await File.ReadAllTextAsync(path, ct))!;
        if (document["paths"] is not JsonObject paths) return;
        var operations = new Dictionary<string, Dictionary<string, string>>(StringComparer.Ordinal);
        foreach ((string route, JsonNode? item) in paths)
        {
            if (ResolveFormNode(document, item) is not JsonObject pathItem) continue;
            foreach (string method in new[] { "get", "put", "post", "delete", "options", "head", "patch", "trace" })
            {
                if (pathItem[method] is not JsonObject operation || ResolveFormNode(document, operation["requestBody"])?["content"]?["application/x-www-form-urlencoded"] is not JsonObject media) continue;
                var rules = media["encoding"] as JsonObject ?? new JsonObject();
                // Only style-based form encodings are handled; other content representations remain on their original path.
                if (rules.Any(r => r.Value is not JsonObject rule || rule["contentType"] != null || rule["headers"] != null ||
                    rule["style"] != null && rule["style"]!.ToString() is not ("form" or "deepObject" or "spaceDelimited" or "pipeDelimited"))) continue;
                string signature = FormRouteSignature(route);
                if (!operations.TryGetValue(signature, out Dictionary<string, string>? methods)) operations[signature] = methods = new(StringComparer.Ordinal);
                methods[method.ToUpperInvariant()] = rules.ToJsonString();
            }
        }
        if (operations.Count == 0) return;
        var changed = new Dictionary<string, string>(StringComparer.Ordinal);
        bool used = false;
        foreach (string file in Directory.EnumerateFiles(root, "*RequestBuilder.cs", SearchOption.AllDirectories))
        {
            ct.ThrowIfCancellationRequested();
            string source = await File.ReadAllTextAsync(file, ct);
            if (!source.Contains("GeneratedCode(\"Kiota\"", StringComparison.Ordinal)) continue;
            Match template = Regex.Match(source, "base\\(requestAdapter, \"\\{\\+baseurl\\}(?<path>[^\"]*)\"");
            if (!template.Success) continue;
            string route = template.Groups["path"].Value;
            int query = route.IndexOf("{?", StringComparison.Ordinal);
            if (query >= 0) route = route[..query];
            if (!operations.TryGetValue(FormRouteSignature(route), out Dictionary<string, string>? methods)) continue;
            string repaired = Regex.Replace(source,
                @"(?<prefix>var requestInfo = new RequestInformation\(Method\.(?<method>\w+),[^;]+;(?:(?!return requestInfo;)[\s\S])*?)(?:requestInfo\.SetContentFromParsable\(RequestAdapter, ""application/x-www-form-urlencoded"", body\);|global::Soenneker\.OpenApi\.Generated\.OpenApiFormSerializer\.SetContent\(requestInfo, body, ""(?:[^""\\]|\\.)*""\);)",
                match =>
                {
                    if (!methods.TryGetValue(match.Groups["method"].Value, out string? rules)) return match.Value;
                    used = true;
                    string literal = "\"" + rules.Replace("\\", "\\\\").Replace("\"", "\\\"").Replace("\r", "\\r").Replace("\n", "\\n") + "\"";
                    return match.Groups["prefix"].Value + "global::Soenneker.OpenApi.Generated.OpenApiFormSerializer.SetContent(requestInfo, body, " + literal + ");";
                });
            if (repaired != source) changed[file] = repaired;
        }
        if (!used) return;
        string output = Path.Combine(root, "Serialization", "OpenApiFormSerializer.cs");
        using Stream resource = typeof(OpenApiFixer).Assembly.GetManifestResourceStream("Soenneker.OpenApi.Fixer.Resources.OpenApiFormSerializer.cs.txt")
            ?? throw new InvalidOperationException("Missing generic form serializer source resource.");
        using var reader = new StreamReader(resource);
        string content = await reader.ReadToEndAsync(ct);
        if (File.Exists(output) && !(await File.ReadAllTextAsync(output, ct)).StartsWith("// <auto-generated/>", StringComparison.Ordinal))
            throw new InvalidOperationException("Refusing to replace a user-authored form serializer.");
        Directory.CreateDirectory(Path.GetDirectoryName(output)!);
        if (!File.Exists(output) || await File.ReadAllTextAsync(output, ct) != content) await File.WriteAllTextAsync(output, content, ct);
        foreach ((string file, string source) in changed) await File.WriteAllTextAsync(file, source, ct);
    }

    private static string FormRouteSignature(string route) => Regex.Replace(Uri.UnescapeDataString(route), @"\{[^}]+\}", "{}");
}
