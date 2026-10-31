using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

internal static class GeneratedCodeAudit
{
    public static async Task Run(string specification, string directory)
    {
        JsonNode document = JsonNode.Parse(await File.ReadAllTextAsync(specification))!;
        JsonObject schemas = document["components"]?["schemas"] as JsonObject ?? new();
        var operations = new Dictionary<(string Route, string Verb), JsonObject?>();
        if (document["paths"] is JsonObject paths)
            foreach ((string route, JsonNode? item) in paths)
                if (item is JsonObject pathItem)
                    foreach ((string verb, JsonNode? value) in pathItem)
                        if (value is JsonObject operation)
                        {
                            var key = (Route(route), verb);
                            if (!operations.TryAdd(key, operation)) operations[key] = null;
                        }
        var failures = new List<string>();
        var streams = new List<string>();
        var unmatchedRoutes = new List<string>();
        int responseContracts = 0;
        int untypedProperties = 0, files = 0;
        var options = new CSharpParseOptions(LanguageVersion.Preview, preprocessorSymbols: ["NETCOREAPP3_1_OR_GREATER", "NETSTANDARD2_1_OR_GREATER", "NET11_0"]);
        foreach (string path in Directory.EnumerateFiles(directory, "*.cs", SearchOption.AllDirectories))
        {
            files++;
            SyntaxNode root = CSharpSyntaxTree.ParseText(await File.ReadAllTextAsync(path), options).GetRoot();
            foreach (Diagnostic diagnostic in root.GetDiagnostics().Where(d => d.Severity == DiagnosticSeverity.Error))
                failures.Add(Path.GetRelativePath(directory, path) + ": " + diagnostic);
            foreach (ClassDeclarationSyntax model in root.DescendantNodes().OfType<ClassDeclarationSyntax>())
            {
                string? template = model.Members.OfType<ConstructorDeclarationSyntax>().SelectMany(c => c.Initializer?.ArgumentList.Arguments ?? default)
                    .Select(a => a.Expression).OfType<LiteralExpressionSyntax>().Select(l => l.Token.ValueText)
                    .FirstOrDefault(s => s.StartsWith("{+baseurl}", StringComparison.Ordinal));
                foreach (MethodDeclarationSyntax method in model.Members.OfType<MethodDeclarationSyntax>())
                {
                    if (IsStream(method.ReturnType) && method.Identifier.ValueText.EndsWith("Async"))
                        streams.Add(model.Identifier.ValueText + "." + method.Identifier.ValueText);
                    string verb = method.Identifier.ValueText.Replace("Async", "", StringComparison.Ordinal).ToLowerInvariant();
                    if (template == null || verb is not ("get" or "post" or "put" or "patch" or "delete" or "head" or "options")) continue;
                    JsonObject? operation = operations.GetValueOrDefault((Route(template), verb));
                    if (operation == null) { unmatchedRoutes.Add(model.Identifier.ValueText + "." + method.Identifier.ValueText); continue; }
                    if (ExpectsTypedJson(operation, document))
                    {
                        responseContracts++;
                        if (IsStream(method.ReturnType) || method.ReturnType.ToString() == "Task")
                            failures.Add($"OFV002 {model.Identifier.ValueText}.{method.Identifier.ValueText} ({template}): JSON success schema lost its return type ({method.ReturnType}).");
                    }
                }
                var properties = model.Members.OfType<PropertyDeclarationSyntax>().ToDictionary(p => p.Identifier.ValueText, p => p.Type.ToString());
                untypedProperties += properties.Values.Count(t => t.Contains("UntypedNode", StringComparison.Ordinal) || t is "object" or "object?");
                if (schemas[model.Identifier.ValueText] is not JsonObject schema || schema["properties"] is not JsonObject fields) continue;
                MethodDeclarationSyntax? deserializers = model.Members.OfType<MethodDeclarationSyntax>().FirstOrDefault(m => m.Identifier.ValueText == "GetFieldDeserializers");
                if (deserializers == null) continue;
                foreach (InitializerExpressionSyntax initializer in deserializers.DescendantNodes().OfType<InitializerExpressionSyntax>())
                {
                    if (initializer.Expressions.Count != 2 || initializer.Expressions[0] is not LiteralExpressionSyntax literal ||
                        !literal.IsKind(SyntaxKind.StringLiteralExpression)) continue;
                    string wire = literal.Token.ValueText;
                    JsonObject? field = Resolve(fields[wire], schemas);
                    if (field?["type"] is not JsonValue kind || !kind.TryGetValue(out string? type) || type is not ("string" or "integer" or "number" or "boolean") ||
                        field.ContainsKey("oneOf") || field.ContainsKey("anyOf") || field.ContainsKey("allOf") || field["x-client-untyped-value"]?.ToJsonString() == "true") continue;
                    foreach (AssignmentExpressionSyntax assignment in initializer.Expressions[1].DescendantNodes().OfType<AssignmentExpressionSyntax>())
                        if (assignment.Left is IdentifierNameSyntax name && properties.TryGetValue(name.Identifier.ValueText, out string? actual) &&
                            (actual.Contains("UntypedNode", StringComparison.Ordinal) || actual is "object" or "object?"))
                            failures.Add($"OFV001 {model.Identifier.ValueText}.{name.Identifier.ValueText}: {wire} has scalar schema {type} but generated type {actual}.");
                }
            }
        }
        await File.WriteAllTextAsync(Path.Combine(Path.GetDirectoryName(specification)!, "code-audit.json"),
            JsonSerializer.Serialize(new { files, responseContracts, untypedProperties, streamMethods = streams, unmatchedRoutes, failures }, new JsonSerializerOptions { WriteIndented = true }));
        if (failures.Count > 0) throw new InvalidOperationException(string.Join(Environment.NewLine, failures.Take(30)));
    }

    private static bool IsStream(TypeSyntax type) => type.DescendantNodesAndSelf().OfType<IdentifierNameSyntax>()
        .Any(name => name.Identifier.ValueText == "Stream");

    private static string Route(string value)
    {
        value = value.Replace("{+baseurl}", "", StringComparison.Ordinal);
        int query = value.IndexOf("{?", StringComparison.Ordinal);
        if (query >= 0) value = value[..query];
        query = value.IndexOf('?');
        if (query >= 0) value = value[..query];
        return Regex.Replace(value, @"\{[^}]+\}", "{}");
    }

    private static bool ExpectsTypedJson(JsonObject operation, JsonNode document)
    {
        if (operation["responses"] is not JsonObject responses) return false;
        bool body = false;
        foreach ((string status, JsonNode? value) in responses)
        {
            if (!status.Equals("2XX", StringComparison.OrdinalIgnoreCase) && !(int.TryParse(status, out int code) && code is >= 200 and < 300)) continue;
            JsonNode? response = value;
            var seen = new HashSet<string>();
            while (response?["$ref"] is JsonValue reference)
            {
                string id = reference.GetValue<string>();
                if (!id.StartsWith("#/components/responses/") || !seen.Add(id)) return false;
                response = document["components"]?["responses"]?[id[23..].Replace("~1", "/").Replace("~0", "~")];
            }
            if (response?["content"] is not JsonObject { Count: > 0 } content) continue;
            bool json = content.Any(p =>
            {
                string media = p.Key.Split(';')[0].Trim();
                JsonObject? schema = Resolve(p.Value?["schema"], document["components"]?["schemas"] as JsonObject ?? new());
                return (media.Equals("application/json", StringComparison.OrdinalIgnoreCase) || media.EndsWith("+json", StringComparison.OrdinalIgnoreCase)) &&
                    schema is { Count: > 0 } && schema["format"]?.ToString() is not ("binary" or "byte" or "base64");
            });
            if (!json) return false;
            body = true;
        }
        return body;
    }

    private static JsonObject? Resolve(JsonNode? node, JsonObject schemas)
    {
        var seen = new HashSet<string>();
        while (node is JsonObject obj && obj["$ref"] is JsonValue reference)
        {
            string id = reference.GetValue<string>();
            if (!id.StartsWith("#/components/schemas/") || !seen.Add(id)) return null;
            node = schemas[id[21..].Replace("~1", "/").Replace("~0", "~")];
        }
        return node as JsonObject;
    }
}
