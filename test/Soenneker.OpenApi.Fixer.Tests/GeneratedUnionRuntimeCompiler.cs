using System;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.Kiota.Abstractions.Serialization;
using Microsoft.Kiota.Serialization.Json;

namespace Soenneker.OpenApi.Fixer.Tests;

internal static class GeneratedUnionRuntimeCompiler
{
    internal static string Build(JsonObject schema, JsonObject? schemas = null)
    {
        var writer = new GeneratedUnionMatcherWriter(schemas ?? new JsonObject());
        writer.BuildCheck(schema, "payload");
        return writer.BuildSource();
    }

    internal static Assembly Compile(params string[] sources)
    {
        string[] paths = ((string)AppContext.GetData("TRUSTED_PLATFORM_ASSEMBLIES")!).Split(Path.PathSeparator)
            .Append(typeof(IParsable).Assembly.Location).Distinct().ToArray();
        var compilation = CSharpCompilation.Create("UnionCheck" + Guid.NewGuid().ToString(),
            sources.Select(source => CSharpSyntaxTree.ParseText(source)), paths.Select(p => MetadataReference.CreateFromFile(p)),
            new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));
        using var stream = new MemoryStream();
        var result = compilation.Emit(stream);
        if (!result.Success) throw new InvalidOperationException(string.Join(Environment.NewLine, result.Diagnostics.Where(d => d.Severity == DiagnosticSeverity.Error)));
        return System.Reflection.Assembly.Load(stream.ToArray());
    }

    internal static Func<string, bool?> Matcher(JsonObject schema, JsonObject? schemas = null)
    {
        var writer = new GeneratedUnionMatcherWriter(schemas ?? new JsonObject());
        string expression = writer.BuildCheck(schema, "payload");
        var method = Compile("public static class Check { public static bool? Match(Microsoft.Kiota.Abstractions.Serialization.UntypedNode payload) => " + expression + "; }", writer.BuildSource())
            .GetType("Check")!.GetMethod("Match")!;
        return json =>
        {
            using JsonDocument document = JsonDocument.Parse(json, new JsonDocumentOptions { MaxDepth = 128 });
            UntypedNode node = new JsonParseNode(document.RootElement.Clone()).GetObjectValue(UntypedNode.CreateFromDiscriminatorValue)!;
            return (bool?)method.Invoke(null, [node]);
        };
    }
}
