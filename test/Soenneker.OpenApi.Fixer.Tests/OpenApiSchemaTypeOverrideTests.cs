using System;
using System.IO;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Soenneker.OpenApi.Fixer.Abstract;
using Soenneker.Tests.HostedUnit;

namespace Soenneker.OpenApi.Fixer.Tests;

[ClassDataSource<Host>(Shared = SharedType.PerTestSession)]
public sealed class OpenApiSchemaTypeOverrideTests(Host host) : HostedUnitTest(host)
{
    private const string Spec = """
        {"openapi":"3.0.3","info":{"title":"Context7","version":"1"},"paths":{},"components":{"schemas":{
          "Library":{"type":"object","properties":{
            "trustScore":{"type":"integer","format":"int32","minimum":0,"maximum":10,"nullable":true,"description":"Source reputation score","example":10},
            "stars":{"type":"integer"},"a/b~c":{"type":"integer","format":"int32"}},
            "example":{"trustScore":{"type":"integer"}}},
          "Alias":{"$ref":"#/components/schemas/Library"}}}}
        """;

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Fix_overrides_only_selected_schema_and_preserves_metadata(bool enabled, CancellationToken token)
    {
        var options = new OpenApiFixerOptions();
        if (enabled)
        {
            options.SchemaTypeOverrides.Add("/components/schemas/Library/properties/trustScore", new() { Type = "number", Format = "double" });
            options.SchemaTypeOverrides.Add("/components/schemas/Library/properties/a~1b~0c", new() { Type = "number" });
        }

        await WithFiles(async (source, target) =>
        {
            await Resolve<IOpenApiFixer>(true).Fix(source, target, options, token);
            JsonNode library = JsonNode.Parse(await File.ReadAllTextAsync(target, token))!["components"]!["schemas"]!["Library"]!;
            JsonNode properties = library["properties"]!;
            JsonNode score = properties["trustScore"]!;
            await Assert.That(score["type"]!.GetValue<string>()).IsEqualTo(enabled ? "number" : "integer");
            await Assert.That(score["format"]!.GetValue<string>()).IsEqualTo(enabled ? "double" : "int32");
            await Assert.That(score["nullable"]!.GetValue<bool>()).IsTrue();
            await Assert.That(score["minimum"]!.GetValue<int>()).IsEqualTo(0);
            await Assert.That(score["maximum"]!.GetValue<int>()).IsEqualTo(10);
            await Assert.That(score["description"]!.GetValue<string>()).IsEqualTo("Source reputation score");
            await Assert.That(properties["stars"]!["type"]!.GetValue<string>()).IsEqualTo("integer");
            await Assert.That(library["example"]!["trustScore"]!["type"]!.GetValue<string>()).IsEqualTo("integer");
            if (enabled)
            {
                await Assert.That(properties["a/b~c"]!["type"]!.GetValue<string>()).IsEqualTo("number");
                await Assert.That(properties["a/b~c"]!["format"]).IsNull();
            }
        }, token);
    }

    [Test]
    public async Task Fix_overrides_inline_array_branch_and_preserves_31_nullability(CancellationToken token)
    {
        const string sourceSpec = """
            {"openapi":"3.1.0","info":{"title":"Inline","version":"1"},"paths":{"/search":{"get":{
              "operationId":"search","responses":{"200":{"description":"OK","content":{"application/json":{"schema":{
                "type":"array","items":{"allOf":[{"type":"integer","format":"int32"}]}}}}}}}}}}
            """;
        var options = new OpenApiFixerOptions();
        options.SchemaTypeOverrides.Add("/paths/~1search/get/responses/200/content/application~1json/schema/items/allOf/0",
            new() { Type = "number", Format = "double" });
        await WithFiles(async (source, target) =>
        {
            JsonNode document = JsonNode.Parse(sourceSpec)!;
            document["paths"]!["/search"]!["get"]!["responses"]!["200"]!["content"]!["application/json"]!["schema"]!["items"]!["allOf"]![0]!["type"] =
                new JsonArray("integer", "null");
            await File.WriteAllTextAsync(source, document.ToJsonString(), token);
            await Resolve<IOpenApiFixer>(true).Fix(source, target, options, token);
            string result = await File.ReadAllTextAsync(target, token);
            // Normalization may inline the composition or extract it to a component. The corrected
            // nullable type must survive either representation all the way to the serialized output.
            bool corrected = false;
            void Visit(JsonNode? node)
            {
                if (node is JsonObject obj)
                {
                    if (obj["type"] is JsonArray types && obj["format"]?.GetValue<string>() == "double")
                        corrected |= types.ToJsonString() == "[\"number\",\"null\"]" || types.ToJsonString() == "[\"null\",\"number\"]";
                    foreach (var property in obj)
                        Visit(property.Value);
                }
                else if (node is JsonArray array)
                    foreach (JsonNode? child in array)
                        Visit(child);
            }
            Visit(JsonNode.Parse(result));
            await Assert.That(corrected).IsTrue();
        }, token);
    }

    [Test]
    [Arguments("/components/schemas/Library/properties/missing", "number")]
    [Arguments("/components/schemas/Library/example/trustScore", "number")]
    [Arguments("/components/schemas/Alias", "number")]
    [Arguments("/components/schemas/Library/properties/a~2b", "number")]
    [Arguments("components/schemas/Library", "number")]
    [Arguments("/components/schemas/Library/properties/trustScore", "invalid")]
    public async Task Fix_rejects_invalid_overrides_without_replacing_output(string pointer, string type, CancellationToken token)
    {
        var options = new OpenApiFixerOptions();
        options.SchemaTypeOverrides.Add(pointer, new() { Type = type });
        await WithFiles(async (source, target) =>
        {
            await File.WriteAllTextAsync(target, "existing output", token);
            ArgumentException? failure = null;
            try
            {
                await Resolve<IOpenApiFixer>(true).Fix(source, target, options, token);
            }
            catch (ArgumentException exception)
            {
                failure = exception;
            }
            await Assert.That(failure).IsNotNull();
            await Assert.That(await File.ReadAllTextAsync(target, token)).IsEqualTo("existing output");
        }, token);
    }

    private static async Task WithFiles(Func<string, string, Task> action, CancellationToken token)
    {
        string source = Path.Combine(Path.GetTempPath(), $"schema-override-{Guid.NewGuid():N}.json");
        string target = source + ".fixed.json";
        try
        {
            await File.WriteAllTextAsync(source, Spec, token);
            await action(source, target);
        }
        finally
        {
            File.Delete(source);
            File.Delete(target);
        }
    }
}
