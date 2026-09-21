using System;
using System.IO;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Soenneker.OpenApi.Fixer.Abstract;
using Soenneker.Tests.HostedUnit;

namespace Soenneker.OpenApi.Fixer.Tests;

[ClassDataSource<Host>(Shared = SharedType.PerTestSession)]
public sealed class OpenApiPremergeTests(Host host) : HostedUnitTest(host)
{
    [Test]
    public async Task Fix_preserves_renamed_primitive_references_in_callbacks_and_headers(CancellationToken token)
    {
        const string source = """
            {"openapi":"3.0.3","info":{"title":"Callbacks","version":"1"},"paths":{},"components":{
              "schemas":{"date_time":{"type":"string","format":"date-time","minLength":20}},
              "callbacks":{"event":{"{$request.body#/url}":{"post":{"requestBody":{"content":{"application/json":{"schema":{"type":"object","properties":{"created":{"$ref":"#/components/schemas/date_time"}}}}}},
                "responses":{"200":{"description":"OK","headers":{"created":{"schema":{"$ref":"#/components/schemas/date_time"}}}}}}}}}
            }}
            """;
        string directory = Path.Combine(Path.GetTempPath(), "fix-premerge-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            string path = Path.Combine(directory, "api.json");
            await File.WriteAllTextAsync(path, source, token);
            await Resolve<IOpenApiFixer>(true).Fix(path, path, token);
            JsonNode root = JsonNode.Parse(await File.ReadAllTextAsync(path, token))!;
            JsonNode operation = root["components"]!["callbacks"]!["event"]!["{$request.body#/url}"]!["post"]!;
            foreach (JsonNode schema in new[] {
                         operation["requestBody"]!["content"]!["application/json"]!["schema"]!["properties"]!["created"]!,
                         operation["responses"]!["200"]!["headers"]!["created"]!["schema"]! })
            {
                string reference = schema["$ref"]!.GetValue<string>();
                await Assert.That(reference).IsEqualTo("#/components/schemas/DateTime");
                await Assert.That(root["components"]!["schemas"]!["DateTime"]!["minLength"]!.GetValue<int>()).IsEqualTo(20);
            }
        }
        finally { Directory.Delete(directory, true); }
    }

    [Test]
    public async Task Fix_bundles_only_referenced_local_schemas_and_preserves_cycles_and_examples(CancellationToken token)
    {
        const string dependency = """
            {"components":{"schemas":{
              "Node":{"type":"object","properties":{"next":{"$ref":"#/components/schemas/Node"},"name":{"type":"string"}}},
              "Unused":{"type":"object","properties":{"unused":{"type":"string"}}}
            }}}
            """;
        const string source = """
            {"openapi":"3.0.3","info":{"title":"Dependencies","version":"1"},"paths":{},"components":{"schemas":{
              "external_Node":{"type":"object","properties":{"existing":{"type":"boolean"}}},
              "Container":{"type":"object","properties":{"node":{"$ref":"external.yml#/components/schemas/Node"}},"example":{"$ref":"literal-payload"}}
            }}}
            """;
        string directory = Path.Combine(Path.GetTempPath(), "fix-premerge-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            string path = Path.Combine(directory, "api.json");
            string dependencyPath = Path.Combine(directory, "external.yml");
            await File.WriteAllTextAsync(path, source, token);
            await File.WriteAllTextAsync(dependencyPath, dependency, token);
            await Resolve<IOpenApiFixer>(true).Fix(path, path, token);
            JsonNode root = JsonNode.Parse(await File.ReadAllTextAsync(path, token))!;
            JsonObject schemas = root["components"]!["schemas"]!.AsObject();
            string reference = schemas["Container"]!["properties"]!["node"]!["$ref"]!.GetValue<string>();
            await Assert.That(reference.StartsWith("#/components/schemas/", StringComparison.Ordinal)).IsTrue();
            JsonNode imported = schemas[reference.Split('/')[^1]]!;
            await Assert.That(imported["properties"]!["name"]).IsNotNull();
            await Assert.That(schemas["ExternalNode"]!["properties"]!["existing"]).IsNotNull();
            await Assert.That(imported["properties"]!["next"]!["$ref"]!.GetValue<string>()).IsEqualTo(reference);
            await Assert.That(schemas.Any(entry => entry.Key.Contains("Unused", StringComparison.Ordinal))).IsFalse();
            await Assert.That(schemas["Container"]!["example"]!["$ref"]!.GetValue<string>()).IsEqualTo("literal-payload");
            await Assert.That(await File.ReadAllTextAsync(dependencyPath, token)).IsEqualTo(dependency);
        }
        finally { Directory.Delete(directory, true); }
    }

    [Test]
    public async Task Fix_rejects_missing_dependency_schema_without_replacing_output(CancellationToken token)
    {
        string directory = Path.Combine(Path.GetTempPath(), "fix-premerge-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            string source = Path.Combine(directory, "api.json");
            string target = Path.Combine(directory, "fixed.json");
            await File.WriteAllTextAsync(source, """
                {"openapi":"3.0.3","info":{"title":"Missing","version":"1"},"paths":{},
                 "components":{"schemas":{"Container":{"$ref":"external.json#/components/schemas/Missing"}}}}
                """, token);
            await File.WriteAllTextAsync(Path.Combine(directory, "external.json"), """{"components":{"schemas":{}}}""", token);
            await File.WriteAllTextAsync(target, "existing output", token);
            bool rejected = false;
            try { await Resolve<IOpenApiFixer>(true).Fix(source, target, token); }
            catch (InvalidOperationException exception) { rejected = exception.Message.Contains("Unresolved schema reference", StringComparison.Ordinal); }
            await Assert.That(rejected).IsTrue();
            await Assert.That(await File.ReadAllTextAsync(target, token)).IsEqualTo("existing output");
        }
        finally { Directory.Delete(directory, true); }
    }
}
