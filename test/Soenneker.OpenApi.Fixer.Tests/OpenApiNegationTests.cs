using System;
using System.IO;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Soenneker.OpenApi.Fixer.Abstract;
using Soenneker.Tests.HostedUnit;

namespace Soenneker.OpenApi.Fixer.Tests;

[ClassDataSource<Host>(Shared = SharedType.PerTestSession)]
public sealed class OpenApiNegationTests(Host host) : HostedUnitTest(host)
{
    [Test]
    [Arguments("3.0.3")]
    [Arguments("3.1.1")]
    public async ValueTask Fix_hoists_negation_without_losing_constraints_or_touching_examples(string version, CancellationToken cancellationToken)
    {
        JsonObject root = await Fix(version, """
            {"Contact":{"allOf":[
                {"type":"object","properties":{"name":{"type":"string"},"phone":{"type":"string"}}},
                {"not":{"required":["phone"]},"description":"Only name is supported.","x-source":"publisher"}],
              "example":{"type":"object","allOf":[{"not":{"required":["literal"]}}]}}}
            """, cancellationToken);
        JsonNode contact = root["components"]!["schemas"]!["Contact"]!;
        await Assert.That(contact["allOf"]).IsNull();
        await Assert.That(contact["not"]!["required"]![0]!.GetValue<string>()).IsEqualTo("phone");
        await Assert.That(contact["properties"]!["name"]).IsNotNull();
        await Assert.That(contact["properties"]!["phone"]).IsNotNull();
        await Assert.That(contact["description"]!.GetValue<string>()).IsEqualTo("Only name is supported.");
        await Assert.That(contact["x-source"]!.GetValue<string>()).IsEqualTo("publisher");
        JsonNode example = contact["example"] ?? contact["examples"]![0]!;
        await Assert.That(example["allOf"]![0]!["not"]!["required"]![0]!.GetValue<string>()).IsEqualTo("literal");
    }

    [Test]
    public async ValueTask Fix_preserves_distinct_negations_and_conflicting_annotations(CancellationToken cancellationToken)
    {
        JsonObject root = await Fix("3.1.1", """
            {"Contact":{"type":"object","properties":{"name":{"type":"string"}},
              "not":{"required":["first"]},"description":"Parent annotation",
              "allOf":[{"not":{"required":["second"]},"description":"Branch annotation"}]}}
            """, cancellationToken);
        JsonNode contact = root["components"]!["schemas"]!["Contact"]!;
        await Assert.That(contact["not"]!["required"]![0]!.GetValue<string>()).IsEqualTo("first");
        await Assert.That(contact["allOf"]![0]!["not"]!["required"]![0]!.GetValue<string>()).IsEqualTo("second");
        await Assert.That(contact["description"]!.GetValue<string>()).IsEqualTo("Parent annotation");
        await Assert.That(contact["allOf"]![0]!["description"]!.GetValue<string>()).IsEqualTo("Branch annotation");
    }

    private async ValueTask<JsonObject> Fix(string version, string schemas, CancellationToken cancellationToken)
    {
        string directory = Path.Combine(Path.GetTempPath(), $"fix-negation-{Guid.NewGuid()}");
        Directory.CreateDirectory(directory);
        try
        {
            string path = Path.Combine(directory, "api.json");
            var root = new JsonObject
            {
                ["openapi"] = version,
                ["info"] = new JsonObject { ["title"] = "Negation", ["version"] = "1" },
                ["paths"] = new JsonObject(),
                ["components"] = new JsonObject { ["schemas"] = JsonNode.Parse(schemas) }
            };
            await File.WriteAllTextAsync(path, root.ToJsonString(), cancellationToken);
            await Resolve<IOpenApiFixer>(true).Fix(path, path, cancellationToken);
            return JsonNode.Parse(await File.ReadAllTextAsync(path, cancellationToken))!.AsObject();
        }
        finally
        {
            Directory.Delete(directory, true);
        }
    }
}
