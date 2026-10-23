using System.IO;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Soenneker.OpenApi.Fixer.Abstract;
using Soenneker.Tests.HostedUnit;

namespace Soenneker.OpenApi.Fixer.Tests;

[ClassDataSource<Host>(Shared = SharedType.PerTestSession)]
public sealed class OpenApiCompositionRegressionTests : HostedUnitTest
{
    private readonly IOpenApiFixer _fixer;

    public OpenApiCompositionRegressionTests(Host host) : base(host)
    {
        _fixer = Resolve<IOpenApiFixer>(true);
    }

    [Test]
    public async ValueTask AllOf_preserves_specialized_result_properties(CancellationToken cancellationToken)
    {
        JsonObject schemas = await Fix("""
            {
              "Envelope":{"type":"object","properties":{"result":{"anyOf":[{"type":"object"},{"type":"string"}]}}},
              "Response":{"allOf":[{"$ref":"#/components/schemas/Envelope"},{"type":"object","properties":{"result":{"type":"object","properties":{"domains":{"type":"array","items":{"type":"string"}}}}}}]}
            }
            """, cancellationToken);

        JsonNode response = Resolve(schemas, schemas["ResponseValue"]!);
        JsonNode result = Resolve(schemas, response["properties"]!["result"]!);
        await Assert.That(HasProperty(schemas, result, "domains")).IsTrue();
        await Assert.That(Resolve(schemas, schemas["Envelope"]!["properties"]!["result"]!)["anyOf"]).IsNotNull();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async ValueTask AllOf_preserves_both_property_constraints(bool reverseBranches, CancellationToken cancellationToken)
    {
        var branches = JsonNode.Parse("""
            [{"type":"object","properties":{"code":{"type":"string","minLength":2}}},
             {"type":"object","properties":{"code":{"type":"string","maxLength":8}}}]
            """)!.AsArray();
        if (reverseBranches)
            branches = new JsonArray(branches[1]!.DeepClone(), branches[0]!.DeepClone());
        var input = new JsonObject { ["Record"] = new JsonObject { ["allOf"] = branches } };
        JsonObject schemas = await Fix(input.ToJsonString(), cancellationToken);
        JsonNode code = Resolve(schemas, schemas["Record"]!["properties"]!["code"]!);

        await Assert.That(code.ToJsonString()).Contains("\"minLength\":2");
        await Assert.That(code.ToJsonString()).Contains("\"maxLength\":8");
    }

    [Test]
    public async ValueTask Union_does_not_copy_branch_properties_onto_the_wrapper(CancellationToken cancellationToken)
    {
        JsonObject schemas = await Fix("""
            {
              "A":{"type":"object","properties":{"name":{"type":"string"},"type":{"type":"string","enum":["A"]}}},
              "Cname":{"type":"object","properties":{"name":{"type":"string"},"type":{"type":"string","enum":["CNAME"]}}},
              "Record":{"type":"object","oneOf":[{"$ref":"#/components/schemas/A"},{"$ref":"#/components/schemas/Cname"}]}
            }
            """, cancellationToken);

        JsonNode record = schemas["Record"]!;
        await Assert.That(record["oneOf"]).IsNotNull();
        await Assert.That(record["properties"]?["name"]).IsNull();
    }

    private async ValueTask<JsonObject> Fix(string schemas, CancellationToken cancellationToken)
    {
        string source = Path.GetTempFileName();
        string target = Path.GetTempFileName();
        try
        {
            var document = new JsonObject
            {
                ["openapi"] = "3.0.3",
                ["info"] = new JsonObject { ["title"] = "Composition regression", ["version"] = "1" },
                ["paths"] = new JsonObject(),
                ["components"] = new JsonObject { ["schemas"] = JsonNode.Parse(schemas) }
            };
            await File.WriteAllTextAsync(source, document.ToJsonString(), cancellationToken);
            await _fixer.Fix(source, target, cancellationToken);
            return JsonNode.Parse(await File.ReadAllTextAsync(target, cancellationToken))!["components"]!["schemas"]!.AsObject();
        }
        finally
        {
            File.Delete(source);
            File.Delete(target);
        }
    }

    private static bool HasProperty(JsonObject schemas, JsonNode schema, string property)
    {
        schema = Resolve(schemas, schema);
        if (schema["properties"]?[property] is not null)
            return true;
        if (schema["allOf"] is JsonArray branches)
            foreach (JsonNode? branch in branches)
                if (HasProperty(schemas, branch!, property))
                    return true;
        return false;
    }

    private static JsonNode Resolve(JsonObject schemas, JsonNode schema)
    {
        while (schema["$ref"] is JsonValue reference)
        {
            string path = reference.GetValue<string>();
            schema = schemas[path[(path.LastIndexOf('/') + 1)..]]!;
        }
        return schema;
    }
}
