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
    public async ValueTask Cloudflare_null_only_result_stays_inline(CancellationToken cancellationToken)
    {
        JsonObject schemas = await Fix("""
            {
              "RulesetsFailureResponse":{"allOf":[
                {"type":"object","properties":{"errors":{"type":"array","items":{"type":"string"}}}},
                {"type":"object","properties":{
                  "result":{"type":"object","nullable":true,"enum":[null]},
                  "success":{"type":"boolean","enum":[false]}
                }}
              ]}
            }
            """, cancellationToken);

        JsonNode response = Resolve(schemas, schemas["RulesetsFailureResponse"]!);
        JsonNode result = response["properties"]!["result"]!;
        await Assert.That(result["$ref"]).IsNull();
        await Assert.That(result["type"]!.GetValue<string>()).IsEqualTo("object");
        await Assert.That(result["nullable"]!.GetValue<bool>()).IsTrue();
        await Assert.That(result["enum"]!.ToJsonString()).IsEqualTo("[null]");
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

    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async ValueTask Compatible_targeting_alternatives_have_one_client_model(bool project, CancellationToken cancellationToken)
    {
        JsonObject schemas = await Fix("""
            {
              "Target":{"anyOf":[{"type":"object","properties":{"segments":{"type":"array","items":{"type":"string"}}}},
                {"type":"object","properties":{"players":{"type":"array","items":{"type":"string"}}}}]},
              "Notification":{"allOf":[{"$ref":"#/components/schemas/Target"},
                {"type":"object","required":["app_id"],"properties":{"app_id":{"type":"string"},"contents":{"type":"string"}}}]}
            }
            """, cancellationToken, new OpenApiFixerOptions { FlattenCompatibleObjectAlternatives = project });
        JsonNode notification = schemas["Notification"]!;
        if (project)
        {
            await Assert.That(notification["anyOf"]).IsNull();
            foreach (string property in new[] { "app_id", "contents", "segments", "players" })
                await Assert.That(notification["properties"]![property]).IsNotNull();
            await Assert.That(notification["required"]!.ToJsonString()).IsEqualTo("[\"app_id\"]");
            await Assert.That(notification["x-client-original-anyOf"]!.AsArray().Count).IsEqualTo(2);
        }
        else
        {
            await Assert.That(notification["anyOf"]).IsNotNull();
            await Assert.That(notification["properties"]).IsNull();
        }
    }

    [Test]
    [Arguments("\"required\":[\"left\"],", "\"required\":[\"right\"],", "anyOf")]
    [Arguments("\"additionalProperties\":false,", "", "anyOf")]
    [Arguments("", "", "oneOf")]
    [Arguments("\"minProperties\":1,", "", "anyOf")]
    public async ValueTask Constrained_alternatives_keep_composition(string left, string right, string keyword, CancellationToken cancellationToken)
    {
        JsonObject schemas = await Fix("""
            {"AlternativeContainer":{"KEYWORD":[
              {"type":"object",LEFT"properties":{"left":{"type":"string"}}},
              {"type":"object",RIGHT"properties":{"right":{"type":"string"}}}]}}
            """.Replace("KEYWORD", keyword).Replace("LEFT", left).Replace("RIGHT", right), cancellationToken);
        await Assert.That(schemas["AlternativeContainer"]![keyword]).IsNotNull();
        await Assert.That(schemas["AlternativeContainer"]!["x-client-original-anyOf"]).IsNull();
    }

    [Test]
    public async ValueTask Conflicting_optional_properties_keep_composition(CancellationToken cancellationToken)
    {
        JsonObject schemas = await Fix("""
            {"AlternativeContainer":{"anyOf":[{"type":"object","properties":{"value":{"type":"string"}}},
                {"type":"object","properties":{"value":{"type":"number"}}}]}}
            """, cancellationToken);
        await Assert.That(schemas["AlternativeContainer"]!["anyOf"]).IsNotNull();
    }

    private async ValueTask<JsonObject> Fix(string schemas, CancellationToken cancellationToken, OpenApiFixerOptions? options = null)
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
            await _fixer.Fix(source, target, options, cancellationToken);
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
