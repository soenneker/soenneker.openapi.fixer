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

    [Test]
    public async ValueTask Tagged_union_keeps_required_discriminator_constraints(CancellationToken cancellationToken)
    {
        JsonObject schemas = await Fix("""
            {"Avatar":{"oneOf":[{"$ref":"#/components/schemas/CustomAvatar"},{"$ref":"#/components/schemas/DefaultAvatar"}],
              "discriminator":{"propertyName":"kind","mapping":{"custom":"#/components/schemas/CustomAvatar","default":"#/components/schemas/DefaultAvatar"}}},
             "CustomAvatar":{"type":"object","required":["kind"],"properties":{"kind":{"type":"string","enum":["custom"]},"url":{"type":"string"}}},
             "DefaultAvatar":{"type":"object","required":["kind"],"properties":{"kind":{"type":"string","enum":["default"]},"name":{"type":"string"}}}}
            """, cancellationToken);
        JsonNode avatar = schemas["Avatar"]!;
        JsonNode Resolve(JsonNode field) => field["$ref"] is JsonValue reference ? schemas[reference.GetValue<string>().Split('/')[^1]]! : field;
        await Assert.That(Resolve(schemas["CustomAvatar"]!["properties"]!["kind"]!)["enum"]![0]!.GetValue<string>()).IsEqualTo("custom");
        await Assert.That(Resolve(schemas["DefaultAvatar"]!["properties"]!["kind"]!)["enum"]![0]!.GetValue<string>()).IsEqualTo("default");
        await Assert.That(schemas["CustomAvatar"]!["required"]!.AsArray().Count).IsEqualTo(1);
        await Assert.That(schemas["DefaultAvatar"]!["required"]!.AsArray().Count).IsEqualTo(1);
        await Assert.That(avatar["oneOf"]!.AsArray().Count).IsEqualTo(2);
        await Assert.That(avatar["x-client-original-discriminator"]!["mapping"]!["custom"]!.GetValue<string>()).IsEqualTo("#/components/schemas/CustomAvatar");
    }

    [Test]
    [Arguments("anyOf")]
    [Arguments("oneOf")]
    public async ValueTask Error_union_projection_survives_the_full_pipeline(string keyword, CancellationToken cancellationToken)
    {
        string source = Path.GetTempFileName();
        string target = Path.GetTempFileName();
        try
        {
            await File.WriteAllTextAsync(source, """
                {"openapi":"3.0.3","info":{"title":"Errors","version":"1"},
                "paths":{"/test":{"get":{"operationId":"getTest","responses":{
                  "422":{"description":"Invalid","content":{"application/json":{"schema":{"$ref":"#/components/schemas/ValidationFailure"}}}}
                }}}},"components":{"schemas":{"ValidationFailure":{"anyOf":[
                  {"type":"object","required":["detail"],"properties":{"detail":{"type":"string"}}},
                  {"type":"object","required":["errors"],"properties":{"errors":{"type":"array","items":{"type":"string"}}}}
                ]}}}}
                """.Replace("anyOf", keyword), cancellationToken);
            await _fixer.Fix(source, target, cancellationToken);
            JsonNode result = JsonNode.Parse(await File.ReadAllTextAsync(target, cancellationToken))!;
            JsonNode response = result["paths"]!["/test"]!["get"]!["responses"]!["422"]!["content"]!["application/json"]!["schema"]!;
            JsonNode model = Resolve(result["components"]!["schemas"]!.AsObject(), response);
            await Assert.That(model["type"]!.GetValue<string>()).IsEqualTo("object");
            await Assert.That(model["properties"]!["detail"]).IsNotNull();
            await Assert.That(model["properties"]!["errors"]).IsNotNull();
            await Assert.That(model["x-client-original-error-schema"]).IsNotNull();
        }
        finally
        {
            File.Delete(source);
            File.Delete(target);
        }
    }

    [Test]
    public async ValueTask Example_reconciliation_runs_in_pipeline_and_can_be_disabled(CancellationToken cancellationToken)
    {
        const string input = """
            {"Bucket":{"type":"object","required":["name"],"properties":{"name":{"type":"string"}},"example":"{\"name\":\"bucket\"}"},
             "Binding":{"type":"object","properties":{"json":{"type":"object","x-stainless-any":true}}}}
            """;
        JsonObject repaired = await Fix(input, cancellationToken);
        await Assert.That(repaired["Bucket"]!["example"]!["name"]!.GetValue<string>()).IsEqualTo("bucket");
        JsonNode json = Resolve(repaired, repaired["Binding"]!["properties"]!["json"]!);
        await Assert.That(json["x-client-untyped-value"]!.GetValue<bool>()).IsTrue();
        JsonObject original = await Fix(input, cancellationToken, new OpenApiFixerOptions { ReconcileSchemaExamples = false });
        await Assert.That(original["Bucket"]!["example"]!.GetValue<string>()).IsEqualTo("{\"name\":\"bucket\"}");
    }

    [Test]
    public async ValueTask Converted_constants_do_not_leave_a_null_constraint_and_preserve_scalar_types(CancellationToken cancellationToken)
    {
        JsonObject schemas = await Fix("""
            {"Tag":{"type":"string","const":"organization"},"Enabled":{"type":"boolean","const":true},"Count":{"type":"integer","const":3}}
            """, cancellationToken, specVersion: "3.1.0");
        await Assert.That(schemas["Tag"]!.AsObject().ContainsKey("const")).IsFalse();
        await Assert.That(schemas["Tag"]!["enum"]![0]!.GetValue<string>()).IsEqualTo("organization");
        await Assert.That(schemas["Enabled"]!["x-client-original-const"]!.GetValue<bool>()).IsTrue();
        await Assert.That(schemas["Count"]!["x-client-original-const"]!.GetValue<int>()).IsEqualTo(3);
        await Assert.That(schemas["Count"]!["enum"]).IsNull();
    }

    private async ValueTask<JsonObject> Fix(string schemas, CancellationToken cancellationToken, OpenApiFixerOptions? options = null, string specVersion = "3.0.3")
    {
        string source = Path.GetTempFileName();
        string target = Path.GetTempFileName();
        try
        {
            var document = new JsonObject
            {
                ["openapi"] = specVersion,
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
