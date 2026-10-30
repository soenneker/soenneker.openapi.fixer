using System.Reflection;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Fixer.Tests;

public sealed class OpenApiResponseCompatibilityTests
{
    [Test]
    [Arguments("422")]
    [Arguments("429")]
    [Arguments("403")]
    [Arguments("4XX")]
    [Arguments("default")]
    public async ValueTask Object_error_unions_get_a_dedicated_model(string status)
    {
        JsonNode input = Document(status);
        JsonNode result = Run(input);
        string reference = result["paths"]!["/test"]!["get"]!["responses"]![status]!["content"]!["application/json"]!["schema"]!["$ref"]!.GetValue<string>();
        JsonNode model = result["components"]!["schemas"]![reference[21..]]!;
        await Assert.That(model["type"]!.GetValue<string>()).IsEqualTo("object");
        await Assert.That(model["properties"]!["detail"]).IsNotNull();
        await Assert.That(model["properties"]!["errors"]).IsNotNull();
        await Assert.That(model["required"]).IsNull();
        await Assert.That(model["x-client-original-error-schema"]).IsNotNull();
        await Assert.That(JsonNode.DeepEquals(input["components"]!["schemas"]!["Failure"], result["components"]!["schemas"]!["Failure"])).IsTrue();
        await Assert.That(JsonNode.DeepEquals(input["paths"]!["/test"]!["get"]!["responses"]!["200"], result["paths"]!["/test"]!["get"]!["responses"]!["200"])).IsTrue();
        await Assert.That(JsonNode.DeepEquals(result, Run(result))).IsTrue();
    }

    [Test]
    [Arguments("closed")]
    [Arguments("scalar")]
    [Arguments("disabled")]
    public async ValueTask Unsafe_or_disabled_projections_preserve_the_response(string reason)
    {
        JsonNode input = Document("422");
        JsonNode branch = input["components"]!["schemas"]!["Failure"]!["anyOf"]![1]!;
        if (reason == "closed") branch["additionalProperties"] = false;
        if (reason == "scalar") branch.ReplaceWith(new JsonObject { ["type"] = "string" });
        JsonNode result = Run(input, new OpenApiFixerOptions { ProjectCompatibleErrorResponses = reason != "disabled" });
        await Assert.That(JsonNode.DeepEquals(input, result)).IsTrue();
    }

    [Test]
    [Arguments("Shared")]
    [Arguments("422")]
    public async ValueTask Reusable_responses_are_copied_only_for_errors(string name)
    {
        JsonNode input = Document("422");
        JsonNode responses = input["paths"]!["/test"]!["get"]!["responses"]!;
        input["components"]!["responses"] = new JsonObject { [name] = responses["422"]!.DeepClone() };
        responses["422"] = new JsonObject { ["$ref"] = "#/components/responses/" + name };
        responses["200"] = responses["422"]!.DeepClone();
        JsonNode result = Run(input);
        await Assert.That(result["paths"]!["/test"]!["get"]!["responses"]!["422"]!["$ref"]).IsNull();
        await Assert.That(JsonNode.DeepEquals(input["components"]!["responses"], result["components"]!["responses"])).IsTrue();
        await Assert.That(JsonNode.DeepEquals(responses["200"], result["paths"]!["/test"]!["get"]!["responses"]!["200"])).IsTrue();
    }

    [Test]
    public async ValueTask Format_cleanup_is_opt_in_and_ignores_payloads()
    {
        JsonNode input = Document("422");
        input["components"]!["schemas"]!["Formats"] = JsonNode.Parse("""
            {"type":"object","properties":{
              "email":{"type":"string","format":"email"},
              "uri":{"type":"string","format":"uri"},
              "id":{"type":"string","format":"uuid"},
              "date":{"type":"string","format":"date-time"},
              "custom":{"type":"string","format":"custom"}},
              "example":{"type":"string","format":"email"}}
            """);
        JsonNode defaults = Run(input);
        await Assert.That(defaults["components"]!["schemas"]!["Formats"]!["properties"]!["email"]!["format"]).IsNotNull();
        JsonNode result = Run(input, new OpenApiFixerOptions { NormalizeUnsupportedStringFormats = true });
        JsonNode formats = result["components"]!["schemas"]!["Formats"]!;
        foreach (string name in new[] { "email", "uri" })
        {
            await Assert.That(formats["properties"]![name]!["format"]).IsNull();
            await Assert.That(formats["properties"]![name]!["x-client-original-format"]!.GetValue<string>()).IsEqualTo(name);
        }
        foreach (string name in new[] { "id", "date", "custom" })
            await Assert.That(formats["properties"]![name]!["format"]).IsNotNull();
        await Assert.That(formats["example"]!["format"]!.GetValue<string>()).IsEqualTo("email");
        await Assert.That(JsonNode.DeepEquals(result, Run(result, new OpenApiFixerOptions { NormalizeUnsupportedStringFormats = true }))).IsTrue();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async ValueTask Discriminator_wrapper_keeps_constraints_not_guaranteed_by_branches(bool requiredTag)
    {
        JsonNode input = JsonNode.Parse("""
            {"components":{"schemas":{"Tagged":{"type":"object","properties":{"kind":{"type":"string","minLength":3}},
            "required":["kind"],"discriminator":{"propertyName":"kind"},"oneOf":[
              {"type":"object","properties":{"kind":{"type":"string","enum":["a"]}}},
              {"type":"object","properties":{"kind":{"type":"string","enum":["b"]}}}
            ]}}}}
            """)!;
        if (requiredTag)
            foreach (JsonNode? branch in input["components"]!["schemas"]!["Tagged"]!["oneOf"]!.AsArray())
                branch!["required"] = new JsonArray("kind");
        string result = (string)typeof(OpenApiFixer).GetMethod("PreserveGeneratorUnionBranches", BindingFlags.Static | BindingFlags.NonPublic)!
            .Invoke(null, [input.ToJsonString()])!;
        await Assert.That(JsonNode.DeepEquals(input, JsonNode.Parse(result))).IsTrue();
    }

    [Test]
    public async ValueTask Nested_discriminator_target_keeps_its_common_string_tag()
    {
        JsonNode input = JsonNode.Parse("""
            {"components":{"schemas":{
              "Outer":{"oneOf":[{"$ref":"#/components/schemas/Inner"},{"$ref":"#/components/schemas/Other"}],
                "discriminator":{"propertyName":"engine","mapping":{"deepgram":"#/components/schemas/Inner"}}},
              "Inner":{"properties":{"model":{"type":"string"}},"required":["model"],
                "discriminator":{"propertyName":"model","mapping":{"nova":"#/components/schemas/Nova","flux":"#/components/schemas/Flux"}},
                "oneOf":[{"$ref":"#/components/schemas/Nova"},{"$ref":"#/components/schemas/Flux"}]},
              "Nova":{"type":"object","required":["model"],"properties":{"model":{"type":"string","enum":["nova"]}}},
              "Flux":{"type":"object","required":["model"],"properties":{"model":{"type":"string","enum":["flux"]}}},
              "Other":{"type":"object","properties":{"engine":{"type":"string"}}}
            }}}
            """)!;
        string result = (string)typeof(OpenApiFixer).GetMethod("PreserveGeneratorUnionBranches", BindingFlags.Static | BindingFlags.NonPublic)!
            .Invoke(null, [input.ToJsonString()])!;
        JsonNode inner = JsonNode.Parse(result)!["components"]!["schemas"]!["Inner"]!;
        await Assert.That(inner["properties"]!["model"]!["type"]!.GetValue<string>()).IsEqualTo("string");
        await Assert.That(inner["required"]!.ToJsonString()).IsEqualTo("[\"model\"]");
    }

    [Test]
    public async ValueTask Error_fields_merge_annotations_and_enums_but_preserve_incompatible_payloads()
    {
        JsonNode input = Document("422");
        JsonNode failure = input["components"]!["schemas"]!["Failure"]!;
        failure["discriminator"] = new JsonObject { ["propertyName"] = "code" };
        failure["anyOf"] = JsonNode.Parse("""
            [{"type":"object","properties":{"message":{"type":"string","description":"First"},"code":{"type":"string","enum":["a"]},"detail":{"type":"string"}}},
             {"type":"object","properties":{"message":{"type":"string","description":"Second"},"code":{"type":"string","enum":["b"]},"detail":{"type":"object"}}}]
            """);
        JsonNode result = Run(input);
        string reference = result["paths"]!["/test"]!["get"]!["responses"]!["422"]!["content"]!["application/json"]!["schema"]!["$ref"]!.GetValue<string>();
        JsonNode model = result["components"]!["schemas"]![reference[21..]]!;
        await Assert.That(model["properties"]!["message"]!["type"]!.GetValue<string>()).IsEqualTo("string");
        await Assert.That(model["properties"]!["code"]!["enum"]!.ToJsonString()).IsEqualTo("[\"a\",\"b\"]");
        await Assert.That(model["properties"]!["detail"]!["x-client-untyped-value"]!.GetValue<bool>()).IsTrue();
        await Assert.That(model["discriminator"]).IsNull();
        await Assert.That(JsonNode.DeepEquals(failure, result["components"]!["schemas"]!["Failure"])).IsTrue();
        await Assert.That(JsonNode.DeepEquals(result, Run(result))).IsTrue();
    }

    [Test]
    public async ValueTask Nested_error_unions_project_without_modifying_shared_variants()
    {
        JsonNode input = Document("422");
        JsonArray alternatives = input["components"]!["schemas"]!["Failure"]!["anyOf"]!.AsArray();
        alternatives[1] = new JsonObject { ["oneOf"] = new JsonArray(alternatives[1]!.DeepClone(), new JsonObject
        {
            ["type"] = "object", ["properties"] = new JsonObject { ["reason"] = new JsonObject { ["type"] = "string" } }
        }), ["discriminator"] = new JsonObject { ["propertyName"] = "reason" } };
        JsonNode result = Run(input);
        string reference = result["paths"]!["/test"]!["get"]!["responses"]!["422"]!["content"]!["application/json"]!["schema"]!["$ref"]!.GetValue<string>();
        JsonNode fields = result["components"]!["schemas"]![reference[21..]]!["properties"]!;
        await Assert.That(fields["detail"]).IsNotNull();
        await Assert.That(fields["errors"]).IsNotNull();
        await Assert.That(fields["reason"]).IsNotNull();
        await Assert.That(JsonNode.DeepEquals(input["components"]!["schemas"]!["Failure"], result["components"]!["schemas"]!["Failure"])).IsTrue();
    }

    private static JsonNode Document(string status)
    {
        JsonNode document = JsonNode.Parse("""
            {"openapi":"3.0.3","info":{"title":"Test","version":"1"},
            "paths":{"/test":{"get":{"responses":{}}}},"components":{"schemas":{
            "Failure":{"anyOf":[
                {"type":"object","required":["detail"],"properties":{"detail":{"type":"string"}}},
                {"type":"object","required":["errors"],"properties":{"errors":{"type":"array","items":{"type":"string"}}}}
            ]}}}}
            """)!;
        JsonNode responses = document["paths"]!["/test"]!["get"]!["responses"]!;
        responses[status] = JsonNode.Parse("""
            {"description":"Response","content":{"application/json":{"schema":{"$ref":"#/components/schemas/Failure"}}}}
            """);
        responses["200"] = responses[status]!.DeepClone();
        return document;
    }

    private static JsonNode Run(JsonNode input, OpenApiFixerOptions? options = null) => JsonNode.Parse((string)typeof(OpenApiFixer)
        .GetMethod("NormalizeKiotaResponseCompatibility", BindingFlags.NonPublic | BindingFlags.Static)!
        .Invoke(null, [input.ToJsonString(), options ?? new OpenApiFixerOptions()])!)!;
}
