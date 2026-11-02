using System.Reflection;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Fixer.Tests;

public sealed class OpenApiQueryParameterTests
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async ValueTask Only_optional_empty_closed_get_bodies_are_removed(bool required)
    {
        JsonNode input = JsonNode.Parse("""
            {"paths":{"/test":{"get":{"requestBody":{"content":{"application/x-www-form-urlencoded":{
              "schema":{"type":"object","properties":{},"additionalProperties":false}}}}}}}}
            """)!;
        input["paths"]!["/test"]!["get"]!["requestBody"]!["required"] = required;
        JsonNode output = Run(input);
        await Assert.That(output["paths"]!["/test"]!["get"]!.AsObject().ContainsKey("requestBody")).IsEqualTo(required);
        await Assert.That(JsonNode.DeepEquals(output, Run(output))).IsTrue();
    }

    [Test]
    public async ValueTask Scalar_or_range_query_retains_scalar_and_exposes_bracket_fields()
    {
        JsonNode input = JsonNode.Parse("""
            {"paths":{"/customers":{"get":{"parameters":[{"name":"created","in":"query","style":"deepObject","explode":true,
              "schema":{"anyOf":[{"type":"integer"},{"type":"object","properties":{"gte":{"type":"integer"},"lt":{"type":"integer"}}}]}}]}}}}
            """)!;
        JsonNode output = Run(input);
        JsonArray parameters = output["paths"]!["/customers"]!["get"]!["parameters"]!.AsArray();
        await Assert.That(parameters.Count).IsEqualTo(3);
        await Assert.That(parameters[0]!["schema"]!["type"]!.GetValue<string>()).IsEqualTo("integer");
        await Assert.That(parameters[1]!["name"]!.GetValue<string>()).IsEqualTo("created[gte]");
        await Assert.That(parameters[2]!["name"]!.GetValue<string>()).IsEqualTo("created[lt]");
        await Assert.That(parameters[0]!["x-client-original-query-schema"]).IsNotNull();
        await Assert.That(JsonNode.DeepEquals(output, Run(output))).IsTrue();
    }

    [Test]
    public async ValueTask Nonempty_get_bodies_are_preserved()
    {
        JsonNode input = JsonNode.Parse("""
            {"paths":{"/test":{"get":{"requestBody":{"content":{"application/json":{
              "schema":{"type":"object","properties":{"value":{"type":"string"}},"additionalProperties":false}}}}}}}}
            """)!;
        await Assert.That(JsonNode.DeepEquals(input, Run(input))).IsTrue();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async ValueTask Deep_object_scalar_arrays_keep_bracketed_wire_names_and_constraints(bool required)
    {
        JsonNode input = JsonNode.Parse("""
            {"paths":{"/invoices/{invoice}":{"get":{"parameters":[{"name":"expand","in":"query",
              "style":"deepObject","explode":true,"description":"Fields to expand",
              "schema":{"type":"array","maxItems":4,"items":{"type":"string","maxLength":5000}}}]}}}}
            """)!;
        JsonNode original = input["paths"]!["/invoices/{invoice}"]!["get"]!["parameters"]![0]!;
        original["required"] = required;
        JsonNode output = Run(input);
        JsonNode parameter = output["paths"]!["/invoices/{invoice}"]!["get"]!["parameters"]![0]!;
        await Assert.That(parameter["name"]!.GetValue<string>()).IsEqualTo("expand[]");
        await Assert.That(parameter["style"]!.GetValue<string>()).IsEqualTo("form");
        await Assert.That(parameter["explode"]!.GetValue<bool>()).IsTrue();
        await Assert.That(parameter["required"]!.GetValue<bool>()).IsEqualTo(required);
        await Assert.That(JsonNode.DeepEquals(parameter["schema"], original["schema"])).IsTrue();
        await Assert.That(JsonNode.DeepEquals(parameter["x-client-original-query-parameter"], original)).IsTrue();
        await Assert.That(JsonNode.DeepEquals(output, Run(output))).IsTrue();
    }

    [Test]
    public async ValueTask Referenced_path_arrays_are_normalized_without_changing_shared_parameters()
    {
        JsonNode input = JsonNode.Parse("""
            {"paths":{"/test":{"parameters":[{"$ref":"#/components/parameters/Expand"}]}},
             "components":{"parameters":{"Expand":{"name":"expand","in":"query","style":"deepObject","explode":true,
                "schema":{"$ref":"#/components/schemas/Fields"}}},
              "schemas":{"Fields":{"type":"array","items":{"$ref":"#/components/schemas/Field"}},"Field":{"type":"string"}}}}
            """)!;
        JsonNode output = Run(input);
        await Assert.That(output["paths"]!["/test"]!["parameters"]![0]!["name"]!.GetValue<string>()).IsEqualTo("expand[]");
        await Assert.That(JsonNode.DeepEquals(input["components"], output["components"])).IsTrue();
        await Assert.That(JsonNode.DeepEquals(output, Run(output))).IsTrue();
    }

    [Test]
    [Arguments("form", true, "string", "expand")]
    [Arguments("form", false, "string", "expand")]
    [Arguments("pipeDelimited", false, "string", "expand")]
    [Arguments("deepObject", false, "string", "expand")]
    [Arguments("deepObject", true, "object", "expand")]
    [Arguments("deepObject", true, "array", "expand")]
    [Arguments("deepObject", true, "string", "expand[]")]
    public async ValueTask Other_array_encodings_and_complex_items_are_preserved(string style, bool explode, string itemType, string name)
    {
        JsonNode input = JsonNode.Parse("""
            {"paths":{"/test":{"get":{"parameters":[{"in":"query","schema":{"type":"array","items":{}}}]}}}}
            """)!;
        JsonNode parameter = input["paths"]!["/test"]!["get"]!["parameters"]![0]!;
        parameter["name"] = name;
        parameter["style"] = style;
        parameter["explode"] = explode;
        parameter["schema"]!["items"]!["type"] = itemType;
        await Assert.That(JsonNode.DeepEquals(input, Run(input))).IsTrue();
    }

    [Test]
    public async ValueTask Existing_bracketed_parameters_are_not_overwritten()
    {
        JsonNode input = JsonNode.Parse("""
            {"paths":{"/test":{"get":{"parameters":[
              {"name":"expand","in":"query","style":"deepObject","explode":true,"schema":{"type":"array","items":{"type":"string"}}},
              {"name":"expand[]","in":"query","schema":{"type":"string"}}]}}}}
            """)!;
        await Assert.That(JsonNode.DeepEquals(input, Run(input))).IsTrue();
    }

    private static JsonNode Run(JsonNode input) => JsonNode.Parse((string)typeof(OpenApiFixer)
        .GetMethod("NormalizeKiotaQueryParameters", BindingFlags.NonPublic | BindingFlags.Static)!.Invoke(null, [input.ToJsonString()])!)!;
}
