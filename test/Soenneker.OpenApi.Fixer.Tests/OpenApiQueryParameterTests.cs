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

    private static JsonNode Run(JsonNode input) => JsonNode.Parse((string)typeof(OpenApiFixer)
        .GetMethod("NormalizeKiotaQueryParameters", BindingFlags.NonPublic | BindingFlags.Static)!.Invoke(null, [input.ToJsonString()])!)!;
}
