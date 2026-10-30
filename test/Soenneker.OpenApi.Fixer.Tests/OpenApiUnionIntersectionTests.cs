using System.Reflection;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Fixer.Tests;

public sealed class OpenApiUnionIntersectionTests
{
    [Test]
    public async ValueTask Tagged_object_unions_do_not_become_base_classes_or_scalar_alternatives()
    {
        const string source = """
            {"openapi":"3.0.3","components":{"schemas":{
              "Choice":{"properties":{"kind":{"type":"string"}},"required":["kind"],"discriminator":{"propertyName":"kind"},"oneOf":[{"$ref":"#/components/schemas/Left"},{"$ref":"#/components/schemas/Right"}]},
              "Left":{"type":"object","properties":{"kind":{"type":"string","enum":["left"]}},"required":["kind"],"additionalProperties":false},
              "Right":{"type":"object","properties":{"kind":{"type":"string","enum":["right"]}},"required":["kind"],"additionalProperties":false}
            }}}
            """;
        string repaired = Repair("NormalizeGeneratorUnionIntersections", source);
        JsonNode schemas = JsonNode.Parse(repaired)!["components"]!["schemas"]!;
        await Assert.That(schemas["Choice"]!["properties"]).IsNull();
        await Assert.That(schemas["Choice"]!["discriminator"]).IsNull();
        await Assert.That(schemas["Choice"]!["x-client-original-discriminator"]).IsNotNull();
        await Assert.That(schemas["Left"]!["properties"]!["kind"]!["enum"]![0]!.GetValue<string>()).IsEqualTo("left");
        await Assert.That(Repair("NormalizeGeneratorUnionIntersections", repaired)).IsEqualTo(repaired);
    }

    [Test]
    public async ValueTask Equivalent_array_intersections_preserve_items_and_read_only_annotations()
    {
        const string source = """
            {"components":{"schemas":{"Items":{"type":"array","items":{"type":"string"},"allOf":[{"type":"array","items":{"type":"string"}},{"type":"array","items":{"type":"string"},"readOnly":true}]}}}}
            """;
        JsonNode schema = JsonNode.Parse(Repair("NormalizeEquivalentArrayIntersections", source))!["components"]!["schemas"]!["Items"]!;
        await Assert.That(schema["allOf"]).IsNull();
        await Assert.That(schema["readOnly"]!.GetValue<bool>()).IsTrue();
        await Assert.That(schema["items"]!["type"]!.GetValue<string>()).IsEqualTo("string");
        string constrained = source.Replace("\"readOnly\":true", "\"maxItems\":2");
        await Assert.That(Repair("NormalizeEquivalentArrayIntersections", constrained)).IsEqualTo(constrained);
    }

    private static string Repair(string method, string source) => (string)typeof(OpenApiFixer)
        .GetMethod(method, BindingFlags.Static | BindingFlags.NonPublic)!.Invoke(null, [source])!;
}
