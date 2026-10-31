using System.Reflection;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Fixer.Tests;

public sealed class OpenApiUnionIntersectionTests
{
    [Test]
    public async ValueTask Referenced_nested_intersections_converge_independent_of_component_order()
    {
        const string source = """
            {"components":{"schemas":{
              "Outer":{"properties":{"outer":{"type":"string"}},"oneOf":[{"$ref":"#/components/schemas/Inner"},{"type":"object","properties":{"other":{"type":"boolean"}}}]},
              "Inner":{"properties":{"inner":{"type":"string"}},"oneOf":[{"type":"object","properties":{"left":{"type":"string"}}},{"type":"object","properties":{"right":{"type":"integer"}}}]}
            }}}
            """;
        string repaired = Repair("NormalizeGeneratorUnionIntersections", source);
        JsonNode schemas = JsonNode.Parse(repaired)!["components"]!["schemas"]!;
        await Assert.That(schemas["Outer"]!["properties"]).IsNull();
        JsonNode nested = schemas[schemas["Outer"]!["oneOf"]![0]!["$ref"]!.GetValue<string>()[21..]]!;
        foreach (JsonNode? branch in nested["oneOf"]!.AsArray())
        {
            JsonNode leaf = branch!["$ref"] != null ? schemas[branch["$ref"]!.GetValue<string>()[21..]]! : branch;
            await Assert.That(leaf["properties"]!["outer"]).IsNotNull();
            await Assert.That(leaf["properties"]!["inner"]).IsNotNull();
        }
        await Assert.That(Repair("NormalizeGeneratorUnionIntersections", repaired)).IsEqualTo(repaired);
    }

    [Test]
    public async ValueTask Nested_unions_keep_their_operator_and_receive_common_fields()
    {
        const string source = """
            {"components":{"schemas":{"Choice":{"properties":{"common":{"type":"boolean"}},"anyOf":[
              {"oneOf":[{"type":"object","properties":{"left":{"type":"string"}}},{"type":"object","properties":{"right":{"type":"integer"}}}]},
              {"type":"object","properties":{"third":{"type":"string"}}}]}}}}
            """;
        JsonNode schemas = JsonNode.Parse(Repair("NormalizeGeneratorUnionIntersections", source))!["components"]!["schemas"]!;
        JsonNode nested = schemas[schemas["Choice"]!["anyOf"]![0]!["$ref"]!.GetValue<string>()[21..]]!;
        await Assert.That(nested["oneOf"]!.AsArray().Count).IsEqualTo(2);
        foreach (JsonNode? branch in nested["oneOf"]!.AsArray())
            await Assert.That(branch!["properties"]!["common"]!["type"]!.GetValue<string>()).IsEqualTo("boolean");
    }

    [Test]
    public async ValueTask Dictionary_branches_preserve_map_constraints_and_explicit_fields()
    {
        const string source = """
            {"components":{"schemas":{"Choice":{"properties":{"sample":{"type":"string"}},"anyOf":[
              {"type":"object","additionalProperties":{"type":"string"}},
              {"type":"object","properties":{"other":{"type":"string"}}}]}}}}
            """;
        JsonNode schemas = JsonNode.Parse(Repair("NormalizeGeneratorUnionIntersections", source))!["components"]!["schemas"]!;
        JsonNode branch = schemas[schemas["Choice"]!["anyOf"]![0]!["$ref"]!.GetValue<string>()[21..]]!;
        await Assert.That(branch["properties"]!["sample"]!["type"]!.GetValue<string>()).IsEqualTo("string");
        await Assert.That(branch["additionalProperties"]!["type"]!.GetValue<string>()).IsEqualTo("string");
    }

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

    [Test]
    public async ValueTask Common_fields_are_distributed_without_a_discriminator()
    {
        const string source = """
            {"components":{"schemas":{
              "Choice":{"type":"object","nullable":true,"properties":{"common":{"type":"string"}},"required":["common"],"anyOf":[{"$ref":"#/components/schemas/Left"},{"$ref":"#/components/schemas/Right"}]},
              "Left":{"type":"object","properties":{"left":{"type":"string"}}},
              "Right":{"type":"object","properties":{"right":{"type":"integer"}}}
            }}}
            """;
        string repaired = Repair("NormalizeGeneratorUnionIntersections", source);
        JsonNode schemas = JsonNode.Parse(repaired)!["components"]!["schemas"]!;
        foreach (JsonNode? branch in schemas["Choice"]!["anyOf"]!.AsArray())
        {
            string name = branch!["$ref"]!.GetValue<string>()[21..];
            await Assert.That(schemas[name]!["properties"]!["common"]!["type"]!.GetValue<string>()).IsEqualTo("string");
            await Assert.That(schemas[name]!["required"]![0]!.GetValue<string>()).IsEqualTo("common");
        }
        await Assert.That(schemas["Choice"]!["nullable"]!.GetValue<bool>()).IsTrue();
        await Assert.That(schemas["Left"]!["properties"]!["common"]).IsNull();
        await Assert.That(Repair("NormalizeGeneratorUnionIntersections", repaired)).IsEqualTo(repaired);
    }

    [Test]
    public async ValueTask Closed_parent_fields_remain_closed_after_distribution()
    {
        const string source = """
            {"components":{"schemas":{"Choice":{"type":"object","additionalProperties":false,
              "properties":{"common":{"type":"string"},"phone":{"type":"string"},"email":{"type":"string"}},
              "anyOf":[{"type":"object","required":["phone"],"properties":{"phone":{"type":"string"}}},
                       {"type":"object","required":["email"],"properties":{"email":{"type":"string"}}}]}}}}
            """;
        string repaired = Repair("NormalizeGeneratorUnionIntersections", source);
        JsonNode schemas = JsonNode.Parse(repaired)!["components"]!["schemas"]!;
        foreach (JsonNode? branch in schemas["Choice"]!["anyOf"]!.AsArray())
        {
            string name = branch!["$ref"]!.GetValue<string>()[21..];
            await Assert.That(schemas[name]!["additionalProperties"]!.GetValue<bool>()).IsFalse();
            await Assert.That(schemas[name]!["properties"]!.AsObject().Count).IsEqualTo(3);
        }
        await Assert.That(schemas["Choice"]!["additionalProperties"]).IsNull();
        await Assert.That(Repair("NormalizeGeneratorUnionIntersections", repaired)).IsEqualTo(repaired);
    }
}
