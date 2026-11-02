using System.Reflection;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Fixer.Tests;

public sealed class OpenApiTaggedUnionTests
{
    [Test]
    [Arguments("Choice", "ChoiceCat", "ChoiceCatVariant")]
    [Arguments("Choice", "Cat", "ChoiceCat2")]
    public async ValueTask Projected_names_avoid_repeated_parents_and_case_insensitive_collisions(string parent, string branch, string expected)
    {
        var names = new System.Collections.Generic.HashSet<string>(System.StringComparer.OrdinalIgnoreCase) { "choicecat" };
        string result = Name(parent, branch, names);
        await Assert.That(result).IsEqualTo(expected);
    }

    [Test]
    public async ValueTask Long_projected_names_are_bounded_deterministic_and_distinct()
    {
        string parent = "YamlPaths" + new string('A', 100);
        string first = Name(parent, parent + "OneOf1", new(System.StringComparer.OrdinalIgnoreCase));
        string second = Name(parent, parent + "OneOf2", new(System.StringComparer.OrdinalIgnoreCase));
        await Assert.That(first.Length <= 96).IsTrue();
        await Assert.That(first.StartsWith("TaggedCatVariant")).IsTrue();
        await Assert.That(first == second).IsFalse();
        await Assert.That(Name(parent, parent + "OneOf1", new(System.StringComparer.OrdinalIgnoreCase))).IsEqualTo(first);
    }

    private static string Name(string parent, string branch, System.Collections.Generic.HashSet<string> names) =>
        (string)typeof(OpenApiFixer).GetMethod("GetTaggedUnionModelName", BindingFlags.NonPublic | BindingFlags.Static)!
            .Invoke(null, [parent, branch, "cat", names])!;

    [Test]
    public async ValueTask Required_disjoint_tags_produce_an_idempotent_hierarchy_and_preserve_original_union()
    {
        JsonNode input = Document();
        JsonNode result = Project(input);
        JsonNode schemas = result["components"]!["schemas"]!;
        await Assert.That(schemas["Choice"]!["oneOf"]).IsNull();
        await Assert.That(schemas["Choice"]!["discriminator"]!["mapping"]!["cat"]!.GetValue<string>()).IsEqualTo("#/components/schemas/ChoiceCat");
        await Assert.That(JsonNode.DeepEquals(schemas["Choice"]!["x-client-original-tagged-union"], input["components"]!["schemas"]!["Choice"])).IsTrue();
        await Assert.That(schemas["ChoiceCat"]!["allOf"]![0]!["$ref"]!.GetValue<string>()).IsEqualTo("#/components/schemas/Choice");
        await Assert.That(JsonNode.DeepEquals(schemas["Cat"]!["properties"], input["components"]!["schemas"]!["Cat"]!["properties"])).IsTrue();
        await Assert.That(JsonNode.DeepEquals(schemas["Cat"], input["components"]!["schemas"]!["Cat"])).IsTrue();
        await Assert.That(JsonNode.DeepEquals(schemas["ChoiceCat"]!["allOf"]![1], schemas["Cat"])).IsTrue();
        await Assert.That(JsonNode.DeepEquals(result, Project(result))).IsTrue();
    }

    [Test]
    public async ValueTask Array_union_items_expose_their_discriminator_without_changing_array_constraints()
    {
        JsonNode input = Document();
        input["components"]!["schemas"]!["ListOrText"] = JsonNode.Parse("""
            {"anyOf":[{"type":"string"},{"type":"array","minItems":1,"items":{"$ref":"#/components/schemas/Choice"}}]}
            """);
        JsonNode example = input["components"]!["schemas"]!["ListOrText"]!.DeepClone();
        input["components"]!["schemas"]!["Cat"]!["example"] = example.DeepClone();
        JsonNode result = Project(input);
        JsonNode array = result["components"]!["schemas"]!["ListOrText"]!["anyOf"]![1]!;
        await Assert.That(array["type"]!.GetValue<string>()).IsEqualTo("array");
        await Assert.That(array["minItems"]!.GetValue<int>()).IsEqualTo(1);
        await Assert.That(array["items"]!["$ref"]!.GetValue<string>()).IsEqualTo("#/components/schemas/Choice");
        await Assert.That(array["discriminator"]!["propertyName"]!.GetValue<string>()).IsEqualTo("kind");
        await Assert.That(array["required"]![0]!.GetValue<string>()).IsEqualTo("kind");
        await Assert.That(JsonNode.DeepEquals(result["components"]!["schemas"]!["Cat"]!["example"], example)).IsTrue();
        await Assert.That(JsonNode.DeepEquals(result, Project(result))).IsTrue();
    }

    [Test]
    [Arguments("optional")]
    [Arguments("overlap")]
    [Arguments("case-collision")]
    [Arguments("inheritance")]
    [Arguments("readonly")]
    [Arguments("parent-constraint")]
    public async ValueTask Unproven_or_unsupported_hierarchies_are_left_unchanged(string reason)
    {
        JsonNode input = Document();
        JsonNode schemas = input["components"]!["schemas"]!;
        switch (reason)
        {
            case "optional": schemas["Cat"]!.AsObject().Remove("required"); break;
            case "overlap": schemas["Dog"]!["properties"]!["kind"]!["enum"]![0] = "cat"; break;
            case "case-collision": schemas["Dog"]!["properties"]!["kind"]!["enum"]![0] = "CAT"; break;
            case "inheritance": schemas["Cat"]!["allOf"] = new JsonArray(new JsonObject { ["type"] = "object" }); break;
            case "readonly": schemas["Cat"]!["properties"]!["kind"]!["readOnly"] = true; break;
            case "parent-constraint": schemas["Choice"]!["maxProperties"] = 2; break;
        }
        await Assert.That(JsonNode.DeepEquals(input, Project(input))).IsTrue();
    }

    private static JsonNode Document() => JsonNode.Parse("""
        {"components":{"schemas":{
          "Choice":{"oneOf":[{"$ref":"#/components/schemas/Cat"},{"$ref":"#/components/schemas/Dog"}]},
          "Cat":{"type":"object","required":["kind"],"properties":{"kind":{"type":"string","enum":["cat"]}}},
          "Dog":{"type":"object","required":["kind"],"properties":{"kind":{"type":"string","enum":["dog"]}}}
        }}}
        """)!;

    private static JsonNode Project(JsonNode input) => JsonNode.Parse((string)typeof(OpenApiFixer)
        .GetMethod("ProjectTaggedObjectUnions", BindingFlags.NonPublic | BindingFlags.Static)!.Invoke(null, [input.ToJsonString()])!)!;
}
