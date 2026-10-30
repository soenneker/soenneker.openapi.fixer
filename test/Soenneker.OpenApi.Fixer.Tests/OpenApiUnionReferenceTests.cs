using System.Reflection;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Fixer.Tests;

public sealed class OpenApiUnionReferenceTests
{
    [Test]
    public async ValueTask Identical_recursive_unions_share_references_without_changing_payload_examples_or_other_unions()
    {
        const string json = """
            {"components":{"schemas":{
              "A":{"oneOf":[{"$ref":"#/components/schemas/Left"},{"$ref":"#/components/schemas/Right"}]},
              "B":{"oneOf":[{"$ref":"#/components/schemas/Left"},{"$ref":"#/components/schemas/Right"}]},
              "Different":{"oneOf":[{"$ref":"#/components/schemas/Left"},{"$ref":"#/components/schemas/Right"}],"nullable":true},
              "Left":{"type":"object","properties":{"child":{"$ref":"#/components/schemas/B"},"other":{"$ref":"#/components/schemas/Different"}},"example":{"$ref":"#/components/schemas/B"}},
              "Right":{"type":"object","properties":{"child":{"$ref":"#/components/schemas/A"}}}
            }}}
            """;
        string fixedJson = Repair(json);
        JsonNode schemas = JsonNode.Parse(fixedJson)!["components"]!["schemas"]!;
        await Assert.That(schemas["Left"]!["properties"]!["child"]!["$ref"]!.GetValue<string>()).IsEqualTo("#/components/schemas/A");
        await Assert.That(schemas["Left"]!["properties"]!["other"]!["$ref"]!.GetValue<string>()).IsEqualTo("#/components/schemas/Different");
        await Assert.That(schemas["Left"]!["example"]!["$ref"]!.GetValue<string>()).IsEqualTo("#/components/schemas/B");
        await Assert.That(schemas["B"]!["oneOf"]).IsNotNull();
        await Assert.That(Repair(fixedJson)).IsEqualTo(fixedJson);
    }

    private static string Repair(string json) => (string)typeof(OpenApiFixer).GetMethod("ShareEquivalentUnionReferences", BindingFlags.Static | BindingFlags.NonPublic)!.Invoke(null, [json])!;
}
