using System;
using System.Reflection;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Fixer.Tests;

public sealed class OpenApiUnionPredicateTests
{
    [Test]
    public async ValueTask Structural_checks_include_types_constants_and_excluded_fields()
    {
        string check = Build("""
            {"type":"object","required":["value"],"properties":{"value":{"type":"integer","const":7},"enabled":{"type":"boolean","const":true}},"not":{"required":["other"]}}
            """)!;
        await Assert.That(check).Contains("UntypedInteger");
        await Assert.That(check).Contains("UntypedBoolean");
        await Assert.That(check).Contains("7m");
        await Assert.That(check).Contains("!(");
        await Assert.That(check).Contains("ContainsKey(\"other\")");
    }

    [Test]
    public async ValueTask Unsupported_negated_rules_are_not_approximated()
    {
        await Assert.That(Build("""{"not":{"type":"string","pattern":"^forbidden"}}""")).IsNull();
    }

    [Test]
    public async ValueTask Nested_property_checks_do_not_repeat_the_entire_parent_access_chain()
    {
        JsonObject schema = new() { ["type"] = "string" };
        for (int depth = 0; depth < 12; depth++)
            schema = new JsonObject
            {
                ["type"] = "object",
                ["required"] = new JsonArray("next"),
                ["properties"] = new JsonObject
                {
                    ["next"] = schema,
                    ["label"] = new JsonObject { ["type"] = "string" },
                    ["enabled"] = new JsonObject { ["type"] = "boolean" }
                }
            };
        string check = Build(schema.ToJsonString())!;
        await Assert.That(check.Length).IsLessThan(20000);
        await Assert.That(check).Contains("ContainsKey(\"next\")");
        await Assert.That(check).Contains("UntypedString");
        await Assert.That(check).Contains("UntypedBoolean");
    }

    private static string? Build(string json)
    {
        Func<JsonNode?, JsonObject?> resolve = node => node as JsonObject;
        return (string?)typeof(OpenApiFixer).GetMethod("BuildGeneratedUnionStructuralCheck", BindingFlags.Static | BindingFlags.NonPublic)!
            .Invoke(null, [JsonNode.Parse(json)!.AsObject(), "payload", resolve]);
    }

    [Test]
    public async ValueTask Shared_reference_graphs_have_a_bounded_expansion_budget()
    {
        var schemas = new JsonObject { ["Leaf"] = new JsonObject { ["type"] = "string" } };
        string previous = "Leaf";
        for (int i = 0; i < 16; i++)
        {
            string name = "Level" + i;
            schemas[name] = new JsonObject { ["type"] = "object", ["properties"] = new JsonObject
            {
                ["left"] = new JsonObject { ["$ref"] = previous },
                ["right"] = new JsonObject { ["$ref"] = previous }
            } };
            previous = name;
        }
        Func<JsonNode?, JsonObject?> resolve = node => node?["$ref"] is JsonValue reference
            ? schemas[reference.GetValue<string>()] as JsonObject : node as JsonObject;
        string? result = (string?)typeof(OpenApiFixer).GetMethod("BuildGeneratedUnionStructuralCheck", BindingFlags.Static | BindingFlags.NonPublic)!
            .Invoke(null, [schemas[previous]!.AsObject(), "payload", resolve]);
        await Assert.That(result).IsNull();
    }
}
