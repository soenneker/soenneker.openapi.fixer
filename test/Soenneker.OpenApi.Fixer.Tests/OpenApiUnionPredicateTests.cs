using System.Text.Json.Nodes;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Fixer.Tests;

public sealed class OpenApiUnionPredicateTests
{
    [Test]
    public async ValueTask Reference_siblings_and_numeric_bounds_are_not_discarded()
    {
        var schemas = JsonNode.Parse("""{"Number":{"type":"integer","minimum":1}}""")!.AsObject();
        var match = GeneratedUnionRuntimeCompiler.Matcher(JsonNode.Parse("""{"$ref":"Number","maximum":5}""")!.AsObject(), schemas);
        await Assert.That(match("3")).IsTrue();
        await Assert.That(match("0")).IsFalse();
        await Assert.That(match("6")).IsFalse();
        await Assert.That(match("1.5")).IsFalse();
    }

    [Test]
    public async ValueTask Unsupported_pattern_properties_cannot_make_additional_fields_look_invalid()
    {
        var match = GeneratedUnionRuntimeCompiler.Matcher(JsonNode.Parse("""{"type":"object","patternProperties":{"^x":{"type":"string"}},"additionalProperties":false}""")!.AsObject());
        await Assert.That(match("""{"x":"value"}""")).IsNull();
    }

    [Test]
    public async ValueTask Array_items_constants_and_unknown_rules_keep_three_valued_semantics()
    {
        var match = GeneratedUnionRuntimeCompiler.Matcher(JsonNode.Parse("""{"type":"array","minItems":1,"items":{"type":"boolean","const":true}}""")!.AsObject());
        await Assert.That(match("[true,true]")).IsTrue();
        await Assert.That(match("[true,false]")).IsFalse();
        await Assert.That(match("[]")).IsFalse();
        var union = GeneratedUnionRuntimeCompiler.Matcher(JsonNode.Parse("""{"anyOf":[{"const":"ok"},{"unsupportedAssertion":true}]}""")!.AsObject());
        await Assert.That(union("\"ok\"")).IsTrue();
        await Assert.That(union("\"other\"")).IsNull();
    }

    [Test]
    public async ValueTask Unsupported_rules_are_unknown_and_cannot_be_negated_into_a_match()
    {
        var match = GeneratedUnionRuntimeCompiler.Matcher(JsonNode.Parse("""{"not":{"type":"string","pattern":"^forbidden"}}""")!.AsObject());
        await Assert.That(match("\"anything\"")).IsNull();
        await Assert.That(match("123")).IsTrue();
    }

    [Test]
    public async ValueTask Optional_unsupported_fields_only_block_selection_when_present()
    {
        var match = GeneratedUnionRuntimeCompiler.Matcher(JsonNode.Parse("""
            {"type":"object","required":["id"],"properties":{"id":{"type":"string"},"value":{"type":"string","pattern":"^a"}}}
            """)!.AsObject());
        await Assert.That(match("""{"id":"ok"}""")).IsTrue();
        await Assert.That(match("""{"id":"ok","value":"a"}""")).IsNull();
        await Assert.That(match("""{"id":123}""")).IsFalse();
        await Assert.That(match("""{"value":"a"}""")).IsFalse();
    }

    [Test]
    public async ValueTask Shared_and_recursive_references_are_evaluated_without_expanding_the_graph()
    {
        JsonObject schemas = JsonNode.Parse("""
            {"Node":{"type":"object","required":["id"],"properties":{"id":{"type":"string"},"left":{"$ref":"Node"},"right":{"$ref":"Node"}}}}
            """)!.AsObject();
        string generated = GeneratedUnionRuntimeCompiler.Build(schemas["Node"]!.AsObject(), schemas);
        await Assert.That(generated.Split("bool? MatchSchema").Length).IsEqualTo(3);
        var match = GeneratedUnionRuntimeCompiler.Matcher(schemas["Node"]!.AsObject(), schemas);
        await Assert.That(match("""{"id":"root","left":{"id":"left"},"right":{"id":"right"}}""")).IsTrue();
        await Assert.That(match("""{"id":"root","left":{"id":123}}""")).IsFalse();
        JsonObject payload = new() { ["id"] = "leaf" };
        for (int i = 0; i < 70; i++) payload = new JsonObject { ["id"] = "node", ["left"] = payload };
        await Assert.That(match(payload.ToJsonString())).IsNull();
    }

    [Test]
    public async ValueTask Nullable_values_and_unicode_lengths_are_checked_correctly()
    {
        var match = GeneratedUnionRuntimeCompiler.Matcher(JsonNode.Parse("""{"type":"string","nullable":true,"minLength":1,"maxLength":1}""")!.AsObject());
        await Assert.That(match("null")).IsTrue();
        await Assert.That(match("\"😀\"")).IsTrue();
        await Assert.That(match("\"ab\"")).IsFalse();
        await Assert.That(match("12")).IsFalse();
    }

    [Test]
    public async ValueTask OneOf_retains_unknown_and_rejects_overlapping_matches()
    {
        var match = GeneratedUnionRuntimeCompiler.Matcher(JsonNode.Parse("""{"oneOf":[{"type":"string"},{"type":"string","pattern":"^a"}]}""")!.AsObject());
        await Assert.That(match("\"a\"")).IsNull();
        await Assert.That(match("12")).IsFalse();
        var overlap = GeneratedUnionRuntimeCompiler.Matcher(JsonNode.Parse("""{"oneOf":[{"type":"string"},{"type":"string"}]}""")!.AsObject());
        await Assert.That(overlap("\"a\"")).IsFalse();
    }

    [Test]
    public async ValueTask Schema_budget_does_not_turn_an_incomplete_branch_into_a_match()
    {
        var fields = new JsonObject();
        for (int i = 0; i < 260; i++) fields["field" + i] = new JsonObject { ["type"] = "string" };
        var match = GeneratedUnionRuntimeCompiler.Matcher(new JsonObject { ["type"] = "object", ["properties"] = fields });
        await Assert.That(match("{}")).IsNull();
    }
}
