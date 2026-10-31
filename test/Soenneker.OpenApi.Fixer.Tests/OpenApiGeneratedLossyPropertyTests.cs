using System.Reflection;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Fixer.Tests;

public sealed class OpenApiGeneratedLossyPropertyTests
{
    [Test]
    [Arguments("00:00:00")]
    [Arguments("09:00:00+05:30")]
    public async ValueTask Time_constructor_defaults_become_strings(string defaultValue)
    {
        string source = $$"""
            public partial class LocalTimeCondition : IParsable
            {
                public Time? OnOrAfter { get; set; }
                public Time? Before { get; set; }
                public LocalTimeCondition()
                {
                    OnOrAfter = new Time(DateTimeOffset.Parse("{{defaultValue}}").DateTime);
                }
                public void Read() { var fields = new Dictionary<string, Action<IParseNode>> { { "on_or_after", n => { OnOrAfter = n.GetTimeValue(); } }, { "before", n => { Before = n.GetTimeValue(); } } }; }
                public void Serialize(ISerializationWriter writer) { writer.WriteTimeValue("on_or_after", OnOrAfter); writer.WriteTimeValue("before", Before); }
            }
            """;
        JsonObject schemas = JsonNode.Parse("""{"LocalTimeCondition":{"properties":{"on_or_after":{"type":"string","format":"time"},"before":{"type":"string","format":"time"}}}}""")!.AsObject();
        string repaired = Repair(source, schemas);
        await Assert.That(repaired).Contains($"OnOrAfter = \"{defaultValue}\";");
        await Assert.That(repaired).Contains("public string? OnOrAfter");
        await Assert.That(repaired).Contains("OnOrAfter = n.GetStringValue()");
        await Assert.That(repaired).Contains("writer.WriteStringValue(\"on_or_after\", OnOrAfter)");
        await Assert.That(repaired).DoesNotContain("new Time(");
        await Assert.That(repaired).DoesNotContain("Before = \"");
        await Assert.That(Repair(repaired, schemas)).IsEqualTo(repaired);
    }

    [Test]
    public async ValueTask Time_strings_keep_offsets_and_use_a_nullable_annotation_context()
    {
        const string source = """
            public partial class Model : IParsable
            {
                public Time? Start { get; set; }
                public void Read() { var fields = new Dictionary<string, Action<IParseNode>> { { "start", n => { Start = n.GetTimeValue(); } } }; }
                public void Serialize(ISerializationWriter writer) { writer.WriteTimeValue("start", Start); }
            }
            """;
        JsonObject schemas = JsonNode.Parse("""{"Model":{"properties":{"start":{"type":"string","format":"time"}}}}""")!.AsObject();
        string repaired = Repair(source, schemas);
        await Assert.That(repaired).Contains("#nullable enable");
        await Assert.That(repaired).Contains("public string? Start");
        await Assert.That(repaired).Contains("Start = n.GetStringValue()");
        await Assert.That(Repair(repaired, schemas)).IsEqualTo(repaired);
    }

    [Test]
    public async ValueTask Mixed_primitive_array_items_are_not_deserialized_as_strings()
    {
        const string source = """
            public partial class Model : IParsable
            {
                public List<string>? Loc { get; set; }
                public void Read() { var fields = new Dictionary<string, Action<IParseNode>> { { "loc", n => { Loc = n.GetCollectionOfPrimitiveValues<string>()?.AsList(); } } }; }
                public void Serialize(ISerializationWriter writer) { writer.WriteCollectionOfPrimitiveValues<string>("loc", Loc); }
            }
            """;
        JsonObject schemas = JsonNode.Parse("""{"Model":{"properties":{"loc":{"type":"array","items":{"anyOf":[{"type":"string"},{"type":"integer"}]}}}}} """)!.AsObject();
        string repaired = Repair(source, schemas);
        await Assert.That(repaired).Contains("List<global::Microsoft.Kiota.Abstractions.Serialization.UntypedNode>? Loc");
        await Assert.That(repaired).Contains("GetCollectionOfObjectValues<global::Microsoft.Kiota.Abstractions.Serialization.UntypedNode>");
        await Assert.That(repaired).DoesNotContain("GetCollectionOfPrimitiveValues<string>");
        await Assert.That(Repair(repaired, schemas)).IsEqualTo(repaired);
    }

    [Test]
    public async ValueTask Collapsed_union_enums_keep_values_from_every_alternative()
    {
        const string source = """
            public partial class Model : IParsable
            {
                public global::Models.FirstKind? Kind { get; set; }
                public void Read() { var fields = new Dictionary<string, Action<IParseNode>> { { "kind", n => { Kind = n.GetEnumValue<global::Models.FirstKind>(); } } }; }
                public void Serialize(ISerializationWriter writer) { writer.WriteEnumValue<global::Models.FirstKind>("kind", Kind); }
            }
            """;
        JsonObject schemas = JsonNode.Parse("""{"Model":{"oneOf":[{"properties":{"kind":{"type":"string","enum":["first"]}}},{"properties":{"kind":{"type":"string","enum":["second"]}}}]}}""")!.AsObject();
        string repaired = Repair(source, schemas);
        await Assert.That(repaired).Contains("public string? Kind");
        await Assert.That(repaired).Contains("Kind = n.GetStringValue()");
        await Assert.That(repaired).Contains("writer.WriteStringValue(\"kind\", Kind)");
    }

    [Test]
    public async ValueTask Arbitrary_json_properties_use_untyped_nodes_for_reading_and_writing()
    {
        const string source = """
            public partial class Model : IParsable
            {
                public global::Models.JsonValue? Json { get; set; }
                public void Read() { var fields = new Dictionary<string, Action<IParseNode>> { { "json", n => { Json = n.GetObjectValue<global::Models.JsonValue>(global::Models.JsonValue.CreateFromDiscriminatorValue); } } }; }
                public void Serialize(ISerializationWriter writer) { writer.WriteObjectValue<global::Models.JsonValue>("json", Json); }
            }
            """;
        JsonObject schemas = JsonNode.Parse("""{"Model":{"properties":{"json":{"$ref":"#/components/schemas/Any"}}},"Any":{"x-client-untyped-value":true}}""")!.AsObject();
        string repaired = Repair(source, schemas);
        await Assert.That(repaired).Contains("public global::Microsoft.Kiota.Abstractions.Serialization.UntypedNode? Json");
        await Assert.That(repaired).DoesNotContain("global::Models.JsonValue");
        await Assert.That(Repair(repaired, schemas)).IsEqualTo(repaired);
    }

    private static string Repair(string source, JsonObject schemas) => (string)typeof(OpenApiFixer).GetMethod("RepairGeneratedLossyUnionProperties", BindingFlags.Static | BindingFlags.NonPublic)!.Invoke(null, [source, schemas])!;

    [Test]
    public async ValueTask Repaired_backing_fields_reuse_the_existing_nullable_context()
    {
        const string source = """
            public partial class Model : IParsable
            {
            #nullable enable
                public string? Reaction { get => _explicitNullValueReaction; set => _explicitNullValueReaction = value; }
                private string? _explicitNullValueReaction = default!;
            #nullable restore
                public void Read() { var fields = new Dictionary<string, Action<IParseNode>> { { "reaction", n => { Reaction = n.GetStringValue(); } } }; }
                public void Serialize(ISerializationWriter writer) { writer.WriteStringValue("reaction", Reaction); }
            }
            """;
        JsonObject schemas = JsonNode.Parse("""{"Model":{"properties":{"reaction":{"type":"string","enum":["+1","-1"]}}}}""")!.AsObject();
        await Assert.That(Repair(source, schemas)).IsEqualTo(source);
    }
}
