using System.Reflection;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Fixer.Tests;

public sealed class OpenApiGeneratedLossyPropertyTests
{
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

    private static string Repair(string source, JsonObject schemas) => (string)typeof(OpenApiFixer).GetMethod("RepairGeneratedLossyUnionProperties", BindingFlags.Static | BindingFlags.NonPublic)!.Invoke(null, [source, schemas])!;
}
