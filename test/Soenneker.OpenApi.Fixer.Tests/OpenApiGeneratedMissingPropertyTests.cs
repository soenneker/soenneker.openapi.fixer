using System.Collections.Generic;
using System.Reflection;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Fixer.Tests;

public sealed class OpenApiGeneratedMissingPropertyTests
{
    [Test]
    public async ValueTask Field_map_capacity_matches_generated_entries_and_preserves_other_constructors()
    {
        const string constructor = "return new Dictionary<string, Action<IParseNode>>";
        const string source = """
            return new Dictionary<string, Action<IParseNode>>
            {
                { "a", n => { A = n.GetStringValue(); } },
                { "b", n => { B = n.GetStringValue(); } },
                { "c", n => { C = n.GetStringValue(); } },
                { "d", n => { D = n.GetStringValue(); } },
            };
            """;

        MethodInfo size = typeof(OpenApiFixer).GetMethod("SizeGeneratedFieldMaps", BindingFlags.NonPublic | BindingFlags.Static)!;
        string Apply(string text) => (string)size.Invoke(null, [text])!;
        string sized = source.Replace(constructor, constructor + "(4)");
        await Assert.That(Apply(source)).IsEqualTo(sized);
        await Assert.That(Apply(sized)).IsEqualTo(sized);
        await Assert.That(Apply(source.Replace(constructor, constructor + "(3)"))).IsEqualTo(sized);
        string inherited = source.Replace(constructor, constructor + "(base.GetFieldDeserializers())");
        await Assert.That(Apply(inherited)).IsEqualTo(inherited);
    }

    [Test]
    public async ValueTask Partially_generated_intersections_recover_inherited_fields_without_duplicates()
    {
        const string source = """
            namespace Models
            {
                public partial class Node : IParsable, IAdditionalDataHolder {
                    public IDictionary<string, object> AdditionalData { get; set; }
                    public string Label { get; set; }
                    public IDictionary<string, Action<IParseNode>> GetFieldDeserializers() {
                        return new Dictionary<string, Action<IParseNode>> { { "label & name", n => { Label = n.GetStringValue(); } } };
                    }
                    public void Serialize(ISerializationWriter writer) {
                        writer.WriteStringValue("label & name", Label);
                        writer.WriteAdditionalData(AdditionalData);
                    }
                }
            }
            """;
        JsonObject schemas = JsonNode.Parse("""{"Node":{"properties":{"label & name":{"type":"string"}},"allOf":[{"$ref":"#/components/schemas/Base"}]},"Base":{"properties":{"label & name":{"type":"string"},"count":{"type":"integer","format":"int64"}}}}""")!.AsObject();
        var enums = new Dictionary<string, string>();
        var classes = new Dictionary<string, string>();
        MethodInfo repair = typeof(OpenApiFixer).GetMethod("RepairGeneratedMissingProperties", BindingFlags.NonPublic | BindingFlags.Static)!;
        string result = (string)repair.Invoke(null, [source, schemas, classes, enums])!;
        await Assert.That(result).Contains("public long? Count");
        await Assert.That(result).Contains("WriteLongValue(\"count\", Count)");
        await Assert.That(result).DoesNotContain("public string? Label");
        await Assert.That((string)repair.Invoke(null, [result, schemas, classes, enums])!).IsEqualTo(result);
    }

    [Test]
    [Arguments("")]
    [Arguments("(4)")]
    public async ValueTask Depth_limited_empty_models_regain_typed_fields_and_recursive_collections(string capacity)
    {
        const string source = """
            namespace Models
            {
                public partial class Node : IParsable, IAdditionalDataHolder {
                    public IDictionary<string, object> AdditionalData { get; set; }
                    public IDictionary<string, Action<IParseNode>> GetFieldDeserializers() {
                        return new Dictionary<string, Action<IParseNode>> { };
                    }
                    public void Serialize(ISerializationWriter writer) {
                        writer.WriteAdditionalData(AdditionalData);
                    }
                }
            }
            """;
        JsonObject schemas = JsonNode.Parse("""{"Node":{"type":"object","properties":{"label":{"type":"string"},"children":{"type":"array","items":{"$ref":"#/components/schemas/Node"}},"operator":{"$ref":"#/components/schemas/NodeOperator"}}},"NodeOperator":{"type":"string","enum":["NODE"]}}""")!.AsObject();
        var enums = new Dictionary<string, string>();
        var classes = new Dictionary<string, string> { ["Models.Node"] = source };
        MethodInfo repair = typeof(OpenApiFixer).GetMethod("RepairGeneratedMissingProperties", BindingFlags.NonPublic | BindingFlags.Static)!;
        string input = source.Replace("IParseNode>> { };", "IParseNode>>" + capacity + " { };");
        string result = (string)repair.Invoke(null, [input, schemas, classes, enums])!;
        await Assert.That(result).Contains("public string? Label");
        await Assert.That(result).Contains("List<global::Models.Node>? Children");
        await Assert.That(result).Contains("GetCollectionOfObjectValues<global::Models.Node>");
        await Assert.That(result).Contains("WriteCollectionOfObjectValues<global::Models.Node>");
        await Assert.That(result).Contains("global::Models.NodeOperator? Operator");
        await Assert.That(enums["Models.NodeOperator"]).Contains("EnumMember(Value = \"NODE\")");
        await Assert.That((string)repair.Invoke(null, [result, schemas, classes, enums])!).IsEqualTo(result);
    }
}
