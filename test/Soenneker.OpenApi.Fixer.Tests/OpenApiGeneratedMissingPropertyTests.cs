using System.Collections.Generic;
using System.Reflection;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Fixer.Tests;

public sealed class OpenApiGeneratedMissingPropertyTests
{
    [Test]
    public async ValueTask Depth_limited_empty_models_regain_typed_fields_and_recursive_collections()
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
        string result = (string)repair.Invoke(null, [source, schemas, classes, enums])!;
        await Assert.That(result).Contains("public string? Label");
        await Assert.That(result).Contains("List<global::Models.Node>? Children");
        await Assert.That(result).Contains("GetCollectionOfObjectValues<global::Models.Node>");
        await Assert.That(result).Contains("WriteCollectionOfObjectValues<global::Models.Node>");
        await Assert.That(result).Contains("global::Models.NodeOperator? Operator");
        await Assert.That(enums["Models.NodeOperator"]).Contains("EnumMember(Value = \"NODE\")");
        await Assert.That((string)repair.Invoke(null, [result, schemas, classes, enums])!).IsEqualTo(result);
    }
}
