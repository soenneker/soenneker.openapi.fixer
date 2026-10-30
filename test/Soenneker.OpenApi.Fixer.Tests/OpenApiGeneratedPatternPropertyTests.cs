using System.Reflection;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Fixer.Tests;

public sealed class OpenApiGeneratedPatternPropertyTests
{
    [Test]
    public async ValueTask Closed_pattern_maps_retain_dynamic_values_without_opening_closed_objects()
    {
        const string source = """
            public partial class Metadata : IParsable
            {
                public virtual void Serialize(ISerializationWriter writer) { }
            }
            """;
        JsonObject schemas = JsonNode.Parse("""{"Metadata":{"type":"object","additionalProperties":false,"patternProperties":{"^[a-z]+$":{"type":"string"}}}}""")!.AsObject();
        string repaired = Repair(source, schemas);
        await Assert.That(repaired).Contains("IAdditionalDataHolder");
        await Assert.That(repaired).Contains("writer.WriteAdditionalData(AdditionalData)");
        await Assert.That(Repair(repaired, schemas)).IsEqualTo(repaired);
        schemas["Metadata"]!.AsObject().Remove("patternProperties");
        await Assert.That(Repair(source, schemas)).IsEqualTo(source);
    }

    private static string Repair(string source, JsonObject schemas) => (string)typeof(OpenApiFixer)
        .GetMethod("RepairGeneratedPatternProperties", BindingFlags.NonPublic | BindingFlags.Static)!.Invoke(null, [source, schemas])!;
}
