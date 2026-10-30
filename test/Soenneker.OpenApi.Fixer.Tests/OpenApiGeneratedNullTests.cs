using System.Collections.Generic;
using System.Reflection;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Fixer.Tests;

public sealed class OpenApiGeneratedNullTests
{
    [Test]
    public async ValueTask A_nested_union_does_not_disable_null_tracking_on_the_containing_model()
    {
        const string source = """
            public partial class Model : IParsable
            {
                public string? Name { get; set; }
                public virtual void Serialize(ISerializationWriter writer)
                {
                    writer.WriteStringValue("name", Name);
                }
                public partial class Nested : IParsable, IComposedTypeWrapper
                {
                    public string? Value { get; set; }
                }
            }
            """;
        string repaired = Run(source);
        await Assert.That(repaired).Contains("_explicitNullValueName");
        await Assert.That(repaired).DoesNotContain("_explicitNullValueValue");
        await Assert.That(Run(repaired)).IsEqualTo(repaired);
    }

    [Test]
    public async ValueTask Explicit_null_tracking_is_separate_from_default_values_and_idempotent()
    {
        const string source = """
            public partial class Model : IParsable, IAdditionalDataHolder
            {
                public string? Name { get; set; }
                public int? Count { get; set; }
                public static Model CreateFromDiscriminatorValue(IParseNode parseNode) { return new Model(); }
                public virtual void Serialize(ISerializationWriter writer)
                {
                    writer.WriteStringValue("name", Name);
                    writer.WriteIntValue("count", Count);
                    writer.WriteAdditionalData(AdditionalData);
                }
            }
            """;
        string repaired = Run(source);
        await Assert.That(repaired).Contains("if (value is null)");
        await Assert.That(repaired).Contains("_explicitNullProperties?.Remove(\"name\")");
        await Assert.That(repaired).Contains("writer.WriteNullValue(name)");
        await Assert.That(repaired).Contains("OpenApiNullTracking.Capture(new Model(), parseNode)");
        await Assert.That(repaired).Contains("parseNode.GetChildNode(\"name\")");
        await Assert.That(Run(repaired)).IsEqualTo(repaired);
    }

    private static string Run(string source) => (string)typeof(OpenApiFixer)
        .GetMethod("RepairGeneratedExplicitNulls", BindingFlags.NonPublic | BindingFlags.Static)!
        .Invoke(null, [source, new Dictionary<string, string>()])!;
}
