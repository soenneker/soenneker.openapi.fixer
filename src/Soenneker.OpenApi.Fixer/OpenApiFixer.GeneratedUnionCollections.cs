using System.Text.RegularExpressions;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static string RepairGeneratedUnionCollectionChecks(string source)
    {
        if (!source.Contains("IComposedTypeWrapper")) return source;

        // JsonParseNode returns an empty collection for a non-array node. Without a shape check,
        // a union factory therefore selects the array branch even for an object payload.
        return Regex.Replace(source,
            @"(?<prefix>\bif\s*\()(?<collection>parseNode\.GetCollectionOf(?:Primitive|Object|Enum)Values<)",
            "${prefix}parseNode.GetObjectValue<global::Microsoft.Kiota.Abstractions.Serialization.UntypedNode>(global::Microsoft.Kiota.Abstractions.Serialization.UntypedNode.CreateFromDiscriminatorValue) is global::Microsoft.Kiota.Abstractions.Serialization.UntypedArray && ${collection}");
    }
}
