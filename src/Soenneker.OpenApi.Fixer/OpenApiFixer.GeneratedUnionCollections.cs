using System.Text.RegularExpressions;
using System.Text.Json.Nodes;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static string RepairGeneratedAnonymousObjectCollections(string source, JsonObject? schemas)
    {
        const string marker = "// Check anonymous object array elements before selecting a union branch.";
        if (schemas == null || !source.Contains("UnrecognizedValue") || source.Contains(marker)) return source;
        const string ns = "global::Microsoft.Kiota.Abstractions.Serialization.";
        string updated = Regex.Replace(source, @"parseNode\.GetCollectionOfObjectValues<(?<type>global::[\w.]+)>\(", match =>
        {
            string type = match.Groups["type"].Value;
            string name = type[(type.LastIndexOf('.') + 1)..];
            if (schemas[name] != null || name == "UntypedNode") return match.Value;
            string payload = "parseNode.GetObjectValue<" + ns + "UntypedNode>(" + ns + "UntypedNode.CreateFromDiscriminatorValue)";
            return payload + " is " + ns + "UntypedArray && global::System.Linq.Enumerable.All(((" + ns + "UntypedArray)" + payload + ").GetValue(), item => item is " + ns + "UntypedObject) && " + match.Value;
        });
        return updated == source ? source : marker + "\n" + updated;
    }

    private static string RepairGeneratedUntypedCollections(string source) => Regex.Replace(source,
        @"GetCollectionOfPrimitiveValues<(?<type>(?:global::Microsoft\.Kiota\.Abstractions\.Serialization\.)?UntypedNode)>\(\)",
        "GetCollectionOfObjectValues<${type}>(${type}.CreateFromDiscriminatorValue)");

    private static string RepairGeneratedUnionCollectionChecks(string source)
    {
        if (!source.Contains("IComposedTypeWrapper")) return source;
        source = Regex.Replace(source, @"(parseNode\.GetChildNode\(""[^""]+""\))\.GetObjectValue<", "$1?.GetObjectValue<");

        // JsonParseNode returns an empty collection for a non-array node. Without a shape check,
        // a union factory therefore selects the array branch even for an object payload.
        source = Regex.Replace(source,
            @"(?<prefix>\bif\s*\()(?<collection>parseNode\.GetCollectionOf(?:Primitive|Object|Enum)Values<)",
            "${prefix}parseNode.GetObjectValue<global::Microsoft.Kiota.Abstractions.Serialization.UntypedNode>(global::Microsoft.Kiota.Abstractions.Serialization.UntypedNode.CreateFromDiscriminatorValue) is global::Microsoft.Kiota.Abstractions.Serialization.UntypedArray && ${collection}");

        // JsonParseNode's string/date/enum getters throw on non-string JSON rather than returning
        // null. A union must test the wire shape before attempting any of those conversions.
        source = Regex.Replace(source,
            @"(?<prefix>\bif\s*\()(?<getter>parseNode\.(?:GetStringValue|GetGuidValue|GetDateTimeOffsetValue|GetDateValue|GetTimeValue|GetTimeSpanValue|GetByteArrayValue|GetEnumValue<[^>]+>)\(\))",
            "${prefix}parseNode.GetObjectValue<global::Microsoft.Kiota.Abstractions.Serialization.UntypedNode>(global::Microsoft.Kiota.Abstractions.Serialization.UntypedNode.CreateFromDiscriminatorValue) is global::Microsoft.Kiota.Abstractions.Serialization.UntypedString && ${getter}");

        return Regex.Replace(source, @"(?<prefix>\bif\s*\()(?<getter>parseNode\.(?<method>GetBoolValue|GetIntValue|GetLongValue|GetFloatValue|GetDoubleValue|GetDecimalValue)\(\))", match =>
        {
            const string ns = "global::Microsoft.Kiota.Abstractions.Serialization.";
            string shape = match.Groups["method"].Value switch
            {
                "GetBoolValue" => ns + "UntypedBoolean",
                "GetIntValue" => ns + "UntypedInteger",
                "GetLongValue" => "(" + ns + "UntypedInteger or " + ns + "UntypedLong)",
                _ => "(" + ns + "UntypedInteger or " + ns + "UntypedLong or " + ns + "UntypedFloat or " + ns + "UntypedDouble or " + ns + "UntypedDecimal)"
            };
            return match.Groups["prefix"].Value + "parseNode.GetObjectValue<" + ns + "UntypedNode>(" + ns + "UntypedNode.CreateFromDiscriminatorValue) is " + shape + " && " + match.Groups["getter"].Value;
        });
    }
}
