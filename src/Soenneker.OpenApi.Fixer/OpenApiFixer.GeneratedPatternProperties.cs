using System;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static string RepairGeneratedPatternProperties(string source, JsonObject? schemas)
    {
        Match declaration = Regex.Match(source, @"public partial class (?<name>\w+)\s*:\s*IParsable\s*\{");
        if (!declaration.Success || schemas?[declaration.Groups["name"].Value] is not JsonObject schema ||
            schema["patternProperties"] is not JsonObject { Count: > 0 } ||
            source.Contains("AdditionalData", StringComparison.Ordinal)) return source;

        // Kiota ignores patternProperties and treats additionalProperties:false as an empty object.
        // Retain dynamic keys in the client; validation constraints remain in the source schema.
        string replacement = declaration.Value.Replace(": IParsable", ": IParsable, global::Microsoft.Kiota.Abstractions.Serialization.IAdditionalDataHolder", StringComparison.Ordinal) +
            "\n        public global::System.Collections.Generic.IDictionary<string, object> AdditionalData { get; set; } = new global::System.Collections.Generic.Dictionary<string, object>();\n";
        source = source.Remove(declaration.Index, declaration.Length).Insert(declaration.Index, replacement);
        return Regex.Replace(source, @"(public virtual void Serialize\(ISerializationWriter writer\)\s*\{)",
            "$1\n            writer.WriteAdditionalData(AdditionalData);");
    }
}
