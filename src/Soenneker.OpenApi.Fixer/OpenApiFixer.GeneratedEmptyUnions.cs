using System.Linq;
using System.Text.RegularExpressions;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static string RepairGeneratedEmptyUnionSerialization(string source)
    {
        const string marker = "// Empty composed wrappers serialize as JSON null.";
        MatchCollection declarations = Regex.Matches(source, @"public partial class \w+[^{]+\{");
        for (int index = declarations.Count - 1; index >= 0; index--)
        {
            Match declaration = declarations[index];
            if (!declaration.Value.Contains("IComposedTypeWrapper")) continue;
            int end = index + 1 < declarations.Count ? declarations[index + 1].Index : source.Length;
            string segment = source[declaration.Index..end];
            if (!segment.Contains(marker) && segment.Contains("writer.WriteNullValue(null)")) continue;
            const string factoryMarker = "// A null union input must not initialize an object branch.";
            if (!segment.Contains(factoryMarker))
                segment = Regex.Replace(segment, @"(public static (?<type>[\w:.]+) CreateFromDiscriminatorValue\(IParseNode parseNode\)\s*\{)", match =>
                    match.Value + "\n            " + factoryMarker + "\n            if (global::System.Object.ReferenceEquals(parseNode, null)) throw new global::System.ArgumentNullException(nameof(parseNode));\n            if (parseNode.GetObjectValue<global::Microsoft.Kiota.Abstractions.Serialization.UntypedNode>(global::Microsoft.Kiota.Abstractions.Serialization.UntypedNode.CreateFromDiscriminatorValue) is null or global::Microsoft.Kiota.Abstractions.Serialization.UntypedNull) return new " + match.Groups["type"].Value + "();\n");
            if (segment.Contains(marker))
            {
                source = source[..declaration.Index] + segment + source[end..];
                continue;
            }
            string[] properties = Regex.Matches(segment, @"public [\w:.<>?\[\]]+ (?<name>\w+) \{ get; set; \}")
                .Cast<Match>().Select(m => m.Groups["name"].Value).Where(n => n != "AdditionalData").Distinct().ToArray();
            if (properties.Length == 0) continue;
            string guard = "\n            " + marker + "\n            if (global::System.Object.ReferenceEquals(writer, null)) throw new global::System.ArgumentNullException(nameof(writer));\n            if (" +
                string.Join(" && ", properties.Select(p => p + " == null")) + ") { writer.WriteNullValue(null); return; }\n";
            segment = Regex.Replace(segment, @"(public virtual void Serialize\(ISerializationWriter writer\)\s*\{)", "$1" + guard);
            source = source[..declaration.Index] + segment + source[end..];
        }
        return source;
    }
}
