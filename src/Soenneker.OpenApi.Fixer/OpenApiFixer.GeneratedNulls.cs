using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static string RepairGeneratedExplicitNulls(string source, Dictionary<string, string> classes)
    {
        MatchCollection declarations = Regex.Matches(source, @"public partial class \w+[^{]+\{");
        if (declarations.Count > 1)
        {
            int nested = declarations[1].Index;
            return RepairGeneratedExplicitNulls(source[..nested], classes) + source[nested..];
        }
        const string marker = "// Track explicitly assigned nulls separately from absent properties.";
        if (!source.Contains("void Serialize(", StringComparison.Ordinal) ||
            source.Contains(marker, StringComparison.Ordinal)) return source;
        Match declaration = Regex.Match(source, @"public partial class \w+[^{]+\{");
        if (!declaration.Success || declaration.Value.Contains("IComposedTypeWrapper", StringComparison.Ordinal)) return source;
        Match parent = Regex.Match(source, @"public partial class \w+\s*:\s*(?:global::)?(?<base>[\w.]+)");
        bool inherited = parent.Success && classes.TryGetValue(parent.Groups["base"].Value, out string? baseSource) &&
            baseSource.Contains("IParsable", StringComparison.Ordinal) && !Regex.Match(baseSource, @"public partial class \w+[^{]+\{").Value.Contains("IComposedTypeWrapper", StringComparison.Ordinal);
        if (!declaration.Value.Contains("IParsable", StringComparison.Ordinal) && !inherited) return source;
        var wireNames = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (Match match in Regex.Matches(source, "writer\\.Write\\w+(?:<[^>]+>)?\\(\"(?<wire>(?:[^\"\\\\]|\\\\.)*)\", (?<property>\\w+)\\);"))
            wireNames[match.Groups["property"].Value] = match.Groups["wire"].Value;
        var tracked = new Dictionary<string, string>(StringComparer.Ordinal);
        source = Regex.Replace(source, @"public (?<type>[\w:.<>?\[\]]+) (?<property>\w+) \{ get; set; \}", match =>
        {
            string property = match.Groups["property"].Value;
            if (!wireNames.TryGetValue(property, out string? wire)) return match.Value;
            string type = match.Groups["type"].Value;
            if (type is "bool" or "int" or "long" or "float" or "double" or "decimal" or "Guid" or "DateTimeOffset" or "TimeSpan") return match.Value;
            tracked[property] = wire;
            string field = "_explicitNullValue" + property;
            return "public " + type + " " + property +
                " { get => " + field + "; set { " + field + " = value; if (value is null) (_explicitNullProperties ??= new global::System.Collections.Generic.HashSet<string>()).Add(\"" + wire +
                "\"); else _explicitNullProperties?.Remove(\"" + wire + "\"); } }\n        private " + type + " " + field + " = default!;";
        });
        string capture = "\n        public " + (inherited ? "override" : "virtual") +
            " void CaptureExplicitNulls(global::Microsoft.Kiota.Abstractions.Serialization.IParseNode parseNode)\n        {\n";
        if (inherited) capture += "            base.CaptureExplicitNulls(parseNode);\n";
        foreach ((string property, string wire) in tracked)
            capture += "            if (parseNode.GetChildNode(\"" + wire + "\") is { } nullNode" + property + " && nullNode" + property + ".GetObjectValue<global::Microsoft.Kiota.Abstractions.Serialization.UntypedNode>(global::Microsoft.Kiota.Abstractions.Serialization.UntypedNode.CreateFromDiscriminatorValue) is null or global::Microsoft.Kiota.Abstractions.Serialization.UntypedNull) " + property + " = null;\n";
        capture += "        }\n";
        string updatedDeclaration = inherited ? declaration.Value : declaration.Value.Insert(declaration.Value.LastIndexOf('{'),
            ", global::Soenneker.OpenApi.Generated.IOpenApiExplicitNulls ");
        source = source.Remove(declaration.Index, declaration.Length).Insert(declaration.Index, updatedDeclaration + "\n        " + marker +
            "\n        private global::System.Collections.Generic.HashSet<string> _explicitNullProperties = null!;\n" + capture);
        source = Regex.Replace(source, @"return new (?<type>[\w:.]+)\(\);", "return global::Soenneker.OpenApi.Generated.OpenApiNullTracking.Capture(new ${type}(), parseNode);");
        // Place nulls after normal fields, so an explicitly cleared property is emitted exactly once.
        source = Regex.Replace(source, @"(public (?:override |virtual )?void Serialize\(ISerializationWriter writer\)\s*\{[\s\S]*?)(writer\.WriteAdditionalData\(AdditionalData\);|\n        \})",
            "$1if (_explicitNullProperties != null) foreach (string name in _explicitNullProperties) writer.WriteNullValue(name);\n            $2");
        return source;
    }
}

