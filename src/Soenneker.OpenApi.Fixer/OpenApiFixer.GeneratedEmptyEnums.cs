using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static string RepairGeneratedEmptyEnum(string source, JsonObject? schemas, Dictionary<string, string> emptyEnums)
    {
        Match declaration = Regex.Match(source, @"public enum (?<name>\w+)[^{]*\{");
        Match ns = Regex.Match(source, @"namespace (?<name>[\w.]+)");
        if (!declaration.Success || !ns.Success) return source;
        string name = declaration.Groups["name"].Value;
        if (schemas?[name]?["enum"] is not JsonArray values || !values.Any(v => v is JsonValue value && value.TryGetValue<string>(out string? text) && text.Length == 0)) return source;
        Match existing = Regex.Match(source, "\\[(?:(?:global::)?System\\.Runtime\\.Serialization\\.)?EnumMember(?:Attribute)?\\(Value = \"\"\\)\\]\\s*(?:#pragma[^\\r\\n]+\\s*)*(?<name>\\w+),");
        string member = existing.Success ? existing.Groups["name"].Value : "EmptyString";
        if (!existing.Success)
        {
            while (Regex.IsMatch(source, @"\b" + member + @"\b")) member += "Value";
            source = source.Insert(declaration.Index + declaration.Length, "\n        [global::System.Runtime.Serialization.EnumMember(Value = \"\")]\n        " + member + ",\n");
        }
        emptyEnums["global::" + ns.Groups["name"].Value + "." + name] = member;
        return source;
    }

    private static string RepairGeneratedEmptyEnumReads(string source, Dictionary<string, string> emptyEnums)
    {
        const string marker = "// Preserve the declared empty-string enum member.";
        if (source.Contains(marker, StringComparison.Ordinal)) return source;
        string updated = Regex.Replace(source, @"(?<node>\b\w+)\.GetEnumValue<(?<type>[\w:.]+)>\(\)", match =>
        {
            string type = match.Groups["type"].Value;
            if (!emptyEnums.TryGetValue(type, out string? member)) return match.Value;
            return "(" + match.Groups["node"].Value + ".GetStringValue() == \"\" ? " + type + "." + member + " : " + match.Value + ")";
        });
        return updated == source ? source : marker + "\n" + updated;
    }
}
