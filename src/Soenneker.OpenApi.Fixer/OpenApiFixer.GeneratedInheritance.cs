using System;
using System.Collections.Generic;
using System.Text.RegularExpressions;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static void CollectGeneratedClasses(string source, Dictionary<string, string> classes)
    {
        Match ns = Regex.Match(source, @"(?m)^namespace\s+(?<name>[\w.]+)");
        Match type = Regex.Match(source, @"\bpublic partial class (?<name>\w+)");
        if (ns.Success && type.Success) classes.TryAdd(ns.Groups["name"].Value + "." + type.Groups["name"].Value, source);
    }

    private static string RepairGeneratedInheritedProperties(string source, Dictionary<string, string> classes)
    {
        Match parent = Regex.Match(source, @"\bpublic partial class \w+\s*:\s*(?:global::)?(?<base>[\w.]+)");
        if (!parent.Success || !classes.TryGetValue(parent.Groups["base"].Value, out string? baseSource) ||
            !source.Contains("base.Serialize(writer);", StringComparison.Ordinal)) return source;

        const string propertyPattern = @"public\s+(?<type>[\w:.<>?, ]+?)\s+(?<name>\w+)\s*\{\s*get;\s*set;\s*\}";
        var inherited = new HashSet<(string Type, string Name)>();
        foreach (Match property in Regex.Matches(baseSource, propertyPattern))
            inherited.Add((property.Groups["type"].Value, property.Groups["name"].Value));

        foreach (Match property in Regex.Matches(source, propertyPattern))
        {
            string type = property.Groups["type"].Value, name = property.Groups["name"].Value;
            if (!inherited.Contains((type, name))) continue;
            string writePattern = @"(?m)^[ \t]*writer\.(?<method>Write\w+Value(?:<[^\r\n]+>)?)\(""(?<wire>[^""]+)"", " + Regex.Escape(name) + @"\);\r?\n";
            Match write = Regex.Match(source, writePattern);
            if (!write.Success || !baseSource.Contains("writer." + write.Groups["method"].Value + "(\"" + write.Groups["wire"].Value + "\", " + name + ");", StringComparison.Ordinal)) continue;
            // Both declarations describe the same wire field. Share the base storage and serialize/parse it once.
            source = Regex.Replace(source, propertyPattern, match =>
                match.Groups["name"].Value == name && inherited.Contains((match.Groups["type"].Value, name))
                    ? "public new " + match.Groups["type"].Value + " " + name + " { get => base." + name + "; set => base." + name + " = value; }"
                    : match.Value);
            source = Regex.Replace(source, writePattern, "");
            source = Regex.Replace(source, @"(?m)^[ \t]*\{ """ + Regex.Escape(write.Groups["wire"].Value) + @""", n => \{ " + Regex.Escape(name) + @" = [^\r\n]+\r?\n", "");
        }
        return source;
    }
}
