using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static void CollectGeneratedEnumDefaults(string source, Dictionary<string, string> replacements)
    {
        Match namespaceMatch = Regex.Match(source, @"(?m)^namespace\s+(?<name>[\w.]+)");
        Match enumMatch = Regex.Match(source, @"\bpublic enum (?<name>\w+)");
        if (!namespaceMatch.Success || !enumMatch.Success)
            return;

        string prefix = namespaceMatch.Groups["name"].Value + "." + enumMatch.Groups["name"].Value + ".";
        MatchCollection members = _generatedEnumMemberRegex.Matches(source);
        HashSet<string> validMembers = members.Select(m => m.Groups["name"].Value.Trim()).ToHashSet(StringComparer.Ordinal);
        foreach (Match member in members)
        {
            string value = Regex.Unescape(member.Groups["value"].Value);
            if (value.Length == 0)
                continue;
            // Kiota emits defaults using the wire value with only its first letter capitalized.
            string emittedName = char.ToUpperInvariant(value[0]) + value[1..];
            if (IsValidEnumMemberIdentifier(emittedName) && !validMembers.Contains(emittedName))
                replacements.TryAdd(prefix + emittedName, prefix + member.Groups["name"].Value.Trim());
        }
    }

    private static string RepairGeneratedEnumDefaults(string source, Dictionary<string, string> replacements) =>
        Regex.Replace(source, @"(?m)^(?<prefix>\s*\w+\s*=\s*)(?<value>(?:global::)?[\w.]+)(?<suffix>;\s*)$", match =>
        {
            string value = match.Groups["value"].Value;
            string global = value.StartsWith("global::", StringComparison.Ordinal) ? "global::" : "";
            string key = global.Length == 0 ? value : value[global.Length..];
            return replacements.TryGetValue(key, out string? replacement)
                ? match.Groups["prefix"].Value + global + replacement + match.Groups["suffix"].Value
                : match.Value;
        });
}
