using System;
using System.Text.RegularExpressions;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static string SanitizeGeneratedIndexerNames(string source)
    {
        if (!Regex.IsMatch(source, @"public\s+[\w:.<>?]+\s+Item\s*(?:=>|\{)") ||
            !source.Contains("this[", StringComparison.Ordinal) || source.Contains("IndexerName(", StringComparison.Ordinal)) return source;
        string name = "ItemByKey";
        int suffix = 1;
        while (Regex.IsMatch(source, @"\b" + name + @"\b")) name = "ItemByKey" + suffix++;
        return Regex.Replace(source, @"(?m)^(?<indent>[ \t]*)public (?<rest>[^\r\n]+\bthis\[)", match =>
            match.Groups["indent"].Value + "[global::System.Runtime.CompilerServices.IndexerName(\"" + name + "\")]\n" +
            match.Groups["indent"].Value + "public " + match.Groups["rest"].Value);
    }
}
