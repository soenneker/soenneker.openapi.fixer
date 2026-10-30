using System;
using System.Text.RegularExpressions;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static string RepairGeneratedUnknownEnums(string source)
    {
        if (!source.Contains("_explicitNullProperties", StringComparison.Ordinal) || !source.Contains("AdditionalData {", StringComparison.Ordinal)) return source;
        return Regex.Replace(source,
            "\\{ \"(?<wire>[^\"]+)\", (?<node>\\w+) => \\{ (?<property>\\w+) = \\k<node>\\.GetEnumValue<(?<type>[^>]+)>\\(\\); \\} \\}", match =>
            {
                string property = match.Groups["property"].Value;
                if (!source.Contains("_explicitNullValue" + property, StringComparison.Ordinal)) return match.Value;
                string wire = match.Groups["wire"].Value;
                string node = match.Groups["node"].Value;
                return "{ \"" + wire + "\", " + node + " => { var parsed = " + node + ".GetEnumValue<" + match.Groups["type"].Value +
                    ">(); if (parsed == null && " + node + ".GetStringValue() is string unknown) { _explicitNullValue" + property +
                    " = default; _explicitNullProperties?.Remove(\"" + wire + "\"); AdditionalData[\"" + wire + "\"] = unknown; } else { " + property +
                    " = parsed; AdditionalData.Remove(\"" + wire + "\"); } } }";
            });
    }
}
