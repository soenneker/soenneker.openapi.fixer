using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;

namespace Soenneker.OpenApi.Fixer;

internal static class OpenApiNameNormalizer
{
    private static readonly Regex _tokenRegex = new(@"\p{Lu}+(?=\p{Lu}\p{Ll}|\p{Nd}|$)|\p{Lu}?\p{Ll}+|\p{Nd}+|\p{L}+",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Dictionary<string, string> _acronyms = new(StringComparer.OrdinalIgnoreCase)
    {
        ["api"] = "Api",
        ["dns"] = "Dns",
        ["http"] = "Http",
        ["https"] = "Https",
        ["tls"] = "Tls",
        ["url"] = "Url",
        ["uri"] = "Uri",
        ["id"] = "Id",
        ["ip"] = "Ip",
        ["ipv4"] = "Ipv4",
        ["ipv6"] = "Ipv6",
        ["ssl"] = "Ssl",
        ["json"] = "Json",
        ["xml"] = "Xml",
        ["openapi"] = "OpenApi",
        ["uuid"] = "Uuid",
        ["oauth"] = "OAuth",
        ["jwt"] = "Jwt",
        ["sql"] = "Sql"
    };

    private static readonly HashSet<string> _csharpKeywords = new(StringComparer.Ordinal)
    {
        "abstract",
        "as",
        "base",
        "bool",
        "break",
        "byte",
        "case",
        "catch",
        "char",
        "checked",
        "class",
        "const",
        "continue",
        "decimal",
        "default",
        "delegate",
        "do",
        "double",
        "else",
        "enum",
        "event",
        "explicit",
        "extern",
        "false",
        "finally",
        "fixed",
        "float",
        "for",
        "foreach",
        "goto",
        "if",
        "implicit",
        "in",
        "int",
        "interface",
        "internal",
        "is",
        "lock",
        "long",
        "namespace",
        "new",
        "null",
        "object",
        "operator",
        "out",
        "override",
        "params",
        "private",
        "protected",
        "public",
        "readonly",
        "ref",
        "return",
        "sbyte",
        "sealed",
        "short",
        "sizeof",
        "stackalloc",
        "static",
        "string",
        "struct",
        "switch",
        "this",
        "throw",
        "true",
        "try",
        "typeof",
        "uint",
        "ulong",
        "unchecked",
        "unsafe",
        "ushort",
        "using",
        "virtual",
        "void",
        "volatile",
        "while"
    };

    private static readonly HashSet<string> _weakTypeNames = new(StringComparer.OrdinalIgnoreCase)
    {
        "Data",
        "Item",
        "Items",
        "Model",
        "Object",
        "Payload",
        "Request",
        "Response",
        "Result",
        "Results",
        "Schema",
        "Type",
        "Value",
        "Values"
    };

    public static string NormalizeComponentName(string? input, string fallback = "UnnamedComponent")
    {
        return NormalizePascalIdentifier(input, fallback, "Type", true);
    }

    public static string NormalizeNamePart(string? input, string fallback = "Part")
    {
        return NormalizePascalIdentifier(input, fallback, "Value", false);
    }

    public static string NormalizeOperationId(string? operationId, HttpMethod? method, string? path)
    {
        string? source = operationId;

        if (string.IsNullOrWhiteSpace(source))
            source = BuildRouteOperationName(method, path);

        return NormalizePascalIdentifier(source, "UnnamedOperation", "Operation", true);
    }

    public static string NormalizeMediaTypeName(string? mediaType, string fallback = "Media")
    {
        if (string.IsNullOrWhiteSpace(mediaType))
            return fallback;

        string baseType = mediaType.Split(';')[0].Trim();

        if (baseType.Length == 0 || baseType.Contains('*', StringComparison.Ordinal))
            return fallback;

        string source = baseType;
        int slashIndex = source.IndexOf('/');

        if (slashIndex >= 0 && slashIndex + 1 < source.Length)
            source = source[(slashIndex + 1)..];

        if (source.StartsWith("vnd.", StringComparison.OrdinalIgnoreCase))
            source = source[4..];

        source = source.Replace("+", " ", StringComparison.Ordinal)
                       .Replace("-", " ", StringComparison.Ordinal)
                       .Replace(".", " ", StringComparison.Ordinal)
                       .Replace("_", " ", StringComparison.Ordinal);

        return NormalizeNamePart(source, fallback);
    }

    public static string ReserveComponentName(IEnumerable<string> existingNames, string baseName, string fallbackSuffix)
    {
        var reserved = new HashSet<string>(existingNames, StringComparer.OrdinalIgnoreCase);
        string candidate = NormalizeComponentName(baseName);

        if (!reserved.Contains(candidate))
            return candidate;

        string suffixCandidate = NormalizeComponentName($"{baseName} {fallbackSuffix}");
        if (!reserved.Contains(suffixCandidate))
            return suffixCandidate;

        var index = 2;
        string numberedCandidate;

        do
        {
            numberedCandidate = NormalizeComponentName($"{baseName} {fallbackSuffix} {index++}");
        }
        while (reserved.Contains(numberedCandidate));

        return numberedCandidate;
    }

    public static string MakeUniqueIdentifier(string candidate, ISet<string> usedNames)
    {
        string normalized = NormalizeComponentName(candidate);

        if (!usedNames.Contains(normalized))
            return normalized;

        var index = 2;
        string uniqueName;

        do
        {
            uniqueName = $"{normalized}{index++}";
        }
        while (usedNames.Contains(uniqueName));

        return uniqueName;
    }

    public static bool IsValidCSharpIdentifier(string? id)
    {
        if (string.IsNullOrWhiteSpace(id))
            return false;

        if (!char.IsLetter(id[0]) && id[0] != '_')
            return false;

        for (var i = 1; i < id.Length; i++)
        {
            char c = id[i];
            if (!char.IsLetterOrDigit(c) && c != '_')
                return false;
        }

        return !IsCSharpKeyword(id);
    }

    private static string NormalizePascalIdentifier(string? input, string fallback, string reservedKeywordSuffix, bool avoidWeakName)
    {
        string candidate = BuildPascalIdentifier(input);

        if (string.IsNullOrWhiteSpace(candidate))
        {
            string fallbackCandidate = BuildPascalIdentifier(fallback);

            if (!string.IsNullOrWhiteSpace(fallbackCandidate))
                candidate = fallbackCandidate;
            else
                candidate = "Unnamed";
        }
        else if (avoidWeakName && IsWeakTypeName(candidate))
        {
            string fallbackCandidate = BuildPascalIdentifier(fallback);

            if (!string.IsNullOrWhiteSpace(fallbackCandidate) && !IsWeakTypeName(fallbackCandidate) &&
                !fallbackCandidate.StartsWith("Unnamed", StringComparison.OrdinalIgnoreCase))
            {
                candidate = fallbackCandidate;
            }
            else
            {
                candidate = $"{candidate}Value";
            }
        }

        if (!char.IsLetter(candidate[0]) && candidate[0] != '_')
            candidate = $"Value{candidate}";

        if (IsCSharpKeyword(candidate))
            candidate = $"{candidate}{reservedKeywordSuffix}";

        return candidate;
    }

    private static string BuildPascalIdentifier(string? input)
    {
        if (string.IsNullOrWhiteSpace(input))
            return string.Empty;

        var builder = new StringBuilder(input.Length);
        MatchCollection matches = _tokenRegex.Matches(input);

        foreach (Match match in matches)
        {
            if (!match.Success || match.Value.Length == 0)
                continue;

            AppendToken(builder, match.Value);
        }

        return builder.ToString();
    }

    private static void AppendToken(StringBuilder builder, string token)
    {
        if (IsAllDigits(token))
        {
            if (builder.Length == 0)
                builder.Append("Value");

            builder.Append(token);
            return;
        }

        if (_acronyms.TryGetValue(token, out string? acronym))
        {
            builder.Append(acronym);
            return;
        }

        string lower = token.ToLowerInvariant();

        if (_acronyms.TryGetValue(lower, out acronym))
        {
            builder.Append(acronym);
            return;
        }

        builder.Append(char.ToUpperInvariant(lower[0]));

        if (lower.Length > 1)
            builder.Append(lower, 1, lower.Length - 1);
    }

    private static bool IsAllDigits(string value)
    {
        for (var i = 0; i < value.Length; i++)
        {
            if (!char.IsDigit(value[i]))
                return false;
        }

        return true;
    }

    private static bool IsWeakTypeName(string value)
    {
        return _weakTypeNames.Contains(value);
    }

    private static bool IsCSharpKeyword(string value)
    {
        return _csharpKeywords.Contains(value.ToLowerInvariant());
    }

    private static string BuildRouteOperationName(HttpMethod? method, string? path)
    {
        var builder = new StringBuilder();

        if (method == null || string.IsNullOrWhiteSpace(method.Method))
            builder.Append("Operation");
        else
            builder.Append(method.Method);

        if (string.IsNullOrWhiteSpace(path) || path == "/")
        {
            builder.Append(" Root");
            return builder.ToString();
        }

        string[] segments = path.Split('/', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);

        foreach (string segment in segments)
        {
            if (segment.Length == 0)
                continue;

            if (segment[0] == '{' && segment[^1] == '}' && segment.Length > 2)
            {
                builder.Append(" By ");
                builder.Append(segment[1..^1]);
                continue;
            }

            builder.Append(' ');
            builder.Append(segment);
        }

        return builder.ToString();
    }
}
