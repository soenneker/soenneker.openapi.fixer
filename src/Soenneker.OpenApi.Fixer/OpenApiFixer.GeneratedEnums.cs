using Microsoft.Extensions.Logging;
using Microsoft.OpenApi;
using Soenneker.Extensions.Task;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static readonly Regex _generatedEnumMemberRegex = new(
        @"(?<prefix>\[EnumMember\(Value = ""(?<value>(?:[^""\\]|\\.)*)""\)\]\s*#pragma warning disable CS1591\s*)(?<name>[^\r\n,]+)(?<suffix>,\s*#pragma warning restore CS1591)",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Dictionary<string, string> _multiCharacterEnumTokens = new(StringComparer.Ordinal)
    {
        ["!="] = "ExclamationEqual",
        ["!~"] = "ExclamationTilde",
        ["<="] = "LessThanOrEqual",
        [">="] = "GreaterThanOrEqual",
        ["=="] = "DoubleEqual"
    };

    private static readonly Dictionary<char, string> _enumSymbolTokens = new()
    {
        ['!'] = "Exclamation",
        ['"'] = "Quote",
        ['#'] = "Hash",
        ['$'] = "Dollar",
        ['%'] = "Percent",
        ['&'] = "Ampersand",
        ['\''] = "Apostrophe",
        ['('] = "LeftParenthesis",
        [')'] = "RightParenthesis",
        ['*'] = "Asterisk",
        ['+'] = "Plus",
        [','] = "Comma",
        ['-'] = "Minus",
        ['.'] = "Dot",
        ['/'] = "Slash",
        [':'] = "Colon",
        [';'] = "Semicolon",
        ['<'] = "LessThan",
        ['='] = "Equal",
        ['>'] = "GreaterThan",
        ['?'] = "QuestionMark",
        ['@'] = "At",
        ['['] = "LeftBracket",
        ['\\'] = "BackSlash",
        [']'] = "RightBracket",
        ['^'] = "Caret",
        ['{'] = "LeftBrace",
        ['|'] = "Pipe",
        ['}'] = "RightBrace",
        ['~'] = "Tilde"
    };

    private string InjectKiotaEnumValueNames(string json)
    {
        if (string.IsNullOrWhiteSpace(json))
            return json;

        JsonNode? root;

        try
        {
            root = JsonNode.Parse(json);
        }
        catch (JsonException ex)
        {
            _logger.LogDebug(ex, "Unable to parse serialized OpenAPI JSON when injecting Kiota enum names");
            return json;
        }

        if (root is null)
            return json;

        bool changed = InjectKiotaEnumValueNames(root, null);

        return changed ? root.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) : json;
    }

    private static bool InjectKiotaEnumValueNames(JsonNode? node, string? suggestedName)
    {
        switch (node)
        {
            case JsonObject obj:
            {
                bool changed = TryInjectKiotaEnumValueNames(obj, suggestedName);

                foreach ((string key, JsonNode? child) in obj)
                {
                    switch (key)
                    {
                        case "schemas":
                        case "properties":
                            if (child is JsonObject namedChildren)
                            {
                                foreach ((string childName, JsonNode? namedChild) in namedChildren)
                                {
                                    changed |= InjectKiotaEnumValueNames(namedChild, childName);
                                }
                            }

                            break;
                        case "items":
                            changed |= InjectKiotaEnumValueNames(child, $"{suggestedName ?? "Item"}Item");
                            break;
                        case "additionalProperties":
                            changed |= InjectKiotaEnumValueNames(child, $"{suggestedName ?? "AdditionalProperty"}AdditionalProperty");
                            break;
                        default:
                            changed |= InjectKiotaEnumValueNames(child, suggestedName);
                            break;
                    }
                }

                return changed;
            }
            case JsonArray array:
            {
                bool changed = false;

                foreach (JsonNode? child in array)
                {
                    changed |= InjectKiotaEnumValueNames(child, suggestedName);
                }

                return changed;
            }
            default:
                return false;
        }
    }

    private static bool TryInjectKiotaEnumValueNames(JsonObject schemaObject, string? suggestedName)
    {
        if (schemaObject["enum"] is not JsonArray enumArray || enumArray.Count == 0)
            return false;

        JsonObject? existingXMsEnum = schemaObject["x-ms-enum"] as JsonObject;
        JsonArray? existingValuesArray = existingXMsEnum?["values"] as JsonArray;

        var usedNames = new HashSet<string>(StringComparer.Ordinal);
        var namesToInject = new Dictionary<string, string>(StringComparer.Ordinal);

        foreach (JsonNode? enumNode in enumArray)
        {
            if (enumNode is not JsonValue enumValue || !enumValue.TryGetValue(out string? enumText))
                continue;

            string? existingName = GetExistingEnumValueName(existingValuesArray, enumText);

            if (ShouldInjectKiotaEnumName(enumText, existingName))
                continue;

            string usedName = !string.IsNullOrWhiteSpace(existingName) ? existingName : enumText;

            if (IsStandardCSharpEnumMemberIdentifier(usedName))
                usedNames.Add(usedName);
        }

        foreach (JsonNode? enumNode in enumArray)
        {
            if (enumNode is not JsonValue enumValue || !enumValue.TryGetValue(out string? enumText))
                continue;

            string? existingName = GetExistingEnumValueName(existingValuesArray, enumText);

            if (!ShouldInjectKiotaEnumName(enumText, existingName))
                continue;

            string enumName = MakeUniqueEnumMemberName(BuildSafeEnumMemberName(enumText), usedNames);
            usedNames.Add(enumName);
            namesToInject[enumText] = enumName;
        }

        if (namesToInject.Count == 0)
            return false;

        bool changed = false;
        JsonObject xMsEnum = schemaObject["x-ms-enum"] as JsonObject ?? new JsonObject();

        if (schemaObject["x-ms-enum"] is null)
        {
            schemaObject["x-ms-enum"] = xMsEnum;
            changed = true;
        }

        if (xMsEnum["name"] is null)
        {
            xMsEnum["name"] = BuildSafeEnumMemberName(suggestedName ?? "GeneratedEnum");
            changed = true;
        }

        if (xMsEnum["modelAsString"] is null)
        {
            xMsEnum["modelAsString"] = false;
            changed = true;
        }

        JsonArray valuesArray = xMsEnum["values"] as JsonArray ?? new JsonArray();

        if (xMsEnum["values"] is null)
        {
            xMsEnum["values"] = valuesArray;
            changed = true;
        }

        foreach ((string enumValue, string enumName) in namesToInject)
        {
            JsonObject? existingValue = valuesArray.OfType<JsonObject>()
                                                   .FirstOrDefault(valueObject =>
                                                       valueObject["value"] is JsonValue value && value.TryGetValue(out string? existingEnumValue) &&
                                                       string.Equals(existingEnumValue, enumValue, StringComparison.Ordinal));

            if (existingValue is null)
            {
                valuesArray.Add(new JsonObject
                {
                    ["value"] = enumValue,
                    ["name"] = enumName
                });
                changed = true;
                continue;
            }

            if (existingValue["name"] is not JsonValue nameValue || !nameValue.TryGetValue(out string? existingName) ||
                string.IsNullOrWhiteSpace(existingName) || !string.Equals(existingName, enumName, StringComparison.Ordinal))
            {
                existingValue["name"] = enumName;
                changed = true;
            }
        }

        return changed;
    }

    private static string? GetExistingEnumValueName(JsonArray? valuesArray, string enumValue)
    {
        if (valuesArray is null)
            return null;

        JsonObject? existingValue = valuesArray.OfType<JsonObject>()
                                               .FirstOrDefault(valueObject =>
                                                   valueObject["value"] is JsonValue value && value.TryGetValue(out string? existingEnumValue) &&
                                                   string.Equals(existingEnumValue, enumValue, StringComparison.Ordinal));

        return existingValue?["name"] is JsonValue nameValue && nameValue.TryGetValue(out string? existingName) ? existingName : null;
    }

    private static bool ShouldInjectKiotaEnumName(string enumValue, string? existingName)
    {
        if (string.IsNullOrEmpty(enumValue))
            return false;

        if (!string.IsNullOrWhiteSpace(existingName))
            return !IsStandardCSharpEnumMemberIdentifier(existingName);

        return !string.Equals(enumValue, BuildSafeEnumMemberName(enumValue), StringComparison.Ordinal);
    }

    private static bool IsStandardCSharpEnumMemberIdentifier(string? name)
    {
        if (string.IsNullOrWhiteSpace(name) || name.Contains('_', StringComparison.Ordinal))
            return false;

        return char.IsUpper(name[0]) && OpenApiNameNormalizer.IsValidCSharpIdentifier(name);
    }

    public async ValueTask SanitizeGeneratedEnumMembers(string generatedRoot, CancellationToken cancellationToken = default)
    {
        if (!await _directoryUtil.Exists(generatedRoot, cancellationToken))
            return;

        foreach (string filePath in await _directoryUtil.GetFilesByExtension(generatedRoot, ".cs", recursive: true, cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            string original = await _fileUtil.Read(filePath, cancellationToken: cancellationToken)
                                             .NoSync();

            if (!original.Contains("GeneratedCode(\"Kiota\"", StringComparison.Ordinal) && !original.Contains("<auto-generated/>", StringComparison.Ordinal))
            {
                continue;
            }

            string sanitized = SanitizeGeneratedEnumMembersForContent(original);

            if (!string.Equals(original, sanitized, StringComparison.Ordinal))
                await _fileUtil.Write(filePath, sanitized, cancellationToken: cancellationToken)
                               .NoSync();
        }
    }

    private static string SanitizeGeneratedEnumMembersForContent(string fileContents)
    {
        MatchCollection matches = _generatedEnumMemberRegex.Matches(fileContents);

        if (matches.Count == 0)
            return fileContents;

        HashSet<string> usedNames = matches.Select(match => match.Groups["name"]
                                                                 .Value.Trim())
                                           .Where(name => name.Length > 0)
                                           .ToHashSet(StringComparer.Ordinal);

        return _generatedEnumMemberRegex.Replace(fileContents, match =>
        {
            string currentName = match.Groups["name"]
                                      .Value.Trim();

            if (IsValidEnumMemberIdentifier(currentName))
                return match.Value;

            usedNames.Remove(currentName);

            string enumValue = Regex.Unescape(match.Groups["value"].Value);
            string replacementName = BuildSafeEnumMemberName(enumValue);
            string uniqueName = MakeUniqueEnumMemberName(replacementName, usedNames);

            usedNames.Add(uniqueName);

            return $"{match.Groups["prefix"].Value}{uniqueName}{match.Groups["suffix"].Value}";
        });
    }

    private static bool IsValidEnumMemberIdentifier(string name) =>
        !string.IsNullOrWhiteSpace(name) && Regex.IsMatch(name, @"^[_\p{L}][_\p{L}\p{Nd}]*$");

    private static string MakeUniqueEnumMemberName(string candidate, HashSet<string> usedNames)
    {
        string safeCandidate = string.IsNullOrWhiteSpace(candidate) ? "EnumValue" : candidate;

        if (!usedNames.Contains(safeCandidate))
            return safeCandidate;

        int suffix = 1;
        string uniqueCandidate;

        do
        {
            uniqueCandidate = $"{safeCandidate}{suffix++}";
        }
        while (usedNames.Contains(uniqueCandidate));

        return uniqueCandidate;
    }

    private static string BuildSafeEnumMemberName(string enumValue)
    {
        if (string.IsNullOrEmpty(enumValue))
            return "EnumValue";

        if (enumValue.All(char.IsWhiteSpace))
            return BuildWhitespaceOnlyEnumMemberName(enumValue);

        if (_multiCharacterEnumTokens.TryGetValue(enumValue, out string? combinedToken))
            return combinedToken;

        if (CanNormalizeEnumValueAsWords(enumValue))
            return OpenApiNameNormalizer.NormalizeNamePart(enumValue, "EnumValue");

        var builder = new StringBuilder(enumValue.Length * 2);
        bool capitalizeNext = true;

        for (int i = 0; i < enumValue.Length; i++)
        {
            if (TryGetMultiCharacterEnumToken(enumValue, i, out string? multiCharacterToken, out int tokenLength))
            {
                builder.Append(multiCharacterToken);
                capitalizeNext = true;
                i += tokenLength - 1;
                continue;
            }

            char character = enumValue[i];

            if (char.IsLetterOrDigit(character))
            {
                if (builder.Length == 0 && char.IsDigit(character))
                    builder.Append("Value");

                builder.Append(capitalizeNext ? char.ToUpperInvariant(character) : character);
                capitalizeNext = false;
                continue;
            }

            if (character == '_' || char.IsWhiteSpace(character))
            {
                capitalizeNext = true;
                continue;
            }

            if (IsWordSeparator(enumValue, i))
            {
                capitalizeNext = true;
                continue;
            }

            if (_enumSymbolTokens.TryGetValue(character, out string? symbolToken))
            {
                builder.Append(symbolToken);
                capitalizeNext = true;
            }
        }

        string sanitized = builder.ToString();

        if (string.IsNullOrWhiteSpace(sanitized))
            sanitized = "EnumValue";

        if (!char.IsLetter(sanitized[0]) && sanitized[0] != '_')
            sanitized = $"Value{sanitized}";

        if (!OpenApiNameNormalizer.IsValidCSharpIdentifier(sanitized))
            sanitized = OpenApiNameNormalizer.NormalizeNamePart(sanitized, "EnumValue");

        return sanitized;
    }

    private static bool CanNormalizeEnumValueAsWords(string enumValue)
    {
        var hasLetterOrDigit = false;

        foreach (char character in enumValue)
        {
            if (char.IsLetterOrDigit(character))
            {
                hasLetterOrDigit = true;
                continue;
            }

            if (character == '_' || character == '-' || character == '.' || character == '/' || char.IsWhiteSpace(character))
                continue;

            return false;
        }

        return hasLetterOrDigit;
    }

    private static bool IsWordSeparator(string value, int index)
    {
        char character = value[index];

        if (character != '-' && character != '.' && character != '/')
            return false;

        return HasLetterOrDigitBefore(value, index) && HasLetterOrDigitAfter(value, index);
    }

    private static bool HasLetterOrDigitBefore(string value, int index)
    {
        for (int i = index - 1; i >= 0; i--)
        {
            if (char.IsLetterOrDigit(value[i]))
                return true;

            if (!char.IsWhiteSpace(value[i]))
                return false;
        }

        return false;
    }

    private static bool HasLetterOrDigitAfter(string value, int index)
    {
        for (int i = index + 1; i < value.Length; i++)
        {
            if (char.IsLetterOrDigit(value[i]))
                return true;

            if (!char.IsWhiteSpace(value[i]))
                return false;
        }

        return false;
    }

    private static bool TryGetMultiCharacterEnumToken(string value, int startIndex, out string? token, out int tokenLength)
    {
        foreach ((string symbol, string mappedToken) in _multiCharacterEnumTokens)
        {
            if (startIndex + symbol.Length > value.Length)
                continue;

            if (string.CompareOrdinal(value, startIndex, symbol, 0, symbol.Length) == 0)
            {
                token = mappedToken;
                tokenLength = symbol.Length;
                return true;
            }
        }

        token = null;
        tokenLength = 0;
        return false;
    }

    private static string BuildWhitespaceOnlyEnumMemberName(string enumValue)
    {
        var builder = new StringBuilder(enumValue.Length * 8);

        foreach (char character in enumValue)
        {
            builder.Append(character switch
            {
                ' ' => "Space",
                '\t' => "Tab",
                '\r' => "CarriageReturn",
                '\n' => "LineFeed",
                '\f' => "FormFeed",
                '\v' => "VerticalTab",
                _ when char.IsWhiteSpace(character) => "Whitespace",
                _ => string.Empty
            });
        }

        string sanitized = builder.ToString();
        return string.IsNullOrWhiteSpace(sanitized) ? "EnumValue" : sanitized;
    }

}
