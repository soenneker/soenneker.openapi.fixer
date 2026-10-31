using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static string RepairGeneratedLossyUnionProperties(string source, JsonObject? schemas)
    {
        Match declaration = Regex.Match(source, @"public partial class (?<name>\w+)[^{]+\{");
        if (schemas == null || !declaration.Success || declaration.Value.Contains("IComposedTypeWrapper", StringComparison.Ordinal) || schemas[declaration.Groups["name"].Value] is not JsonObject schema) return source;
        if ((schema["nullable"]?.ToJsonString() == "true" || schema["type"] is JsonArray types && types.Any(t => t?.ToJsonString() == "\"null\"")) &&
            !source.Contains("// Preserve a nullable root response.", StringComparison.Ordinal))
        {
            const string node = "global::Microsoft.Kiota.Abstractions.Serialization.";
            var guard = new Regex(@"if\(ReferenceEquals\(parseNode, null\)\) throw new ArgumentNullException\(nameof\(parseNode\)\);");
            source = guard.Replace(source, "$0\n            // Preserve a nullable root response.\n            if (parseNode.GetObjectValue<" + node + "UntypedNode>(" + node + "UntypedNode.CreateFromDiscriminatorValue) is null or " + node + "UntypedNull) return null!;", 1);
        }
        JsonObject? Resolve(JsonNode? node)
        {
            var seen = new HashSet<string>(StringComparer.Ordinal);
            while (node is JsonObject obj && obj["$ref"] is JsonValue reference)
            {
                string id = reference.GetValue<string>();
                if (!id.StartsWith("#/components/schemas/", StringComparison.Ordinal) || !seen.Add(id)) return null;
                node = schemas[id[21..].Replace("~1", "/").Replace("~0", "~")];
            }
            return node as JsonObject;
        }
        var fields = new Dictionary<string, List<JsonObject>>(StringComparer.Ordinal);
        IEnumerable<JsonObject> variants = (schema["oneOf"] ?? schema["anyOf"]) is JsonArray alternatives
            ? alternatives.Select(Resolve).OfType<JsonObject>() : new[] { schema };
        foreach (JsonObject variant in variants)
            if (variant["properties"] is JsonObject properties)
                foreach ((string wire, JsonNode? field) in properties)
                    if (Resolve(field) is JsonObject resolved)
                    {
                        if (!fields.TryGetValue(wire, out List<JsonObject>? definitions)) fields[wire] = definitions = [];
                        definitions.Add(resolved);
                    }
        foreach ((string wire, List<JsonObject> definitions) in fields)
        {
            Match assignment = Regex.Match(source, "\\{ \"" + Regex.Escape(wire) + "\", n => \\{ (?<property>\\w+) = n\\.(?<getter>Get\\w+)(?:<(?<type>[^>]+)>)?\\(");
            if (!assignment.Success) continue;
            string property = assignment.Groups["property"].Value;
            bool stringUnion = definitions.Count > 1 && definitions.All(d => d["type"]?.ToJsonString() == "\"string\"") &&
                definitions.Select(d => d["enum"]?.ToJsonString()).Distinct(StringComparer.Ordinal).Count() > 1 && assignment.Groups["getter"].Value is "GetEnumValue" or "GetStringValue";
            bool ambiguousEnum = assignment.Groups["getter"].Value is "GetEnumValue" or "GetStringValue" && definitions.Any(d => d["enum"] is JsonArray values &&
                values.OfType<JsonValue>().Where(v => v.TryGetValue<string>(out _)).Select(v => v.GetValue<string>()).GroupBy(v => new string(v.Where(char.IsLetterOrDigit).Select(char.ToLowerInvariant).ToArray())).Any(g => g.Distinct(StringComparer.Ordinal).Count() > 1));
            bool time = definitions.Count == 1 && definitions[0]["format"]?.ToJsonString() == "\"time\"" && assignment.Groups["getter"].Value is "GetTimeValue" or "GetStringValue";
            stringUnion |= ambiguousEnum || time;
            JsonObject? items = definitions.Count == 1 ? Resolve(definitions[0]["items"]) : null;
            JsonArray? choices = (items?["anyOf"] ?? items?["oneOf"]) as JsonArray;
            bool mixedArray = choices is { Count: > 1 } && choices.Select(c => Resolve(c)?["type"]?.ToJsonString()).Distinct(StringComparer.Ordinal).Count() > 1 &&
                (assignment.Groups["getter"].Value == "GetCollectionOfPrimitiveValues" ||
                 assignment.Groups["getter"].Value == "GetCollectionOfObjectValues" && assignment.Groups["type"].Value.EndsWith(".UntypedNode", StringComparison.Ordinal));
            JsonArray? valueChoices = definitions.Count == 1 ? (definitions[0]["oneOf"] ?? definitions[0]["anyOf"]) as JsonArray : null;
            bool nestedUnion = valueChoices is { Count: > 1 } && valueChoices.Select(c => Resolve(c)?["type"]?.ToJsonString()).Distinct(StringComparer.Ordinal).Count() > 1 &&
                assignment.Groups["getter"].Value == "GetObjectValue" && assignment.Groups["type"].Value.Contains("." + declaration.Groups["name"].Value + ".", StringComparison.Ordinal);
            bool untypedValue = definitions.Count == 1 && definitions[0]["x-client-untyped-value"]?.ToJsonString() == "true";
            nestedUnion |= untypedValue;
            if (!stringUnion && !mixedArray && !nestedUnion) continue;
            if (time)
            {
                // Keep constructor defaults as wire strings when replacing Kiota's Time property.
                source = Regex.Replace(source, @"\b" + Regex.Escape(property) + @"\s*=\s*new (?:global::)?(?:Microsoft\.Kiota\.Abstractions\.)?Time\((?:global::)?(?:System\.)?DateTimeOffset\.Parse\((?<literal>""(?:\\.|[^""\\])*"")\)\.DateTime\)",
                    match => property + " = " + match.Groups["literal"].Value);
            }
            const string untyped = "global::Microsoft.Kiota.Abstractions.Serialization.UntypedNode";
            string type = stringUnion ? "string" : nestedUnion ? untyped : "List<" + untyped + ">";
            source = Regex.Replace(source, @"(?<visibility>public|private) [\w:.]+(?:<[^>\r\n]+>)?(?<nullable>\?)? (?<property>" + Regex.Escape(property) + "|_explicitNullValue" + Regex.Escape(property) + @")\b",
                "${visibility} " + type + "${nullable} ${property}");
            string getter = stringUnion ? "GetStringValue()" : (nestedUnion ? "GetObjectValue<" : "GetCollectionOfObjectValues<") + untyped + ">(" + untyped + ".CreateFromDiscriminatorValue)";
            source = Regex.Replace(source, "(?<prefix>\\{ \"" + Regex.Escape(wire) + "\", n => \\{ " + Regex.Escape(property) + @" = n\.)Get\w+(?:<[^>]+>)?\([^)]*\)", "${prefix}" + getter);
            string writer = stringUnion ? "WriteStringValue" : (nestedUnion ? "WriteObjectValue<" : "WriteCollectionOfObjectValues<") + untyped + ">";
            source = Regex.Replace(source, @"writer\.Write\w+(?:<[^>]+>)?\(""" + Regex.Escape(wire) + @""", " + Regex.Escape(property) + @"\);",
                "writer." + writer + "(\"" + wire + "\", " + property + ");");
            if (stringUnion)
                source = Regex.Replace(source, @"(?m)^[ \t]*(?:public|private) string\? (?:" + Regex.Escape(property) + "|_explicitNullValue" + Regex.Escape(property) + @")\b[^\r\n]*", match =>
                    NullableAnnotationsEnabled(source, match.Index) ? match.Value : "#nullable enable\n" + match.Value + "\n#nullable restore");
        }
        return source;
    }

    private static bool NullableAnnotationsEnabled(string source, int position)
    {
        MatchCollection directives = Regex.Matches(source[..position], @"(?m)^[ \t]*#nullable[ \t]+(?<state>enable|disable|restore)(?:[ \t]+(?<target>annotations|warnings))?");
        for (int i = directives.Count - 1; i >= 0; i--)
            if (directives[i].Groups["target"].Value != "warnings")
                return directives[i].Groups["state"].Value == "enable";
        return false;
    }
}
