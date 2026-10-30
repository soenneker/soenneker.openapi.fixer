using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static string? BuildGeneratedUnionStructuralCheck(JsonObject schema, string value, Func<JsonNode?, JsonObject?> resolve)
    {
        const string prefix = "global::Microsoft.Kiota.Abstractions.Serialization.";
        var active = new HashSet<JsonObject>();
        int variable = 0;
        string? Literal(JsonNode? literal, string node)
        {
            if (literal == null) return "(" + node + " == null || " + node + " is " + prefix + "UntypedNull)";
            if (literal is not JsonValue scalar) return null;
            string name = "literal" + variable++;
            if (scalar.TryGetValue<string>(out string? text))
                return "(" + node + " is " + prefix + "UntypedString " + name + " && " + JsonValue.Create(text)!.ToJsonString() + ".Equals(" + name + ".GetValue(), global::System.StringComparison.Ordinal))";
            if (scalar.TryGetValue<bool>(out bool boolean))
                return "(" + node + " is " + prefix + "UntypedBoolean " + name + " && " + name + ".GetValue() == " + (boolean ? "true" : "false") + ")";
            if (scalar.TryGetValue<decimal>(out decimal number))
            {
                string constant = number.ToString(System.Globalization.CultureInfo.InvariantCulture) + "m";
                return "(" + string.Join(" || ", new[] { "Integer", "Long", "Float", "Double", "Decimal" }.Select(kind =>
                {
                    string numeric = "number" + variable++;
                    return "(" + node + " is " + prefix + "Untyped" + kind + " " + numeric + " && " + numeric + ".GetValue() == " + (kind is "Float" or "Double" ? "(double)" : "") + constant + ")";
                })) + ")";
            }
            return null;
        }
        string? Build(JsonObject current, string node, bool exact, int depth)
        {
            if (depth > 16 || !active.Add(current)) return null;
            try
            {
                // Negating an approximation would reject valid payloads. Only negate supported rules.
                if (exact && current.Any(p => p.Key is not ("type" or "nullable" or "required" or "properties" or "enum" or "const" or "allOf" or "anyOf" or "oneOf" or "not" or "additionalProperties" or "description" or "title" or "example" or "examples" or "default" or "readOnly" or "writeOnly" or "deprecated") && !p.Key.StartsWith("x-", StringComparison.Ordinal))) return null;
                var checks = new List<string>();
                string? TypeCheck(string type) => type switch
                {
                    "null" => "(" + node + " == null || " + node + " is " + prefix + "UntypedNull)",
                    "string" => node + " is " + prefix + "UntypedString",
                    "boolean" => node + " is " + prefix + "UntypedBoolean",
                    "object" => node + " is " + prefix + "UntypedObject",
                    "array" => node + " is " + prefix + "UntypedArray",
                    "number" => node + " is " + prefix + "UntypedInteger or " + prefix + "UntypedLong or " + prefix + "UntypedFloat or " + prefix + "UntypedDouble or " + prefix + "UntypedDecimal",
                    "integer" => "(" + node + " is " + prefix + "UntypedInteger or " + prefix + "UntypedLong || (" + node + " is " + prefix + "UntypedDouble integral" + variable + " && integral" + variable++ + ".GetValue() % 1 == 0) || (" + node + " is " + prefix + "UntypedDecimal integral" + variable + " && integral" + variable++ + ".GetValue() % 1 == 0) || (" + node + " is " + prefix + "UntypedFloat integral" + variable + " && integral" + variable++ + ".GetValue() % 1 == 0))",
                    _ => null
                };
                if (current["type"] != null)
                {
                    var types = current["type"] is JsonArray array ? array.Select(t => t!.GetValue<string>()) : new[] { current["type"]!.GetValue<string>() };
                    var shapes = types.Select(TypeCheck).ToList();
                    if (shapes.Any(s => s == null)) return null;
                    if (current["nullable"]?.ToJsonString() == "true") shapes.Add(TypeCheck("null"));
                    checks.Add("(" + string.Join(" || ", shapes.Select(s => "(" + s + ")")) + ")");
                }
                if (current.ContainsKey("const") || current.ContainsKey("x-client-original-const"))
                {
                    string? literal = Literal(current.ContainsKey("const") ? current["const"] : current["x-client-original-const"], node);
                    if (literal == null) return null;
                    checks.Add(literal);
                }
                if (current["enum"] is JsonArray values)
                {
                    var literals = values.Select(v => Literal(v, node)).ToList();
                    if (literals.Any(l => l == null)) return null;
                    checks.Add(literals.Count == 0 ? "false" : "(" + string.Join(" || ", literals) + ")");
                }
                var members = new List<string>();
                string objectName = "shapeObject" + variable++;
                string map = "shapeMembers" + variable++;
                if (current["required"] is JsonArray required)
                    members.AddRange(required.Select(r => map + ".ContainsKey(" + JsonValue.Create(r!.GetValue<string>())!.ToJsonString() + ")"));
                if (current["properties"] is JsonObject fields)
                    foreach ((string name, JsonNode? field) in fields)
                    {
                        JsonObject? resolved = resolve(field);
                        string key = JsonValue.Create(name)!.ToJsonString();
                        string? test = resolved == null ? null : Build(resolved, map + "[" + key + "]", exact, depth + 1);
                        if (test == null) { if (exact) return null; continue; }
                        members.Add("(!" + map + ".ContainsKey(" + key + ") || (" + test + "))");
                    }
                if (current["additionalProperties"]?.ToJsonString() == "false")
                {
                    if (current["patternProperties"] != null) return null;
                    string keys = current["properties"] is JsonObject declared && declared.Count > 0
                        ? string.Join(" || ", declared.Select(p => "key == " + JsonValue.Create(p.Key)!.ToJsonString())) : "false";
                    members.Add("global::System.Linq.Enumerable.All(" + map + ".Keys, key => " + keys + ")");
                }
                else if (exact && current["additionalProperties"] is JsonObject) return null;
                if (members.Count > 0) checks.Add("(" + node + " is not " + prefix + "UntypedObject " + objectName + " || (" + objectName + ".GetValue() is var " + map + " && (" + string.Join(" && ", members) + ")))");
                foreach (string keyword in new[] { "allOf", "anyOf", "oneOf" })
                    if (current[keyword] is JsonArray alternatives)
                    {
                        if (alternatives.Count == 0)
                        {
                            checks.Add(keyword == "allOf" ? "true" : "false");
                            continue;
                        }
                        var branches = alternatives.Select(b => resolve(b) is JsonObject child ? Build(child, node, exact || keyword == "oneOf", depth + 1) : null).ToList();
                        if (branches.Any(b => b == null)) return null;
                        checks.Add(keyword == "oneOf" ? "(" + string.Join(" + ", branches.Select(b => "((" + b + ") ? 1 : 0)")) + " == 1)" : "(" + string.Join(keyword == "allOf" ? " && " : " || ", branches.Select(b => "(" + b + ")")) + ")");
                    }
                if (current["not"] != null)
                {
                    string? excluded = resolve(current["not"]) is JsonObject child ? Build(child, node, true, depth + 1) : null;
                    if (excluded == null) return null;
                    checks.Add("!(" + excluded + ")");
                }
                return checks.Count == 0 ? "true" : string.Join(" && ", checks);
            }
            finally { active.Remove(current); }
        }
        return Build(schema, value, false, 0);
    }
}
