using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Text.Json.Nodes;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static string BuildGeneratedUnionStructuralCheck(JsonObject schema, string value, Func<JsonNode?, JsonObject?> resolve)
        => BuildGeneratedUnionStructuralChecks([schema], value, resolve) + "[0]";

    private static string BuildGeneratedUnionStructuralChecks(IReadOnlyList<JsonObject> schemas, string value, Func<JsonNode?, JsonObject?> resolve)
    {
        const string ns = "global::Microsoft.Kiota.Abstractions.Serialization.";
        int maximumSchemas = Math.Min(8192, 512 * schemas.Count);
        var names = new Dictionary<JsonObject, string>(ReferenceEqualityComparer.Instance);
        var pending = new Queue<JsonObject>();
        var methods = new StringBuilder();
        string Quote(string text) => JsonValue.Create(text)!.ToJsonString();
        string Call(JsonNode? candidate, string argument)
        {
            if (candidate is JsonValue boolean && boolean.TryGetValue<bool>(out bool allowed)) return allowed ? "(bool?)true" : "(bool?)false";
            JsonObject? target = candidate is JsonObject { Count: > 1 } withSiblings && withSiblings.ContainsKey("$ref")
                ? withSiblings : resolve(candidate);
            if (target == null) return "(bool?)null";
            if (!names.TryGetValue(target, out string? name))
            {
                if (names.Count >= maximumSchemas) return "(bool?)null";
                name = "MatchSchema" + names.Count;
                names.Add(target, name);
                pending.Enqueue(target);
            }
            return name + "(" + argument + ", depth + 1)";
        }
        string Type(string type) => type switch
        {
            "null" => "node == null || node is " + ns + "UntypedNull",
            "object" => "node is " + ns + "UntypedObject",
            "array" => "node is " + ns + "UntypedArray",
            "string" => "node is " + ns + "UntypedString",
            "boolean" => "node is " + ns + "UntypedBoolean",
            "number" => "node is " + ns + "UntypedInteger or " + ns + "UntypedLong or " + ns + "UntypedFloat or " + ns + "UntypedDouble or " + ns + "UntypedDecimal",
            "integer" => "Number(node) is decimal integer ? integer % 1 == 0 : IsNumber(node) ? (bool?)null : false",
            _ => "(bool?)null"
        };
        string Literal(JsonNode? literal)
        {
            if (literal == null) return "(node == null || node is " + ns + "UntypedNull)";
            if (literal is not JsonValue scalar) return "(bool?)null";
            if (scalar.TryGetValue<string>(out string? text)) return "(node is " + ns + "UntypedString text && " + Quote(text) + ".Equals(text.GetValue(), global::System.StringComparison.Ordinal))";
            if (scalar.TryGetValue<bool>(out bool flag)) return "(node is " + ns + "UntypedBoolean flag && flag.GetValue() == " + (flag ? "true" : "false") + ")";
            if (scalar.TryGetValue<decimal>(out decimal number)) return "(Number(node) is decimal number ? number == " + number.ToString(CultureInfo.InvariantCulture) + "m : IsNumber(node) ? (bool?)null : false)";
            return "(bool?)null";
        }
        // Queue each schema once. References become calls, not recursively expanded expressions.
        string[] roots = schemas.Select(schema => Call(schema, value).Replace("depth + 1", "0", StringComparison.Ordinal)).ToArray();
        while (pending.TryDequeue(out JsonObject? current))
        {
            var code = new StringBuilder();
            code.Append("bool? ").Append(names[current]).Append('(').Append(ns).Append("UntypedNode? node, int depth) {\n")
                .Append("if (depth > 64 || --remaining < 0) return null;\nbool? result = true;\n");
            if (current.Any(p => p.Value is JsonArray { Count: > 256 } or JsonObject { Count: > 256 }))
            {
                methods.Append("bool? ").Append(names[current]).Append('(').Append(ns).Append("UntypedNode? node, int depth) => null;\n");
                continue;
            }
            void Check(string expression)
            {
                if (expression == "(bool?)null") code.Append("result = null;\n");
                else code.Append("{ result &= (").Append(expression).Append("); if (result == false) return false; }\n");
            }
            if (current["nullable"]?.ToJsonString() == "true") code.Append("if (node == null || node is ").Append(ns).Append("UntypedNull) return true;\n");
            foreach ((string keyword, _) in current)
                if (keyword is not ("type" or "nullable" or "required" or "properties" or "enum" or "const" or "allOf" or "anyOf" or "oneOf" or "not" or "additionalProperties" or "items" or "minItems" or "maxItems" or "minLength" or "maxLength" or "minimum" or "maximum" or "description" or "title" or "example" or "examples" or "default" or "readOnly" or "writeOnly" or "deprecated" or "discriminator" or "format" or "$schema" or "$id" or "$ref") && !keyword.StartsWith("x-", StringComparison.Ordinal))
                    code.Append("result = null;\n");
            if (current["$ref"] is JsonNode reference)
                Check(Call(resolve(new JsonObject { ["$ref"] = reference.DeepClone() }), "node"));
            if (current["type"] is JsonValue kind) Check(Type(kind.GetValue<string>()));
            else if (current["type"] is JsonArray types)
                Check("Any(" + string.Join(",", types.Select(t => "(bool?)(" + Type(t!.GetValue<string>()) + ")")) + ")");
            if (current.ContainsKey("const") || current.ContainsKey("x-client-original-const"))
                Check(Literal(current.ContainsKey("const") ? current["const"] : current["x-client-original-const"]));
            if (current["enum"] is JsonArray values)
            {
                code.Append("bool? enumMatch = false;\n");
                foreach (JsonNode? literal in values)
                    code.Append("{ enumMatch |= (").Append(Literal(literal)).Append("); }\n");
                Check("enumMatch");
            }
            if (current["properties"] is JsonObject || current["required"] is JsonArray || current.ContainsKey("additionalProperties"))
            {
                code.Append("if (node is ").Append(ns).Append("UntypedObject obj) { var fields = obj.GetValue();\n");
                if (current["required"] is JsonArray required)
                    foreach (JsonNode? field in required) Check("fields.ContainsKey(" + Quote(field!.GetValue<string>()) + ")");
                JsonObject? properties = current["properties"] as JsonObject;
                if (properties != null)
                    foreach ((string field, JsonNode? fieldSchema) in properties)
                        Check("!fields.ContainsKey(" + Quote(field) + ") ? (bool?)true : " + Call(fieldSchema, "fields[" + Quote(field) + "]"));
                // Pattern keys can make an apparent additional field valid. Until pattern matching
                // is supported, this constraint is unknown rather than a reason to reject the branch.
                if (current["additionalProperties"] is JsonNode additional && current["patternProperties"] == null)
                {
                    code.Append("foreach (var field in fields) {\n");
                    if (properties?.Count > 0)
                        code.Append("if (").Append(string.Join(" || ", properties.Select(p => "field.Key == " + Quote(p.Key)))).Append(") continue;\n");
                    Check(Call(additional, "field.Value"));
                    code.Append("}\n");
                }
                code.Append("}\n");
            }
            foreach (string keyword in new[] { "allOf", "anyOf", "oneOf" })
                if (current[keyword] is JsonArray branches)
                {
                    string calls = string.Join(",", branches.Select(b => Call(b, "node")));
                    Check((keyword == "allOf" ? "All" : keyword == "anyOf" ? "Any" : "One") + "(" + calls + ")");
                }
            if (current["not"] is JsonNode excluded) Check("!(" + Call(excluded, "node") + ")");
            if (current["items"] is JsonNode items)
            {
                code.Append("if (node is ").Append(ns).Append("UntypedArray itemsArray) foreach (var item in itemsArray.GetValue()) {\n");
                Check(Call(items, "item"));
                code.Append("}\n");
            }
            foreach (string keyword in new[] { "minItems", "maxItems", "minLength", "maxLength", "minimum", "maximum" })
                if (current[keyword] is JsonValue bound && bound.TryGetValue<decimal>(out decimal limit))
                {
                    string comparison = keyword.StartsWith("min", StringComparison.Ordinal) ? ">=" : "<=";
                    string number = limit.ToString(CultureInfo.InvariantCulture) + "m";
                    if (keyword.EndsWith("Length", StringComparison.Ordinal))
                        Check("node is not " + ns + "UntypedString text ? (bool?)true : text.GetValue() is string textValue ? StringLength(textValue) " + comparison + " " + number + " : null");
                    else if (keyword.EndsWith("Items", StringComparison.Ordinal))
                        Check("node is not " + ns + "UntypedArray array || global::System.Linq.Enumerable.Count(array.GetValue()) " + comparison + " " + number);
                    else Check("Number(node) is decimal number ? number " + comparison + " " + number + " : IsNumber(node) ? (bool?)null : true");
                }
            code.Append("return result;\n}\n");
            methods.Append(code);
        }
        const string helpers = """
            bool IsNumber(global::Microsoft.Kiota.Abstractions.Serialization.UntypedNode? node) => node is global::Microsoft.Kiota.Abstractions.Serialization.UntypedInteger or global::Microsoft.Kiota.Abstractions.Serialization.UntypedLong or global::Microsoft.Kiota.Abstractions.Serialization.UntypedFloat or global::Microsoft.Kiota.Abstractions.Serialization.UntypedDouble or global::Microsoft.Kiota.Abstractions.Serialization.UntypedDecimal;
            int StringLength(string text) { int count = 0; for (int i = 0; i < text.Length; i++, count++) if (char.IsHighSurrogate(text[i]) && i + 1 < text.Length && char.IsLowSurrogate(text[i + 1])) i++; return count; }
            bool? All(params bool?[] values) { bool? result = true; foreach (var item in values) result &= item; return result; }
            bool? Any(params bool?[] values) { bool? result = false; foreach (var item in values) result |= item; return result; }
            bool? One(params bool?[] values) { int matches = 0; bool unknown = false; foreach (var item in values) { if (item == true) matches++; if (item == null) unknown = true; } return matches > 1 ? false : unknown ? null : matches == 1; }
            decimal? Number(global::Microsoft.Kiota.Abstractions.Serialization.UntypedNode? node) {
                try { return node switch {
                    global::Microsoft.Kiota.Abstractions.Serialization.UntypedInteger n => n.GetValue(),
                    global::Microsoft.Kiota.Abstractions.Serialization.UntypedLong n => n.GetValue(),
                    global::Microsoft.Kiota.Abstractions.Serialization.UntypedFloat n => (decimal)n.GetValue(),
                    global::Microsoft.Kiota.Abstractions.Serialization.UntypedDouble n => (decimal)n.GetValue(),
                    global::Microsoft.Kiota.Abstractions.Serialization.UntypedDecimal n => n.GetValue(), _ => null }; }
                catch (global::System.OverflowException) { return null; }
            }
            """;
        // Emit only helpers referenced by this graph, avoiding unused local-function warnings.
        string methodSource = methods.ToString();
        string[] helperLines = helpers.Split('\n');
        string[] helperNames = ["IsNumber", "StringLength", "All", "Any", "One"];
        var usedHelpers = new StringBuilder();
        for (int i = 0; i < helperNames.Length; i++)
            if (methodSource.Contains(helperNames[i] + "(", StringComparison.Ordinal)) usedHelpers.AppendLine(helperLines[i]);
        if (methodSource.Contains("Number(", StringComparison.Ordinal)) usedHelpers.AppendJoin('\n', helperLines.Skip(helperNames.Length));
        var results = new StringBuilder("var matches = new bool?[").Append(roots.Length).Append("];\n");
        for (int i = 0; i < roots.Length; i++)
            results.Append("remaining = 10000; matches[").Append(i).Append("] = ").Append(roots[i]).Append(";\n");
        // Each candidate retains its own work budget, while shared referenced schemas are emitted once.
        return "((global::System.Func<bool?[]>)(() => { int remaining = 10000;\n" + usedHelpers + "\n" + methodSource + results + "return matches; }))()";
    }
}
