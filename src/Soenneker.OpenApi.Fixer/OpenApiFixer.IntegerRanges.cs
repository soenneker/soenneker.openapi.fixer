using System;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static string NormalizeIntegerRanges(string json)
    {
        JsonNode root = JsonNode.Parse(json)!;
        bool changed = OpenApiJsonSchemaWalker.Visit(root, (schema, _) =>
        {
            bool integer = schema["type"]?.ToJsonString() == "\"integer\"" || schema["type"] is JsonArray types &&
                types.Any(t => t?.ToJsonString() == "\"integer\"") && types.All(t => t?.ToJsonString() is "\"integer\"" or "\"null\"");
            if (!integer || schema["format"]?.ToString() is not (null or "int32" or "int64")) return false;
            decimal[] evidence = new[] { "minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum", "default", "example" }
                .Select(key => schema[key]).Concat(schema["enum"] is JsonArray values ? values : [])
                .OfType<JsonValue>().Where(value => value.TryGetValue<decimal>(out decimal numericEvidence)).Select(value => value.GetValue<decimal>()).ToArray();
            if (!evidence.Any(value => value > int.MaxValue || value < int.MinValue)) return false;
            if (evidence.All(value => value <= long.MaxValue && value >= long.MinValue))
            {
                if (schema["format"]?.ToString() == "int64") return false;
                if (schema["format"] != null) schema["x-client-original-format"] ??= schema["format"]!.DeepClone();
                schema["format"] = "int64";
                return true;
            }
            // Decimal exactly represents the integers in this range; multipleOf retains integer
            // validation when the generator-facing type becomes number. Do not round evidence.
            decimal multiple = 1;
            if (schema["multipleOf"] is JsonValue step)
            {
                if (!step.TryGetValue(out decimal numerator) || numerator <= 0) return false;
                decimal denominator = 1;
                try
                {
                    while (numerator != decimal.Truncate(numerator)) { numerator *= 10; denominator *= 10; }
                    decimal a = numerator, b = denominator;
                    while (b != 0) { decimal remainder = a % b; a = b; b = remainder; }
                    multiple = numerator / a;
                }
                catch (OverflowException) { return false; }
                schema["x-client-original-multipleOf"] ??= step.DeepClone();
            }
            if (schema["format"] != null) schema["x-client-original-format"] ??= schema["format"]!.DeepClone();
            schema["x-client-original-type"] ??= schema["type"]!.DeepClone();
            if (schema["type"] is JsonArray nullableTypes)
                for (int index = 0; index < nullableTypes.Count; index++)
                {
                    if (nullableTypes[index]?.ToJsonString() == "\"integer\"") nullableTypes[index] = "number";
                }
            else schema["type"] = "number";
            schema["format"] = "decimal";
            schema["multipleOf"] = multiple;
            return true;
        });
        return changed ? root.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) : json;
    }
}
