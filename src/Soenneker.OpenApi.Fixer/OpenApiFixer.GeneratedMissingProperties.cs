using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static string RepairGeneratedMissingProperties(string source, JsonObject? schemas, Dictionary<string, string> classes, Dictionary<string, string> newEnums)
    {
        const string dictionary = @"return new Dictionary<string, Action<IParseNode>>\s*\{";
        Match declaration = Regex.Match(source, @"public partial class (?<name>\w+)[^{]+\{");
        Match ns = Regex.Match(source, @"(?m)^namespace\s+(?<name>[\w.]+)");
        if (schemas == null || !declaration.Success || !ns.Success || declaration.Value.Contains("IComposedTypeWrapper", StringComparison.Ordinal) ||
            source.Contains("base.Serialize(writer)", StringComparison.Ordinal) || !Regex.IsMatch(source, dictionary) ||
            !source.Contains("writer.WriteAdditionalData(AdditionalData);", StringComparison.Ordinal) ||
            schemas[declaration.Groups["name"].Value] is not JsonObject schema) return source;
        string space = ns.Groups["name"].Value;
        const string untyped = "global::Microsoft.Kiota.Abstractions.Serialization.UntypedNode";
        string Quote(string value) => JsonSerializer.Serialize(value);
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
        var properties = new JsonObject();
        var visited = new HashSet<JsonObject>(ReferenceEqualityComparer.Instance);
        void Collect(JsonObject target)
        {
            if (!visited.Add(target)) return;
            if (target["allOf"] is JsonArray parents)
                foreach (JsonNode? parent in parents)
                    if (Resolve(parent) is JsonObject resolved) Collect(resolved);
            if (target["properties"] is JsonObject fields)
                foreach ((string wire, JsonNode? field) in fields) properties[wire] = field?.DeepClone();
        }
        Collect(schema);
        var existingFields = Regex.Matches(source, "\\{\\s*(?<wire>\"(?:\\\\.|[^\"\\\\])*\")\\s*,\\s*\\w+\\s*=>")
            .Select(match => JsonSerializer.Deserialize<string>(match.Groups["wire"].Value)!).ToHashSet(StringComparer.Ordinal);
        foreach (string wire in properties.Select(p => p.Key).ToArray())
            if (existingFields.Contains(wire)) properties.Remove(wire);
        if (properties.Count == 0) return source;
        string Shape(JsonObject value) => new JsonObject(value.Where(p => p.Key is not ("description" or "title" or "example" or "examples" or "default" or "deprecated") && !p.Key.StartsWith("x-", StringComparison.Ordinal))
            .Select(p => new KeyValuePair<string, JsonNode?>(p.Key, p.Value?.DeepClone()))).ToJsonString();
        (string Type, string Read, string Write, bool ValueType, bool Object) Field(JsonNode? node, string hint, int depth = 0)
        {
            JsonObject? target = Resolve(node);
            string? referenceName = (node as JsonObject)?["$ref"]?.GetValue<string>().Split('/').Last();
            string? model = referenceName == null ? null : classes.Keys.FirstOrDefault(k => k == space + "." + referenceName);
            if (model == null && target != null && (target["oneOf"] ?? target["anyOf"]) is JsonArray)
            {
                string shape = Shape(target);
                model = classes.Keys.FirstOrDefault(k => k.StartsWith(space + ".", StringComparison.Ordinal) && schemas[k[(space.Length + 1)..]] is JsonObject candidate && Shape(candidate) == shape);
            }
            if (model != null)
            {
                string type = "global::" + model;
                return (type, "GetObjectValue<" + type + ">(" + type + ".CreateFromDiscriminatorValue)", "WriteObjectValue<" + type + ">", false, true);
            }
            if (target?["type"]?.ToJsonString() == "\"string\"" && target["enum"] is JsonArray values && values.All(v => v is JsonValue scalar && scalar.TryGetValue<string>(out _)))
            {
                string name = BuildSafeEnumMemberName(referenceName ?? hint);
                var used = new HashSet<string>(StringComparer.Ordinal);
                var members = new StringBuilder();
                foreach (JsonNode? value in values)
                {
                    string wire = value!.GetValue<string>();
                    string member = MakeUniqueEnumMemberName(BuildSafeEnumMemberName(wire), used);
                    members.Append("    [global::System.Runtime.Serialization.EnumMember(Value = ").Append(Quote(wire)).Append(")]\n    ").Append(member).Append(",\n");
                }
                newEnums.TryAdd(space + "." + name, "// <auto-generated/>\nnamespace " + space + ";\npublic enum " + name + "\n{\n" + members + "}\n");
                string type = "global::" + space + "." + name;
                return (type, "GetEnumValue<" + type + ">()", "WriteEnumValue<" + type + ">", true, false);
            }
            string? kind = target?["type"] is JsonValue raw && raw.TryGetValue<string>(out string? typeName) ? typeName : null;
            string? format = target?["format"]?.GetValue<string>();
            if (kind == "array" && depth == 0)
            {
                var item = Field(target!["items"], hint + "Item", depth + 1);
                string element = item.Type + (item.ValueType ? "?" : "");
                string read = item.Object ? "GetCollectionOfObjectValues<" + element + ">(" + item.Type + ".CreateFromDiscriminatorValue)" : item.Read.StartsWith("GetEnumValue", StringComparison.Ordinal)
                    ? "GetCollectionOfEnumValues<" + item.Type + ">()" : "GetCollectionOfPrimitiveValues<" + element + ">()";
                string write = item.Object ? "WriteCollectionOfObjectValues<" + element + ">" : item.Read.StartsWith("GetEnumValue", StringComparison.Ordinal)
                    ? "WriteCollectionOfEnumValues<" + item.Type + ">" : "WriteCollectionOfPrimitiveValues<" + element + ">";
                return ("global::System.Collections.Generic.List<" + element + ">", read + "?.AsList()", write, false, false);
            }
            return kind switch
            {
                "string" when format == "uuid" => ("global::System.Guid", "GetGuidValue()", "WriteGuidValue", true, false),
                "string" when format == "date-time" => ("global::System.DateTimeOffset", "GetDateTimeOffsetValue()", "WriteDateTimeOffsetValue", true, false),
                "string" => ("string", "GetStringValue()", "WriteStringValue", false, false),
                "boolean" => ("bool", "GetBoolValue()", "WriteBoolValue", true, false),
                "integer" when format == "int32" => ("int", "GetIntValue()", "WriteIntValue", true, false),
                "integer" => ("long", "GetLongValue()", "WriteLongValue", true, false),
                "number" when format == "float" => ("float", "GetFloatValue()", "WriteFloatValue", true, false),
                "number" => ("double", "GetDoubleValue()", "WriteDoubleValue", true, false),
                _ => (untyped, "GetObjectValue<" + untyped + ">(" + untyped + ".CreateFromDiscriminatorValue)", "WriteObjectValue<" + untyped + ">", false, true)
            };
        }
        var declarations = new StringBuilder("\n        // Restore fields omitted by the generator's model-depth limit.\n#nullable enable\n");
        var reads = new StringBuilder();
        var writes = new StringBuilder();
        var names = new HashSet<string>(StringComparer.Ordinal) { declaration.Groups["name"].Value, "AdditionalData", "Serialize", "GetFieldDeserializers", "CreateFromDiscriminatorValue" };
        foreach (Match member in Regex.Matches(source, @"public\s+[\w.<>?,]+\s+(?<name>\w+)\s*\{")) names.Add(member.Groups["name"].Value);
        foreach ((string wire, JsonNode? property) in properties)
        {
            string name = MakeUniqueEnumMemberName(BuildSafeEnumMemberName(wire), names);
            var field = Field(property, declaration.Groups["name"].Value + name);
            declarations.Append("        public ").Append(field.Type).Append("? ").Append(name).Append(" { get; set; }\n");
            reads.Append("                { ").Append(Quote(wire)).Append(", n => { ").Append(name).Append(" = n.").Append(field.Read).Append("; } },\n");
            writes.Append("            writer.").Append(field.Write).Append('(').Append(Quote(wire)).Append(", ").Append(name).Append(");\n");
        }
        declarations.Append("#nullable restore\n");
        source = source.Insert(declaration.Index + declaration.Length, declarations.ToString());
        source = Regex.Replace(source, dictionary, match => match.Value + "\n" + reads);
        return source.Replace("            writer.WriteAdditionalData(AdditionalData);", writes + "            writer.WriteAdditionalData(AdditionalData);", StringComparison.Ordinal);
    }
}
