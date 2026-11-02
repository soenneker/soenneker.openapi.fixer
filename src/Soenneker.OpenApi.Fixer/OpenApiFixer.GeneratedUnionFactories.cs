using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static async ValueTask<JsonObject?> ReadGeneratedUnionSchemas(string root, CancellationToken cancellationToken)
    {
        string lockPath = Path.Combine(root, "kiota-lock.json");
        if (!File.Exists(lockPath)) return null;
        JsonNode? configuration = JsonNode.Parse(await File.ReadAllTextAsync(lockPath, cancellationToken));
        string? location = configuration?["descriptionLocation"]?.GetValue<string>();
        if (string.IsNullOrWhiteSpace(location)) return null;
        string path;
        if (Uri.TryCreate(location, UriKind.Absolute, out Uri? uri))
        {
            if (!uri.IsFile) return null;
            path = uri.LocalPath;
        }
        else path = Path.GetFullPath(Path.Combine(root, location));
        if (!File.Exists(path) || !Path.GetExtension(path).Equals(".json", StringComparison.OrdinalIgnoreCase)) return null;
        return JsonNode.Parse(await File.ReadAllTextAsync(path, cancellationToken))?["components"]?["schemas"] as JsonObject ?? new JsonObject();
    }

    private static string RepairGeneratedUnionFactories(string source, JsonObject? schemas)
    {
        const string marker = "// Preserve ambiguous union payloads without inventing a discriminator.";
        if (schemas == null || !source.Contains("IComposedTypeWrapper", StringComparison.Ordinal) || source.Contains(marker, StringComparison.Ordinal))
            return source;
        Match declaration = Regex.Match(source, @"public partial class (?<name>\w+)[^{]+\{");
        string name = declaration.Groups["name"].Value;
        if (!declaration.Success || !declaration.Value.Contains("IComposedTypeWrapper", StringComparison.Ordinal) || schemas[name] is not JsonObject schema ||
            (schema["oneOf"] ?? schema["anyOf"]) is not JsonArray alternatives) return source;

        Match factory = Regex.Match(source, @"public static [\w:.<>]+ CreateFromDiscriminatorValue\(IParseNode parseNode\)\s*\{[\s\S]*?return result;\s*\}");
        if (!factory.Success) return source;
        var properties = Regex.Matches(source, @"public (?<type>[\w:.<>]+)\?? (?<name>\w+) \{ get; set; \}")
            .Cast<Match>().Where(m => m.Groups["name"].Value != "AdditionalData")
            .GroupBy(m => m.Groups["name"].Value).ToDictionary(g => g.Key, g => g.First().Groups["type"].Value, StringComparer.Ordinal);
        if (properties.Count == 0 || properties.ContainsKey("UnrecognizedValue")) return source;
        var classProperties = Regex.Matches(factory.Value, @"result\.(?<property>\w+) = new [\w:.]+\(\);")
            .Cast<Match>().Select(m => m.Groups["property"].Value).Distinct(StringComparer.Ordinal).ToArray();

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

        bool ObjectShape(JsonObject target, HashSet<JsonObject> seen)
        {
            if (!seen.Add(target)) return false;
            if (target["type"]?.ToJsonString() == "\"object\"") return true;
            if (target["allOf"] is JsonArray intersections)
                return intersections.Any(shape => Resolve(shape) is JsonObject child && ObjectShape(child, new HashSet<JsonObject>(seen)));
            if ((target["anyOf"] ?? target["oneOf"]) is JsonArray shapes)
                return shapes.All(shape => Resolve(shape) is JsonObject child && ObjectShape(child, new HashSet<JsonObject>(seen)));
            return false;
        }

        var objectBranches = new Dictionary<string, JsonObject>(StringComparer.Ordinal);
        foreach (JsonNode? alternative in alternatives)
        {
            if (alternative?["$ref"] is not JsonValue reference) continue;
            string id = reference.GetValue<string>();
            if (!id.StartsWith("#/components/schemas/", StringComparison.Ordinal)) continue;
            string component = id[21..].Replace("~1", "/").Replace("~0", "~");
            JsonObject? branch = Resolve(alternative);
            if (branch == null || !ObjectShape(branch, new HashSet<JsonObject>())) continue;
            foreach ((string property, string type) in properties)
                if (type.EndsWith("." + component, StringComparison.Ordinal)) objectBranches[property] = branch;
        }

        const string untyped = "global::Microsoft.Kiota.Abstractions.Serialization.";
        string none = string.Join(" && ", properties.Keys.Select(p => "result." + p + " == null"));
        string instanceNone = string.Join(" && ", properties.Keys.Select(p => p + " == null"));
        string updatedFactory = factory.Value;
        int objectCollections = properties.Values.Count(t => t.StartsWith("List<global::", StringComparison.Ordinal));
        string readPayload = "parseNode.GetObjectValue<" + untyped + "UntypedNode>(" + untyped + "UntypedNode.CreateFromDiscriminatorValue)";
        int enumIndex = 0;
        updatedFactory = Regex.Replace(updatedFactory, @"parseNode\.GetEnumValue<(?<type>[^>]+)>\(\)", match =>
        {
            string component = match.Groups["type"].Value.Split('.').Last();
            if (Resolve(schemas[component])?["enum"] is not JsonArray { Count: > 0 } values ||
                !values.All(v => v is JsonValue value && value.TryGetValue<string>(out _))) return match.Value;
            string variable = "unionEnumValue" + enumIndex++;
            string labels = string.Join(" || ", values.Select(v => JsonValue.Create(v!.GetValue<string>())!.ToJsonString() + ".Equals(" + variable + ".GetValue(), global::System.StringComparison.OrdinalIgnoreCase)"));
            return readPayload + " is " + untyped + "UntypedString " + variable + " && (" + labels + ") && " + match.Value;
        });
        string arrayDispatch = "";
        if (objectCollections > 1)
        {
            arrayDispatch = "\n            if (" + readPayload + " is " + untyped + "UntypedArray unionArrayPayload) {\n";
            var arrayBranches = new Dictionary<string, JsonObject>(StringComparer.Ordinal);
            foreach ((string property, string type) in properties.Where(p => p.Value.StartsWith("List<global::", StringComparison.Ordinal)))
            {
                string itemType = type[5..^1];
                JsonObject? candidate = alternatives.Select(Resolve).FirstOrDefault(branch =>
                    branch?["type"]?.ToJsonString() == "\"array\"" && branch["items"]?["$ref"] is JsonValue reference &&
                    reference.TryGetValue(out string? path) && path.StartsWith("#/components/schemas/", StringComparison.Ordinal) &&
                    itemType.EndsWith("." + path[21..].Replace("~1", "/").Replace("~0", "~"), StringComparison.Ordinal));
                if (candidate != null) arrayBranches[property] = candidate;
            }
            if (arrayBranches.Count == alternatives.Count && arrayBranches.Count == objectCollections &&
                arrayBranches.Values.Distinct(ReferenceEqualityComparer.Instance).Count() == arrayBranches.Count)
            {
                arrayDispatch += "var arrayMatches = " + BuildGeneratedUnionStructuralChecks(arrayBranches.Values.ToArray(), "unionArrayPayload", Resolve) + ";\n";
                int arrayIndex = 0;
                foreach (string property in arrayBranches.Keys)
                {
                    string itemType = properties[property][5..^1];
                    string unique = string.Join(" && ", Enumerable.Range(0, arrayBranches.Count).Where(i => i != arrayIndex).Select(i => "arrayMatches[" + i + "] == false"));
                    arrayDispatch += "if (arrayMatches[" + arrayIndex++ + "] == true && " + unique + ") { result." + property +
                        " = global::System.Linq.Enumerable.ToList(parseNode.GetCollectionOfObjectValues<" + itemType + ">(" + itemType +
                        ".CreateFromDiscriminatorValue)!); return result; }\n";
                }
            }
            arrayDispatch += "result.UnrecognizedValue = unionArrayPayload; return result; }\n";
        }

        updatedFactory = Regex.Replace(updatedFactory, @"parseNode\.GetCollectionOf(?<kind>Primitive|Object)Values<(?<type>[^>]+)>", match =>
        {
            string type = match.Groups["type"].Value;
            string? shape = type switch
            {
                "string" or "String" => "UntypedString",
                "bool" or "Boolean" => "UntypedBoolean",
                _ => null
            };
            if (match.Groups["kind"].Value == "Object" && type.LastIndexOf('.') is int dot && dot >= 0 &&
                Resolve(schemas[type[(dot + 1)..]]) is JsonObject target &&
                BuildGeneratedUnionStructuralCheck(target, "item", Resolve) is string itemCheck)
                return readPayload + " is " + untyped + "UntypedArray && global::System.Linq.Enumerable.All(((" + untyped + "UntypedArray)" + readPayload +
                    ").GetValue(), item => (" + itemCheck + ") == true) && " + match.Value;
            if (shape == null) return match.Value;
            return readPayload + " is " + untyped + "UntypedArray && global::System.Linq.Enumerable.All(((" + untyped + "UntypedArray)" + readPayload +
                ").GetValue(), item => item is " + untyped + shape + ") && " + match.Value;
        });
        // Kiota eagerly initializes every anyOf object, so overlapping enum fields are deserialized
        // into the wrong model. Select only a provable branch; retain ambiguous payloads losslessly.
        updatedFactory = Regex.Replace(updatedFactory, @"result\.(?<property>\w+) = new (?<type>[\w:.]+)\(\);", match =>
            properties.ContainsKey(match.Groups["property"].Value) ? "" : match.Value);

        var conditions = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (string property in objectBranches.Keys)
            conditions[property] = "unionBranchMatches![" + conditions.Count + "]";
        // An unresolved object branch is still a possible competitor, never a rejected branch.
        foreach (string property in classProperties)
            if (!conditions.ContainsKey(property)) conditions[property] = "(bool?)null";
        string dispatch = "\n            var unionPayload = parseNode.GetObjectValue<" + untyped + "UntypedNode>(" + untyped + "UntypedNode.CreateFromDiscriminatorValue);\n";
        if (objectBranches.Count > 0)
            dispatch += "            var unionBranchMatches = unionPayload is " + untyped + "UntypedObject ? " +
                BuildGeneratedUnionStructuralChecks(objectBranches.Values.ToArray(), "unionPayload", Resolve) + " : null;\n";
        // Separate booleans avoid duplicate pattern-variable declarations across candidate expressions.
        int index = 0;
        var candidates = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach ((string property, string condition) in conditions)
        {
            string candidate = "matchesUnionBranch" + index++;
            candidates[property] = candidate;
            // Read independently evaluated candidates from the shared schema graph.
            dispatch += "            bool? " + candidate + " = unionPayload is " + untyped + "UntypedObject ? " + condition + " : (bool?)false;\n";
        }
        if (candidates.Count > 0) dispatch += "            int matchingUnionBranches = 0;\n";
        foreach (string candidate in candidates.Values) dispatch += "            if (" + candidate + " != false) matchingUnionBranches++;\n";
        if (candidates.Count > 0) dispatch += "            bool noSelectedUnionBranch = " + none + ";\n";
        foreach ((string property, string candidate) in candidates)
        {
            dispatch += "            if (noSelectedUnionBranch && " + candidate + " == true && matchingUnionBranches == 1) result." + property + " = global::Soenneker.OpenApi.Generated.OpenApiNullTracking.Capture(new " + properties[property] + "(), parseNode);\n";
        }
        dispatch += "            if (" + none + ") result.UnrecognizedValue = unionPayload;\n";
        int returnIndex = updatedFactory.LastIndexOf("return result;", StringComparison.Ordinal);
        updatedFactory = updatedFactory.Insert(returnIndex, dispatch);
        if (arrayDispatch.Length > 0)
            updatedFactory = Regex.Replace(updatedFactory, @"(var result = new [\w:.]+\(\);)", match => match.Value + arrayDispatch);
        source = source.Remove(factory.Index, factory.Length).Insert(factory.Index, updatedFactory);
        string fallback = "\n        " + marker + "\n        /// <summary>The complete payload when no unique typed union branch can be selected.</summary>\n        public " + untyped + "UntypedNode UnrecognizedValue { get; set; } = null!;\n";
        if (!source.Contains("public IDictionary<string, object> AdditionalData", StringComparison.Ordinal))
        {
            fallback += "        public global::System.Collections.Generic.IDictionary<string, object> AdditionalData { get; set; } = new global::System.Collections.Generic.Dictionary<string, object>();\n";
            string originalDeclaration = declaration.Value;
            string newDeclaration = originalDeclaration.Insert(originalDeclaration.LastIndexOf('{'), ", global::Microsoft.Kiota.Abstractions.Serialization.IAdditionalDataHolder ");
            source = source.Remove(declaration.Index, declaration.Length).Insert(declaration.Index, newDeclaration + fallback);
        }
        else source = source.Insert(declaration.Index + declaration.Length, fallback);

        // A composed wrapper has no surrounding JSON object. AdditionalData must be written by its
        // selected object branch, never after that branch's closing brace.
        source = Regex.Replace(source, @"(?<call>writer\.WriteObjectValue<[^>]+>\(null, (?<property>\w+)\);)", match =>
        {
            string property = match.Groups["property"].Value;
            return "if (" + property + " is " + untyped + "IAdditionalDataHolder unionData) foreach (var entry in AdditionalData) unionData.AdditionalData[entry.Key] = entry.Value;\n                " + match.Value;
        });
        // Raw object keys are already preserved verbatim; prevent the parser from inferring
        // dates in AdditionalData and overwriting those original string values.
        source = Regex.Replace(source, @"(public virtual IDictionary<string, Action<IParseNode>> GetFieldDeserializers\(\)\s*\{)",
            "$1\n            if (UnrecognizedValue is " + untyped + "UntypedObject rawFields) { var fields = new global::System.Collections.Generic.Dictionary<string, global::System.Action<IParseNode>>(); foreach (var entry in rawFields.GetValue()) fields[entry.Key] = _ => { }; return fields; }\n");
        source = source.Replace("writer.WriteAdditionalData(AdditionalData);", "", StringComparison.Ordinal);
        string serializePrefix = "\n            if (global::System.Object.ReferenceEquals(writer, null)) throw new global::System.ArgumentNullException(nameof(writer));\n            if (" + instanceNone + ") { if (UnrecognizedValue is " + untyped + "UntypedObject rawObject) writer.WriteObjectValue(null, new global::Soenneker.OpenApi.Generated.OpenApiUnionAdditionalData(AdditionalData, rawObject)); else if (UnrecognizedValue != null) writer.WriteObjectValue(null, UnrecognizedValue); else if (AdditionalData.Count > 0) writer.WriteObjectValue(null, new global::Soenneker.OpenApi.Generated.OpenApiUnionAdditionalData(AdditionalData)); else writer.WriteNullValue(null); return; }\n";
        if (classProperties.Length > 0) serializePrefix += "            int selectedUnionBranches = 0;\n";
        foreach (string property in properties.Keys.Where(_ => classProperties.Length > 0)) serializePrefix += "            if (" + property + " != null) selectedUnionBranches++;\n";
        foreach (string property in classProperties)
        {
            serializePrefix += "            if (" + property + " != null && selectedUnionBranches == 1) { if (" + property + " is " + untyped +
                "IAdditionalDataHolder selectedData) foreach (var entry in AdditionalData) selectedData.AdditionalData[entry.Key] = entry.Value; writer.WriteObjectValue(null, " + property + "); return; }\n";
        }
        source = Regex.Replace(source, @"(public virtual void Serialize\(ISerializationWriter writer\)\s*\{)", "$1" + serializePrefix);
        return source;
    }

    private static async ValueTask WriteGeneratedUnionSupport(string root, CancellationToken cancellationToken)
    {
        const string source = """
            // <auto-generated/>
            #nullable enable
            namespace Soenneker.OpenApi.Generated;
            internal sealed class OpenApiUnionAdditionalData : global::Microsoft.Kiota.Abstractions.Serialization.IParsable
            {
                private readonly global::System.Collections.Generic.IDictionary<string, object> _values;
                public OpenApiUnionAdditionalData(global::System.Collections.Generic.IDictionary<string, object> values,
                    global::Microsoft.Kiota.Abstractions.Serialization.UntypedObject? original = null)
                {
                    _values = new global::System.Collections.Generic.Dictionary<string, object>();
                    if (original != null) foreach (var entry in original.GetValue()) _values[entry.Key] = entry.Value;
                    foreach (var entry in values) _values[entry.Key] = entry.Value;
                }
                public global::System.Collections.Generic.IDictionary<string, global::System.Action<global::Microsoft.Kiota.Abstractions.Serialization.IParseNode>> GetFieldDeserializers() =>
                    new global::System.Collections.Generic.Dictionary<string, global::System.Action<global::Microsoft.Kiota.Abstractions.Serialization.IParseNode>>();
                public void Serialize(global::Microsoft.Kiota.Abstractions.Serialization.ISerializationWriter writer) => writer.WriteAdditionalData(_values);
            }
            """;
        await File.WriteAllTextAsync(Path.Combine(root, "OpenApiUnionAdditionalData.cs"), source, cancellationToken);
        await File.WriteAllTextAsync(Path.Combine(root, "IOpenApiExplicitNulls.cs"), """
            // <auto-generated/>
            namespace Soenneker.OpenApi.Generated;
            /// <summary>Records explicitly present null fields before the parser skips them.</summary>
            public interface IOpenApiExplicitNulls
            {
                /// <summary>Records nulls in the supplied object, including inherited fields.</summary>
                void CaptureExplicitNulls(global::Microsoft.Kiota.Abstractions.Serialization.IParseNode parseNode);
            }
            """, cancellationToken);
        await File.WriteAllTextAsync(Path.Combine(root, "OpenApiNullTracking.cs"), """
            // <auto-generated/>
            namespace Soenneker.OpenApi.Generated;
            internal static class OpenApiNullTracking
            {
                internal static T Capture<T>(T model, global::Microsoft.Kiota.Abstractions.Serialization.IParseNode node)
                {
                    if (model is IOpenApiExplicitNulls target) target.CaptureExplicitNulls(node);
                    return model;
                }
            }
            """, cancellationToken);
    }
}
