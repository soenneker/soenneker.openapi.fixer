using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using Microsoft.OpenApi;

namespace Soenneker.OpenApi.Fixer;

internal static class OpenApiJsonCompositionNormalizer
{
    private const JsonSchemaType AllTypes = JsonSchemaType.Null | JsonSchemaType.Boolean | JsonSchemaType.Integer |
                                           JsonSchemaType.Number | JsonSchemaType.String | JsonSchemaType.Array | JsonSchemaType.Object;

    internal static string Normalize(string json)
    {
        for (int pass = 0; pass < 16; pass++)
        {
            string normalized = NormalizePass(json);
            if (normalized == json) return json;
            json = normalized;
        }
        throw new InvalidOperationException("Schema composition normalization did not converge.");
    }

    private static string NormalizePass(string json)
    {
        JsonNode root = JsonNode.Parse(json)!;
        JsonObject? components = root["components"]?["schemas"] as JsonObject;
        var active = new HashSet<JsonObject>(ReferenceEqualityComparer.Instance);
        var completed = new HashSet<JsonObject>(ReferenceEqualityComparer.Instance);
        bool changed = false;

        JsonObject? Resolve(JsonNode? node)
        {
            var refs = new HashSet<string>(StringComparer.Ordinal);
            while (node is JsonObject reference && reference.Count == 1 && reference["$ref"] is JsonValue value)
            {
                string id = value.GetValue<string>();
                if (!id.StartsWith("#/components/schemas/", StringComparison.Ordinal) || !refs.Add(id)) return null;
                node = components?[id[21..].Replace("~1", "/").Replace("~0", "~")];
            }
            if (node is not JsonObject result || active.Contains(result)) return null;
            Visit(result);
            return result;
        }

        JsonSchemaType UnrestrictedTypes(JsonObject schema, HashSet<JsonObject>? seen = null)
        {
            seen ??= new HashSet<JsonObject>(ReferenceEqualityComparer.Instance);
            if (!seen.Add(schema)) return 0;
            if (schema.All(p => Annotation(p.Key) || p.Key is "type" or "nullable" ||
                    (p.Key == "items" && p.Value is JsonObject { Count: 0 }))) return Types(schema);
            if (schema.All(p => Annotation(p.Key) || p.Key is "anyOf" or "type" or "nullable") && schema["anyOf"] is JsonArray union)
            {
                JsonSchemaType types = 0;
                foreach (JsonNode? branch in union)
                    if (Resolve(branch) is JsonObject resolved) types |= UnrestrictedTypes(resolved, seen);
                return types & Types(schema);
            }
            if (schema.All(p => Annotation(p.Key) || p.Key is "oneOf" or "type" or "nullable") && schema["oneOf"] is JsonArray exclusive)
            {
                JsonSchemaType types = 0;
                JsonSchemaType overlap = 0;
                foreach (JsonNode? branch in exclusive)
                {
                    if (Resolve(branch) is not JsonObject resolved) return 0;
                    JsonSchemaType branchTypes = UnrestrictedTypes(resolved, seen);
                    if (branchTypes == 0) return 0;
                    overlap |= types & branchTypes;
                    types |= branchTypes;
                }
                return types & ~overlap & Types(schema);
            }
            return 0;
        }

        JsonSchemaType AllowedTypes(JsonObject schema, HashSet<JsonObject>? seen = null)
        {
            seen ??= new HashSet<JsonObject>(ReferenceEqualityComparer.Instance);
            if (!seen.Add(schema)) return AllTypes;
            JsonSchemaType types = EffectiveTypes(schema);
            if (schema["allOf"] is JsonArray intersection)
                foreach (JsonNode? branch in intersection)
                    if (Resolve(branch) is JsonObject target) types &= AllowedTypes(target, seen);
            foreach (string key in new[] { "anyOf", "oneOf" })
                if (schema[key] is JsonArray union)
                {
                    JsonSchemaType alternatives = 0;
                    JsonSchemaType overlap = 0;
                    bool unrestricted = key == "oneOf";
                    foreach (JsonNode? branch in union)
                    {
                        JsonObject? target = Resolve(branch);
                        JsonSchemaType branchTypes = target != null ? AllowedTypes(target, seen) : AllTypes;
                        unrestricted &= target != null && UnrestrictedTypes(target) != 0;
                        overlap |= alternatives & branchTypes;
                        alternatives |= branchTypes;
                    }
                    types &= unrestricted ? alternatives & ~overlap : alternatives;
                }
            seen.Remove(schema);
            return types;
        }

        bool Allows(JsonObject broad, JsonObject narrow, HashSet<(JsonObject, JsonObject)>? seen = null)
        {
            seen ??= new HashSet<(JsonObject, JsonObject)>();
            if (!seen.Add((broad, narrow))) return false;
            JsonSchemaType accepted = UnrestrictedTypes(broad);
            JsonSchemaType required = AllowedTypes(narrow);
            if (accepted != 0 && (required & ~accepted) == 0) return true;
            if (broad["anyOf"] is JsonArray union && broad.All(p => Annotation(p.Key) || p.Key is "anyOf" or "type" or "nullable") &&
                (required & ~Types(broad)) == 0 && union.Any(b => Resolve(b) is JsonObject branch && Allows(branch, narrow, seen))) return true;
            if (narrow["allOf"] is JsonArray intersection && intersection.Any(b => Resolve(b) is JsonObject branch && Allows(broad, branch, seen))) return true;
            return required != 0 && (required & ~(JsonSchemaType.Array | JsonSchemaType.Null)) == 0 &&
                (required & ~Types(broad)) == 0 && broad.All(p => Annotation(p.Key) || p.Key is "type" or "nullable" or "items") &&
                Resolve(broad["items"]) is JsonObject broadItems && Resolve(narrow["items"]) is JsonObject narrowItems &&
                Allows(broadItems, narrowItems, seen);
        }

        void Visit(JsonObject schema)
        {
            if (completed.Contains(schema) || !active.Add(schema)) return;
            try
            {
                foreach (string keyword in new[] { "anyOf", "oneOf" })
                {
                    if (schema[keyword] is not JsonArray { Count: 1 } single ||
                        !schema.All(p => Annotation(p.Key) || p.Key == keyword) || Resolve(single[0]) is not JsonObject target) continue;
                    JsonObject singleReplacement = target.DeepClone().AsObject();
                    if (target["type"]?.ToJsonString() == "\"string\"" && target["enum"] == null) singleReplacement["x-client-inline-scalar"] = true;
                    foreach ((string key, JsonNode? value) in schema)
                        if (Annotation(key)) singleReplacement[key] = value?.DeepClone();
                    schema.Clear();
                    foreach ((string key, JsonNode? value) in singleReplacement) schema[key] = value?.DeepClone();
                    changed = true;
                }
                if (schema["enum"] is JsonArray ownValues && !ownValues.Any(v => (ValueType(v) & Types(schema)) != 0))
                {
                    MakeImpossible(schema);
                    changed = true;
                    return;
                }
                foreach (string keyword in new[] { "anyOf", "oneOf" })
                {
                    if (schema[keyword] is not JsonArray union || union.Count == 0 ||
                        !schema.All(p => Annotation(p.Key) || p.Key is "type" or "nullable" || p.Key == keyword)) continue;
                    JsonObject?[] branches = union.Select(Resolve).ToArray();
                    if (branches.Any(b => b == null || !Finite(b))) continue;
                    var values = new Dictionary<string, (JsonNode? Value, int Count)>(StringComparer.Ordinal);
                    foreach (JsonObject branch in branches!)
                    {
                        foreach (JsonNode? item in branch["enum"]!.AsArray().DistinctBy(v => v?.ToJsonString() ?? "null"))
                        {
                            if ((ValueType(item) & Types(branch) & Types(schema)) == 0) continue;
                            string key = item?.ToJsonString() ?? "null";
                            values.TryGetValue(key, out var prior);
                            values[key] = (item, prior.Count + 1);
                        }
                    }
                    schema.Remove(keyword);
                    JsonNode?[] allowed = values.Values.Where(v => keyword == "anyOf" || v.Count == 1).Select(v => v.Value?.DeepClone()).ToArray();
                    if (allowed.Length == 0) schema["not"] = new JsonObject();
                    else
                    {
                        schema["enum"] = new JsonArray(allowed);
                        InferEnumType(schema, allowed);
                    }
                    changed = true;
                }

                // An unrestricted branch already accepts constrained branches of the same type.
                // Retaining string | string-enum makes Kiota emit a propertyless object model.
                if (schema["anyOf"] is JsonArray redundantAlternatives)
                {
                    for (int index = redundantAlternatives.Count - 1; index >= 0 && redundantAlternatives.Count > 1; index--)
                    {
                        JsonObject? narrow = Resolve(redundantAlternatives[index]);
                        if (narrow == null) continue;
                        for (int other = 0; other < redundantAlternatives.Count; other++)
                        {
                            if (other == index || Resolve(redundantAlternatives[other]) is not JsonObject broad || !Allows(broad, narrow)) continue;
                            redundantAlternatives.RemoveAt(index);
                            changed = true;
                            break;
                        }
                    }
                }

                if (schema["allOf"] is not JsonArray { Count: > 0 } allOf) return;
                JsonObject?[] resolvedBranches = allOf.Select(Resolve).ToArray();
                if (resolvedBranches.Any(b => b == null)) return;
                List<JsonObject> branchesToMerge = resolvedBranches.Select(b => b!).ToList();
                JsonSchemaType intersectionTypes = branchesToMerge.Aggregate(EffectiveTypes(schema), (mask, branch) => mask & AllowedTypes(branch));
                if (intersectionTypes == 0)
                {
                    JsonObject shape = schema["properties"] is JsonObject ? schema.DeepClone().AsObject() :
                        (branchesToMerge.FirstOrDefault(b => b["properties"] is JsonObject) ?? branchesToMerge[0]).DeepClone().AsObject();
                    foreach ((string key, JsonNode? value) in schema)
                        if (Annotation(key)) shape[key] = value?.DeepClone();
                    MakeImpossible(shape);
                    schema.Clear();
                    foreach ((string key, JsonNode? value) in shape) schema[key] = value?.DeepClone();
                    changed = true;
                    return;
                }
                if ((intersectionTypes & JsonSchemaType.Null) == 0)
                    for (int index = 0; index < branchesToMerge.Count; index++)
                        if ((AllowedTypes(branchesToMerge[index]) & JsonSchemaType.Null) != 0)
                        {
                            JsonObject narrowed = WithoutNull(branchesToMerge[index]);
                            if (JsonNode.DeepEquals(narrowed, branchesToMerge[index])) continue;
                            branchesToMerge[index] = narrowed;
                            allOf[index] = narrowed.DeepClone();
                            changed = true;
                        }

                // Distribute an intersection over object alternatives instead of pretending a union is a base class.
                for (int index = 0; index < branchesToMerge.Count; index++)
                {
                    JsonObject unionSchema = branchesToMerge[index];
                    string? unionKey = unionSchema["anyOf"] is JsonArray ? "anyOf" : unionSchema["oneOf"] is JsonArray ? "oneOf" : null;
                    if (unionKey == null || unionSchema[unionKey] is not JsonArray { Count: > 0 and <= 32 } alternatives ||
                        !unionSchema.All(p => Annotation(p.Key) || p.Key == unionKey || p.Key == "type") ||
                        !alternatives.All(a => Resolve(a) is JsonObject item && Types(item) == JsonSchemaType.Object)) continue;
                    JsonObject common = schema.DeepClone().AsObject();
                    common.Remove("allOf");
                    JsonObject unionConstraints = unionSchema.DeepClone().AsObject();
                    unionConstraints.Remove(unionKey);
                    var distributed = new JsonArray();
                    foreach (JsonNode? alternative in alternatives)
                    {
                        var intersection = new JsonArray();
                        if (common.Count > 0) intersection.Add(common.DeepClone());
                        if (unionConstraints.Count > 0) intersection.Add(unionConstraints.DeepClone());
                        for (int other = 0; other < allOf.Count; other++)
                            if (other != index) intersection.Add(allOf[other]?.DeepClone());
                        intersection.Add(alternative?.DeepClone());
                        var leaf = new JsonObject { ["allOf"] = intersection };
                        Visit(leaf);
                        distributed.Add(leaf);
                    }
                    foreach (string key in schema.Select(p => p.Key).Where(key => !Annotation(key)).ToArray()) schema.Remove(key);
                    schema[unionKey] = distributed;
                    changed = true;
                    return;
                }

                bool OpenObject(JsonObject candidate) => Types(candidate) == JsonSchemaType.Object &&
                    candidate.All(p => Annotation(p.Key) || p.Key is "type" or "properties" or "required");
                bool mergeIntoObject = schema.All(p => Annotation(p.Key) || p.Key is "allOf" or "type" or "properties" or "required") &&
                    (schema["type"] == null || Types(schema) == JsonSchemaType.Object);
                if (mergeIntoObject)
                {
                    for (int index = branchesToMerge.Count - 1; index >= 0; index--)
                    {
                        JsonObject branch = branchesToMerge[index];
                        if (!OpenObject(branch)) continue;
                        schema["type"] = "object";
                        if (branch["properties"] is JsonObject properties)
                        {
                            schema["properties"] ??= new JsonObject();
                            JsonObject destination = schema["properties"]!.AsObject();
                            foreach ((string key, JsonNode? value) in properties)
                            {
                                if (destination[key] is JsonObject existing && !JsonNode.DeepEquals(existing, value))
                                {
                                    var intersection = new JsonObject { ["allOf"] = new JsonArray(existing.DeepClone(), value?.DeepClone()) };
                                    Visit(intersection);
                                    destination[key] = intersection;
                                }
                                else destination[key] = value?.DeepClone();
                            }
                        }
                        if (branch["required"] is JsonArray required)
                        {
                            schema["required"] ??= new JsonArray();
                            JsonArray destination = schema["required"]!.AsArray();
                            foreach (JsonNode? name in required)
                                if (!destination.Any(n => JsonNode.DeepEquals(n, name))) destination.Add(name?.DeepClone());
                        }
                        branchesToMerge.RemoveAt(index);
                        allOf.RemoveAt(index);
                        changed = true;
                    }
                    if (allOf.Count == 0) { schema.Remove("allOf"); return; }
                }

                // A broad type alternative contributes no constraint when another branch already requires that type.
                for (int i = branchesToMerge.Count - 1; i >= 0; i--)
                {
                    JsonObject ownConstraints = schema.DeepClone().AsObject();
                    ownConstraints.Remove("allOf");
                    if ((schema["type"] != null && Allows(branchesToMerge[i], ownConstraints)) ||
                        branchesToMerge.Where((_, index) => index != i).Any(b => Allows(branchesToMerge[i], b)))
                    {
                        branchesToMerge.RemoveAt(i);
                        allOf.RemoveAt(i);
                        changed = true;
                    }
                }
                if (allOf.Count == 0) { schema.Remove("allOf"); return; }

                bool parentIsAnnotations = schema.All(p => Annotation(p.Key) || p.Key is "allOf" or "type" or "nullable" or "enum");
                if (!parentIsAnnotations) return;
                if (schema["enum"] is JsonArray)
                {
                    JsonObject ownConstraints = schema.DeepClone().AsObject();
                    ownConstraints.Remove("allOf");
                    branchesToMerge.Add(ownConstraints);
                }
                JsonObject? replacement = null;
                if (branchesToMerge.Count == 1 && (Types(branchesToMerge[0]) & ~Types(schema)) == 0)
                    replacement = branchesToMerge[0].DeepClone().AsObject();
                else if (branchesToMerge.All(b => b.All(p => Annotation(p.Key) || p.Key is "type" or "nullable" or "enum")))
                {
                    JsonSchemaType types = branchesToMerge.Aggregate(Types(schema), (mask, branch) => mask & Types(branch));
                    JsonObject? finite = branchesToMerge.FirstOrDefault(Finite);
                    if (finite != null)
                    {
                        JsonNode?[] allowed = finite["enum"]!.AsArray().Where(value => (ValueType(value) & types) != 0 &&
                            branchesToMerge.All(b => b["enum"] is not JsonArray values || values.Any(v => JsonNode.DeepEquals(v, value))))
                            .Select(v => v?.DeepClone()).ToArray();
                        replacement = finite.DeepClone().AsObject();
                        if (allowed.Length == 0) { replacement.Remove("enum"); replacement["not"] = new JsonObject(); }
                        else
                        {
                            replacement["enum"] = new JsonArray(allowed);
                            InferEnumType(replacement, allowed);
                        }
                    }
                }
                if (replacement == null) return;
                foreach ((string key, JsonNode? value) in schema)
                    if (Annotation(key)) replacement[key] = value?.DeepClone();
                schema.Clear();
                foreach ((string key, JsonNode? value) in replacement) schema[key] = value?.DeepClone();
                changed = true;
            }
            finally { active.Remove(schema); completed.Add(schema); }
        }

        OpenApiJsonSchemaWalker.Visit(root, (schema, _) => { Visit(schema); return false; });
        return changed ? root.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) : json;
    }

    private static bool Annotation(string key) => key is "title" or "description" or "default" or "example" or "examples" or
        "deprecated" or "readOnly" or "writeOnly" or "xml" or "externalDocs" || key.StartsWith("x-", StringComparison.Ordinal);

    private static bool Finite(JsonObject schema) => schema["enum"] is JsonArray &&
        schema.All(p => Annotation(p.Key) || p.Key is "type" or "nullable" or "enum");

    private static void MakeImpossible(JsonObject schema)
    {
        // Keep the contradiction explicit instead of asking Kiota to inherit from scalar/enum types.
        schema.Remove("allOf");
        schema.Remove("anyOf");
        schema.Remove("oneOf");
        schema.Remove("enum");
        schema.Remove("x-ms-enum");
        schema["not"] = new JsonObject();
    }

    private static JsonObject WithoutNull(JsonObject schema)
    {
        JsonObject result = schema.DeepClone().AsObject();
        if (result["type"] != null) result.Remove("nullable");
        if (result["type"] is JsonArray types)
        {
            for (int index = types.Count - 1; index >= 0; index--)
                if (types[index]?.GetValue<string>() == "null") types.RemoveAt(index);
            if (types.Count == 1) result["type"] = types[0]!.DeepClone();
        }
        foreach (string key in new[] { "anyOf", "oneOf", "allOf" })
            if (result[key] is JsonArray branches)
                for (int index = 0; index < branches.Count; index++)
                    if (branches[index] is JsonObject branch) branches[index] = WithoutNull(branch);
        return result;
    }

    private static JsonSchemaType EffectiveTypes(JsonObject schema)
    {
        if (schema["not"] is JsonObject { Count: 0 }) return 0;
        JsonSchemaType types = Types(schema);
        if (schema["enum"] is JsonArray values)
            types &= values.Aggregate((JsonSchemaType)0, (mask, value) => mask | ValueType(value));
        return types;
    }

    private static void InferEnumType(JsonObject schema, JsonNode?[] values)
    {
        if (schema["type"] != null) return;
        JsonSchemaType kinds = values.Aggregate((JsonSchemaType)0, (types, value) => types | ValueType(value));
        string? type = (kinds & ~JsonSchemaType.Null) switch
        {
            JsonSchemaType.Boolean => "boolean", JsonSchemaType.String => "string", JsonSchemaType.Integer => "integer",
            JsonSchemaType.Number or (JsonSchemaType.Number | JsonSchemaType.Integer) => "number",
            JsonSchemaType.Array => "array", JsonSchemaType.Object => "object", _ => null
        };
        if (type == null) return;
        schema["type"] = type;
        if ((kinds & JsonSchemaType.Null) != 0) schema["nullable"] = true;
    }

    private static JsonSchemaType Types(JsonObject schema)
    {
        JsonSchemaType SingleType(string name) => name switch
        {
            "null" => JsonSchemaType.Null, "boolean" => JsonSchemaType.Boolean, "integer" => JsonSchemaType.Integer,
            "number" => JsonSchemaType.Number | JsonSchemaType.Integer, "string" => JsonSchemaType.String,
            "array" => JsonSchemaType.Array, "object" => JsonSchemaType.Object, _ => AllTypes
        };
        JsonSchemaType types = schema["type"] switch
        {
            JsonValue type => SingleType(type.GetValue<string>()),
            JsonArray typesArray => typesArray.Aggregate((JsonSchemaType)0, (mask, entry) => mask | SingleType(entry!.GetValue<string>())),
            _ => AllTypes
        };
        if (schema["nullable"] is JsonValue nullable && nullable.TryGetValue(out bool allowed) && allowed) types |= JsonSchemaType.Null;
        return types;
    }

    private static JsonSchemaType ValueType(JsonNode? value) => value?.GetValueKind() switch
    {
        null or JsonValueKind.Null => JsonSchemaType.Null,
        JsonValueKind.True or JsonValueKind.False => JsonSchemaType.Boolean,
        JsonValueKind.String => JsonSchemaType.String,
        JsonValueKind.Number => value!.AsValue().TryGetValue(out decimal number) && decimal.Truncate(number) == number
            ? JsonSchemaType.Integer : JsonSchemaType.Number,
        JsonValueKind.Array => JsonSchemaType.Array, JsonValueKind.Object => JsonSchemaType.Object, _ => AllTypes
    };
}
