using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static string NormalizeNullableObjectUnions(string json)
    {
        JsonNode root = JsonNode.Parse(json)!;
        bool version30 = root["openapi"]?.GetValue<string>().StartsWith("3.0", StringComparison.Ordinal) == true;
        static bool Annotation(string name) => name is "title" or "description" or "$comment" or "example" or "examples" or
            "default" or "deprecated" or "readOnly" or "writeOnly" or "externalDocs" or "xml" || name.StartsWith("x-", StringComparison.Ordinal);
        JsonObject? Resolve(JsonNode? node)
        {
            var seen = new HashSet<string>(StringComparer.Ordinal);
            while (node is JsonObject obj && obj["$ref"] is JsonValue reference)
            {
                if (!obj.All(p => p.Key == "$ref" || Annotation(p.Key)) || !reference.TryGetValue(out string? id) ||
                    !id.StartsWith("#/components/schemas/", StringComparison.Ordinal) || !seen.Add(id)) return null;
                node = root["components"]?["schemas"]?[id[21..].Replace("~1", "/").Replace("~0", "~")];
            }
            return node as JsonObject;
        }
        bool changed = OpenApiJsonSchemaWalker.Visit(root, (schema, _) =>
        {
            string keyword = schema["oneOf"] != null ? "oneOf" : "anyOf";
            if (schema[keyword] is not JsonArray { Count: 2 } alternatives ||
                !schema.All(p => p.Key == keyword || Annotation(p.Key))) return false;
            JsonNode? nonNull = null;
            int nulls = 0;
            foreach (JsonNode? branch in alternatives)
            {
                if (branch is JsonObject { Count: 1 } nullBranch && nullBranch["type"]?.ToJsonString() == "\"null\"") nulls++;
                else nonNull = branch;
            }
            if (nulls != 1 || Resolve(nonNull) is not JsonObject target || target["type"]?.ToJsonString() != "\"object\"" ||
                !target.All(p => Annotation(p.Key) || p.Key is "type" or "properties" or "required" or "additionalProperties" or
                    "patternProperties" or "propertyNames" or "minProperties" or "maxProperties")) return false;
            JsonObject copy = target.DeepClone().AsObject();
            // Reference annotations apply to the nullable model as well as annotations on the union itself.
            if (nonNull is JsonObject referenceSchema)
                foreach ((string key, JsonNode? value) in referenceSchema.Where(p => Annotation(p.Key))) copy[key] = value?.DeepClone();
            foreach ((string key, JsonNode? value) in schema.Where(p => Annotation(p.Key))) copy[key] = value?.DeepClone();
            copy["x-client-original-nullable-union"] = schema.DeepClone();
            if (version30) copy["nullable"] = true;
            else copy["type"] = new JsonArray("object", "null");
            schema.Clear();
            foreach ((string key, JsonNode? value) in copy) schema[key] = value?.DeepClone();
            return true;
        });
        return changed ? root.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) : json;
    }
}
