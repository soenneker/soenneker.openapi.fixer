using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static string NormalizeEquivalentArrayIntersections(string json)
    {
        JsonNode root = JsonNode.Parse(json)!;
        bool changed = OpenApiJsonSchemaWalker.Visit(root, (schema, _) =>
        {
            if (schema["type"]?.ToJsonString() != "\"array\"" || schema["items"] == null || schema["allOf"] is not JsonArray { Count: > 0 } branches) return false;
            if (!branches.All(b => b is JsonObject branch && branch.All(p =>
                    p.Key is "description" or "title" or "example" or "examples" or "readOnly" or "writeOnly" ||
                    ((p.Key is "type" or "items") && JsonNode.DeepEquals(p.Value, schema[p.Key]))))) return false;
            foreach (string flag in new[] { "readOnly", "writeOnly" })
                if (branches.Any(b => b?[flag]?.ToJsonString() == "true")) schema[flag] = true;
            schema.Remove("allOf");
            return true;
        });
        return changed ? root.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) : json;
    }
}
