using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static string ProjectComplexFormRequests(string json)
    {
        JsonNode root = JsonNode.Parse(json)!;
        bool changed = false;
        bool ObjectForm(JsonNode? node, HashSet<JsonObject> active)
        {
            JsonObject? schema = ResolveFormNode(root, node);
            if (schema == null || !active.Add(schema)) return false;
            try
            {
                if (schema["type"]?.ToJsonString() == "\"object\"") return true;
                foreach (string keyword in new[] { "oneOf", "anyOf" })
                    if (schema[keyword] is JsonArray { Count: > 0 } alternatives)
                        return alternatives.All(branch => ObjectForm(branch, active));
                return schema["allOf"] is JsonArray { Count: > 0 } all && all.Any(branch => ObjectForm(branch, active));
            }
            finally { active.Remove(schema); }
        }
        foreach (string section in new[] { "paths", "webhooks" })
        {
            if (root[section] is not JsonObject paths) continue;
            foreach (JsonObject item in paths.Select(p => p.Value).OfType<JsonObject>())
                foreach (string verb in new[] { "post", "put", "patch", "delete", "get", "head", "options", "trace" })
                {
                    if (item[verb] is not JsonObject operation || ResolveFormNode(root, operation["requestBody"]) is not JsonObject body ||
                        body["content"] is not JsonObject content ||
                        content["application/x-www-form-urlencoded"] is not JsonObject media ||
                        ResolveFormNode(root, media["schema"]) is not JsonObject schema) continue;
                    if (content["application/json"] is JsonObject preferredJson &&
                        (JsonNode.DeepEquals(preferredJson["schema"], media["schema"]) ||
                         ResolveFormNode(root, preferredJson["schema"]) is JsonObject preferredSchema && JsonNode.DeepEquals(preferredSchema, schema)))
                    {
                        // Kiota selects JSON when both representations describe the same data.
                        JsonObject jsonBody = body.DeepClone().AsObject();
                        jsonBody["x-client-original-alternative-form-content"] = media.DeepClone();
                        jsonBody["content"]!.AsObject().Remove("application/x-www-form-urlencoded");
                        operation["requestBody"] = jsonBody;
                        changed = true;
                        continue;
                    }
                    if (schema["type"]?.ToJsonString() == "\"string\"" && media["encoding"] == null &&
                        content.All(p => p.Key is "application/x-www-form-urlencoded" or "text/plain" &&
                            p.Value is JsonObject alternative && ResolveFormNode(root, alternative["schema"])?["type"]?.ToJsonString() == "\"string\""))
                    {
                        // A string here is already encoded form data, not an object to encode again.
                        JsonObject rawBody = body.DeepClone().AsObject();
                        if (content.Count > 1) rawBody["x-client-original-form-alternatives"] = content.DeepClone();
                        rawBody["x-client-original-form-content"] = media.DeepClone();
                        rawBody["content"] = new JsonObject { ["text/plain"] = media.DeepClone() };
                        operation["requestBody"] = rawBody;
                        changed = true;
                        continue;
                    }
                    if (!ObjectForm(schema, new HashSet<JsonObject>(ReferenceEqualityComparer.Instance))) continue;
                    bool complex = schema["type"]?.ToString() != "object" || schema["properties"] is JsonObject fields &&
                        fields.Any(p => ResolveFormNode(root, p.Value) is JsonObject field &&
                            (field["type"] == null || field["type"]?.ToString() is "object" or "array" ||
                             field["type"] is JsonArray fieldTypes && fieldTypes.Any(t => t?.ToJsonString() is "\"object\"" or "\"array\"") ||
                             field.ContainsKey("oneOf") || field.ContainsKey("anyOf") || field.ContainsKey("allOf")));
                    if (!complex) continue;
                    bool equivalentMultipart = content.Count == 2 && content["multipart/form-data"] is JsonObject multipart &&
                        (JsonNode.DeepEquals(multipart["schema"], media["schema"]) ||
                         ResolveFormNode(root, multipart["schema"]) is JsonObject multipartSchema && JsonNode.DeepEquals(multipartSchema, schema));
                    if (content.Count != 1 && !equivalentMultipart) continue;
                    if (media["encoding"] is JsonObject encoding && encoding.Any(p => p.Value is not JsonObject rule ||
                            rule["contentType"] != null || rule["headers"] != null || rule["style"] != null &&
                            rule["style"]!.ToString() is not ("form" or "deepObject" or "spaceDelimited" or "pipeDelimited"))) continue;
                    JsonObject copy = body.DeepClone().AsObject();
                    if (equivalentMultipart) copy["x-client-original-form-alternatives"] = content.DeepClone();
                    copy["x-client-original-form-content"] = media.DeepClone();
                    JsonObject generationMedia = media.DeepClone().AsObject();
                    generationMedia.Remove("encoding");
                    copy["content"] = new JsonObject { ["application/json"] = generationMedia };
                    operation["requestBody"] = copy;
                    changed = true;
                }
        }
        return changed ? root.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) : json;
    }
}
