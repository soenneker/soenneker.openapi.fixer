using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Linq;
using Microsoft.OpenApi;

namespace Soenneker.OpenApi.Fixer;

public static class OpenApiUtil
{

    /// <summary>
    /// Performs a definitive and final erasure of all 'example' and 'examples' properties
    /// from the entire OpenAPI document. This method directly targets and iterates through
    /// every known collection that can contain examples, nullifying them at the source.
    /// This is the most direct and reliable approach for this complex document.
    /// </summary>
    /// <param name="document">The OpenAPI document to sanitize.</param>
    /// <param name="logger">The logger instance for logging operations.</param>
    private static void NukeAllExamples(OpenApiDocument document, ILogger logger)
    {
        logger.LogInformation("--- Starting FINAL, BRUTE-FORCE ERASURE of all examples ---");

        // Use a HashSet to track which schemas have already been nuked to prevent infinite loops.
        var visitedSchemas = new HashSet<IOpenApiSchema>();

        // Recursive helper specifically for schemas, which can be deeply nested.
        void NukeSchema(IOpenApiSchema? schema)
        {
            if (schema == null || !visitedSchemas.Add(schema))
            {
                return;
            }

            // In v2.3, we need to cast to concrete type to modify read-only properties
            if (schema is OpenApiSchema concreteSchema)
            {
                concreteSchema.Example = null;
            }

            if (schema.Items != null) NukeSchema(schema.Items);
            if (schema.AdditionalProperties != null) NukeSchema(schema.AdditionalProperties);
            if (schema.Properties != null)
                foreach (IOpenApiSchema p in schema.Properties.Values)
                    NukeSchema(p);
            if (schema.AllOf != null)
                foreach (IOpenApiSchema s in schema.AllOf)
                    NukeSchema(s);
            if (schema.AnyOf != null)
                foreach (IOpenApiSchema s in schema.AnyOf)
                    NukeSchema(s);
            if (schema.OneOf != null)
                foreach (IOpenApiSchema s in schema.OneOf)
                    NukeSchema(s);
        }

        // 1. Nuke all component definitions
        if (document.Components != null)
        {
            document.Components.Examples = null;

            if (document.Components.Schemas != null)
                foreach (var schema in document.Components.Schemas.Values)
                    if (schema != null)
                        NukeSchema(schema);

            if (document.Components.Parameters != null)
                foreach (IOpenApiParameter p in document.Components.Parameters.Values)
                {
                    // In v2.3, we need to cast to concrete type to modify read-only properties
                    if (p is OpenApiParameter concreteParam)
                    {
                        concreteParam.Example = null;
                        concreteParam.Examples = null;
                    }
                }

            if (document.Components.Headers != null)
                foreach (IOpenApiHeader h in document.Components.Headers.Values)
                {
                    // In v2.3, we need to cast to concrete type to modify read-only properties
                    if (h is OpenApiHeader concreteHeader)
                    {
                        concreteHeader.Example = null;
                        concreteHeader.Examples = null;
                    }
                }

            if (document.Components.RequestBodies != null)
                foreach (IOpenApiRequestBody rb in document.Components.RequestBodies.Values)
                    if (rb.Content != null)
                        foreach (OpenApiMediaType mt in rb.Content.Values)
                        {
                            // In v2.3, we need to cast to concrete type to modify read-only properties
                            if (mt is OpenApiMediaType concreteMediaType)
                            {
                                concreteMediaType.Example = null;
                                concreteMediaType.Examples = null;
                            }
                        }

            if (document.Components.Responses != null)
                foreach (IOpenApiResponse resp in document.Components.Responses.Values)
                    if (resp.Content != null)
                        foreach (OpenApiMediaType mt in resp.Content.Values)
                        {
                            // In v2.3, we need to cast to concrete type to modify read-only properties
                            if (mt is OpenApiMediaType concreteMediaType)
                            {
                                concreteMediaType.Example = null;
                                concreteMediaType.Examples = null;
                            }
                        }
        }

        // 2. Nuke all path and operation-level definitions
        if (document.Paths != null)
        {
            foreach (IOpenApiPathItem pathItem in document.Paths.Values)
            {
                // Clean parameters directly on the path
                if (pathItem.Parameters != null)
                    foreach (IOpenApiParameter p in pathItem.Parameters)
                    {
                        // In v2.3, we need to cast to concrete type to modify read-only properties
                        if (p is OpenApiParameter concreteParam)
                        {
                            concreteParam.Example = null;
                            concreteParam.Examples = null;
                        }
                        if (p.Schema != null)
                            NukeSchema(p.Schema);
                    }

                // Clean items within each operation
                if (pathItem.Operations != null)
                {
                    foreach (OpenApiOperation operation in pathItem.Operations.Values)
                {
                    if (operation.Parameters != null)
                        foreach (IOpenApiParameter p in operation.Parameters)
                        {
                            // In v2.3, we need to cast to concrete type to modify read-only properties
                            if (p is OpenApiParameter concreteParam)
                            {
                                concreteParam.Example = null;
                                concreteParam.Examples = null;
                            }
                            if (p.Schema != null)
                                NukeSchema(p.Schema);
                        }

                    if (operation.RequestBody?.Content != null)
                        foreach (OpenApiMediaType mt in operation.RequestBody.Content.Values)
                        {
                            // In v2.3, we need to cast to concrete type to modify read-only properties
                            if (mt is OpenApiMediaType concreteMediaType)
                            {
                                concreteMediaType.Example = null;
                                concreteMediaType.Examples = null;
                            }
                            if (mt.Schema != null)
                                NukeSchema(mt.Schema);
                        }

                    if (operation.Responses != null)
                        foreach (IOpenApiResponse resp in operation.Responses.Values)
                        {
                            if (resp.Headers != null)
                                foreach (IOpenApiHeader h in resp.Headers.Values)
                                {
                                    // In v2.3, we need to cast to concrete type to modify read-only properties
                                    if (h is OpenApiHeader concreteHeader)
                                    {
                                        concreteHeader.Example = null;
                                        concreteHeader.Examples = null;
                                    }
                                    if (h.Schema != null)
                                        NukeSchema(h.Schema);
                                }

                            if (resp.Content != null)
                                foreach (OpenApiMediaType mt in resp.Content.Values)
                                {
                                    // In v2.3, we need to cast to concrete type to modify read-only properties
                                    if (mt is OpenApiMediaType concreteMediaType)
                                    {
                                        concreteMediaType.Example = null;
                                        concreteMediaType.Examples = null;
                                    }
                                    if (mt.Schema != null)
                                        NukeSchema(mt.Schema);
                                }
                        }
                }
                }
            }
        }

        logger.LogInformation("--- EXAMPLE ERASURE COMPLETE ---");
    }

    /// <summary>
    /// Strips every Discriminator (and its mappings) from all component schemas
    /// to prevent Kiota from attempting polymorphic inheritance on them.
    /// </summary>
    public static void StripAllDiscriminators(OpenApiDocument document, ILogger logger)
    {
        var visited = new HashSet<IOpenApiSchema>();

        void Strip(IOpenApiSchema schema)
        {
            if (schema == null || !visited.Add(schema))
                return;

            // In v2.3, we need to cast to concrete type to modify read-only properties
            if (schema is OpenApiSchema concreteSchema)
            {
                concreteSchema.Discriminator = null;
            }

            if (schema.AllOf != null)
                foreach (IOpenApiSchema child in schema.AllOf)
                    Strip(child);
            if (schema.OneOf != null)
                foreach (IOpenApiSchema child in schema.OneOf)
                    Strip(child);
            if (schema.AnyOf != null)
                foreach (IOpenApiSchema child in schema.AnyOf)
                    Strip(child);
            if (schema.Properties != null)
                foreach (IOpenApiSchema child in schema.Properties.Values)
                    Strip(child);
            if (schema.Items != null)
                Strip(schema.Items);
            if (schema.AdditionalProperties != null)
                Strip(schema.AdditionalProperties);
        }

        if (document.Components?.Schemas == null)
            return;

        foreach (IOpenApiSchema root in document.Components.Schemas.Values)
            Strip(root);
    }

    public static string NormalizeMediaType(string mediaType)
    {
        if (string.IsNullOrWhiteSpace(mediaType))
            return "application/json";
        string baseType = mediaType.Split(';')[0].Trim();
        if (baseType.Contains('*') || !baseType.Contains('/'))
            return "application/json";
        return baseType;
    }

    public static bool IsMediaEmpty(OpenApiMediaType media)
    {
        IOpenApiSchema? s = media.Schema;
        bool schemaEmpty = s == null || (s.Type == null && (s.Properties == null || !s.Properties.Any()) && s.Items == null &&
                                         (s.AllOf == null || !s.AllOf.Any()) // ← don't treat allOf children as "empty"
                                         && (s.AnyOf == null || !s.AnyOf.Any()) && (s.OneOf == null || !s.OneOf.Any()));
        bool hasExample = s?.Example != null || (media.Examples?.Any() == true);
        return schemaEmpty && !hasExample;
    }

    public static bool IsSchemaEmpty(IOpenApiSchema schema)
    {
        return schema == null || (schema.Type == null && (schema.Properties == null || !schema.Properties.Any()) && (schema.AllOf == null || !schema.AllOf.Any()) &&
                                  (schema.OneOf == null || !schema.OneOf.Any()) && (schema.AnyOf == null || !schema.AnyOf.Any()) && schema.Items == null && (schema.Enum == null || !schema.Enum.Any()) &&
                                  schema.AdditionalProperties == null && !schema.AdditionalPropertiesAllowed);
    }

    public static void EnsureResponseDescriptions(OpenApiResponses responses)
    {
        foreach (KeyValuePair<string, IOpenApiResponse> kv in responses)
        {
            string code = kv.Key;
            IOpenApiResponse resp = kv.Value;
            if (string.IsNullOrWhiteSpace(resp.Description))
            {
                resp.Description = code == "default" ? "Default response" : $"{code} response";
            }
        }
    }

    public static void CleanEmptyKeysOn<T>(IDictionary<string, T> dict, string dictName, ILogger logger)
    {
        if (dict == null) return;
        foreach (string key in dict.Keys.ToList())
        {
            if (string.IsNullOrWhiteSpace(key))
            {
                logger.LogWarning("Dropping empty key from " + dictName);
                dict.Remove(key);
            }
        }
    }
} 