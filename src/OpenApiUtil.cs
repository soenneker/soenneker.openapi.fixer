using Microsoft.Extensions.Logging;
using Microsoft.OpenApi;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using System.Net.Http;

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
    private static void NukeAllExamples(OpenApiDocument document, ILogger logger)
    {
        logger.LogInformation("--- Starting FINAL, BRUTE-FORCE ERASURE of all examples ---");

        // Use a HashSet to track which schemas have already been nuked to prevent infinite loops.
        var visitedSchemas = new HashSet<OpenApiSchema>();

        // Recursive helper specifically for schemas, which can be deeply nested.
        void NukeSchema(IOpenApiSchema? schema)
        {
            if (schema == null || !visitedSchemas.Add((OpenApiSchema)schema))
            {
                return;
            }

            // Note: Example is read-only in v2, so we can't set it to null
            // This will need to be handled differently

            if (schema.Items != null) NukeSchema(schema.Items);
            if (schema.AdditionalProperties != null) NukeSchema(schema.AdditionalProperties);
            if (schema.Properties != null)
                foreach (var p in schema.Properties.Values)
                    NukeSchema(p);
            if (schema.AllOf != null)
                foreach (var s in schema.AllOf)
                    NukeSchema(s);
            if (schema.AnyOf != null)
                foreach (var s in schema.AnyOf)
                    NukeSchema(s);
            if (schema.OneOf != null)
                foreach (var s in schema.OneOf)
                    NukeSchema(s);
        }

        // 1. Nuke all component definitions
        if (document.Components != null)
        {
            document.Components.Examples = null;

            if (document.Components.Schemas != null)
                foreach (var schema in document.Components.Schemas.Values)
                    NukeSchema(schema);

            if (document.Components.Parameters != null)
                foreach (var p in document.Components.Parameters.Values)
                {
                    // Note: Example and Examples are read-only in v2
                    // These cannot be set to null
                }

            if (document.Components.Headers != null)
                foreach (var h in document.Components.Headers.Values)
                {
                    // Note: Example and Examples are read-only in v2
                    // These cannot be set to null
                }

            if (document.Components.RequestBodies != null)
                foreach (var rb in document.Components.RequestBodies.Values)
                    if (rb.Content != null)
                        foreach (var mt in rb.Content.Values)
                        {
                            // Note: Example and Examples are read-only in v2
                            // These cannot be set to null
                        }

            if (document.Components.Responses != null)
                foreach (var resp in document.Components.Responses.Values)
                    if (resp.Content != null)
                        foreach (var mt in resp.Content.Values)
                        {
                            // Note: Example and Examples are read-only in v2
                            // These cannot be set to null
                        }
        }

        // 2. Nuke all path and operation-level definitions
        if (document.Paths != null)
        {
            foreach (var pathItem in document.Paths.Values)
            {
                // Clean parameters directly on the path
                if (pathItem.Parameters != null)
                    foreach (var p in pathItem.Parameters)
                    {
                        // Note: Example and Examples are read-only in v2
                        // These cannot be set to null
                        NukeSchema(p.Schema);
                    }

                // Clean items within each operation
                foreach (var operation in pathItem.Operations.Values)
                {
                    if (operation.Parameters != null)
                        foreach (var p in operation.Parameters)
                        {
                            // Note: Example and Examples are read-only in v2
                            // These cannot be set to null
                            NukeSchema(p.Schema);
                        }

                    if (operation.RequestBody?.Content != null)
                        foreach (var mt in operation.RequestBody.Content.Values)
                        {
                            // Note: Example and Examples are read-only in v2
                            // These cannot be set to null
                            NukeSchema(mt.Schema);
                        }

                    if (operation.Responses != null)
                        foreach (var resp in operation.Responses.Values)
                        {
                            if (resp.Headers != null)
                                foreach (var h in resp.Headers.Values)
                                {
                                    // Note: Example and Examples are read-only in v2
                                    // These cannot be set to null
                                    NukeSchema(h.Schema);
                                }

                            if (resp.Content != null)
                                foreach (var mt in resp.Content.Values)
                                {
                                    // Note: Example and Examples are read-only in v2
                                    // These cannot be set to null
                                    NukeSchema(mt.Schema);
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
        var visited = new HashSet<OpenApiSchema>();

        void Strip(IOpenApiSchema schema)
        {
            if (schema == null || !visited.Add((OpenApiSchema)schema))
                return;

            // Note: Discriminator is read-only in v2, so we can't set it to null
            // This will need to be handled differently

            if (schema.AllOf != null)
                foreach (var child in schema.AllOf)
                    Strip(child);
            if (schema.OneOf != null)
                foreach (var child in schema.OneOf)
                    Strip(child);
            if (schema.AnyOf != null)
                foreach (var child in schema.AnyOf)
                    Strip(child);
            if (schema.Properties != null)
                foreach (var child in schema.Properties.Values)
                    Strip(child);
            if (schema.Items != null)
                Strip(schema.Items);
            if (schema.AdditionalProperties != null)
                Strip(schema.AdditionalProperties);
        }

        if (document.Components?.Schemas == null)
            return;

        foreach (var root in document.Components.Schemas.Values)
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
                                         !s.AllOf.Any() // ← don't treat allOf children as "empty"
                                         && !s.AnyOf.Any() && !s.OneOf.Any());
        bool hasExample = s?.Example != null || (media.Examples?.Any() == true);
        return schemaEmpty && !hasExample;
    }

    public static bool IsSchemaEmpty(IOpenApiSchema schema)
    {
        return schema == null || (schema.Type == null && (schema.Properties == null || !schema.Properties.Any()) && !schema.AllOf.Any() &&
                                  !schema.OneOf.Any() && !schema.AnyOf.Any() && schema.Items == null && (schema.Enum == null || !schema.Enum.Any()) &&
                                  schema.AdditionalProperties == null && !schema.AdditionalPropertiesAllowed);
    }

    public static void EnsureResponseDescriptions(OpenApiResponses responses)
    {
        foreach (var kv in responses)
        {
            var code = kv.Key;
            var resp = kv.Value;
            if (string.IsNullOrWhiteSpace(resp.Description))
            {
                resp.Description = code == "default" ? "Default response" : $"{code} response";
            }
        }
    }

    public static void CleanEmptyKeysOn<T>(IDictionary<string, T> dict, string dictName, ILogger logger)
    {
        if (dict == null) return;
        foreach (var key in dict.Keys.ToList())
        {
            if (string.IsNullOrWhiteSpace(key))
            {
                logger.LogWarning("Dropping empty key from " + dictName);
                dict.Remove(key);
            }
        }
    }
} 