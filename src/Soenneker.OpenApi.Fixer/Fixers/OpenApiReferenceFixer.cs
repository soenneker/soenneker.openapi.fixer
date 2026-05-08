using Microsoft.Extensions.Logging;
using Microsoft.OpenApi;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Threading;
using Soenneker.OpenApi.Fixer.Fixers.Abstract;

namespace Soenneker.OpenApi.Fixer.Fixers;

/// <summary>
/// Provides functionality to manage and fix OpenAPI schema references, including replacing, validating, and scrubbing broken references.
/// </summary>
public sealed class OpenApiReferenceFixer : IOpenApiReferenceFixer
{
    private readonly ILogger<OpenApiReferenceFixer> _logger;

    public OpenApiReferenceFixer(ILogger<OpenApiReferenceFixer> logger)
    {
        _logger = logger;
    }

    public void ReplaceAllRefs(OpenApiDocument document, string oldKey, string newKey)
    {
        UpdateAllReferences(document, new Dictionary<string, string>
        {
            [oldKey] = newKey
        });
    }

    /// <inheritdoc />
    public void UpdateAllReferences(OpenApiDocument doc, Dictionary<string, string> mapping)
    {
        if (mapping.Count == 0)
            return;

        var visited = new HashSet<IOpenApiSchema>();
        var replacementCount = 0;

        bool TryCreateReplacement(IOpenApiSchema? schema, out IOpenApiSchema replacement)
        {
            replacement = null!;

            if (schema is not OpenApiSchemaReference schemaRef)
                return false;

            string? referenceId = schemaRef.Reference.Id;
            if (!string.IsNullOrWhiteSpace(referenceId) && mapping.TryGetValue(referenceId, out string? newKey))
            {
                replacement = new OpenApiSchemaReference(newKey);
                return true;
            }

            string? referenceV3 = schemaRef.Reference.ReferenceV3;
            const string schemaPrefix = "#/components/schemas/";

            if (!string.IsNullOrWhiteSpace(referenceV3) && referenceV3.StartsWith(schemaPrefix, StringComparison.Ordinal) &&
                mapping.TryGetValue(referenceV3[schemaPrefix.Length..], out newKey))
            {
                replacement = new OpenApiSchemaReference(newKey);
                return true;
            }

            return false;
        }

        void PatchSchema(IOpenApiSchema? schema)
        {
            if (schema == null || !visited.Add(schema))
                return;

            if (schema.Properties != null)
            {
                foreach (KeyValuePair<string, IOpenApiSchema> kvp in schema.Properties.ToList())
                {
                    if (TryCreateReplacement(kvp.Value, out IOpenApiSchema replacement))
                    {
                        schema.Properties[kvp.Key] = replacement;
                        replacementCount++;
                    }
                    else
                    {
                        PatchSchema(kvp.Value);
                    }
                }
            }

            if (schema is not OpenApiSchema concreteSchema)
                return;

            if (concreteSchema.Items != null)
            {
                if (TryCreateReplacement(concreteSchema.Items, out IOpenApiSchema replacement))
                {
                    concreteSchema.Items = replacement;
                    replacementCount++;
                }
                else
                {
                    PatchSchema(concreteSchema.Items);
                }
            }

            if (concreteSchema.AllOf != null)
                PatchSchemaList(concreteSchema.AllOf);

            if (concreteSchema.AnyOf != null)
                PatchSchemaList(concreteSchema.AnyOf);

            if (concreteSchema.OneOf != null)
                PatchSchemaList(concreteSchema.OneOf);

            if (concreteSchema.AdditionalProperties != null)
            {
                if (TryCreateReplacement(concreteSchema.AdditionalProperties, out IOpenApiSchema replacement))
                {
                    concreteSchema.AdditionalProperties = replacement;
                    replacementCount++;
                }
                else
                {
                    PatchSchema(concreteSchema.AdditionalProperties);
                }
            }
        }

        void PatchSchemaList(IList<IOpenApiSchema> schemas)
        {
            for (var i = 0; i < schemas.Count; i++)
            {
                if (TryCreateReplacement(schemas[i], out IOpenApiSchema replacement))
                {
                    schemas[i] = replacement;
                    replacementCount++;
                }
                else
                {
                    PatchSchema(schemas[i]);
                }
            }
        }

        void PatchMediaContent(IDictionary<string, IOpenApiMediaType>? content)
        {
            if (content == null)
                return;

            foreach (IOpenApiMediaType mediaInterface in content.Values)
            {
                if (mediaInterface is not OpenApiMediaType media)
                    continue;

                if (TryCreateReplacement(media.Schema, out IOpenApiSchema replacement))
                {
                    media.Schema = replacement;
                    replacementCount++;
                }
                else
                {
                    PatchSchema(media.Schema);
                }
            }
        }

        void PatchParameter(IOpenApiParameter? parameter)
        {
            if (parameter is not OpenApiParameter concreteParam)
                return;

            if (TryCreateReplacement(concreteParam.Schema, out IOpenApiSchema replacement))
            {
                concreteParam.Schema = replacement;
                replacementCount++;
            }
            else
            {
                PatchSchema(concreteParam.Schema);
            }
        }

        if (doc.Components?.Schemas != null)
        {
            foreach (KeyValuePair<string, IOpenApiSchema> kvp in doc.Components.Schemas.ToList())
            {
                if (TryCreateReplacement(kvp.Value, out IOpenApiSchema replacement))
                {
                    doc.Components.Schemas[kvp.Key] = replacement;
                    replacementCount++;
                }
                else
                {
                    PatchSchema(kvp.Value);
                }
            }
        }

        if (doc.Paths != null)
        {
            foreach (IOpenApiPathItem pathItem in doc.Paths.Values)
            {
                if (pathItem?.Parameters != null)
                {
                    foreach (IOpenApiParameter parameter in pathItem.Parameters)
                    {
                        PatchParameter(parameter);
                    }
                }

                if (pathItem?.Operations == null)
                    continue;

                foreach (OpenApiOperation operation in pathItem.Operations.Values)
                {
                    PatchMediaContent(operation.RequestBody?.Content);

                    if (operation.Responses != null)
                    {
                        foreach (IOpenApiResponse response in operation.Responses.Values)
                        {
                            PatchMediaContent(response?.Content);
                        }
                    }

                    if (operation.Parameters != null)
                    {
                        foreach (IOpenApiParameter parameter in operation.Parameters)
                        {
                            PatchParameter(parameter);
                        }
                    }
                }
            }
        }

        if (doc.Components?.Parameters != null)
        {
            foreach (IOpenApiParameter parameter in doc.Components.Parameters.Values)
            {
                PatchParameter(parameter);
            }
        }

        if (doc.Components?.RequestBodies != null)
        {
            foreach (IOpenApiRequestBody requestBody in doc.Components.RequestBodies.Values)
            {
                PatchMediaContent(requestBody?.Content);
            }
        }

        if (doc.Components?.Responses != null)
        {
            foreach (IOpenApiResponse response in doc.Components.Responses.Values)
            {
                PatchMediaContent(response?.Content);
            }
        }

        if (doc.Components?.Headers != null)
        {
            foreach (IOpenApiHeader header in doc.Components.Headers.Values)
            {
                if (header is not OpenApiHeader concreteHeader)
                    continue;

                if (TryCreateReplacement(concreteHeader.Schema, out IOpenApiSchema replacement))
                {
                    concreteHeader.Schema = replacement;
                    replacementCount++;
                }
                else
                {
                    PatchSchema(concreteHeader.Schema);
                }
            }
        }

        _logger.LogDebug("Updated {ReferenceCount} schema references across {MappingCount} schema renames.", replacementCount, mapping.Count);
    }

    /// <inheritdoc />
    public bool IsValidSchemaReference(OpenApiSchemaReference? reference, OpenApiDocument doc)
    {
        if (reference == null)
        {
            _logger.LogTrace("IsValidSchemaReference check: Reference object is null.");
            return false;
        }

        if (string.IsNullOrWhiteSpace(reference.Reference.Id))
        {
            _logger.LogTrace("IsValidSchemaReference check: Reference.Id is null or whitespace.");
            return false;
        }

        OpenApiComponents? comps = doc.Components;
        if (comps == null)
        {
            _logger.LogWarning("IsValidSchemaReference check failed: doc.Components is null.");
            return false;
        }

        bool keyExists = comps.Schemas?.ContainsKey(reference.Reference.Id) ?? false;

        if (!keyExists)
        {
            _logger.LogWarning("IsValidSchemaReference failed for Ref ID '{RefId}'. Key does not exist in the schemas component dictionary.",
                reference.Reference.Id);
        }

        return keyExists;
    }

    /// <inheritdoc />
    public void ScrubBrokenRefs(IDictionary<string, IOpenApiMediaType>? contentDict, OpenApiDocument doc)
    {
        if (contentDict == null)
            return;

        var visited = new HashSet<IOpenApiSchema>();

        foreach (string key in contentDict.Keys.ToList())
        {
            IOpenApiMediaType media = contentDict[key];
            IOpenApiSchema? schema = media.Schema;
            if (schema is OpenApiSchemaReference schemaRef && !IsValidSchemaReference(schemaRef, doc))
            {
                _logger.LogWarning("Found broken media-type ref @ {Key}", key);
            }

            ScrubAllRefs(schema, doc, visited);
        }
    }

    /// <inheritdoc />
    public void FixRefsPointingIntoPathsExamples(OpenApiDocument doc)
    {
        if (doc.Paths == null)
            return;

        // Ensure components/schemas exists
        doc.Components ??= new OpenApiComponents();
        doc.Components.Schemas ??= new Dictionary<string, IOpenApiSchema>();
        IDictionary<string, IOpenApiSchema>? comps = doc.Components.Schemas;

        // Create (or reuse) a generic placeholder schema
        const string placeholderName = "ExamplePayload";
        if (!comps.ContainsKey(placeholderName))
        {
            comps[placeholderName] = new OpenApiSchema
            {
                Type = JsonSchemaType.Object,
                Description = "Placeholder schema for path example refs"
            };
        }

        void FixMedia(OpenApiMediaType? media)
        {
            if (media?.Schema is OpenApiSchemaReference schemaRef)
            {
                string? refPath = schemaRef.Reference.ReferenceV3;
                if (!string.IsNullOrWhiteSpace(refPath) && refPath.StartsWith("#/paths/", StringComparison.OrdinalIgnoreCase))
                {
                    // Retarget to placeholder
                    media.Schema = new OpenApiSchemaReference(placeholderName);
                }
            }
        }

        foreach (var path in doc.Paths.Values)
        {
            if (path?.Operations == null)
                continue;
            foreach (OpenApiOperation op in path.Operations.Values)
            {
                // Request body
                if (op.RequestBody?.Content != null)
                {
                    foreach (OpenApiMediaType mt in op.RequestBody.Content.Values)
                        FixMedia(mt);
                }

                // Responses
                if (op.Responses != null)
                {
                    foreach (var resp in op.Responses.Values)
                    {
                        if (resp?.Content == null)
                            continue;
                        foreach (OpenApiMediaType mt in resp.Content.Values)
                            FixMedia(mt);
                    }
                }
            }
        }
    }

    /// <inheritdoc />
    public void ScrubAllRefs(IOpenApiSchema? schema, OpenApiDocument doc, HashSet<IOpenApiSchema> visited)
    {
        if (schema == null || !visited.Add(schema))
            return;

        if (schema is OpenApiSchemaReference schemaRef && !IsValidSchemaReference(schemaRef, doc))
        {
            // _logger.LogWarning("Found broken ref for schema {Schema}", schema.Title ?? "(no title)");
        }

        if (schema.AllOf != null)
            foreach (IOpenApiSchema? s in schema.AllOf)
                ScrubAllRefs(s, doc, visited);

        if (schema.OneOf != null)
            foreach (IOpenApiSchema? s in schema.OneOf)
                ScrubAllRefs(s, doc, visited);

        if (schema.AnyOf != null)
            foreach (IOpenApiSchema? s in schema.AnyOf)
                ScrubAllRefs(s, doc, visited);

        if (schema.Properties != null)
            foreach (IOpenApiSchema? p in schema.Properties.Values)
                ScrubAllRefs(p, doc, visited);

        if (schema.Items != null)
            ScrubAllRefs(schema.Items, doc, visited);

        if (schema.AdditionalProperties != null)
            ScrubAllRefs(schema.AdditionalProperties, doc, visited);
    }

    /// <inheritdoc />
    public void ScrubComponentRefs(OpenApiDocument doc, CancellationToken cancellationToken)
    {
        var visited = new HashSet<IOpenApiSchema>();

        void PatchSchema(IOpenApiSchema? sch)
        {
            if (sch != null)
            {
                ScrubAllRefs(sch, doc, visited);
            }
        }

        void PatchContent(IDictionary<string, IOpenApiMediaType>? content)
        {
            if (content == null)
                return;
            foreach (OpenApiMediaType media in content.Values)
            {
                PatchSchema(media.Schema);
            }
        }

        if (doc.Components == null)
            return;

        if (doc.Components.RequestBodies != null)
        {
            foreach (KeyValuePair<string, IOpenApiRequestBody> kv in doc.Components.RequestBodies)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (kv.Value?.Content != null)
                    PatchContent(kv.Value.Content);
            }
        }

        if (doc.Components.Responses != null)
        {
            foreach (KeyValuePair<string, IOpenApiResponse> kv in doc.Components.Responses)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (kv.Value?.Content != null)
                    PatchContent(kv.Value.Content);
            }
        }

        if (doc.Components?.Parameters != null)
        {
            foreach (KeyValuePair<string, IOpenApiParameter> kv in doc.Components.Parameters)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (kv.Value != null && kv.Value.Schema != null)
                    PatchSchema(kv.Value.Schema);
            }
        }

        if (doc.Components?.Headers != null)
        {
            foreach (KeyValuePair<string, IOpenApiHeader> kv in doc.Components.Headers)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (kv.Value != null && kv.Value.Schema != null)
                    PatchSchema(kv.Value.Schema);
            }
        }
    }
}
