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

    /// <inheritdoc />
    public void ReplaceAllRefs(OpenApiDocument document, string oldKey, string newKey)
    {
        var oldRef = $"#/components/schemas/{oldKey}";
        var newRef = $"#/components/schemas/{newKey}";

        var visited = new HashSet<IOpenApiSchema>();

        void Recurse(IOpenApiSchema? schema, string? context = null)
        {
            if (schema == null || !visited.Add(schema))
                return;

            if (schema is OpenApiSchemaReference schemaRef && schemaRef.Reference.ReferenceV3 == oldRef)
            {
                _logger.LogInformation("Rewriting $ref from '{OldRef}' to '{NewRef}' at {Context}", oldRef, newRef, context ?? "unknown location");
                // Note: This case is handled by the parent context that calls this method
                // The actual replacement happens at the parent level where the reference is stored
            }

            if (schema.Properties != null)
            {
                foreach (KeyValuePair<string, IOpenApiSchema> kvp in schema.Properties.ToList())
                {
                    if (kvp.Value is OpenApiSchemaReference propSchemaRef && propSchemaRef.Reference.ReferenceV3 == oldRef)
                    {
                        _logger.LogInformation("Replacing schema reference in properties from '{OldRef}' to '{NewRef}'", oldRef, newRef);
                        schema.Properties[kvp.Key] = new OpenApiSchemaReference(newKey);
                    }
                    else
                    {
                        Recurse(kvp.Value, $"{context ?? "schema"}.properties.{kvp.Key}");
                    }
                }
            }

            // For read-only properties, we need to cast to concrete type to modify them
            if (schema is OpenApiSchema concreteSchema)
            {
                if (concreteSchema.Items != null)
                {
                    if (concreteSchema.Items is OpenApiSchemaReference itemsRef && itemsRef.Reference.ReferenceV3 == oldRef)
                    {
                        _logger.LogInformation("Replacing schema reference in items from '{OldRef}' to '{NewRef}'", oldRef, newRef);
                        concreteSchema.Items = new OpenApiSchemaReference(newKey);
                    }
                    else
                    {
                        Recurse(concreteSchema.Items, $"{context ?? "schema"}.items");
                    }
                }

                if (concreteSchema.AllOf != null)
                {
                    for (int i = 0; i < concreteSchema.AllOf.Count; i++)
                    {
                        if (concreteSchema.AllOf[i] is OpenApiSchemaReference allOfRef && allOfRef.Reference.ReferenceV3 == oldRef)
                        {
                            _logger.LogInformation("Replacing schema reference in allOf[{0}] from '{OldRef}' to '{NewRef}'", i, oldRef, newRef);
                            concreteSchema.AllOf[i] = new OpenApiSchemaReference(newKey);
                        }
                        else
                        {
                            Recurse(concreteSchema.AllOf[i], $"{context ?? "schema"}.allOf[{i}]");
                        }
                    }
                }

                if (concreteSchema.AnyOf != null)
                {
                    for (int i = 0; i < concreteSchema.AnyOf.Count; i++)
                    {
                        if (concreteSchema.AnyOf[i] is OpenApiSchemaReference anyOfRef && anyOfRef.Reference.ReferenceV3 == oldRef)
                        {
                            _logger.LogInformation("Replacing schema reference in anyOf[{0}] from '{OldRef}' to '{NewRef}'", i, oldRef, newRef);
                            concreteSchema.AnyOf[i] = new OpenApiSchemaReference(newKey);
                        }
                        else
                        {
                            Recurse(concreteSchema.AnyOf[i], $"{context ?? "schema"}.anyOf[{i}]");
                        }
                    }
                }

                if (concreteSchema.OneOf != null)
                {
                    for (int i = 0; i < concreteSchema.OneOf.Count; i++)
                    {
                        if (concreteSchema.OneOf[i] is OpenApiSchemaReference oneOfRef && oneOfRef.Reference.ReferenceV3 == oldRef)
                        {
                            _logger.LogInformation("Replacing schema reference in oneOf[{0}] from '{OldRef}' to '{NewRef}'", i, oldRef, newRef);
                            concreteSchema.OneOf[i] = new OpenApiSchemaReference(newKey);
                        }
                        else
                        {
                            Recurse(concreteSchema.OneOf[i], $"{context ?? "schema"}.oneOf[{i}]");
                        }
                    }
                }

                if (concreteSchema.AdditionalProperties != null)
                {
                    if (concreteSchema.AdditionalProperties is OpenApiSchemaReference additionalPropsRef && additionalPropsRef.Reference.ReferenceV3 == oldRef)
                    {
                        _logger.LogInformation("Replacing schema reference in additionalProperties from '{OldRef}' to '{NewRef}'", oldRef, newRef);
                        concreteSchema.AdditionalProperties = new OpenApiSchemaReference(newKey);
                    }
                    else
                    {
                        Recurse(concreteSchema.AdditionalProperties, $"{context ?? "schema"}.additionalProperties");
                    }
                }
            }
        }

        // Walk through all schemas in components
        if (document.Components?.Schemas != null)
        {
            foreach ((string key, IOpenApiSchema schema) in document.Components.Schemas)
                Recurse(schema, $"components.schemas.{key}");
        }

        // Walk through all paths and operations
        if (document.Paths != null)
        {
            foreach ((string pathKey, var pathItem) in document.Paths)
            {
                if (pathItem?.Operations != null)
                {
                    foreach ((HttpMethod method, OpenApiOperation operation) in pathItem.Operations)
                    {
                        var operationContext = $"paths.{pathKey}.{method}";

                        // Check request body
                        if (operation.RequestBody?.Content != null)
                        {
                            foreach ((string mediaType, IOpenApiMediaType mediaInterface) in operation.RequestBody.Content)
                            {
                                if (mediaInterface is not OpenApiMediaType media)
                                    continue;

                                if (media.Schema is OpenApiSchemaReference mediaSchemaRef && mediaSchemaRef.Reference.ReferenceV3 == oldRef)
                                {
                                    _logger.LogInformation("Replacing schema reference in request body from '{OldRef}' to '{NewRef}'", oldRef, newRef);
                                    media.Schema = new OpenApiSchemaReference(newKey);
                                }
                                else
                                {
                                    Recurse(media.Schema, $"{operationContext}.requestBody.{mediaType}");
                                }
                            }
                        }

                        // Check responses
                        if (operation.Responses != null)
                        {
                            foreach ((string responseCode, var response) in operation.Responses)
                            {
                                if (response?.Content != null)
                                {
                                    foreach ((string mediaType, IOpenApiMediaType mediaInterface) in response.Content)
                                    {
                                        if (mediaInterface is not OpenApiMediaType media)
                                            continue;

                                        if (media.Schema is OpenApiSchemaReference responseSchemaRef && responseSchemaRef.Reference.ReferenceV3 == oldRef)
                                        {
                                            _logger.LogInformation("Replacing schema reference in response from '{OldRef}' to '{NewRef}'", oldRef, newRef);
                                            media.Schema = new OpenApiSchemaReference(newKey);
                                        }
                                        else
                                        {
                                            Recurse(media.Schema, $"{operationContext}.responses[{responseCode}].{mediaType}");
                                        }
                                    }
                                }
                            }
                        }

                        // Check parameters
                        if (operation.Parameters != null)
                        {
                            foreach (var param in operation.Parameters)
                            {
                                if (param is OpenApiParameter concreteParam)
                                {
                                    if (concreteParam.Schema is OpenApiSchemaReference paramSchemaRef && paramSchemaRef.Reference.ReferenceV3 == oldRef)
                                    {
                                        _logger.LogInformation("Replacing schema reference in parameter from '{OldRef}' to '{NewRef}'", oldRef, newRef);
                                        concreteParam.Schema = new OpenApiSchemaReference(newKey);
                                    }
                                    else
                                    {
                                        Recurse(concreteParam.Schema, $"{operationContext}.parameters[{param?.Name}]");
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        // Check components for other reference types
        if (document.Components != null)
        {
            // Check parameters
            if (document.Components.Parameters != null)
            {
                foreach (IOpenApiParameter param in document.Components.Parameters.Values)
                {
                    if (param is OpenApiParameter concreteParam)
                    {
                        if (concreteParam.Schema is OpenApiSchemaReference paramSchemaRef && paramSchemaRef.Reference.ReferenceV3 == oldRef)
                        {
                            _logger.LogInformation("Replacing schema reference in component parameter from '{OldRef}' to '{NewRef}'", oldRef, newRef);
                            concreteParam.Schema = new OpenApiSchemaReference(newKey);
                        }
                        else
                        {
                            Recurse(concreteParam.Schema, "components.parameters");
                        }
                    }
                }
            }

            // Check request bodies
            if (document.Components.RequestBodies != null)
            {
                foreach (var requestBody in document.Components.RequestBodies.Values)
                {
                    if (requestBody?.Content != null)
                    {
                        foreach (var mediaInterface in requestBody.Content.Values)
                        {
                            if (mediaInterface is not OpenApiMediaType media)
                                continue;

                            if (media.Schema is OpenApiSchemaReference requestBodySchemaRef && requestBodySchemaRef.Reference.ReferenceV3 == oldRef)
                            {
                                _logger.LogInformation("Replacing schema reference in component request body from '{OldRef}' to '{NewRef}'", oldRef, newRef);
                                media.Schema = new OpenApiSchemaReference(newKey);
                            }
                            else
                            {
                                Recurse(media.Schema, "components.requestBodies");
                            }
                        }
                    }
                }
            }

            // Check responses
            if (document.Components.Responses != null)
            {
                foreach (var response in document.Components.Responses.Values)
                {
                    if (response?.Content != null)
                    {
                        foreach (var mediaInterface in response.Content.Values)
                        {
                            if (mediaInterface is not OpenApiMediaType media)
                                continue;

                            if (media.Schema is OpenApiSchemaReference responseSchemaRef && responseSchemaRef.Reference.ReferenceV3 == oldRef)
                            {
                                _logger.LogInformation("Replacing schema reference in component response from '{OldRef}' to '{NewRef}'", oldRef, newRef);
                                media.Schema = new OpenApiSchemaReference(newKey);
                            }
                            else
                            {
                                Recurse(media.Schema, "components.responses");
                            }
                        }
                    }
                }
            }

            // Check headers
            if (document.Components.Headers != null)
            {
                foreach (IOpenApiHeader header in document.Components.Headers.Values)
                {
                    if (header is OpenApiHeader concreteHeader)
                    {
                        if (concreteHeader.Schema is OpenApiSchemaReference headerSchemaRef && headerSchemaRef.Reference.ReferenceV3 == oldRef)
                        {
                            _logger.LogInformation("Replacing schema reference in component header from '{OldRef}' to '{NewRef}'", oldRef, newRef);
                            concreteHeader.Schema = new OpenApiSchemaReference(newKey);
                        }
                        else
                        {
                            Recurse(concreteHeader.Schema, "components.headers");
                        }
                    }
                }
            }
        }
    }

    /// <inheritdoc />
    public void UpdateAllReferences(OpenApiDocument doc, Dictionary<string, string> mapping)
    {
        // Delegate to ReplaceAllRefs for each mapping pair
        foreach ((string oldKey, string newKey) in mapping)
        {
            ReplaceAllRefs(doc, oldKey, newKey);
        }
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

