using Microsoft.OpenApi;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static void FlattenObjectAllOfCompositions(OpenApiDocument doc)
    {
        if (doc.Components?.Schemas == null || doc.Components.Schemas.Count == 0)
            return;

        IDictionary<string, IOpenApiSchema> comps = doc.Components.Schemas;
        var visited = new HashSet<OpenApiSchema>();

        OpenApiSchema? Resolve(IOpenApiSchema? schema)
        {
            var references = new HashSet<string>(StringComparer.Ordinal);
            while (schema is OpenApiSchemaReference schemaRef && string.IsNullOrEmpty(schemaRef.Reference.ExternalResource) &&
                   !string.IsNullOrWhiteSpace(schemaRef.Reference.Id) &&
                   references.Add(schemaRef.Reference.Id) &&
                   comps.TryGetValue(schemaRef.Reference.Id, out IOpenApiSchema? target))
            {
                schema = target;
            }

            return schema as OpenApiSchema;
        }

        static bool IsPlainObjectBranch(OpenApiSchema schema) =>
            schema.Discriminator == null &&
            schema.Type == JsonSchemaType.Object &&
            schema.Properties is { Count: > 0 } &&
            (schema.AnyOf?.Count ?? 0) == 0 &&
            (schema.OneOf?.Count ?? 0) == 0 &&
            OpenApiSchemaConstraints.HasOnly(schema, "type", "properties", "required");

        IOpenApiSchema IntersectProperties(IOpenApiSchema existing, IOpenApiSchema incoming)
        {
            if (ReferenceEquals(existing, incoming) ||
                (GetSchemaRefId(existing) is string id && id == GetSchemaRefId(incoming)))
                return existing;

            // allOf is an intersection: neither branch may silently replace the other's constraints.
            return new OpenApiSchema { AllOf = [existing, incoming] };
        }

        void MergeObjectBranch(OpenApiSchema target, OpenApiSchema source)
        {
            target.Type = JsonSchemaType.Object;
            target.Properties ??= new Dictionary<string, IOpenApiSchema>();

            foreach ((string propertyName, IOpenApiSchema propertySchema) in source.Properties ?? new Dictionary<string, IOpenApiSchema>())
            {
                if (target.Properties.TryGetValue(propertyName, out IOpenApiSchema? existing))
                    target.Properties[propertyName] = IntersectProperties(existing, propertySchema);
                else
                    target.Properties[propertyName] = propertySchema;
            }

            if (source.Required is { Count: > 0 })
            {
                target.Required ??= new HashSet<string>();
                foreach (string required in source.Required)
                    target.Required.Add(required);
            }
        }

        void Visit(IOpenApiSchema? schema)
        {
            if (schema is not OpenApiSchema concrete || !visited.Add(concrete))
                return;

            if (concrete.Properties != null)
                foreach (IOpenApiSchema property in concrete.Properties.Values)
                    Visit(property);

            if (concrete.Items != null)
                Visit(concrete.Items);

            if (concrete.AdditionalProperties != null)
                Visit(concrete.AdditionalProperties);

            if (concrete.AllOf != null)
            {
                var remainingBranches = new List<IOpenApiSchema>();

                foreach (IOpenApiSchema branch in concrete.AllOf)
                {
                    Visit(branch);
                    OpenApiSchema? resolvedBranch = Resolve(branch);

                    if (resolvedBranch != null && IsPlainObjectBranch(resolvedBranch) &&
                        concrete.AdditionalPropertiesAllowed && concrete.AdditionalProperties == null)
                    {
                        MergeObjectBranch(concrete, resolvedBranch);
                        continue;
                    }

                    remainingBranches.Add(branch);
                }

                concrete.AllOf = remainingBranches.Count > 0 ? remainingBranches : null;
            }

            if (concrete.AnyOf != null)
                foreach (IOpenApiSchema branch in concrete.AnyOf)
                    Visit(branch);

            if (concrete.OneOf != null)
                foreach (IOpenApiSchema branch in concrete.OneOf)
                    Visit(branch);
        }

        foreach (IOpenApiSchema schema in comps.Values)
            Visit(schema);
    }

    private static void RemoveMetadataOnlyAllOfBranches(OpenApiDocument doc)
    {
        var visited = new HashSet<OpenApiSchema>();

        void Visit(IOpenApiSchema? schema)
        {
            if (schema is not OpenApiSchema concrete || !visited.Add(concrete))
                return;

            if (concrete.Properties != null)
            {
                foreach (IOpenApiSchema property in concrete.Properties.Values)
                    Visit(property);
            }

            if (concrete.Items != null)
                Visit(concrete.Items);

            if (concrete.AdditionalProperties != null)
                Visit(concrete.AdditionalProperties);

            if (concrete.Not != null)
                Visit(concrete.Not);

            if (concrete.AllOf != null)
            {
                var remainingBranches = new List<IOpenApiSchema>();

                foreach (IOpenApiSchema branch in concrete.AllOf)
                {
                    Visit(branch);

                    if (branch is OpenApiSchema branchSchema && IsMetadataOnlyAllOfBranch(branchSchema))
                    {
                        MergeMetadataOnlyAllOfBranch(concrete, branchSchema);
                        continue;
                    }

                    remainingBranches.Add(branch);
                }

                concrete.AllOf = remainingBranches.Count > 0 ? remainingBranches : null;
            }

            if (concrete.AnyOf != null)
            {
                foreach (IOpenApiSchema branch in concrete.AnyOf)
                    Visit(branch);
            }

            if (concrete.OneOf != null)
            {
                foreach (IOpenApiSchema branch in concrete.OneOf)
                    Visit(branch);
            }
        }

        if (doc.Components?.Schemas != null)
        {
            foreach (IOpenApiSchema schema in doc.Components.Schemas.Values)
                Visit(schema);
        }

        if (doc.Paths == null)
            return;

        foreach (IOpenApiPathItem path in doc.Paths.Values)
        {
            if (path?.Parameters != null)
            {
                foreach (IOpenApiParameter parameter in path.Parameters)
                    Visit(parameter?.Schema);
            }

            if (path?.Operations == null)
                continue;

            foreach (OpenApiOperation operation in path.Operations.Values)
            {
                if (operation?.Parameters != null)
                {
                    foreach (IOpenApiParameter parameter in operation.Parameters)
                        Visit(parameter?.Schema);
                }

                if (operation?.RequestBody?.Content != null)
                {
                    foreach (IOpenApiMediaType mediaType in operation.RequestBody.Content.Values)
                        Visit(mediaType?.Schema);
                }

                if (operation?.Responses == null)
                    continue;

                foreach (IOpenApiResponse response in operation.Responses.Values)
                {
                    if (response?.Content != null)
                    {
                        foreach (IOpenApiMediaType mediaType in response.Content.Values)
                            Visit(mediaType?.Schema);
                    }

                    if (response?.Headers == null)
                        continue;

                    foreach (IOpenApiHeader header in response.Headers.Values)
                        Visit(header?.Schema);
                }
            }
        }
    }

    private static bool IsMetadataOnlyAllOfBranch(OpenApiSchema schema)
    {
        if (HasExplicitNonObjectType(schema) || !OpenApiSchemaConstraints.HasOnly(schema, "type", "required"))
            return false;

        return schema.Properties is not { Count: > 0 } &&
               schema.PatternProperties is not { Count: > 0 } &&
               schema.Items == null &&
               schema.AdditionalProperties == null &&
               schema.AllOf is not { Count: > 0 } &&
               schema.AnyOf is not { Count: > 0 } &&
               schema.OneOf is not { Count: > 0 } &&
               schema.Not == null &&
               schema.Enum is not { Count: > 0 } &&
               schema.Discriminator == null;
    }

    private static void MergeMetadataOnlyAllOfBranch(OpenApiSchema target, OpenApiSchema source)
    {
        target.Title ??= source.Title;
        target.Description ??= source.Description;
        target.Default ??= source.Default;
        target.Example ??= source.Example;
        target.Xml ??= source.Xml;
        target.ExternalDocs ??= source.ExternalDocs;
        target.Deprecated = target.Deprecated || source.Deprecated;
        target.ReadOnly = target.ReadOnly || source.ReadOnly;
        target.WriteOnly = target.WriteOnly || source.WriteOnly;

        if (source.Required is { Count: > 0 })
        {
            target.Required ??= new HashSet<string>();

            foreach (string required in source.Required)
                target.Required.Add(required);
        }

        if (source.Extensions is not { Count: > 0 })
            return;

        target.Extensions ??= new Dictionary<string, IOpenApiExtension>();

        foreach ((string key, IOpenApiExtension extension) in source.Extensions)
            target.Extensions.TryAdd(key, extension);
    }

    private static bool IsPureMapSchema(OpenApiSchema schema)
    {
        bool hasNoCompositions = (schema.AllOf?.Count ?? 0) == 0 && (schema.AnyOf?.Count ?? 0) == 0 && (schema.OneOf?.Count ?? 0) == 0;
        bool hasNoNamedProperties = schema.Properties == null || schema.Properties.Count == 0;
        bool hasNoItems = schema.Items == null;
        bool isObjectOrUnset = IsObjectOrNullableObject(schema);
        bool hasMapSemantics = schema.AdditionalProperties != null || schema.AdditionalPropertiesAllowed;

        return hasNoCompositions && hasNoNamedProperties && hasNoItems && isObjectOrUnset && hasMapSemantics;
    }

    private static bool IsObjectOrNullableObject(OpenApiSchema schema)
    {
        if (!schema.Type.HasValue)
            return true;

        JsonSchemaType type = schema.Type.Value;
        JsonSchemaType unsupportedTypes = type & ~(JsonSchemaType.Object | JsonSchemaType.Null);

        return type.HasFlag(JsonSchemaType.Object) && unsupportedTypes == 0;
    }

    /// <summary>
    /// Replaces references to map-only component schemas with inline map schemas.
    /// This avoids Kiota trying to materialize map-only component refs as model classes.
    /// </summary>
    private static void InlineMapOnlySchemaReferences(OpenApiDocument doc)
    {
        if (doc?.Components?.Schemas == null || doc.Components.Schemas.Count == 0)
            return;

        IDictionary<string, IOpenApiSchema> comps = doc.Components.Schemas;

        var inlinedMaps = new Dictionary<string, OpenApiSchema>(StringComparer.Ordinal);
        var visitedMaps = new HashSet<IOpenApiSchema>();

        bool ReferencesMap(IOpenApiSchema? schema, string id, HashSet<IOpenApiSchema> seen)
        {
            if (schema == null || !seen.Add(schema)) return false;
            if (schema is OpenApiSchemaReference reference)
            {
                if (!string.IsNullOrEmpty(reference.Reference.ExternalResource)) return false;
                if (reference.Reference.Id == id) return true;
                return reference.Reference.Id is string key && comps.TryGetValue(key, out IOpenApiSchema? target) && ReferencesMap(target, id, seen);
            }
            return (schema.Properties?.Values.Any(child => ReferencesMap(child, id, seen)) == true) ||
                ReferencesMap(schema.Items, id, seen) || ReferencesMap(schema.AdditionalProperties, id, seen) ||
                (schema.AllOf?.Any(child => ReferencesMap(child, id, seen)) == true) ||
                (schema.AnyOf?.Any(child => ReferencesMap(child, id, seen)) == true) ||
                (schema.OneOf?.Any(child => ReferencesMap(child, id, seen)) == true);
        }

        IOpenApiSchema Rewrite(IOpenApiSchema schema)
        {
            if (schema is OpenApiSchemaReference sr && string.IsNullOrEmpty(sr.Reference.ExternalResource) && sr.Reference?.Id is { Length: > 0 } id && comps.TryGetValue(id, out IOpenApiSchema? target) &&
                target is OpenApiSchema targetSchema && IsPureMapSchema(targetSchema))
            {
                // Recursive map references must remain references: expanding them creates an
                // infinite inline schema.
                if (ReferencesMap(targetSchema.AdditionalProperties, id, []))
                    return schema;
                if (!inlinedMaps.TryGetValue(id, out OpenApiSchema? copy))
                    inlinedMaps[id] = copy = (OpenApiSchema)targetSchema.CreateShallowCopy();
                return copy;
            }

            return schema;
        }

        void Visit(IOpenApiSchema? schema)
        {
            if (schema is not OpenApiSchema os || !visitedMaps.Add(os))
                return;

            if (os.Properties != null)
            {
                foreach (string key in os.Properties.Keys.ToList())
                {
                    IOpenApiSchema rewritten = Rewrite(os.Properties[key]);
                    os.Properties[key] = rewritten;
                    Visit(rewritten);
                }
            }

            if (os.Items != null)
            {
                os.Items = Rewrite(os.Items);
                Visit(os.Items);
            }

            if (os.AdditionalProperties != null)
            {
                os.AdditionalProperties = Rewrite(os.AdditionalProperties);
                Visit(os.AdditionalProperties);
            }

            if (os.AllOf != null)
            {
                for (var i = 0; i < os.AllOf.Count; i++)
                {
                    os.AllOf[i] = Rewrite(os.AllOf[i]);
                    Visit(os.AllOf[i]);
                }
            }

            if (os.AnyOf != null)
            {
                for (var i = 0; i < os.AnyOf.Count; i++)
                {
                    os.AnyOf[i] = Rewrite(os.AnyOf[i]);
                    Visit(os.AnyOf[i]);
                }
            }

            if (os.OneOf != null)
            {
                for (var i = 0; i < os.OneOf.Count; i++)
                {
                    os.OneOf[i] = Rewrite(os.OneOf[i]);
                    Visit(os.OneOf[i]);
                }
            }
        }

        foreach (IOpenApiSchema root in comps.Values)
            Visit(root);

        if (doc.Paths != null)
        {
            foreach (IOpenApiPathItem path in doc.Paths.Values)
            {
                if (path?.Parameters != null)
                {
                    foreach (IOpenApiParameter parameter in path.Parameters)
                    {
                        if (parameter is OpenApiParameter concreteParameter && concreteParameter.Schema != null)
                        {
                            concreteParameter.Schema = Rewrite(concreteParameter.Schema);
                            Visit(concreteParameter.Schema);
                        }
                    }
                }

                if (path?.Operations == null)
                    continue;

                foreach (OpenApiOperation operation in path.Operations.Values)
                {
                    if (operation?.Parameters != null)
                    {
                        foreach (IOpenApiParameter parameter in operation.Parameters)
                        {
                            if (parameter is OpenApiParameter concreteParameter && concreteParameter.Schema != null)
                            {
                                concreteParameter.Schema = Rewrite(concreteParameter.Schema);
                                Visit(concreteParameter.Schema);
                            }
                        }
                    }

                    if (operation?.RequestBody is OpenApiRequestBody body && body.Content != null)
                    {
                        foreach (IOpenApiMediaType mediaType in body.Content.Values)
                        {
                            if (mediaType is OpenApiMediaType concreteMediaType && concreteMediaType.Schema != null)
                            {
                                concreteMediaType.Schema = Rewrite(concreteMediaType.Schema);
                                Visit(concreteMediaType.Schema);
                            }
                        }
                    }

                    if (operation?.Responses == null)
                        continue;

                    foreach (IOpenApiResponse response in operation.Responses.Values)
                    {
                        if (response?.Content != null)
                        {
                            foreach (IOpenApiMediaType mediaType in response.Content.Values)
                            {
                                if (mediaType is OpenApiMediaType concreteMediaType && concreteMediaType.Schema != null)
                                {
                                    concreteMediaType.Schema = Rewrite(concreteMediaType.Schema);
                                    Visit(concreteMediaType.Schema);
                                }
                            }
                        }

                        if (response?.Headers != null)
                        {
                            foreach (IOpenApiHeader header in response.Headers.Values)
                            {
                                if (header is OpenApiHeader concreteHeader && concreteHeader.Schema != null)
                                {
                                    concreteHeader.Schema = Rewrite(concreteHeader.Schema);
                                    Visit(concreteHeader.Schema);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private static void CopySchemaMetadata(OpenApiSchema source, OpenApiSchema target)
    {
        target.Description = source.Description;
        target.Default = source.Default;
        target.Example = source.Example;
        target.Deprecated = source.Deprecated;
        target.ReadOnly = source.ReadOnly;
        target.WriteOnly = source.WriteOnly;

        if (source.Extensions != null && source.Extensions.Count > 0)
            target.Extensions = new Dictionary<string, IOpenApiExtension>(source.Extensions);

        target.Xml = source.Xml;
        target.ExternalDocs = source.ExternalDocs;
    }

    private static void FixErrorMessageArrayCollision(OpenApiDocument doc)
    {
        if (doc?.Paths is null || doc.Components?.Schemas is null)
            return;

        // 1) Collect component schema IDs used by 4xx/5xx JSON responses.
        var targetIds = new HashSet<string>(StringComparer.Ordinal);
        foreach (var (_, path) in doc.Paths)
        {
            if (path?.Operations is null)
                continue;
            foreach (var (_, op) in path.Operations)
            {
                if (op?.Responses is null)
                    continue;
                foreach ((string status, var resp) in op.Responses)
                {
                    if (string.IsNullOrEmpty(status) || (status[0] != '4' && status[0] != '5'))
                        continue;
                    if (resp?.Content is null)
                        continue;

                    foreach (var (_, media) in resp.Content)
                    {
                        IOpenApiSchema? s = media?.Schema;
                        if (s is OpenApiSchemaReference r && r.Reference.Type == ReferenceType.Schema && !string.IsNullOrEmpty(r.Reference.Id))
                        {
                            targetIds.Add(r.Reference.Id);
                        }
                        else if (s is OpenApiSchema inline)
                        {
                            // Inline error body: patch it locally.
                            NormalizeErrorBody(inline);
                        }
                    }
                }
            }
        }

        if (targetIds.Count == 0)
            return;

        // 2) Patch only those component schemas.
        foreach (string id in targetIds)
        {
            if (doc.Components.Schemas.TryGetValue(id, out IOpenApiSchema? schema) && schema is OpenApiSchema os)
                NormalizeErrorBody(os);
        }

        // --- local helper ---
        void NormalizeErrorBody(OpenApiSchema container)
        {
            if (container is null)
                return;

            // Kiota can generate a broken ApiException.Message override for an error wrapper whose
            // only useful message is inside an array item. It dereferences the collection itself as
            // though it were an item (for example, Errors?.MessageEscaped). A root-level string
            // message steers generation toward a safe override while preserving the array payload.
            if (container.Properties is { } containerProps && !containerProps.ContainsKey("message") &&
                containerProps.Values.Any(property => TryGetArrayItemSchema(property, out IOpenApiSchema? itemSchema) &&
                                                       HasDirectStringMessage(itemSchema,
                                                           new HashSet<IOpenApiSchema>(ReferenceEqualityComparer<IOpenApiSchema>.Instance))))
            {
                containerProps["message"] = new OpenApiSchema
                {
                    Type = JsonSchemaType.String,
                    Description = "The primary error message."
                };
            }
        }

        bool TryGetArrayItemSchema(IOpenApiSchema? schema, out IOpenApiSchema? itemSchema)
        {
            itemSchema = null;

            if (schema == null)
                return false;

            if (schema is OpenApiSchemaReference schemaRef && schemaRef.Reference.Type == ReferenceType.Schema &&
                !string.IsNullOrWhiteSpace(schemaRef.Reference.Id) &&
                doc.Components.Schemas.TryGetValue(schemaRef.Reference.Id, out IOpenApiSchema? referencedSchema))
            {
                return referencedSchema is OpenApiSchema concreteReference &&
                       TryGetArrayItemSchema(concreteReference, out itemSchema);
            }

            if (schema is not OpenApiSchema concrete || concrete.Type != JsonSchemaType.Array || concrete.Items == null)
                return false;

            itemSchema = concrete.Items;
            return true;
        }

        bool HasDirectStringMessage(IOpenApiSchema? schema, HashSet<IOpenApiSchema> visited)
        {
            if (schema == null || !visited.Add(schema))
                return false;

            if (schema is OpenApiSchemaReference schemaRef && schemaRef.Reference.Type == ReferenceType.Schema &&
                !string.IsNullOrWhiteSpace(schemaRef.Reference.Id) &&
                doc.Components.Schemas.TryGetValue(schemaRef.Reference.Id, out IOpenApiSchema? referencedSchema))
            {
                return HasDirectStringMessage(referencedSchema, visited);
            }

            if (schema is not OpenApiSchema concrete)
                return false;

            if (concrete.Properties is { } props && props.TryGetValue("message", out IOpenApiSchema? messageSchema))
            {
                if (messageSchema is OpenApiSchemaReference messageRef && messageRef.Reference.Type == ReferenceType.Schema &&
                    !string.IsNullOrWhiteSpace(messageRef.Reference.Id) &&
                    doc.Components.Schemas.TryGetValue(messageRef.Reference.Id, out IOpenApiSchema? referencedMessageSchema))
                {
                    return referencedMessageSchema is OpenApiSchema referencedMessageConcrete && referencedMessageConcrete.Type == JsonSchemaType.String;
                }

                if (messageSchema is OpenApiSchema concreteMessage && concreteMessage.Type == JsonSchemaType.String)
                    return true;
            }

            return false;
        }
    }

}
