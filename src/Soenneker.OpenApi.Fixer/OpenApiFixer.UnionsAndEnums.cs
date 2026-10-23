using Microsoft.OpenApi;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    /// <summary>
    /// Scans component schemas and their properties for cases where a property declares type object
    /// but the content is an enum via allOf → $ref to an enum schema. In those cases we drop the
    /// misleading object type and reference the enum directly to avoid CodeEnum→CodeClass casts.
    /// </summary>
    private void FixEnumAllOfObjectPropertyMismatch(OpenApiDocument doc)
    {
        IDictionary<string, IOpenApiSchema>? comps = doc.Components?.Schemas;
        if (comps is null || comps.Count == 0)
            return;

        // Helper: determine if a referenced schema is an enum-like schema
        bool IsEnumLike(IOpenApiSchema s)
        {
            if (s is OpenApiSchemaReference sr && string.IsNullOrEmpty(sr.Reference.ExternalResource) && sr.Reference.Id != null && comps.TryGetValue(sr.Reference.Id, out IOpenApiSchema? target))
                s = target;
            if (s is not OpenApiSchema os)
                return false;
            return os.Enum is { Count: > 0 } && (os.Type == null || os.Type == JsonSchemaType.String || os.Type == JsonSchemaType.Integer ||
                                                 os.Type == JsonSchemaType.Number);
        }

        foreach (OpenApiSchema schema in comps.Values.OfType<OpenApiSchema>())
        {
            if (schema.Properties == null || schema.Properties.Count == 0)
                continue;

            foreach (string key in schema.Properties.Keys.ToList())
            {
                IOpenApiSchema prop = schema.Properties[key];
                if (prop is not OpenApiSchema ps)
                    continue;

                // Case: property says type object but has allOf with enum ref
                if (ps.Type == JsonSchemaType.Object && ps.AllOf is { Count: 1 } && OpenApiSchemaConstraints.HasOnly(ps, "type", "allOf"))
                {
                    // If any allOf segment is enum-like, collapse property into that ref
                    IOpenApiSchema? enumRef = ps.AllOf.FirstOrDefault(IsEnumLike);
                    if (enumRef != null)
                    {
                        // Replace property with the enum reference or resolved schema
                        if (enumRef is OpenApiSchemaReference sref && sref.Reference.Id != null)
                        {
                            schema.Properties[key] = new OpenApiSchemaReference(sref.Reference.Id);
                        }
                        else
                        {
                            schema.Properties[key] = enumRef;
                        }
                    }
                }
            }
        }

        // Also scan inline schemas throughout the document (paths, parameters, request/response/headers)
        void VisitInline(IOpenApiSchema? s)
        {
            if (s is not OpenApiSchema os)
                return;

            if (os.Properties != null)
            {
                foreach (string key in os.Properties.Keys.ToList())
                {
                    var prop = os.Properties[key] as OpenApiSchema;
                    if (prop == null)
                        continue;

                    if (prop.Type == JsonSchemaType.Object && prop.AllOf is { Count: 1 } && OpenApiSchemaConstraints.HasOnly(prop, "type", "allOf"))
                    {
                        IOpenApiSchema? enumRef = prop.AllOf.FirstOrDefault(IsEnumLike);
                        if (enumRef != null)
                        {
                            if (enumRef is OpenApiSchemaReference sref && sref.Reference.Id != null)
                                os.Properties[key] = new OpenApiSchemaReference(sref.Reference.Id);
                            else
                                os.Properties[key] = enumRef;
                        }
                    }
                }
            }

            if (os.Items != null)
                VisitInline(os.Items);
            if (os.AllOf != null)
                foreach (IOpenApiSchema c in os.AllOf)
                    VisitInline(c);
            if (os.AnyOf != null)
                foreach (IOpenApiSchema c in os.AnyOf)
                    VisitInline(c);
            if (os.OneOf != null)
                foreach (IOpenApiSchema c in os.OneOf)
                    VisitInline(c);
            if (os.AdditionalProperties != null)
                VisitInline(os.AdditionalProperties);
        }

        if (doc.Paths != null)
        {
            foreach (var path in doc.Paths.Values)
            {
                if (path == null)
                    continue;
                if (path.Parameters != null)
                    foreach (var p in path.Parameters)
                        if (p?.Schema != null)
                            VisitInline(p.Schema);

                if (path.Operations != null)
                {
                    foreach (var op in path.Operations.Values)
                    {
                        if (op?.Parameters != null)
                            foreach (var p in op.Parameters)
                                if (p?.Schema != null)
                                    VisitInline(p.Schema);

                        if (op?.RequestBody is OpenApiRequestBody rb && rb.Content != null)
                            foreach (var mt in rb.Content.Values)
                                if (mt?.Schema != null)
                                    VisitInline(mt.Schema);

                        if (op?.Responses != null)
                            foreach (var r in op.Responses.Values)
                            {
                                if (r?.Content != null)
                                    foreach (var mt in r.Content.Values)
                                        if (mt?.Schema != null)
                                            VisitInline(mt.Schema);
                                if (r?.Headers != null)
                                    foreach (var h in r.Headers.Values)
                                        if (h?.Schema != null)
                                            VisitInline(h.Schema);
                            }
                    }
                }
            }
        }
    }


    /// <summary>
    /// Fixes malformed enum values across the document.
    /// This trims accidentally double-quoted string enums and drops enum entries that are really serialized JSON fragments.
    /// Those malformed enums commonly surface in parameters and cause Kiota to emit invalid C# enums.
    /// </summary>
    /// <summary>
    /// Recursively strips empty-string values from enums across the entire document (schemas, parameters, headers, etc.).
    /// </summary>
    private static void StripEmptyStringEnumValues(OpenApiDocument doc)
    {
        // Visit helper
        var visited = new HashSet<OpenApiSchema>();

        void VisitSchema(IOpenApiSchema? s)
        {
            if (s is not OpenApiSchema os)
                return;
            if (!visited.Add(os))
                return;

            if (os.Enum != null && os.Enum.Count > 0)
            {
                bool changed = false;
                var filtered = new List<JsonNode>();
                foreach (JsonNode e in os.Enum)
                {
                    if (e is JsonValue jv && jv.TryGetValue(out string? str) && string.IsNullOrEmpty(str))
                    {
                        changed = true;
                        continue;
                    }

                    filtered.Add(e);
                }

                if (changed)
                {
                    os.Enum.Clear();
                    foreach (JsonNode e in filtered)
                        os.Enum.Add(e);
                }
            }

            if (os.Properties != null)
                foreach (IOpenApiSchema child in os.Properties.Values)
                    VisitSchema(child);
            if (os.Items != null)
                VisitSchema(os.Items);
            if (os.AdditionalProperties != null)
                VisitSchema(os.AdditionalProperties);
            if (os.AllOf != null)
                foreach (IOpenApiSchema c in os.AllOf)
                    VisitSchema(c);
            if (os.AnyOf != null)
                foreach (IOpenApiSchema c in os.AnyOf)
                    VisitSchema(c);
            if (os.OneOf != null)
                foreach (IOpenApiSchema c in os.OneOf)
                    VisitSchema(c);
        }

        // Components.Schemas
        if (doc.Components?.Schemas != null)
            foreach (IOpenApiSchema s in doc.Components.Schemas.Values)
                VisitSchema(s);

        // Parameters
        if (doc.Components?.Parameters != null)
            foreach (var p in doc.Components.Parameters.Values)
                if (p?.Schema != null)
                    VisitSchema(p.Schema);

        // Headers
        if (doc.Components?.Headers != null)
            foreach (var h in doc.Components.Headers.Values)
                if (h?.Schema != null)
                    VisitSchema(h.Schema);

        // RequestBodies
        if (doc.Components?.RequestBodies != null)
        {
            foreach (var rb in doc.Components.RequestBodies.Values)
            {
                if (rb?.Content == null)
                    continue;
                foreach (var mt in rb.Content.Values)
                    if (mt?.Schema != null)
                        VisitSchema(mt.Schema);
            }
        }

        // Responses
        if (doc.Components?.Responses != null)
        {
            foreach (var resp in doc.Components.Responses.Values)
            {
                if (resp?.Content == null)
                    continue;
                foreach (var mt in resp.Content.Values)
                    if (mt?.Schema != null)
                        VisitSchema(mt.Schema);
            }
        }
    }


    private static void NormalizeNonObjectAllOfCompositions(OpenApiDocument doc)
    {
        IDictionary<string, IOpenApiSchema>? comps = doc.Components?.Schemas;
        if (comps is null || comps.Count == 0)
            return;

        var completed = new HashSet<IOpenApiSchema>(ReferenceEqualityComparer<IOpenApiSchema>.Instance);
        var active = new HashSet<IOpenApiSchema>(ReferenceEqualityComparer<IOpenApiSchema>.Instance);

        OpenApiSchema? Resolve(IOpenApiSchema? schema)
        {
            if (schema is OpenApiSchemaReference schemaRef && string.IsNullOrEmpty(schemaRef.Reference.ExternalResource) &&
                !string.IsNullOrWhiteSpace(schemaRef.Reference.Id) &&
                comps.TryGetValue(schemaRef.Reference.Id, out IOpenApiSchema? target) &&
                target is OpenApiSchema targetSchema)
            {
                Visit(targetSchema);
                return targetSchema;
            }

            return schema as OpenApiSchema;
        }

        bool IsMetadataOnlyObjectBranch(IOpenApiSchema branch)
        {
            OpenApiSchema? schema = Resolve(branch);
            if (schema is null)
                return false;

            bool objectOrUnset = schema.Type is null || HasSchemaType(schema, JsonSchemaType.Object);

            return OpenApiSchemaConstraints.HasOnly(schema, "type") && objectOrUnset &&
                   schema.Properties is not { Count: > 0 } &&
                   schema.Items is null &&
                   schema.AdditionalProperties is null &&
                   schema.Discriminator is null &&
                   schema.Enum is not { Count: > 0 } &&
                   schema.AllOf is not { Count: > 0 } &&
                   schema.AnyOf is not { Count: > 0 } &&
                   schema.OneOf is not { Count: > 0 };
        }

        bool IsNonObjectValueBranch(IOpenApiSchema branch)
        {
            OpenApiSchema? schema = Resolve(branch);
            if (schema is null)
                return false;

            if (schema.Enum is { Count: > 0 })
                return true;

            if (HasExplicitNonObjectType(schema))
                return true;

            return HasSchemaType(schema, JsonSchemaType.Array) || (schema.Items is not null && !HasSchemaType(schema, JsonSchemaType.Object));
        }

        static bool HasNullType(OpenApiSchema schema) => HasSchemaType(schema, JsonSchemaType.Null);

        static void MergeSchemaMetadata(OpenApiSchema target, OpenApiSchema source)
        {
            if (source.Type.HasValue && target.Type.HasValue && HasNullType(source) && !HasNullType(target))
                target.Type |= JsonSchemaType.Null;

            target.Format ??= source.Format;
            target.Pattern ??= source.Pattern;
            target.MinLength ??= source.MinLength;
            target.MaxLength ??= source.MaxLength;
            target.Minimum ??= source.Minimum;
            target.Maximum ??= source.Maximum;
            target.ExclusiveMinimum ??= source.ExclusiveMinimum;
            target.ExclusiveMaximum ??= source.ExclusiveMaximum;
            target.MultipleOf ??= source.MultipleOf;
            target.MinItems ??= source.MinItems;
            target.MaxItems ??= source.MaxItems;
            if (source.UniqueItems == true)
                target.UniqueItems = true;
            else
                target.UniqueItems ??= source.UniqueItems;
            target.Default ??= source.Default;
            target.Example ??= source.Example;
            target.Title ??= source.Title;
            target.Description ??= source.Description;
            target.Deprecated = target.Deprecated || source.Deprecated;
            target.ReadOnly = target.ReadOnly || source.ReadOnly;
            target.WriteOnly = target.WriteOnly || source.WriteOnly;
            target.Items ??= source.Items;

            if (target.Enum is not { Count: > 0 } && source.Enum is { Count: > 0 })
                target.Enum = source.Enum;

            target.Xml ??= source.Xml;
            target.ExternalDocs ??= source.ExternalDocs;

            if (source.Extensions is { Count: > 0 })
            {
                target.Extensions ??= new Dictionary<string, IOpenApiExtension>();
                foreach ((string key, IOpenApiExtension value) in source.Extensions)
                {
                    if (!target.Extensions.ContainsKey(key))
                        target.Extensions[key] = value;
                }
            }
        }

        static OpenApiSchema CreateMetadataSnapshot(OpenApiSchema source)
        {
            return new OpenApiSchema
            {
                Type = source.Type,
                Format = source.Format,
                Pattern = source.Pattern,
                MinLength = source.MinLength,
                MaxLength = source.MaxLength,
                Minimum = source.Minimum,
                Maximum = source.Maximum,
                ExclusiveMinimum = source.ExclusiveMinimum,
                ExclusiveMaximum = source.ExclusiveMaximum,
                MultipleOf = source.MultipleOf,
                MinItems = source.MinItems,
                MaxItems = source.MaxItems,
                UniqueItems = source.UniqueItems,
                Default = source.Default,
                Example = source.Example,
                Title = source.Title,
                Description = source.Description,
                Deprecated = source.Deprecated,
                ReadOnly = source.ReadOnly,
                WriteOnly = source.WriteOnly,
                Xml = source.Xml,
                ExternalDocs = source.ExternalDocs,
                Extensions = source.Extensions is { Count: > 0 } ? new Dictionary<string, IOpenApiExtension>(source.Extensions) : null
            };
        }

        static void ApplyValueShape(OpenApiSchema target, OpenApiSchema source)
        {
            target.Type = source.Type;
            target.Format = source.Format;
            target.Pattern = source.Pattern;
            target.MinLength = source.MinLength;
            target.MaxLength = source.MaxLength;
            target.Minimum = source.Minimum;
            target.Maximum = source.Maximum;
            target.ExclusiveMinimum = source.ExclusiveMinimum;
            target.ExclusiveMaximum = source.ExclusiveMaximum;


            target.MultipleOf = source.MultipleOf;
            target.MinItems = source.MinItems;
            target.MaxItems = source.MaxItems;
            target.UniqueItems = source.UniqueItems;
            target.Enum = source.Enum;
            target.Items = source.Items;
            target.Default = source.Default;
            target.Example = source.Example;
            target.Title = source.Title;
            target.Description = source.Description;
            target.Deprecated = source.Deprecated;
            target.ReadOnly = source.ReadOnly;
            target.WriteOnly = source.WriteOnly;
            target.Xml = source.Xml;
            target.ExternalDocs = source.ExternalDocs;
            target.Extensions = source.Extensions is { Count: > 0 } ? new Dictionary<string, IOpenApiExtension>(source.Extensions) : null;

            target.Properties = null;
            target.Required = null;
            target.AdditionalProperties = null;
            target.AdditionalPropertiesAllowed = source.AdditionalPropertiesAllowed;
            target.AllOf = null;
            target.OneOf = null;
            target.AnyOf = null;
            target.Discriminator = null;
        }

        void TryNormalizeAllOf(OpenApiSchema schema)
        {
            if (schema.AllOf is not { Count: > 0 } branches)
                return;

            List<OpenApiSchema> values = branches.Where(IsNonObjectValueBranch).Select(Resolve).OfType<OpenApiSchema>().ToList();
            OpenApiSchema? valueBranch = values.FirstOrDefault(value => !OpenApiSchemaConstraints.HasOnly(value, "type")) ?? values.FirstOrDefault();
            if (valueBranch == null || ReferenceEquals(valueBranch, schema) ||
                !OpenApiSchemaConstraints.HasOnly(valueBranch, "type", "format", "pattern", "minLength", "maxLength", "minimum", "maximum",
                    "exclusiveMinimum", "exclusiveMaximum", "multipleOf", "minItems", "maxItems", "uniqueItems", "enum", "items") ||
                !OpenApiSchemaConstraints.HasOnly(schema, "type", "allOf") ||
                branches.Any(branch => !IsNonObjectValueBranch(branch) && !IsMetadataOnlyObjectBranch(branch)) ||
                values.Any(value => !ReferenceEquals(value, valueBranch) &&
                    (value.Type != valueBranch.Type || !OpenApiSchemaConstraints.HasOnly(value, "type"))))
                return;

            OpenApiSchema parentMetadata = CreateMetadataSnapshot(schema);
            List<OpenApiSchema> branchMetadata = branches.Select(Resolve)
                                                         .Where(resolved => resolved is not null)
                                                         .Cast<OpenApiSchema>()
                                                         .Select(CreateMetadataSnapshot)
                                                         .ToList();

            ApplyValueShape(schema, valueBranch);

            foreach (OpenApiSchema branch in branchMetadata)
                MergeSchemaMetadata(schema, branch);

            MergeSchemaMetadata(schema, parentMetadata);
        }

        void Visit(IOpenApiSchema? schema)
        {
            if (schema is not OpenApiSchema concrete)
                return;

            if (completed.Contains(concrete) || active.Contains(concrete))
                return;

            active.Add(concrete);

            if (concrete.Properties != null)
            {
                foreach (IOpenApiSchema child in concrete.Properties.Values)
                    Visit(child);
            }

            if (concrete.Items != null)
                Visit(concrete.Items);

            if (concrete.AdditionalProperties != null)
                Visit(concrete.AdditionalProperties);

            if (concrete.AllOf != null)
            {
                foreach (IOpenApiSchema child in concrete.AllOf)
                {
                    if (child is OpenApiSchemaReference)
                        Resolve(child);
                    else
                        Visit(child);
                }
            }

            if (concrete.AnyOf != null)
            {
                foreach (IOpenApiSchema child in concrete.AnyOf)
                {
                    if (child is OpenApiSchemaReference)
                        Resolve(child);
                    else
                        Visit(child);
                }
            }

            if (concrete.OneOf != null)
            {
                foreach (IOpenApiSchema child in concrete.OneOf)
                {
                    if (child is OpenApiSchemaReference)
                        Resolve(child);
                    else
                        Visit(child);
                }
            }

            TryNormalizeAllOf(concrete);

            active.Remove(concrete);
            completed.Add(concrete);
        }

        foreach (IOpenApiSchema schema in comps.Values.ToList())
            Visit(schema);

        if (doc.Components?.Parameters != null)
            foreach (IOpenApiParameter parameter in doc.Components.Parameters.Values)
                Visit(parameter?.Schema);

        if (doc.Components?.Headers != null)
            foreach (IOpenApiHeader header in doc.Components.Headers.Values)
                Visit(header?.Schema);

        if (doc.Components?.RequestBodies != null)
        {
            foreach (IOpenApiRequestBody requestBody in doc.Components.RequestBodies.Values)
            {
                if (requestBody?.Content == null)
                    continue;

                foreach (IOpenApiMediaType mediaType in requestBody.Content.Values)
                    Visit(mediaType?.Schema);
            }
        }

        if (doc.Components?.Responses != null)
        {
            foreach (IOpenApiResponse response in doc.Components.Responses.Values)
            {
                if (response == null)
                    continue;

                if (response.Content != null)
                    foreach (IOpenApiMediaType mediaType in response.Content.Values)
                        Visit(mediaType?.Schema);

                if (response.Headers != null)
                    foreach (IOpenApiHeader header in response.Headers.Values)
                        Visit(header?.Schema);
            }
        }

        if (doc.Paths == null)
            return;

        foreach (IOpenApiPathItem path in doc.Paths.Values)
        {
            if (path?.Parameters != null)
                foreach (IOpenApiParameter parameter in path.Parameters)
                    Visit(parameter?.Schema);

            if (path?.Operations == null)
                continue;

            foreach (OpenApiOperation operation in path.Operations.Values)
            {
                if (operation?.Parameters != null)
                    foreach (IOpenApiParameter parameter in operation.Parameters)
                        Visit(parameter?.Schema);

                if (operation?.RequestBody?.Content != null)
                    foreach (IOpenApiMediaType mediaType in operation.RequestBody.Content.Values)
                        Visit(mediaType?.Schema);

                if (operation?.Responses == null)
                    continue;

                foreach (IOpenApiResponse response in operation.Responses.Values)
                {
                    if (response == null)
                        continue;

                    if (response.Content != null)
                        foreach (IOpenApiMediaType mediaType in response.Content.Values)
                            Visit(mediaType?.Schema);

                    if (response.Headers != null)
                        foreach (IOpenApiHeader header in response.Headers.Values)
                            Visit(header?.Schema);
                }
            }
        }
    }

}
