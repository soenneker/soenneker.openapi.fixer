using Microsoft.Extensions.Logging;
using Microsoft.OpenApi;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static void NormalizeAllOfWrappers(OpenApiDocument doc)
    {
        if (doc.Components?.Schemas == null || doc.Components.Schemas.Count == 0)
            return;

        IDictionary<string, IOpenApiSchema> comps = doc.Components.Schemas;

        foreach (KeyValuePair<string, IOpenApiSchema> schemaEntry in comps.ToList())
        {
            if (schemaEntry.Value is not OpenApiSchema container || container.Properties == null || container.Properties.Count == 0)
                continue;

            foreach ((string propName, IOpenApiSchema propSchemaIface) in container.Properties.ToList())
            {
                if (propSchemaIface is not OpenApiSchema propSchema)
                    continue;
                if (propSchema.AllOf is not { Count: > 1 })
                    continue;

                // Skip if already wrapped
                if (propSchema.AllOf.Any(branch => GetSchemaRefId(branch) is string id && id.EndsWith("Wrapper", StringComparison.Ordinal)))
                    continue;

                string? baseRefId = propSchema.AllOf.Select(GetSchemaRefId)
                                              .FirstOrDefault(id => !string.IsNullOrWhiteSpace(id) && comps.ContainsKey(id!));

                if (string.IsNullOrWhiteSpace(baseRefId))
                    continue;

                if (comps.TryGetValue(baseRefId, out IOpenApiSchema? baseSchema) && baseSchema is OpenApiSchema baseOs)
                {
                    if (baseOs.Type == JsonSchemaType.Object || (baseOs.Properties?.Count ?? 0) > 0)
                        continue; // already object-like; no need to wrap
                }

                string legacyWrapperName = $"{baseRefId}_Wrapper";
                string wrapperName = legacyWrapperName;
                if (!comps.ContainsKey(wrapperName))
                    wrapperName = ReserveUniqueSchemaName(comps, baseRefId, "Wrapper");

                if (string.Equals(wrapperName, schemaEntry.Key, StringComparison.Ordinal) ||
                    (comps.TryGetValue(wrapperName, out IOpenApiSchema? existingWrapper) && ReferenceEquals(existingWrapper, container)))
                {
                    continue;
                }

                var valueAllOf = new List<IOpenApiSchema> { new OpenApiSchemaReference(baseRefId) };
                foreach (IOpenApiSchema branch in propSchema.AllOf)
                {
                    string? branchId = GetSchemaRefId(branch);
                    if (branchId != null && string.Equals(branchId, baseRefId, StringComparison.Ordinal))
                        continue;

                    valueAllOf.Add(branch);
                }

                IOpenApiSchema valueSchema;
                if (valueAllOf.Count == 1)
                {
                    valueSchema = valueAllOf[0];
                }
                else
                {
                    valueSchema = new OpenApiSchema { AllOf = valueAllOf };
                }

                OpenApiSchema wrapperSchema;
                if (comps.TryGetValue(wrapperName, out IOpenApiSchema? wrapperCandidate) && wrapperCandidate is OpenApiSchema wrapperOpenApiSchema)
                {
                    wrapperSchema = wrapperOpenApiSchema;
                }
                else
                {
                    wrapperSchema = new OpenApiSchema
                    {
                        Type = JsonSchemaType.Object,
                        Properties = new Dictionary<string, IOpenApiSchema>(),
                        Required = new HashSet<string> { "value" }
                    };
                    comps[wrapperName] = wrapperSchema;
                }

                wrapperSchema.Properties ??= new Dictionary<string, IOpenApiSchema>();
                wrapperSchema.Properties["value"] = valueSchema;
                wrapperSchema.Required ??= new HashSet<string>();
                wrapperSchema.Required.Add("value");

                var replacement = new OpenApiSchema
                {
                    Type = JsonSchemaType.Object,
                    AllOf = new List<IOpenApiSchema> { new OpenApiSchemaReference(wrapperName) }
                };

                CopySchemaMetadata(propSchema, replacement);

                container.Properties[propName] = replacement;
            }
        }
    }

    private static void FlattenObjectAllOfCompositions(OpenApiDocument doc)
    {
        if (doc.Components?.Schemas == null || doc.Components.Schemas.Count == 0)
            return;

        IDictionary<string, IOpenApiSchema> comps = doc.Components.Schemas;
        var visited = new HashSet<OpenApiSchema>();

        OpenApiSchema? Resolve(IOpenApiSchema? schema)
        {
            while (schema is OpenApiSchemaReference schemaRef &&
                   !string.IsNullOrWhiteSpace(schemaRef.Reference.Id) &&
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
            (schema.OneOf?.Count ?? 0) == 0;

        void MergeObjectBranch(OpenApiSchema target, OpenApiSchema source)
        {
            target.Type = JsonSchemaType.Object;
            target.Properties ??= new Dictionary<string, IOpenApiSchema>();

            foreach ((string propertyName, IOpenApiSchema propertySchema) in source.Properties ?? new Dictionary<string, IOpenApiSchema>())
            {
                if (!target.Properties.ContainsKey(propertyName))
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

                    if (resolvedBranch != null && IsPlainObjectBranch(resolvedBranch))
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
        if (HasExplicitNonObjectType(schema))
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

    /// <summary>
    /// Flattens "allOf" compositions where one branch is a pure map schema
    /// (object + additionalProperties) and the remaining branches are plain
    /// object overlays. This prevents Kiota from treating the map reference as
    /// an empty model type in composed contexts.
    /// </summary>
    private static void FlattenMapAllOfCompositions(OpenApiDocument doc)
    {
        if (doc?.Components?.Schemas == null || doc.Components.Schemas.Count == 0)
            return;

        IDictionary<string, IOpenApiSchema> comps = doc.Components.Schemas;

        void Visit(IOpenApiSchema? schema)
        {
            if (schema is not OpenApiSchema os)
                return;

            TryFlattenMapAllOf(os, comps);

            if (os.Properties != null)
            {
                foreach (IOpenApiSchema child in os.Properties.Values)
                    Visit(child);
            }

            if (os.Items != null)
                Visit(os.Items);

            if (os.AdditionalProperties != null)
                Visit(os.AdditionalProperties);

            if (os.AllOf != null)
            {
                foreach (IOpenApiSchema child in os.AllOf)
                    Visit(child);
            }

            if (os.AnyOf != null)
            {
                foreach (IOpenApiSchema child in os.AnyOf)
                    Visit(child);
            }

            if (os.OneOf != null)
            {
                foreach (IOpenApiSchema child in os.OneOf)
                    Visit(child);
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

                    if (operation?.RequestBody is OpenApiRequestBody body && body.Content != null)
                    {
                        foreach (IOpenApiMediaType mediaType in body.Content.Values)
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

                        if (response?.Headers != null)
                        {
                            foreach (IOpenApiHeader header in response.Headers.Values)
                                Visit(header?.Schema);
                        }
                    }
                }
            }
        }
    }

    private static void TryFlattenMapAllOf(OpenApiSchema target, IDictionary<string, IOpenApiSchema> comps)
    {
        if (target.AllOf is not { Count: > 1 })
            return;

        int mapBranchIndex = -1;
        OpenApiSchema? mapBranchSchema = null;

        for (var i = 0; i < target.AllOf.Count; i++)
        {
            IOpenApiSchema branch = target.AllOf[i];
            if (!TryResolveToConcreteSchema(branch, comps, out OpenApiSchema? resolved) || resolved == null)
                continue;

            if (IsPureMapSchema(resolved))
            {
                mapBranchIndex = i;
                mapBranchSchema = resolved;
                break;
            }
        }

        if (mapBranchIndex < 0 || mapBranchSchema == null)
            return;

        var mergedProperties = new Dictionary<string, IOpenApiSchema>(StringComparer.Ordinal);
        var mergedRequired = new HashSet<string>(StringComparer.Ordinal);

        for (var i = 0; i < target.AllOf.Count; i++)
        {
            if (i == mapBranchIndex)
                continue;

            IOpenApiSchema branch = target.AllOf[i];
            if (!TryResolveToConcreteSchema(branch, comps, out OpenApiSchema? resolved) || resolved == null)
                return;

            if (!IsFlattenableOverlayObject(resolved))
                return;

            if (resolved.Properties != null)
            {
                foreach ((string name, IOpenApiSchema propertySchema) in resolved.Properties)
                    mergedProperties[name] = propertySchema;
            }

            if (resolved.Required != null)
            {
                foreach (string requiredProp in resolved.Required)
                    mergedRequired.Add(requiredProp);
            }
        }

        target.Type = JsonSchemaType.Object;
        target.AllOf = null;

        target.Properties ??= new Dictionary<string, IOpenApiSchema>();
        foreach ((string name, IOpenApiSchema propertySchema) in mergedProperties)
            target.Properties[name] = propertySchema;

        if (target.AdditionalProperties == null && !target.AdditionalPropertiesAllowed)
        {
            target.AdditionalProperties = mapBranchSchema.AdditionalProperties;
            target.AdditionalPropertiesAllowed = mapBranchSchema.AdditionalPropertiesAllowed;
        }

        if (mergedRequired.Count > 0)
        {
            target.Required ??= new HashSet<string>();
            foreach (string requiredProp in mergedRequired)
                target.Required.Add(requiredProp);
        }
    }

    private static bool TryResolveToConcreteSchema(IOpenApiSchema schema, IDictionary<string, IOpenApiSchema> comps, out OpenApiSchema? resolved)
    {
        resolved = null;

        if (schema is OpenApiSchema os)
        {
            resolved = os;
            return true;
        }

        if (schema is OpenApiSchemaReference sr && sr.Reference?.Id is { Length: > 0 } id && comps.TryGetValue(id, out IOpenApiSchema? target) &&
            target is OpenApiSchema targetSchema)
        {
            resolved = targetSchema;
            return true;
        }

        return false;
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

    private static bool IsFlattenableOverlayObject(OpenApiSchema schema)
    {
        bool hasNoCompositions = (schema.AllOf?.Count ?? 0) == 0 && (schema.AnyOf?.Count ?? 0) == 0 && (schema.OneOf?.Count ?? 0) == 0;
        bool hasNoMapShape = schema.AdditionalProperties == null && !schema.AdditionalPropertiesAllowed;
        bool hasNoItems = schema.Items == null;
        bool isObjectOrUnset = IsObjectOrNullableObject(schema);

        return hasNoCompositions && hasNoMapShape && hasNoItems && isObjectOrUnset;
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

        IOpenApiSchema Rewrite(IOpenApiSchema schema)
        {
            if (schema is OpenApiSchemaReference sr && sr.Reference?.Id is { Length: > 0 } id && comps.TryGetValue(id, out IOpenApiSchema? target) &&
                target is OpenApiSchema targetSchema && IsPureMapSchema(targetSchema))
            {
                return new OpenApiSchema
                {
                    Type = JsonSchemaType.Object,
                    AdditionalProperties = targetSchema.AdditionalProperties,
                    AdditionalPropertiesAllowed = targetSchema.AdditionalPropertiesAllowed,
                    Description = targetSchema.Description
                };
            }

            return schema;
        }

        void Visit(IOpenApiSchema? schema)
        {
            if (schema is not OpenApiSchema os)
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

    /// <summary>
    /// Recursively fixes malformed enum values in a schema and all its nested schemas.
    /// </summary>
    private static void FixMalformedEnumValuesRecursive(OpenApiSchema schema, HashSet<OpenApiSchema> visited, IDictionary<string, IOpenApiSchema> comps)
    {
        if (schema == null || !visited.Add(schema))
            return;

        // Fix enum values in this schema
        if (schema.Enum != null && schema.Enum.Count > 0)
        {
            bool hasMalformedValues = false;
            var cleanedEnum = new List<JsonNode>();
            bool removedStructuredStringEnum = false;
            bool hadStringEnums = false;

            foreach (JsonNode enumValue in schema.Enum)
            {
                if (enumValue is JsonValue jsonValue && jsonValue.TryGetValue(out string? stringValue))
                {
                    hadStringEnums = true;
                    string trimmed = TrimQuotes(stringValue);
                    if (LooksLikeMalformedStructuredEnumValue(trimmed))
                    {
                        hasMalformedValues = true;
                        removedStructuredStringEnum = true;
                        continue;
                    }

                    if (!string.Equals(trimmed, stringValue, StringComparison.Ordinal))
                    {
                        cleanedEnum.Add(JsonValue.Create(trimmed));
                        hasMalformedValues = true;
                    }
                    else
                    {
                        cleanedEnum.Add(enumValue);
                    }
                }
                else
                {
                    // Non-string enum value, keep it as is
                    cleanedEnum.Add(enumValue);
                }
            }

            if (hasMalformedValues)
            {
                schema.Enum = cleanedEnum.Count > 0 ? cleanedEnum : null;

                if (removedStructuredStringEnum && schema.Type == null && hadStringEnums)
                    schema.Type = JsonSchemaType.String;
            }
        }

        // Fix default if it is a quoted string
        if (schema.Default is JsonValue defVal && defVal.TryGetValue(out string? defStr))
        {
            string trimmedDefault = TrimQuotes(defStr);
            if (!string.Equals(trimmedDefault, defStr, StringComparison.Ordinal))
                schema.Default = JsonValue.Create(trimmedDefault);
        }

        // Fix example if it is a quoted string
        if (schema.Example is JsonValue exVal && exVal.TryGetValue(out string? exStr))
        {
            string trimmedExample = TrimQuotes(exStr);
            if (!string.Equals(trimmedExample, exStr, StringComparison.Ordinal))
                schema.Example = JsonValue.Create(trimmedExample);
        }

        // Recursively fix properties
        if (schema.Properties != null)
        {
            foreach (IOpenApiSchema property in schema.Properties.Values)
            {
                if (property is OpenApiSchema propertySchema)
                {
                    FixMalformedEnumValuesRecursive(propertySchema, visited, comps);
                }
            }
        }

        // Recursively fix items
        if (schema.Items is OpenApiSchema itemsSchema)
        {
            FixMalformedEnumValuesRecursive(itemsSchema, visited, comps);
        }

        // Recursively fix additional properties
        if (schema.AdditionalProperties is OpenApiSchema additionalPropsSchema)
        {
            FixMalformedEnumValuesRecursive(additionalPropsSchema, visited, comps);
        }

        // Recursively fix composition schemas
        if (schema.AllOf != null)
        {
            foreach (IOpenApiSchema allOfSchema in schema.AllOf)
            {
                if (allOfSchema is OpenApiSchema allOfConcreteSchema)
                {
                    FixMalformedEnumValuesRecursive(allOfConcreteSchema, visited, comps);
                }
            }
        }

        if (schema.OneOf != null)
        {
            foreach (IOpenApiSchema oneOfSchema in schema.OneOf)
            {
                if (oneOfSchema is OpenApiSchema oneOfConcreteSchema)
                {
                    FixMalformedEnumValuesRecursive(oneOfConcreteSchema, visited, comps);
                }
            }
        }

        if (schema.AnyOf != null)
        {
            foreach (IOpenApiSchema anyOfSchema in schema.AnyOf)
            {
                if (anyOfSchema is OpenApiSchema anyOfConcreteSchema)
                {
                    FixMalformedEnumValuesRecursive(anyOfConcreteSchema, visited, comps);
                }
            }
        }

        if (schema.Not is OpenApiSchema notSchema)
        {
            FixMalformedEnumValuesRecursive(notSchema, visited, comps);
        }
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

            // Depth-first, but very narrow: touch only the 'message' property at this level.
            if (container.Properties is { } props && props.TryGetValue("message", out IOpenApiSchema? msg) && msg is OpenApiSchema m &&
                string.Equals(m.Type?.ToString(), "array", StringComparison.OrdinalIgnoreCase) && m.Items is OpenApiSchema mi &&
                string.Equals(mi.Type?.ToString(), "string", StringComparison.OrdinalIgnoreCase))
            {
                var replacement = new OpenApiSchema
                {
                    Type = JsonSchemaType.String,
                    Description = m.Description,
                    Example = m.Example
                };
                if (m.Extensions is { Count: > 0 })
                    foreach (KeyValuePair<string, IOpenApiExtension> kv in m.Extensions)
                        replacement.Extensions[kv.Key] = kv.Value;

                container.Properties["message"] = replacement;
            }

            // Kiota can generate a broken ApiException.Message override for wrappers shaped like
            // { errors: [ { message: "..." } ] }. Adding a root-level message property steers
            // generation toward a safe string-backed override while preserving the array payload.
            if (container.Properties is { } containerProps && !containerProps.ContainsKey("message") &&
                containerProps.TryGetValue("errors", out IOpenApiSchema? errorsSchema) && TryGetArrayItemSchema(errorsSchema, out IOpenApiSchema? itemSchema) &&
                HasDirectStringMessage(itemSchema, new HashSet<IOpenApiSchema>(ReferenceEqualityComparer<IOpenApiSchema>.Instance)))
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
                return TryGetArrayItemSchema(referencedSchema, out itemSchema);
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

