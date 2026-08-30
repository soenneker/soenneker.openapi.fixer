using Microsoft.Extensions.Logging;
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
            if (s is OpenApiSchemaReference sr && sr.Reference.Id != null && comps.TryGetValue(sr.Reference.Id, out IOpenApiSchema? target))
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
                if (ps.Type == JsonSchemaType.Object && ps.AllOf is { Count: > 0 })
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

                    if (prop.Type == JsonSchemaType.Object && prop.AllOf is { Count: > 0 })
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
    /// Comprehensive fix that handles ALL possible enum-like schemas in discriminator contexts.
    /// This is a catch-all method that ensures no enum-like schemas can cause Kiota cast errors.
    /// </summary>
    private static void ComprehensiveEnumWrapperFix(OpenApiDocument doc)
    {
        IDictionary<string, IOpenApiSchema>? comps = doc.Components?.Schemas;
        if (comps is null || comps.Count == 0)
            return;

        // First pass: Wrap any enum-like schemas that are referenced in discriminator mappings
        foreach (KeyValuePair<string, IOpenApiSchema> kvp in comps.ToList())
        {
            string parentName = kvp.Key;
            IOpenApiSchema? parent = kvp.Value;
            if (parent?.Discriminator?.Mapping == null)
                continue;

            foreach (KeyValuePair<string, OpenApiSchemaReference> mappingEntry in parent.Discriminator.Mapping.ToList())
            {
                OpenApiSchemaReference mappingValue = mappingEntry.Value;
                string? targetId = mappingValue.Reference.Id;

                if (targetId != null && comps.TryGetValue(targetId, out IOpenApiSchema? targetSchema))
                {
                    // Check if the target schema is non-object-like and needs wrapping
                    if (IsNonObjectLike(targetSchema))
                    {
                        string wrapperName = ReserveUniqueSchemaName(comps, targetId, "Wrapper");

                        if (!comps.ContainsKey(wrapperName))
                        {
                            comps[wrapperName] = new OpenApiSchema
                            {
                                Type = JsonSchemaType.Object,
                                Properties = new Dictionary<string, IOpenApiSchema>
                                {
                                    ["value"] = new OpenApiSchemaReference(targetId)
                                },
                                Required = new HashSet<string> { "value" },
                            };
                        }

                        // Update the mapping to point to the wrapper
                        parent.Discriminator.Mapping[mappingEntry.Key] = new OpenApiSchemaReference(wrapperName);
                    }
                }
            }
        }

        // Second pass: Ensure all branches in discriminator unions are object schemas
        foreach (KeyValuePair<string, IOpenApiSchema> kvp in comps.ToList())
        {
            string parentName = kvp.Key;
            IOpenApiSchema? parent = kvp.Value;
            if (parent?.Discriminator is null)
                continue;

            IList<IOpenApiSchema>? branches = parent.OneOf ?? parent.AnyOf;
            if (branches is null || branches.Count == 0)
                continue;

            for (int i = 0; i < branches.Count; i++)
            {
                IOpenApiSchema branch = branches[i];
                string? branchRefId = GetSchemaRefId(branch);

                // Resolve the actual schema
                IOpenApiSchema resolvedSchema = branch;
                if (branchRefId != null && comps.TryGetValue(branchRefId, out IOpenApiSchema? resolved))
                {
                    resolvedSchema = resolved;
                }

                // Check if this branch is non-object-like (enums, primitives, arrays)
                bool needsWrapping = false;

                if (IsNonObjectLike(resolvedSchema))
                {
                    needsWrapping = true;
                }

                if (needsWrapping)
                {
                    string baseName = branchRefId ?? $"{parentName}_Branch{i + 1}";
                    string wrapperName = ReserveUniqueSchemaName(comps, baseName, "Wrapper");

                    if (!comps.ContainsKey(wrapperName))
                    {
                        IOpenApiSchema valueSchema = branchRefId is not null ? MakeSchemaRef(branchRefId) : resolvedSchema;
                        comps[wrapperName] = new OpenApiSchema
                        {
                            Type = JsonSchemaType.Object,
                            Properties = new Dictionary<string, IOpenApiSchema>
                            {
                                ["value"] = valueSchema
                            },
                            Required = new HashSet<string> { "value" },
                        };
                    }

                    // Replace the branch with the wrapper ref
                    branches[i] = MakeSchemaRef(wrapperName);
                    // Update discriminator mappings
                    if (parent.Discriminator.Mapping != null)
                    {
                        string fallbackInlineId = $"{parentName}_{i + 1}";
                        foreach (string mapKey in parent.Discriminator.Mapping.Keys.ToList())
                        {
                            OpenApiSchemaReference val = parent.Discriminator.Mapping[mapKey];
                            string? valId = val.Reference.Id;

                            if (string.Equals(valId, fallbackInlineId, StringComparison.Ordinal) ||
                                (branchRefId is not null && string.Equals(valId, branchRefId, StringComparison.Ordinal)))
                            {
                                parent.Discriminator.Mapping[mapKey] = new OpenApiSchemaReference(wrapperName);
                            }
                        }
                    }
                }
            }

            // No reassignment needed; branches were mutated in place

        }

        // Third pass: Remove any discriminator mappings that point to non-existent schemas
        foreach (KeyValuePair<string, IOpenApiSchema> kvp in comps.ToList())
        {
            IOpenApiSchema? parent = kvp.Value;
            if (parent?.Discriminator?.Mapping == null)
                continue;

            var keysToRemove = new List<string>();
            foreach (KeyValuePair<string, OpenApiSchemaReference> mappingEntry in parent.Discriminator.Mapping)
            {
                string? targetId = mappingEntry.Value.Reference.Id;
                if (targetId != null && !comps.ContainsKey(targetId))
                {
                    keysToRemove.Add(mappingEntry.Key);
                }
            }

            foreach (string key in keysToRemove)
            {
                parent.Discriminator.Mapping.Remove(key);
            }
        }
    }

    /// <summary>
    /// Fixes malformed enum values across the document.
    /// This trims accidentally double-quoted string enums and drops enum entries that are really serialized JSON fragments.
    /// Those malformed enums commonly surface in parameters and cause Kiota to emit invalid C# enums.
    /// </summary>
    private static void FixMalformedEnumValues(OpenApiDocument doc)
    {
        var visited = new HashSet<OpenApiSchema>();
        IDictionary<string, IOpenApiSchema> comps = doc.Components?.Schemas ?? new Dictionary<string, IOpenApiSchema>();

        void VisitSchema(IOpenApiSchema? schema)
        {
            if (schema is OpenApiSchema concreteSchema)
                FixMalformedEnumValuesRecursive(concreteSchema, visited, comps);
        }

        if (doc.Components?.Schemas != null)
        {
            foreach (IOpenApiSchema schema in doc.Components.Schemas.Values)
                VisitSchema(schema);
        }

        if (doc.Components?.Parameters != null)
        {
            foreach (IOpenApiParameter parameter in doc.Components.Parameters.Values)
                VisitSchema(parameter?.Schema);
        }

        if (doc.Components?.Headers != null)
        {
            foreach (IOpenApiHeader header in doc.Components.Headers.Values)
                VisitSchema(header?.Schema);
        }

        if (doc.Components?.RequestBodies != null)
        {
            foreach (IOpenApiRequestBody requestBody in doc.Components.RequestBodies.Values)
            {
                if (requestBody?.Content == null)
                    continue;

                foreach (IOpenApiMediaType mediaType in requestBody.Content.Values)
                    VisitSchema(mediaType?.Schema);
            }
        }

        if (doc.Components?.Responses != null)
        {
            foreach (IOpenApiResponse response in doc.Components.Responses.Values)
            {
                if (response?.Content == null)
                    continue;

                foreach (IOpenApiMediaType mediaType in response.Content.Values)
                    VisitSchema(mediaType?.Schema);
            }
        }

        if (doc.Paths == null)
            return;

        foreach (IOpenApiPathItem pathItem in doc.Paths.Values)
        {
            if (pathItem?.Parameters != null)
            {
                foreach (IOpenApiParameter parameter in pathItem.Parameters)
                    VisitSchema(parameter?.Schema);
            }

            if (pathItem?.Operations == null)
                continue;

            foreach (OpenApiOperation operation in pathItem.Operations.Values)
            {
                if (operation?.Parameters != null)
                {
                    foreach (IOpenApiParameter parameter in operation.Parameters)
                        VisitSchema(parameter?.Schema);
                }

                if (operation?.RequestBody?.Content != null)
                {
                    foreach (IOpenApiMediaType mediaType in operation.RequestBody.Content.Values)
                        VisitSchema(mediaType?.Schema);
                }

                if (operation?.Responses == null)
                    continue;

                foreach (IOpenApiResponse response in operation.Responses.Values)
                {
                    if (response?.Content == null)
                        continue;

                    foreach (IOpenApiMediaType mediaType in response.Content.Values)
                        VisitSchema(mediaType?.Schema);
                }
            }
        }
    }

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

    /// <summary>
    /// Global final pass to wrap any non-object branches in unions anywhere in the document (components and inline schemas).
    /// This is defensive to avoid Kiota CodeEnum→CodeClass casts.
    /// </summary>
    private static void WrapNonObjectUnionBranchesEverywhere(OpenApiDocument doc)
    {
        var newSchemas = new Dictionary<string, IOpenApiSchema>();
        IDictionary<string, IOpenApiSchema>? comps = doc.Components?.Schemas;
        Dictionary<IOpenApiSchema, string>? reverseLookup = null;

        if (comps != null)
        {
            reverseLookup = new Dictionary<IOpenApiSchema, string>(ReferenceEqualityComparer<IOpenApiSchema>.Instance);
            foreach (KeyValuePair<string, IOpenApiSchema> kv in comps)
            {
                if (kv.Value != null && !reverseLookup.ContainsKey(kv.Value))
                    reverseLookup[kv.Value] = kv.Key;
            }
        }

        static string? AppendContext(string? contextName, string suffix)
        {
            if (string.IsNullOrWhiteSpace(suffix))
                return contextName;
            return string.IsNullOrWhiteSpace(contextName) ? suffix : $"{contextName}_{suffix}";
        }

        static string BuildInlineBranchBaseName(OpenApiSchema parent, string? contextName, int branchIndex)
        {
            if (!string.IsNullOrWhiteSpace(parent.Title))
                return parent.Title!;
            if (!string.IsNullOrWhiteSpace(contextName))
                return $"{contextName}Branch{branchIndex + 1}";
            return $"InlineBranch{branchIndex + 1}";
        }

        void ProcessSchema(IOpenApiSchema? s, IDictionary<string, IOpenApiSchema>? comps, string? contextName)
        {
            if (s is not OpenApiSchema os)
                return;

            // If this schema is a composed primitive-only union with no object branches, avoid forcing object by default
            bool HasObjectBranch(IOpenApiSchema parent)
            {
                IList<IOpenApiSchema>? branches = parent.OneOf ?? parent.AnyOf ?? parent.AllOf;
                if (branches == null || branches.Count == 0)
                    return false;
                foreach (IOpenApiSchema b in branches)
                {
                    IOpenApiSchema resolved = b;
                    string? refId = GetSchemaRefId(b);
                    if (refId != null && comps != null && comps.TryGetValue(refId, out IOpenApiSchema? target))
                        resolved = target;
                    if (resolved is OpenApiSchema rs)
                    {
                        if (IsObjectLikeSchema(rs))
                            return true;
                    }
                }

                return false;
            }

            void ProcessUnion(IOpenApiSchema parent)
            {
                if (parent is not OpenApiSchema pos)
                    return;

                if (string.IsNullOrWhiteSpace(contextName) && reverseLookup != null && reverseLookup.TryGetValue(pos, out string? resolvedContextName))
                    contextName = resolvedContextName;

                if (!HasObjectBranch(parent))
                    return;

                bool changedAny = false;

                void ProcessBranchList(IList<IOpenApiSchema>? branches, bool allowInlineWrap)
                {
                    if (branches is not { Count: > 0 })
                        return;

                    // Collect changes to avoid modifying collection during enumeration
                    var changes = new List<(int index, IOpenApiSchema newBranch)>();

                    for (int i = 0; i < branches.Count; i++)
                    {
                        IOpenApiSchema b = branches[i];
                        IOpenApiSchema resolved = b;
                        string? refId = GetSchemaRefId(b);

                        if (refId == null && reverseLookup != null && b is OpenApiSchema branchSchema &&
                            reverseLookup.TryGetValue(branchSchema, out string? mappedId))
                        {
                            refId = mappedId;
                        }

                        if (refId != null && comps != null && comps.TryGetValue(refId, out IOpenApiSchema? target))
                            resolved = target;
                        bool isWrapperAlready = refId != null && refId.EndsWith("Wrapper", StringComparison.Ordinal);

                        if (IsNonObjectLike(resolved) && !isWrapperAlready && (refId != null || allowInlineWrap))
                        {
                            string baseName = refId ?? BuildInlineBranchBaseName(pos, contextName, i);
                            string wrapperName = ReserveUniqueSchemaName(comps ?? new Dictionary<string, IOpenApiSchema>(), baseName, "Wrapper");
                            if (comps != null && !comps.ContainsKey(wrapperName) && !newSchemas.ContainsKey(wrapperName))
                            {
                                newSchemas[wrapperName] = new OpenApiSchema
                                {
                                    Type = JsonSchemaType.Object,
                                    Properties = new Dictionary<string, IOpenApiSchema>
                                        { ["value"] = refId != null ? new OpenApiSchemaReference(refId) : resolved },
                                    Required = new HashSet<string> { "value" }
                                };
                            }

                            changes.Add((i, new OpenApiSchemaReference(wrapperName)));
                        }
                    }

                    foreach ((int index, IOpenApiSchema newBranch) in changes)
                    {
                        branches[index] = newBranch;
                        changedAny = true;
                    }
                }

                ProcessBranchList(pos.OneOf, allowInlineWrap: true);
                ProcessBranchList(pos.AnyOf, allowInlineWrap: true);
                ProcessBranchList(pos.AllOf, allowInlineWrap: false);

                if (changedAny && pos.Discriminator is not null && pos is OpenApiSchema cp && cp.Discriminator.Mapping != null)
                {
                    // we don't know exact mapping keys here; prior passes handled mapping retargets on components
                }
            }

            ProcessUnion(os);

            if (os.Properties != null)
                foreach ((string propertyName, IOpenApiSchema child) in os.Properties)
                    ProcessSchema(child, comps, AppendContext(contextName, propertyName));
            if (os.Items != null)
                ProcessSchema(os.Items, comps, AppendContext(contextName, "Item"));
            if (os.AllOf != null)
                for (var i = 0; i < os.AllOf.Count; i++)
                    ProcessSchema(os.AllOf[i], comps, AppendContext(contextName, $"AllOf{i + 1}"));
            if (os.AnyOf != null)
                for (var i = 0; i < os.AnyOf.Count; i++)
                    ProcessSchema(os.AnyOf[i], comps, AppendContext(contextName, $"AnyOf{i + 1}"));
            if (os.OneOf != null)
                for (var i = 0; i < os.OneOf.Count; i++)
                    ProcessSchema(os.OneOf[i], comps, AppendContext(contextName, $"OneOf{i + 1}"));
            if (os.AdditionalProperties != null)
                ProcessSchema(os.AdditionalProperties, comps, AppendContext(contextName, "AdditionalProperties"));
        }

        if (comps != null)
        {
            // Process all existing schemas first
            List<KeyValuePair<string, IOpenApiSchema>> existingSchemas = comps.ToList();
            foreach ((string schemaName, IOpenApiSchema s) in existingSchemas)
                ProcessSchema(s, comps, schemaName);

            // Add new schemas after processing is complete
            foreach (KeyValuePair<string, IOpenApiSchema> kvp in newSchemas)
            {
                if (kvp.Value is not { } schema)
                    continue;

                comps[kvp.Key] = schema;
                if (reverseLookup != null && !reverseLookup.ContainsKey(schema))
                    reverseLookup[schema] = kvp.Key;
            }
        }

        // Inline schemas in paths/operations
        if (doc.Paths != null)
        {
            foreach (var path in doc.Paths.Values)
            {
                if (path?.Parameters != null)
                    foreach (var p in path.Parameters)
                        if (p?.Schema != null)
                            ProcessSchema(p.Schema, comps, AppendContext("PathParameter", p.Name ?? "Parameter"));

                if (path?.Operations != null)
                    foreach (var op in path.Operations.Values)
                    {
                        if (op?.Parameters != null)
                            foreach (var p in op.Parameters)
                                if (p?.Schema != null)
                                    ProcessSchema(p.Schema, comps, AppendContext(op?.OperationId ?? "OperationParameter", p.Name ?? "Parameter"));
                        if (op?.RequestBody is OpenApiRequestBody rb && rb.Content != null)
                            foreach ((string mediaType, IOpenApiMediaType mt) in rb.Content)
                                if (mt?.Schema != null)
                                    ProcessSchema(mt.Schema, comps, AppendContext(op?.OperationId ?? "Operation", NormalizeMediaType(mediaType ?? "application/json")));
                        if (op?.Responses != null)
                            foreach ((string statusCode, IOpenApiResponse r) in op.Responses)
                            {
                                if (r?.Content != null)
                                    foreach ((string mediaType, IOpenApiMediaType mt) in r.Content)
                                        if (mt?.Schema != null)
                                            ProcessSchema(mt.Schema, comps,
                                                AppendContext(AppendContext(op?.OperationId ?? "Operation", statusCode ?? "Response"),
                                                    NormalizeMediaType(mediaType ?? "application/json")));
                                if (r?.Headers != null)
                                    foreach ((string headerName, IOpenApiHeader h) in r.Headers)
                                        if (h?.Schema != null)
                                            ProcessSchema(h.Schema, comps,
                                                AppendContext(AppendContext(op?.OperationId ?? "Operation", statusCode ?? "Response"), headerName ?? "Header"));
                            }
                    }
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
            if (schema is OpenApiSchemaReference schemaRef &&
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

            return objectOrUnset &&
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
            target.AdditionalPropertiesAllowed = false;
            target.AllOf = null;
            target.OneOf = null;
            target.AnyOf = null;
            target.Discriminator = null;
        }

        void TryNormalizeAllOf(OpenApiSchema schema)
        {
            if (schema.AllOf is not { Count: > 0 } branches)
                return;

            bool hasValueBranch = branches.Any(IsNonObjectValueBranch);
            bool canCollapse = (branches.Count == 1 && hasValueBranch) ||
                               (hasValueBranch && branches.All(branch => IsNonObjectValueBranch(branch) || IsMetadataOnlyObjectBranch(branch)));

            if (!canCollapse)
                return;

            OpenApiSchema? valueBranch = branches.Select(Resolve)
                                                 .FirstOrDefault(resolved => resolved is not null && IsNonObjectValueBranch(resolved));
            if (valueBranch is null || ReferenceEquals(valueBranch, schema))
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
