using Microsoft.Extensions.Logging;
using Microsoft.OpenApi;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private void InlinePrimitiveComponents(OpenApiDocument document)
    {
        if (document.Components?.Schemas is not IDictionary<string, IOpenApiSchema> comps)
            return;

        // 1. Identify pure‐primitive schemas
        List<string> primitives = comps.Where(kv => kv.Value.Type != null &&
                                                    (kv.Value.Type == JsonSchemaType.String || kv.Value.Type == JsonSchemaType.Integer ||
                                                     kv.Value.Type == JsonSchemaType.Boolean || kv.Value.Type == JsonSchemaType.Number) &&
                                                    (kv.Value.Properties?.Count ?? 0) == 0 && (kv.Value.Enum?.Count ?? 0) == 0 &&
                                                    (kv.Value.OneOf?.Count ?? 0) == 0 && (kv.Value.AnyOf?.Count ?? 0) == 0 &&
                                                    (kv.Value.AllOf?.Count ?? 0) == 0 && kv.Value.Items == null)
                                       .Select(kv => kv.Key)
                                       .ToList();

        if (!primitives.Any())
            return;

        foreach (string primKey in primitives)
        {
            if (comps[primKey] is not OpenApiSchema primitiveSchema)
                continue;

            // Build an inline copy of its constraints
            var inlineSchema = new OpenApiSchema
            {
                Type = primitiveSchema.Type,
                Format = primitiveSchema.Format,
                Description = primitiveSchema.Description,
                MaxLength = primitiveSchema.MaxLength,
                Pattern = primitiveSchema.Pattern,
                Minimum = primitiveSchema.Minimum,
                Maximum = primitiveSchema.Maximum
            };

            var visited = new HashSet<IOpenApiSchema>();

            // Recursively replace schema.$ref → inlineSchema, including nested properties and compositions
            void ReplaceRef(IOpenApiSchema? schema)
            {
                if (schema == null || !visited.Add(schema))
                    return;

                if (schema is OpenApiSchema os)
                {
                    // First, replace direct refs inside dictionaries/lists at this level
                    ReplaceRefsInDictionary(os.Properties);
                    ReplaceRefsInCollection(os.AllOf);
                    ReplaceRefsInCollection(os.OneOf);
                    ReplaceRefsInCollection(os.AnyOf);

                    // Handle Items
                    if (os.Items is OpenApiSchemaReference itemsRef && itemsRef.Reference.Id == primKey)
                    {
                        os.Items = inlineSchema;
                        _logger.LogInformation("Replaced Items reference to '{PrimKey}' with inline schema (nested)", primKey);
                    }
                    else if (os.Items is OpenApiSchema itemsSchema)
                    {
                        ReplaceRef(itemsSchema);
                    }

                    // Handle AdditionalProperties
                    if (os.AdditionalProperties is OpenApiSchemaReference additionalRef && additionalRef.Reference.Id == primKey)
                    {
                        os.AdditionalProperties = inlineSchema;
                        _logger.LogInformation("Replaced AdditionalProperties reference to '{PrimKey}' with inline schema (nested)", primKey);
                    }
                    else if (os.AdditionalProperties is OpenApiSchema additionalSchema)
                    {
                        ReplaceRef(additionalSchema);
                    }

                    // Recurse into child schemas that are concrete schemas after replacements
                    if (os.Properties != null)
                    {
                        foreach (string key in os.Properties.Keys.ToList())
                        {
                            if (os.Properties[key] is OpenApiSchema concreteProp)
                                ReplaceRef(concreteProp);
                        }
                    }

                    if (os.AllOf != null)
                        foreach (IOpenApiSchema c in os.AllOf)
                            if (c is OpenApiSchema concreteC)
                                ReplaceRef(concreteC);
                    if (os.OneOf != null)
                        foreach (IOpenApiSchema c in os.OneOf)
                            if (c is OpenApiSchema concreteC)
                                ReplaceRef(concreteC);
                    if (os.AnyOf != null)
                        foreach (IOpenApiSchema c in os.AnyOf)
                            if (c is OpenApiSchema concreteC)
                                ReplaceRef(concreteC);
                }
            }

            // Replace references in collections
            void ReplaceRefsInCollection<T>(IList<T>? collection) where T : IOpenApiSchema
            {
                if (collection == null)
                    return;

                for (int i = 0; i < collection.Count; i++)
                {
                    if (collection[i] is OpenApiSchemaReference schemaRef && schemaRef.Reference.Id == primKey)
                    {
                        // Replace the reference with the inline schema
                        // We need to cast through IOpenApiSchema since T is constrained to it
                        collection[i] = (T)(IOpenApiSchema)inlineSchema;
                        //_logger.LogInformation("Replaced reference to '{PrimKey}' with inline schema", primKey);
                    }
                    else if (collection[i] is OpenApiSchema concreteSchema)
                    {
                        ReplaceRef(concreteSchema);
                    }
                }
            }

            // Replace references in dictionaries
            void ReplaceRefsInDictionary(IDictionary<string, IOpenApiSchema>? dict)
            {
                if (dict == null)
                    return;

                foreach (string key in dict.Keys.ToList())
                {
                    if (dict[key] is OpenApiSchemaReference schemaRef && schemaRef.Reference.Id == primKey)
                    {
                        // Replace the reference with the inline schema
                        dict[key] = inlineSchema;
                        _logger.LogInformation("Replaced reference to '{PrimKey}' with inline schema", primKey);
                    }
                    else if (dict[key] is OpenApiSchema concreteSchema)
                    {
                        ReplaceRef(concreteSchema);
                    }
                }
            }

            // Handle inlining a parameter $ref → copy its fields, then ReplaceRef(schema)
            void InlineParameter(IOpenApiParameter? param)
            {
                if (param?.Schema != null)
                    ReplaceRef(param.Schema);
            }

            // 2. Replace refs in component schemas
            foreach (IOpenApiSchema cs in comps.Values.ToList())
            {
                if (cs is OpenApiSchema concreteCs)
                {
                    ReplaceRef(concreteCs);
                    ReplaceRefsInCollection(concreteCs.AllOf);
                    ReplaceRefsInCollection(concreteCs.OneOf);
                    ReplaceRefsInCollection(concreteCs.AnyOf);
                    ReplaceRefsInDictionary(concreteCs.Properties);
                    if (concreteCs.Items is OpenApiSchemaReference itemsRef && itemsRef.Reference.Id == primKey)
                    {
                        concreteCs.Items = inlineSchema;
                        _logger.LogInformation("Replaced Items reference to '{PrimKey}' with inline schema", primKey);
                    }

                    if (concreteCs.AdditionalProperties is OpenApiSchemaReference additionalRef && additionalRef.Reference.Id == primKey)
                    {
                        concreteCs.AdditionalProperties = inlineSchema;
                        _logger.LogInformation("Replaced AdditionalProperties reference to '{PrimKey}' with inline schema", primKey);
                    }
                }
            }

            // 3. Replace refs in request‐bodies
            if (document.Components.RequestBodies != null)
                foreach (OpenApiRequestBody? rb in document.Components.RequestBodies.Values)
                    if (rb?.Content != null)
                        foreach (OpenApiMediaType? mt in rb.Content.Values)
                        {
                            if (mt?.Schema is OpenApiSchema concreteSchema)
                            {
                                ReplaceRef(concreteSchema);
                            }
                            else if (mt?.Schema is OpenApiSchemaReference schemaRef && schemaRef.Reference.Id == primKey)
                            {
                                mt.Schema = inlineSchema;
                                _logger.LogInformation("Replaced Component RequestBody schema reference to '{PrimKey}' with inline schema", primKey);
                            }
                        }

            // 4. Replace refs in responses
            if (document.Components.Responses != null)
                foreach (IOpenApiResponse? resp in document.Components.Responses.Values)
                {
                    if (resp is OpenApiResponse concreteResp && concreteResp.Content != null)
                        foreach (OpenApiMediaType? mt in concreteResp.Content.Values)
                        {
                            if (mt?.Schema is OpenApiSchema concreteSchema)
                            {
                                ReplaceRef(concreteSchema);
                            }
                            else if (mt?.Schema is OpenApiSchemaReference schemaRef && schemaRef.Reference.Id == primKey)
                            {
                                mt.Schema = inlineSchema;
                                _logger.LogInformation("Replaced Component Response schema reference to '{PrimKey}' with inline schema", primKey);
                            }
                        }
                }

            // 5. Replace refs in headers
            if (document.Components.Headers != null)
                foreach (IOpenApiHeader? hdr in document.Components.Headers.Values)
                    if (hdr?.Schema is OpenApiSchema concreteSchema)
                        ReplaceRef(concreteSchema);

            // 6. Inline component‐level parameters
            if (document.Components.Parameters != null)
                foreach (IOpenApiParameter? compParam in document.Components.Parameters.Values)
                    InlineParameter(compParam);

            // 7. Inline path‐level and operation‐level parameters
            foreach (OpenApiPathItem? pathItem in document.Paths.Values)
            {
                // path‐level
                if (pathItem.Parameters != null)
                    foreach (IOpenApiParameter? p in pathItem.Parameters)
                        if (p is OpenApiParameter concreteP)
                            InlineParameter(concreteP);

                // each operation
                if (pathItem.Operations != null)
                {
                    foreach (OpenApiOperation? op in pathItem.Operations.Values)
                    {
                        if (op.Parameters != null)
                            foreach (IOpenApiParameter? p in op.Parameters)
                                if (p is OpenApiParameter concreteP)
                                    InlineParameter(concreteP);

                        if (op.RequestBody?.Content != null)
                            foreach (OpenApiMediaType? mt in op.RequestBody.Content.Values)
                            {
                                if (mt?.Schema is OpenApiSchema concreteSchema)
                                {
                                    ReplaceRef(concreteSchema);
                                }
                                else if (mt?.Schema is OpenApiSchemaReference schemaRef && schemaRef.Reference.Id == primKey)
                                {
                                    mt.Schema = inlineSchema;
                                    _logger.LogInformation("Replaced RequestBody schema reference to '{PrimKey}' with inline schema", primKey);
                                }
                            }

                        if (op.Responses != null)
                        {
                            foreach (IOpenApiResponse? resp in op.Responses.Values)
                                if (resp is OpenApiResponse concreteResp && concreteResp.Content != null)
                                    foreach (OpenApiMediaType? mt in concreteResp.Content.Values)
                                    {
                                        if (mt?.Schema is OpenApiSchema concreteSchema)
                                        {
                                            ReplaceRef(concreteSchema);
                                        }
                                        else if (mt?.Schema is OpenApiSchemaReference schemaRef && schemaRef.Reference.Id == primKey)
                                        {
                                            mt.Schema = inlineSchema;
                                            _logger.LogInformation("Replaced Response schema reference to '{PrimKey}' with inline schema", primKey);
                                        }
                                    }
                        }
                    }
                }
            }

            // 8. Finally, remove the now‐inlined component
            comps.Remove(primKey);
        }
    }

    private void FixAllInlineValueEnums(OpenApiDocument document)
    {
        IDictionary<string, IOpenApiSchema>? comps = document.Components?.Schemas;
        if (comps == null)
            return;

        foreach (KeyValuePair<string, IOpenApiSchema> kv in comps.ToList())
        {
            string key = kv.Key;
            IOpenApiSchema schema = kv.Value;
            OpenApiSchema? wrapperSegment = null;

            if (schema.Properties?.ContainsKey("value") == true)
                wrapperSegment = (OpenApiSchema)schema;
            else if (schema.AllOf?.Count == 2 && schema.AllOf[1]
                                                       .Properties?.ContainsKey("value") == true)
                wrapperSegment = (OpenApiSchema)schema.AllOf[1];
            else
                continue;

            IOpenApiSchema? inline = wrapperSegment?.Properties?["value"];
            if (inline?.Enum == null || inline.Enum.Count == 0)
                continue;

            var enumKey = $"{key}_value";
            if (!comps.ContainsKey(enumKey))
            {
                comps[enumKey] = new OpenApiSchema
                {
                    Type = inline.Type,
                    Title = enumKey,
                    Enum = inline.Enum.ToList()
                };
            }

            if (wrapperSegment?.Properties != null)
                wrapperSegment.Properties["value"] = new OpenApiSchemaReference(enumKey);
        }
    }

    private void PromoteEnumBranchesUnderDiscriminator(OpenApiDocument doc)
    {
        if (doc.Components?.Schemas is not { } comps)
            return;

        foreach ((string parentName, IOpenApiSchema parent) in comps.ToList())
        {
            if (parent is not OpenApiSchema ps)
                continue;
            if (ps.Discriminator == null)
                continue; // only wrap when polymorphic discriminator is present
            IList<IOpenApiSchema>? branches = ps.OneOf ?? ps.AnyOf;
            if (branches is not { Count: > 0 } || ps.Discriminator is null)
                continue;

            string disc = ps.Discriminator.PropertyName ?? "type";
            IDictionary<string, OpenApiSchemaReference> mapping = ps.Discriminator.Mapping ??= new Dictionary<string, OpenApiSchemaReference>();
            bool changed = false;

            for (int i = 0; i < branches.Count; i++)
            {
                IOpenApiSchema b = branches[i];

                // Resolve schema and component id if it's a ref
                string? refId = (b as OpenApiSchemaReference)?.Reference.Id;
                IOpenApiSchema resolved = b;
                if (refId != null && comps.TryGetValue(refId, out IOpenApiSchema? compSchema))
                    resolved = compSchema;

                // If branch is (or resolves to) an enum-only schema, wrap it
                if (resolved is OpenApiSchema rs && rs.Enum is { Count: > 0 } &&
                    (rs.Type == null || (!HasSchemaType(rs, JsonSchemaType.Object) && (rs.Properties == null || rs.Properties.Count == 0))))
                {
                    // Create wrapper component
                    string wrapperName = ReserveUniqueSchemaName(comps, $"{refId ?? parentName}", "Wrapper");
                    var wrapper = new OpenApiSchema
                    {
                        Type = JsonSchemaType.Object,
                        Properties = new Dictionary<string, IOpenApiSchema>
                        {
                            ["value"] = refId is not null ? new OpenApiSchemaReference(refId) : rs // inline fallback (rare; most are refs)
                        },
                        Required = new HashSet<string> { "value" }
                    };

                    // Allow discriminator field on the child
                    wrapper.Properties[disc] = new OpenApiSchema { Type = JsonSchemaType.String };

                    comps[wrapperName] = wrapper;

                    // Replace branch with ref to wrapper
                    branches[i] = new OpenApiSchemaReference(wrapperName);
                    changed = true;

                    // Retarget mapping if it was pointing to the enum
                    if (refId != null)
                    {
                        // mapping keys should be discriminator VALUES; if you used ids, keep consistent
                        // but make sure the mapping VALUE now points to wrapperName
                        foreach (string k in mapping.Keys.ToList())
                        {
                            if (mapping[k].Reference.Id == refId)
                                mapping[k] = new OpenApiSchemaReference(wrapperName);
                        }
                    }
                }
            }

            if (changed)
            {
                // Ensure parent has disc property & required (you already do this elsewhere,
                // but harmless to double-check)
                ps.Properties ??= new Dictionary<string, IOpenApiSchema>();
                if (!ps.Properties.ContainsKey(disc))
                    ps.Properties[disc] = new OpenApiSchema { Type = JsonSchemaType.String };
                ps.Required ??= new HashSet<string>();
                ps.Required.Add(disc);
            }
        }
    }

    private void WrapEnumBranchesInCompositions(OpenApiDocument doc)
    {
        if (doc.Components?.Schemas is not { } comps)
            return;

        foreach ((string parentName, IOpenApiSchema parent) in comps.ToList())
        {
            if (parent is not OpenApiSchema ps)
                continue;

            void ProcessBranchList(IList<IOpenApiSchema>? branches)
            {
                if (branches is not { Count: > 0 })
                    return;
                if (!IsObjectLikeSchema(ps) && !HasObjectLikeBranch(branches, comps))
                    return;

                for (int i = 0; i < branches.Count; i++)
                {
                    IOpenApiSchema b = branches[i];

                    string? refId = (b as OpenApiSchemaReference)?.Reference.Id;
                    IOpenApiSchema resolved = b;
                    if (refId != null && comps.TryGetValue(refId, out IOpenApiSchema? compSchema))
                        resolved = compSchema;

                    if (resolved is OpenApiSchema rs && rs.Enum is { Count: > 0 } &&
                        (rs.Type == null || (!HasSchemaType(rs, JsonSchemaType.Object) && (rs.Properties == null || rs.Properties.Count == 0))))
                    {
                        string wrapperName = ReserveUniqueSchemaName(comps, $"{refId ?? parentName}", "Wrapper");
                        var wrapper = new OpenApiSchema
                        {
                            Type = JsonSchemaType.Object,
                            Properties = new Dictionary<string, IOpenApiSchema>
                            {
                                ["value"] = refId is not null ? new OpenApiSchemaReference(refId) : rs
                            },
                            Required = new HashSet<string> { "value" }
                        };

                        comps[wrapperName] = wrapper;
                        branches[i] = new OpenApiSchemaReference(wrapperName);

                        if (ps.Discriminator?.Mapping is { } mapping && refId != null)
                        {
                            foreach (string k in mapping.Keys.ToList())
                            {
                                if (mapping[k].Reference.Id == refId)
                                    mapping[k] = new OpenApiSchemaReference(wrapperName);
                            }
                        }
                    }
                }
            }

            ProcessBranchList(ps.OneOf);
            ProcessBranchList(ps.AnyOf);
            ProcessBranchList(ps.AllOf);
        }
    }

    private static string KeyForDiscriminator(IOpenApiSchema branch, string discProp, string fallback)
    {
        if (branch is OpenApiSchema bs && bs.Properties != null && bs.Properties.TryGetValue(discProp, out IOpenApiSchema? dp) && dp is OpenApiSchema dps &&
            dps.Enum is { Count: > 0 } && dps.Enum.First() is JsonValue jv && jv.TryGetValue(out string? val) && !string.IsNullOrWhiteSpace(val))
            return val;
        return fallback;
    }

    /// <summary>
    /// Applies schema normalizations to the document.
    /// </summary>
    private void ApplySchemaNormalizations(OpenApiDocument document, CancellationToken cancellationToken)
    {
        if (document?.Components?.Schemas == null)
            return;

        IDictionary<string, IOpenApiSchema>? comps = document.Components.Schemas;

        var objectLikeSchemaCache = new Dictionary<IOpenApiSchema, bool>(ReferenceEqualityComparer<IOpenApiSchema>.Instance);

        // Helper to determine if a schema (or any of its referenced/composed branches) is object-like.
        bool IsObjectLike(IOpenApiSchema s)
        {
            return IsObjectLikeCore(s, [], []);
        }

        bool IsObjectLikeCore(IOpenApiSchema s, HashSet<IOpenApiSchema> activeSchemas, HashSet<string> activeRefs)
        {
            if (s is OpenApiSchemaReference sr)
            {
                string? refId = sr.Reference.Id;
                if (refId == null || !comps.TryGetValue(refId, out IOpenApiSchema? resolved))
                    return false;

                if (!activeRefs.Add(refId))
                    return false;

                try
                {
                    return IsObjectLikeCore(resolved, activeSchemas, activeRefs);
                }
                finally
                {
                    activeRefs.Remove(refId);
                }
            }

            if (s is OpenApiSchema os)
            {
                if (IsObjectLikeSchema(os))
                    return true;

                if (objectLikeSchemaCache.TryGetValue(s, out bool cachedSchemaResult))
                    return cachedSchemaResult;

                if (!activeSchemas.Add(s))
                    return false;

                bool result;
                try
                {
                    result = (os.AllOf != null && os.AllOf.Any(branch => IsObjectLikeCore(branch, activeSchemas, activeRefs))) ||
                             (os.AnyOf != null && os.AnyOf.Any(branch => IsObjectLikeCore(branch, activeSchemas, activeRefs))) ||
                             (os.OneOf != null && os.OneOf.Any(branch => IsObjectLikeCore(branch, activeSchemas, activeRefs)));
                }
                finally
                {
                    activeSchemas.Remove(s);
                }

                objectLikeSchemaCache[s] = result;
                if (result)
                    return true;
            }

            return false;
        }

        foreach (KeyValuePair<string, IOpenApiSchema> kv in comps)
        {
            if (kv.Value != null && string.IsNullOrWhiteSpace(kv.Value.Title))
            {
                // In v2.3, Title is read-only, so we can't modify it directly
                // We'll handle this in a different way if needed
                //_logger.LogDebug("Schema '{Key}' has no title, but Title is read-only in v2.3", kv.Key);
            }
        }

        var visited = new HashSet<OpenApiSchema>();
        foreach (IOpenApiSchema schema in comps.Values)
        {
            if (schema is OpenApiSchema concreteSchema)
                _schemaFixer.RemoveEmptyCompositionObjects(concreteSchema, visited);
        }

        foreach (KeyValuePair<string, IOpenApiSchema> kv in comps.ToList())
        {
            cancellationToken.ThrowIfCancellationRequested();
            IOpenApiSchema? schema = kv.Value;
            if (schema == null)
                continue;

            if (schema is OpenApiSchema concreteSchema)
            {
                if (string.Equals(concreteSchema.Format, "datetime", StringComparison.OrdinalIgnoreCase))
                    concreteSchema.Format = "date-time";
                if (string.Equals(concreteSchema.Format, "uuid4", StringComparison.OrdinalIgnoreCase))
                    concreteSchema.Format = "uuid";

                bool hasComposition = (concreteSchema.OneOf?.Any() == true) || (concreteSchema.AnyOf?.Any() == true) || (concreteSchema.AllOf?.Any() == true);
                if (concreteSchema.Type == null && hasComposition)
                {
                    // Only force object when at least one branch is object-like
                    if (IsObjectLike(concreteSchema))
                        concreteSchema.Type = JsonSchemaType.Object;
                }
            }

            // Preserve source union semantics. Do not invent a discriminator or require a synthetic wire property.

            // ──────────────────────────────────────────────────────────────────
            // ENSURE THE DISCRIMINATOR PROPERTY EXISTS
            // ──────────────────────────────────────────────────────────────────
            IList<IOpenApiSchema>? compositionList = schema.OneOf ?? schema.AnyOf;
            if (compositionList?.Any() == true && schema.Discriminator != null)
            {
                schema.Discriminator.Mapping ??= new Dictionary<string, OpenApiSchemaReference>();
                foreach (IOpenApiSchema branch in compositionList)
                {
                    if (branch is OpenApiSchemaReference schemaRef && schemaRef.Reference.Id != null &&
                        !schema.Discriminator.Mapping.ContainsKey(schemaRef.Reference.Id))
                    {
                        string? mappingKey = GetMappingKeyFromRef(schemaRef.Reference.Id);
                        if (!string.IsNullOrEmpty(mappingKey))
                        {
                            schema.Discriminator.Mapping[mappingKey] = new OpenApiSchemaReference(schemaRef.Reference.Id);
                        }
                    }
                }

                if (schema.Discriminator.Mapping.Any())
                {
                    _logger.LogInformation("Populated discriminator mapping for schema '{SchemaKey}'", kv.Key);
                }
            }
        }

        foreach (var schema in comps.Values)
        {
            if (schema == null)
                continue;
            bool hasProps = (schema.Properties?.Any() == true) || schema.AdditionalProperties != null || schema.AdditionalPropertiesAllowed;
            if (hasProps && schema.Type == null && !(schema.Enum?.Any() == true))
            {
                if (schema is OpenApiSchema concreteSchema)
                {
                    concreteSchema.Type = JsonSchemaType.Object;
                }
            }
        }

        foreach (KeyValuePair<string, IOpenApiPathItem> path in document.Paths)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (path.Value?.Operations == null || !path.Value.Operations.Any())
                continue;

            foreach (KeyValuePair<HttpMethod, OpenApiOperation> operation in path.Value.Operations)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (operation.Value == null)
                    continue;

                if (operation.Value.Responses != null)
                {
                    foreach (KeyValuePair<string, IOpenApiResponse> resp in operation.Value.Responses)
                    {
                        if (resp.Value == null)
                            continue;

                        if (resp.Value is not OpenApiResponse concreteResp)
                            continue;

                        if (concreteResp.Content != null)
                            concreteResp.Content = NormalizeMediaTypes(resp.Value.Content);

                        _referenceFixer.ScrubBrokenRefs(concreteResp.Content, document);

                        if (string.IsNullOrWhiteSpace(concreteResp.Description))
                            concreteResp.Description = resp.Key == "default" ? "Default response" : $"{resp.Key} response";
                    }
                }

                if (operation.Value.RequestBody is OpenApiRequestBody requestBody && requestBody.Content != null)
                {
                    requestBody.Content = NormalizeMediaTypes(requestBody.Content);
                    _referenceFixer.ScrubBrokenRefs(requestBody.Content, document);
                }
            }
        }

        foreach (KeyValuePair<string, IOpenApiSchema> kv in comps)
        {
            if (kv.Value == null)
                continue;
            IOpenApiSchema schema = kv.Value;

            if (schema is OpenApiSchema concreteSchema)
            {
                bool onlyHasRequired = concreteSchema.Type == JsonSchemaType.Object &&
                                       (concreteSchema.Properties == null || !concreteSchema.Properties.Any()) && concreteSchema.Items == null &&
                                       (concreteSchema.AllOf?.Any() != true) && (concreteSchema.AnyOf?.Any() != true) &&
                                       (concreteSchema.OneOf?.Any() != true) && concreteSchema.AdditionalProperties == null &&
                                       (concreteSchema.Required?.Any() == true);

                if (onlyHasRequired)
                {
                    List<string> reqs = concreteSchema.Required?.Where(r => !string.IsNullOrWhiteSpace(r))
                                                      .Select(r => r)
                                                      .ToList() ?? new List<string>();
                    if (reqs.Any())
                    {
                        concreteSchema.Properties = reqs.ToDictionary(name => name, _ => (IOpenApiSchema)new OpenApiSchema { Type = JsonSchemaType.Object });
                    }

                    // For empty objects, avoid information-less shapes by allowing free-form object maps
                    concreteSchema.AdditionalProperties = null;
                    concreteSchema.AdditionalPropertiesAllowed = true;
                    concreteSchema.Required = new HashSet<string>();
                }

                bool isTrulyEmpty = concreteSchema.Type == JsonSchemaType.Object && (concreteSchema.Properties == null || !concreteSchema.Properties.Any()) &&
                                    concreteSchema.Items == null && (concreteSchema.AllOf?.Any() != true) && (concreteSchema.AnyOf?.Any() != true) &&
                                    (concreteSchema.OneOf?.Any() != true) && concreteSchema.AdditionalProperties == null;

                if (isTrulyEmpty)
                {
                    // Prefer free-form additionalProperties over a rigid empty object
                    concreteSchema.Properties = new Dictionary<string, IOpenApiSchema>();
                    concreteSchema.AdditionalProperties = null;
                    concreteSchema.AdditionalPropertiesAllowed = true;
                    concreteSchema.Required = new HashSet<string>();
                }
            }
        }

        foreach (var schema in comps.Values)
        {
            if (schema?.Enum == null || !schema.Enum.Any())
                continue;
            if (schema.Enum.All(x => x is JsonValue))
            {
                if (schema is OpenApiSchema concreteSchema && concreteSchema.Type == null)
                {
                    // Only set type if it's not already set, and determine the appropriate type based on enum values
                    var firstEnumValue = concreteSchema.Enum!.First() as JsonValue;
                    if (firstEnumValue != null)
                    {
                        switch (firstEnumValue.GetValueKind())
                        {
                            case JsonValueKind.Number:
                                concreteSchema.Type = JsonSchemaType.Number;
                                break;
                            case JsonValueKind.String:
                                concreteSchema.Type = JsonSchemaType.String;
                                break;
                            case JsonValueKind.True:
                            case JsonValueKind.False:
                                concreteSchema.Type = JsonSchemaType.Boolean;
                                break;
                            default:
                                concreteSchema.Type = JsonSchemaType.String;
                                break;
                        }
                    }
                    else
                    {
                        concreteSchema.Type = JsonSchemaType.String;
                    }
                }
            }
        }

        var visitedSchemas = new HashSet<OpenApiSchema>();
        foreach (IOpenApiSchema root in comps.Values)
        {
            if (root is OpenApiSchema concreteRoot)
                _schemaFixer.InjectTypeForNullable(concreteRoot, visitedSchemas);
        }

    }

    private void ValidateAndFixDiscriminators(OpenApiDocument document)
    {
        if (document.Components?.Schemas == null)
            return;

        foreach ((string schemaName, IOpenApiSchema schema) in document.Components.Schemas)
        {
            if (schema is not OpenApiSchema concreteSchema)
                continue;

            if (concreteSchema.Discriminator?.PropertyName != null)
            {
                string? discProp = concreteSchema.Discriminator.PropertyName;

                // Ensure the discriminator property exists in properties
                concreteSchema.Properties ??= new Dictionary<string, IOpenApiSchema>();
                if (!concreteSchema.Properties.ContainsKey(discProp))
                {
                    concreteSchema.Properties[discProp] = new OpenApiSchema
                    {
                        Type = JsonSchemaType.String,
                        Title = discProp,
                        Description = "Discriminator property"
                    };
                }

                // Ensure the discriminator property is in the required field list
                concreteSchema.Required ??= new HashSet<string>();
                if (!concreteSchema.Required.Contains(discProp))
                {
                    concreteSchema.Required.Add(discProp);
                    _logger.LogInformation("Added discriminator property '{Prop}' to required field list for schema '{Schema}'", discProp, schemaName);
                }
            }
        }
    }

    private string? GetMappingKeyFromRef(string refId)
    {
        if (string.IsNullOrEmpty(refId))
            return null;

        if (refId == "CreateDocument_RequestBody_form_data")
        {
            return "multipart/form-data";
        }

        if (refId.StartsWith("CreateDocument_oneOf_"))
        {
            return refId;
        }

        return refId;
    }


    private static OpenApiResponses CreateFallbackResponses(HttpMethod op)
    {
        string code = CanonicalSuccess(op);

        return new OpenApiResponses
        {
            [code] = new OpenApiResponse
            {
                Description = "Default",
                Content = new Dictionary<string, IOpenApiMediaType>
                {
                    ["application/json"] = new OpenApiMediaType
                    {
                        Schema = new OpenApiSchema
                        {
                            Type = JsonSchemaType.Object,
                            Title = "DefaultResponse",
                            Description = "Default response schema"
                        }
                    }
                }
            }
        };
    }

    private static void InlinePrimitivePropertyRefs(OpenApiDocument doc)
    {
        IDictionary<string, IOpenApiSchema>? comps = doc.Components?.Schemas;
        if (comps is null)
            return;

        var visited = new HashSet<IOpenApiSchema>(ReferenceEqualityComparer<IOpenApiSchema>.Instance);

        bool IsMissing(OpenApiSchemaReference r) => string.IsNullOrWhiteSpace(r.Reference.Id) || !comps.ContainsKey(r.Reference.Id);

        IOpenApiSchema ResolveComponent(IOpenApiSchema s)
        {
            while (s is OpenApiSchemaReference r && !string.IsNullOrWhiteSpace(r.Reference.Id) && comps.TryGetValue(r.Reference.Id, out IOpenApiSchema? target))
            {
                s = target;
            }

            return s;
        }

        // Heuristic: when a referenced primitive component is missing (e.g., after prior inlining/cleanup),
        // infer the intended primitive type from the reference id instead of defaulting to string.
        static JsonSchemaType InferPrimitiveTypeFromId(string? id)
        {
            if (string.IsNullOrWhiteSpace(id))
                return JsonSchemaType.String;

            string token = id!.Trim()
                              .ToLowerInvariant();

            // Booleans
            if (token is "bool" or "boolean")
                return JsonSchemaType.Boolean;

            // Common boolean-like property/id tokens
            if (token.Contains("enabled") || token.StartsWith("is_") || token.StartsWith("is") || token.EndsWith("_enabled") || token.EndsWith("Enabled"))
                return JsonSchemaType.Boolean;

            // Integers
            if (token is "int" or "integer" or "int32" or "int64")
                return JsonSchemaType.Integer;

            // Numbers (floating point / decimal)
            if (token is "number" or "float" or "double" or "decimal")
                return JsonSchemaType.Number;

            // Strings (ids, uuids, etc) fall back to string
            return JsonSchemaType.String;
        }

        bool IsPurePrimitive(IOpenApiSchema s)
        {
            s = ResolveComponent(s);

            if (s is not OpenApiSchema os)
                return false;

            bool primitive = os.Type is JsonSchemaType.String or JsonSchemaType.Integer or JsonSchemaType.Number or JsonSchemaType.Boolean;
            bool noShape = (os.Properties?.Count ?? 0) == 0 && os.Items is null && (os.AllOf?.Count ?? 0) == 0 && (os.AnyOf?.Count ?? 0) == 0 &&
                           (os.OneOf?.Count ?? 0) == 0;
            bool noEnum = (os.Enum?.Count ?? 0) == 0;

            return primitive && noShape && noEnum;
        }

        bool IsPureArray(IOpenApiSchema s)
        {
            s = ResolveComponent(s);

            if (s is not OpenApiSchema os)
                return false;

            return os.Type == JsonSchemaType.Array && os.Items != null && (os.Properties?.Count ?? 0) == 0 && (os.AllOf?.Count ?? 0) == 0 &&
                   (os.AnyOf?.Count ?? 0) == 0 && (os.OneOf?.Count ?? 0) == 0;
        }

        IOpenApiSchema InlineTarget(OpenApiSchemaReference r)
        {
            // When missing, fall back to a string (your example is an ID)
            if (IsMissing(r))
            {
                JsonSchemaType inferred = InferPrimitiveTypeFromId(r.Reference.Id);
                return new OpenApiSchema { Type = inferred };
            }

            IOpenApiSchema target = ResolveComponent(comps[r.Reference.Id]);
            if (IsPurePrimitive(target))
            {
                var os = (OpenApiSchema)target;
                // Copy relevant primitive constraints
                return new OpenApiSchema
                {
                    Type = os.Type,
                    Format = os.Format,
                    Description = os.Description,
                    MaxLength = os.MaxLength,
                    MinLength = os.MinLength,
                    Pattern = os.Pattern,
                    Minimum = os.Minimum,
                    Maximum = os.Maximum
                };
            }

            if (IsPureArray(target))
            {
                var os = (OpenApiSchema)target;

                return new OpenApiSchema
                {
                    Type = JsonSchemaType.Array,
                    Items = os.Items,
                    Description = os.Description,
                    MinItems = os.MinItems,
                    MaxItems = os.MaxItems,
                    UniqueItems = os.UniqueItems
                };
            }

            // Non-primitive: leave it as a ref
            return r;
        }

        void Visit(ref IOpenApiSchema? s)
        {
            if (s is OpenApiSchemaReference sr)
            {
                s = InlineTarget(sr);
                // If we inlined, the new schema might be OpenApiSchema; continue walking
            }

            if (s is OpenApiSchema os)
            {
                if (!visited.Add(os))
                    return;

                if (os.Properties != null)
                {
                    foreach (string key in os.Properties.Keys.ToList())
                    {
                        IOpenApiSchema? child = os.Properties[key];
                        Visit(ref child);
                        os.Properties[key] = child;
                    }
                }

                if (os.Items != null)
                {
                    IOpenApiSchema? items = os.Items;
                    Visit(ref items);
                    os.Items = items;
                }

                if (os.AdditionalProperties != null)
                {
                    IOpenApiSchema? ap = os.AdditionalProperties;
                    Visit(ref ap);
                    os.AdditionalProperties = ap;
                }

                void FixList(IList<IOpenApiSchema>? list)
                {
                    if (list is null)
                        return;
                    for (int i = 0; i < list.Count; i++)
                    {
                        IOpenApiSchema? child = list[i];
                        Visit(ref child);
                        list[i] = child!;
                    }
                }

                FixList(os.AllOf);
                FixList(os.AnyOf);
                FixList(os.OneOf);
            }
        }

        // Components.Schemas
        foreach (KeyValuePair<string, IOpenApiSchema> kv in comps.ToList())
        {
            IOpenApiSchema? s = kv.Value;
            Visit(ref s);
            doc.Components!.Schemas[kv.Key] = s!;
        }

        // Parameters
        if (doc.Components?.Parameters != null)
            foreach (IOpenApiParameter p in doc.Components.Parameters.Values)
                if (p is OpenApiParameter cp && cp.Schema is { } ps)
                {
                    IOpenApiSchema? tmp = ps;
                    Visit(ref tmp);
                    cp.Schema = tmp;
                }

        // Headers
        if (doc.Components?.Headers != null)
            foreach (IOpenApiHeader h in doc.Components.Headers.Values)
                if (h is OpenApiHeader ch && ch.Schema is { } hs)
                {
                    IOpenApiSchema? tmp = hs;
                    Visit(ref tmp);
                    ch.Schema = tmp;
                }

        // RequestBodies / Responses (components)
        if (doc.Components?.RequestBodies != null)
            foreach (var rb in doc.Components.RequestBodies.Values)
                if (rb?.Content != null)
                    foreach (OpenApiMediaType media in rb.Content.Values.OfType<OpenApiMediaType>())
                        if (media.Schema is { } sch)
                        {
                            IOpenApiSchema? tmp = sch;
                            Visit(ref tmp);
                            media.Schema = tmp;
                        }

        if (doc.Components?.Responses != null)
            foreach (var resp in doc.Components.Responses.Values)
                if (resp?.Content != null)
                    foreach (OpenApiMediaType media in resp.Content.Values.OfType<OpenApiMediaType>())
                        if (media.Schema is { } sch)
                        {
                            IOpenApiSchema? tmp = sch;
                            Visit(ref tmp);
                            media.Schema = tmp;
                        }

        // Inline under paths
        if (doc.Paths != null)
        {
            foreach (var path in doc.Paths.Values)
            {
                // path params
                if (path?.Parameters != null)
                    foreach (IOpenApiParameter p in path.Parameters)
                        if (p is OpenApiParameter cp && cp.Schema is { } ps)
                        {
                            IOpenApiSchema? tmp = ps;
                            Visit(ref tmp);
                            cp.Schema = tmp;
                        }

                if (path?.Operations == null)
                    continue;

                foreach (var op in path.Operations.Values)
                {
                    if (op?.Parameters != null)
                        foreach (IOpenApiParameter p in op.Parameters)
                            if (p is OpenApiParameter cp2 && cp2.Schema is { } ps2)
                            {
                                IOpenApiSchema? tmp = ps2;
                                Visit(ref tmp);
                                cp2.Schema = tmp;
                            }

                    if (op?.RequestBody is OpenApiRequestBody rb2 && rb2.Content != null)
                        foreach (OpenApiMediaType media in rb2.Content.Values.OfType<OpenApiMediaType>())
                            if (media.Schema is { } sch)
                            {
                                IOpenApiSchema? tmp = sch;
                                Visit(ref tmp);
                                media.Schema = tmp;
                            }

                    if (op?.Responses != null)
                        foreach (var r in op.Responses.Values)
                        {
                            if (r?.Content != null)
                                foreach (OpenApiMediaType media in r.Content.Values.OfType<OpenApiMediaType>())
                                    if (media.Schema is { } sch)
                                    {
                                        IOpenApiSchema? tmp = sch;
                                        Visit(ref tmp);
                                        media.Schema = tmp;
                                    }

                            if (r?.Headers != null)
                                foreach (IOpenApiHeader h in r.Headers.Values)
                                    if (h is OpenApiHeader ch2 && ch2.Schema is { } hs2)
                                    {
                                        IOpenApiSchema? tmp = hs2;
                                        Visit(ref tmp);
                                        ch2.Schema = tmp;
                                    }
                        }
                }
            }
        }
    }

    private static OpenApiRequestBody CreateFallbackRequestBody()
    {
        return new OpenApiRequestBody
        {
            Description = "Fallback request body",
            Content = new Dictionary<string, IOpenApiMediaType>
            {
                ["application/json"] = new OpenApiMediaType
                {
                    Schema = new OpenApiSchema { Type = JsonSchemaType.Object }
                }
            }
        };
    }

    private void EnsureNoNullSchemas(OpenApiDocument document)
    {
        if (document == null)
            return;

        static OpenApiSchema CreateFallbackSchema(string? description = null)
        {
            return new OpenApiSchema
            {
                Type = JsonSchemaType.Object,
                AdditionalPropertiesAllowed = true,
                Properties = new Dictionary<string, IOpenApiSchema>(),
                Description = description
            };
        }

        void VisitSchema(IOpenApiSchema? schema, HashSet<OpenApiSchema> visited)
        {
            if (schema is not OpenApiSchema concreteSchema || !visited.Add(concreteSchema))
                return;

            if (concreteSchema.Properties != null)
            {
                foreach (string propertyName in concreteSchema.Properties.Keys.ToList())
                {
                    if (concreteSchema.Properties[propertyName] == null)
                    {
                        concreteSchema.Properties.Remove(propertyName);
                        concreteSchema.Required?.Remove(propertyName);
                        continue;
                    }

                    VisitSchema(concreteSchema.Properties[propertyName], visited);
                }
            }

            if (concreteSchema.Type == JsonSchemaType.Array && concreteSchema.Items == null)
                concreteSchema.Items = CreateFallbackSchema("Fallback array item schema");
            else if (concreteSchema.Items != null)
                VisitSchema(concreteSchema.Items, visited);

            if (concreteSchema.AdditionalProperties != null)
                VisitSchema(concreteSchema.AdditionalProperties, visited);

            static List<IOpenApiSchema>? RemoveNullBranches(IList<IOpenApiSchema>? branches) =>
                branches?.Where(branch => branch != null)
                        .ToList();

            concreteSchema.AllOf = RemoveNullBranches(concreteSchema.AllOf);
            concreteSchema.AnyOf = RemoveNullBranches(concreteSchema.AnyOf);
            concreteSchema.OneOf = RemoveNullBranches(concreteSchema.OneOf);

            if (concreteSchema.AllOf != null)
            {
                foreach (IOpenApiSchema branch in concreteSchema.AllOf)
                    VisitSchema(branch, visited);
            }

            if (concreteSchema.AnyOf != null)
            {
                foreach (IOpenApiSchema branch in concreteSchema.AnyOf)
                    VisitSchema(branch, visited);
            }

            if (concreteSchema.OneOf != null)
            {
                foreach (IOpenApiSchema branch in concreteSchema.OneOf)
                    VisitSchema(branch, visited);
            }
        }

        void EnsureContentSchemas(IDictionary<string, IOpenApiMediaType>? content, string context)
        {
            if (content == null)
                return;

            foreach ((string mediaType, IOpenApiMediaType mediaInterface) in content)
            {
                if (mediaInterface is not OpenApiMediaType media)
                    continue;

                if (media.Schema == null)
                {
                    _logger.LogWarning("Injecting fallback schema for null media schema at {Context} ({MediaType})", context, mediaType);
                    media.Schema = CreateFallbackSchema("Fallback media schema");
                }

                VisitSchema(media.Schema, []);
            }
        }

        void EnsureParameterSchema(IOpenApiParameter? parameter, string context)
        {
            if (parameter is not OpenApiParameter concreteParameter)
                return;

            if (concreteParameter.Schema == null)
            {
                _logger.LogWarning("Injecting fallback schema for null parameter schema at {Context}", context);
                concreteParameter.Schema = CreateFallbackSchema("Fallback parameter schema");
            }

            VisitSchema(concreteParameter.Schema, []);
        }

        void EnsureHeaderSchema(IOpenApiHeader? header, string context)
        {
            if (header is not OpenApiHeader concreteHeader)
                return;

            if (concreteHeader.Schema == null)
            {
                _logger.LogWarning("Injecting fallback schema for null header schema at {Context}", context);
                concreteHeader.Schema = CreateFallbackSchema("Fallback header schema");
            }

            VisitSchema(concreteHeader.Schema, []);
        }

        if (document.Components?.Schemas != null)
        {
            foreach (string key in document.Components.Schemas.Keys.ToList())
            {
                IOpenApiSchema? schema = document.Components.Schemas[key];
                if (schema == null)
                {
                    _logger.LogWarning("Injecting fallback component schema for null schema '{SchemaName}'", key);
                    document.Components.Schemas[key] = CreateFallbackSchema($"Fallback component schema for {key}");
                    continue;
                }

                VisitSchema(schema, []);
            }
        }

        if (document.Components?.Parameters != null)
        {
            foreach ((string key, IOpenApiParameter parameter) in document.Components.Parameters)
                EnsureParameterSchema(parameter, $"components.parameters.{key}");
        }

        if (document.Components?.Headers != null)
        {
            foreach ((string key, IOpenApiHeader header) in document.Components.Headers)
                EnsureHeaderSchema(header, $"components.headers.{key}");
        }

        if (document.Components?.RequestBodies != null)
        {
            foreach ((string key, IOpenApiRequestBody requestBody) in document.Components.RequestBodies)
            {
                if (requestBody?.Content != null)
                    EnsureContentSchemas(requestBody.Content, $"components.requestBodies.{key}");
            }
        }

        if (document.Components?.Responses != null)
        {
            foreach ((string key, IOpenApiResponse response) in document.Components.Responses)
            {
                if (response?.Content != null)
                    EnsureContentSchemas(response.Content, $"components.responses.{key}");

                if (response?.Headers != null)
                {
                    foreach ((string headerName, IOpenApiHeader header) in response.Headers)
                        EnsureHeaderSchema(header, $"components.responses.{key}.headers.{headerName}");
                }
            }
        }

        if (document.Paths == null)
            return;

        foreach ((string pathKey, IOpenApiPathItem pathItem) in document.Paths)
        {
            if (pathItem?.Parameters != null)
            {
                foreach (IOpenApiParameter parameter in pathItem.Parameters)
                    EnsureParameterSchema(parameter, $"paths.{pathKey}.parameters");
            }

            if (pathItem?.Operations == null)
                continue;

            foreach ((HttpMethod method, OpenApiOperation operation) in pathItem.Operations)
            {
                string context = $"paths.{pathKey}.{method}";

                if (operation?.Parameters != null)
                {
                    foreach (IOpenApiParameter parameter in operation.Parameters)
                        EnsureParameterSchema(parameter, $"{context}.parameters");
                }

                if (operation?.RequestBody?.Content != null)
                    EnsureContentSchemas(operation.RequestBody.Content, $"{context}.requestBody");

                if (operation?.Responses == null)
                    continue;

                foreach ((string statusCode, IOpenApiResponse response) in operation.Responses)
                {
                    if (response?.Content != null)
                        EnsureContentSchemas(response.Content, $"{context}.responses.{statusCode}");

                    if (response?.Headers != null)
                    {
                        foreach ((string headerName, IOpenApiHeader header) in response.Headers)
                            EnsureHeaderSchema(header, $"{context}.responses.{statusCode}.headers.{headerName}");
                    }
                }
            }
        }
    }

}
