using Microsoft.Extensions.Logging;
using Microsoft.OpenApi;
using Microsoft.OpenApi.Reader;
using Soenneker.Extensions.Task;
using Soenneker.Extensions.ValueTask;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private void LogDanglingOrPrimitivePropertyRefs(OpenApiDocument doc)
    {
        IDictionary<string, IOpenApiSchema> comps = doc.Components?.Schemas ?? new Dictionary<string, IOpenApiSchema>();
        var visited = new HashSet<IOpenApiSchema>(ReferenceEqualityComparer<IOpenApiSchema>.Instance);

        bool IsPrimitive(IOpenApiSchema s)
        {
            if (s is OpenApiSchemaReference r && comps.TryGetValue(r.Reference.Id, out IOpenApiSchema? t))
                s = t;
            if (s is not OpenApiSchema os)
                return false;
            return (os.Type is JsonSchemaType.String or JsonSchemaType.Integer or JsonSchemaType.Number or JsonSchemaType.Boolean) &&
                   (os.Properties?.Count ?? 0) == 0 && os.Items is null && (os.AllOf?.Count ?? 0) == 0 && (os.AnyOf?.Count ?? 0) == 0 &&
                   (os.OneOf?.Count ?? 0) == 0;
        }

        void Visit(string where, IOpenApiSchema? s)
        {
            if (s is OpenApiSchemaReference r)
            {
                string? id = r.Reference.Id;
                if (string.IsNullOrWhiteSpace(id) || !comps.ContainsKey(id))
                    _logger.LogWarning("Dangling $ref to '{Id}' at {Where}", id ?? "(null)", where);
                else if (IsPrimitive(r))
                    _logger.LogInformation("Property $ref points to primitive component '{Id}' at {Where}", id, where);
            }

            if (s is OpenApiSchema os && visited.Add(os) && os.Properties != null)
                foreach ((string k, IOpenApiSchema v) in os.Properties)
                    Visit($"{where}.properties[{k}]", v);
        }

        foreach ((string k, IOpenApiSchema s) in comps)
            Visit($"components.schemas[{k}]", s);
    }

    private static void EnsureInlineObjectTypes(OpenApiDocument doc)
    {
        void Visit(IOpenApiSchema? s)
        {
            if (s is not OpenApiSchema os)
                return;

            bool objectLike = (os.Properties?.Count > 0) || os.AdditionalProperties != null || os.AdditionalPropertiesAllowed;

            if (os.Type is null && objectLike && !(os.Enum?.Count > 0))
                os.Type = JsonSchemaType.Object;

            // Recurse
            if (os.Properties != null)
                foreach (IOpenApiSchema child in os.Properties.Values)
                    Visit(child);
            if (os.Items != null)
                Visit(os.Items);
            if (os.AllOf != null)
                foreach (IOpenApiSchema c in os.AllOf)
                    Visit(c);
            if (os.AnyOf != null)
                foreach (IOpenApiSchema c in os.AnyOf)
                    Visit(c);
            if (os.OneOf != null)
                foreach (IOpenApiSchema c in os.OneOf)
                    Visit(c);
            if (os.AdditionalProperties != null)
                Visit(os.AdditionalProperties);
        }

        // paths
        if (doc.Paths != null)
        {
            foreach (var p in doc.Paths.Values)
            {
                if (p?.Parameters != null)
                    foreach (IOpenApiParameter prm in p.Parameters)
                        if (prm is OpenApiParameter op && op.Schema is { } ps)
                            Visit(ps);

                if (p?.Operations == null)
                    continue;
                foreach (var op in p.Operations.Values)
                {
                    if (op?.Parameters != null)
                        foreach (IOpenApiParameter prm in op.Parameters)
                            if (prm is OpenApiParameter oop && oop.Schema is { } ps)
                                Visit(ps);

                    if (op?.RequestBody is OpenApiRequestBody rb && rb.Content != null)
                        foreach (var mt in rb.Content.Values)
                            if (mt?.Schema is { } s)
                                Visit(s);

                    if (op?.Responses != null)
                        foreach (var r in op.Responses.Values)
                        {
                            if (r?.Content != null)
                                foreach (var mt in r.Content.Values)
                                    if (mt?.Schema is { } s)
                                        Visit(s);
                            if (r?.Headers != null)
                                foreach (IOpenApiHeader h in r.Headers.Values)
                                    if (h is OpenApiHeader oh && oh.Schema is { } hs)
                                        Visit(hs);
                        }
                }
            }
        }
    }

    private void FixContentTypeWrapperCollisions(OpenApiDocument doc)
    {
        if (doc.Components?.Schemas == null || doc.Paths == null)
            return;
        var renameMap = new Dictionary<string, string>();

        foreach (OpenApiOperation op in doc.Paths.Values.Where(p => p?.Operations != null)
                                           .SelectMany(p => p.Operations.Values))
        {
            if (op.RequestBody?.Content == null || op.OperationId == null)
                continue;

            foreach ((string media, IOpenApiMediaType mt) in op.RequestBody.Content)
            {
                string legacyWrapperName = $"{op.OperationId!}{media.Replace('/', '_')}";
                string normalizedLegacyWrapperName = _namingFixer.ValidateComponentName(legacyWrapperName);
                string canonicalWrapperName = OpenApiNameNormalizer.NormalizeComponentName($"{op.OperationId} {OpenApiNameNormalizer.NormalizeMediaTypeName(media)}");
                string? existingWrapperName = doc.Components.Schemas.ContainsKey(legacyWrapperName) ? legacyWrapperName :
                    doc.Components.Schemas.ContainsKey(normalizedLegacyWrapperName) ? normalizedLegacyWrapperName :
                    doc.Components.Schemas.ContainsKey(canonicalWrapperName) ? canonicalWrapperName : null;

                if (existingWrapperName != null && doc.Components.Schemas.TryGetValue(existingWrapperName, out IOpenApiSchema? schema))
                {
                    string newName = ReserveUniqueSchemaName(doc.Components.Schemas, existingWrapperName, "Body");
                    _logger.LogWarning("Schema '{Old}' collides with Kiota wrapper in operation '{Op}'. Renaming to '{New}'.", existingWrapperName,
                        op.OperationId!, newName);

                    doc.Components.Schemas.Remove(existingWrapperName);
                    doc.Components.Schemas[newName] = schema;
                    renameMap[existingWrapperName] = newName;
                }
            }
        }

        if (renameMap.Count > 0)
            _referenceFixer.UpdateAllReferences(doc, renameMap); // you already have this helper
    }

    /// <summary>
    /// Wraps primitive or enum request body schemas into a tiny object { value: &lt;primitive&gt; } to avoid Kiota primitive body issues.
    /// </summary>
    private static void WrapPrimitiveRequestBodies(OpenApiDocument doc)
    {
        if (doc?.Paths == null)
            return;
        foreach (var path in doc.Paths.Values)
        {
            if (path?.Operations == null)
                continue;
            foreach (var op in path.Operations.Values)
            {
                if (op?.RequestBody?.Content == null)
                    continue;
                foreach (OpenApiMediaType media in op.RequestBody.Content.Values.OfType<OpenApiMediaType>())
                {
                    if (media.Schema is not OpenApiSchema s)
                        continue;

                    bool isBareEnum = s.Enum is { Count: > 0 } && (s.Type == null || s.Type != JsonSchemaType.Object) &&
                                      (s.Properties == null || s.Properties.Count == 0);
                    bool isPrimitive = s.Type == JsonSchemaType.String || s.Type == JsonSchemaType.Integer || s.Type == JsonSchemaType.Number ||
                                       s.Type == JsonSchemaType.Boolean;
                    if (isBareEnum || isPrimitive)
                    {
                        media.Schema = new OpenApiSchema
                        {
                            Type = JsonSchemaType.Object,
                            Properties = new Dictionary<string, IOpenApiSchema> { ["value"] = s },
                            Required = new HashSet<string> { "value" }
                        };
                    }
                }
            }
        }
    }

    private void EnsureDiscriminatorForOneOf(OpenApiDocument doc)
    {
        if (doc.Components?.Schemas == null)
            return;

        foreach ((string schemaName, IOpenApiSchema schema) in doc.Components.Schemas)
        {
            IList<IOpenApiSchema>? poly = schema.OneOf ?? schema.AnyOf;
            if (poly is not { Count: > 1 })
                continue; // not polymorphic
            if (schema is OpenApiSchema concretePolyParent && HasExplicitNonObjectType(concretePolyParent))
                continue; // primitive convenience union, not a polymorphic model
            if (!HasObjectLikeBranch(poly, doc.Components.Schemas))
                continue; // primitive convenience union, not a polymorphic model
            if (schema.Discriminator != null)
                continue; // already OK

            const string discProp = "type";

            // In v2.3, we need to cast to concrete type to modify read-only properties
            if (schema is OpenApiSchema concreteSchema)
            {
                concreteSchema.Discriminator = new OpenApiDiscriminator
                {
                    PropertyName = discProp,
                    Mapping = new Dictionary<string, OpenApiSchemaReference>()
                };

                concreteSchema.Properties ??= new Dictionary<string, IOpenApiSchema>();
                if (!concreteSchema.Properties.ContainsKey(discProp))
                {
                    concreteSchema.Properties[discProp] = new OpenApiSchema
                    {
                        Type = JsonSchemaType.String,
                        Description = "Union discriminator"
                    };
                    _logger.LogInformation("Injected discriminator property '{Prop}' into schema '{Schema}'.", discProp, schemaName);
                }

                concreteSchema.Required ??= new HashSet<string>();
                concreteSchema.Required.Add(discProp);

                // build mapping
                for (var i = 0; i < poly.Count; i++)
                {
                    IOpenApiSchema branch = poly[i];
                    string refId;

                    if (branch is OpenApiSchemaReference schemaRef) // referenced branch
                    {
                        refId = schemaRef.Reference.Id ?? $"{schemaName}_{i + 1}";
                    }
                    else // inline branch
                    {
                        // should already have been promoted by PromoteInlinePolymorphs
                        refId = $"{schemaName}_{i + 1}";
                    }

                    // Use actual discriminator value if available, otherwise fall back to schema ID
                    string mappingKey = KeyForDiscriminator(branch, discProp, refId);
                    concreteSchema.Discriminator.Mapping.TryAdd(mappingKey, new OpenApiSchemaReference(refId));
                }

                _logger.LogInformation("Added discriminator mapping for polymorphic schema '{Schema}'.", schemaName);
            }
            else
            {
                _logger.LogWarning("Could not cast schema to OpenApiSchema for discriminator injection");
            }
        }
    }

    private static bool HasObjectLikeBranch(IEnumerable<IOpenApiSchema> branches, IDictionary<string, IOpenApiSchema>? components)
    {
        foreach (IOpenApiSchema branch in branches)
        {
            IOpenApiSchema resolved = branch;
            string? refId = GetSchemaRefId(branch);

            if (refId is not null && components is not null && components.TryGetValue(refId, out IOpenApiSchema? target))
                resolved = target;

            if (resolved is OpenApiSchema schema && IsObjectLikeSchema(schema))
            {
                return true;
            }
        }

        return false;
    }

    private void RewriteCombinedUnionsAsIntersection(OpenApiDocument doc)
    {
        if (doc == null)
            return;

        var visited = new HashSet<IOpenApiSchema>();
        int normalized = 0;

        void Visit(IOpenApiSchema? schema)
        {
            if (schema == null || !visited.Add(schema))
                return;

            if (schema is not OpenApiSchema concrete)
                return;

            if (concrete.OneOf is { Count: > 0 } oneOf && concrete.AnyOf is { Count: > 0 } anyOf)
            {
                concrete.AllOf ??= [];
                concrete.AllOf.Add(new OpenApiSchema { OneOf = oneOf });
                concrete.AllOf.Add(new OpenApiSchema { AnyOf = anyOf });
                concrete.OneOf = null;
                concrete.AnyOf = null;
                normalized++;
            }

            if (concrete.Properties != null)
            {
                foreach (IOpenApiSchema property in concrete.Properties.Values)
                    Visit(property);
            }

            if (concrete.Items != null)
                Visit(concrete.Items);

            if (concrete.AdditionalProperties != null)
                Visit(concrete.AdditionalProperties);

            if (concrete.AllOf != null)
            {
                foreach (IOpenApiSchema child in concrete.AllOf)
                    Visit(child);
            }

            if (concrete.OneOf != null)
            {
                foreach (IOpenApiSchema child in concrete.OneOf)
                    Visit(child);
            }

            if (concrete.AnyOf != null)
            {
                foreach (IOpenApiSchema child in concrete.AnyOf)
                    Visit(child);
            }

            if (concrete.Not != null)
                Visit(concrete.Not);
        }

        if (doc.Components?.Schemas != null)
        {
            foreach (IOpenApiSchema schema in doc.Components.Schemas.Values)
                Visit(schema);
        }

        if (doc.Paths != null)
        {
            foreach (IOpenApiPathItem pathItem in doc.Paths.Values)
            {
                if (pathItem?.Operations == null)
                    continue;

                foreach (OpenApiOperation operation in pathItem.Operations.Values)
                {
                    if (operation?.RequestBody?.Content != null)
                    {
                        foreach (IOpenApiMediaType mediaType in operation.RequestBody.Content.Values)
                            Visit(mediaType?.Schema);
                    }

                    if (operation?.Responses != null)
                    {
                        foreach (IOpenApiResponse response in operation.Responses.Values)
                        {
                            if (response?.Content == null)
                                continue;

                            foreach (IOpenApiMediaType mediaType in response.Content.Values)
                                Visit(mediaType?.Schema);
                        }
                    }

                    if (operation?.Parameters != null)
                    {
                        foreach (IOpenApiParameter parameter in operation.Parameters)
                            Visit(parameter?.Schema);
                    }
                }
            }
        }

        if (normalized > 0)
            _logger.LogInformation(
                "Rewrote {Count} schemas that declared both oneOf and anyOf as an equivalent allOf intersection so generators can process them",
                normalized);
    }

    /// <summary>
    /// Removes misleading enum definitions under vendor extensions and ensures schemas with enum but no type default to string.
    /// Prevents Kiota from creating CodeEnum in places where a class is expected.
    /// </summary>
    private static void FixBadEnums(OpenApiDocument doc)
    {
        if (doc == null)
            return;

        // document-level
        ScrubEnumsInExtensions(doc);

        // servers
        if (doc.Servers != null)
        {
            foreach (OpenApiServer s in doc.Servers)
                ScrubEnumsInExtensions(s);
        }

        // paths, operations, params, request/response/headers
        if (doc.Paths != null)
        {
            foreach (var path in doc.Paths.Values)
            {
                if (path == null)
                    continue;
                if (path is IOpenApiExtensible pathExt)
                    ScrubEnumsInExtensions(pathExt);

                // path-level params
                if (path.Parameters != null)
                {
                    foreach (var p in path.Parameters)
                    {
                        if (p is IOpenApiExtensible pExt)
                            ScrubEnumsInExtensions(pExt);
                        if (p?.Schema is OpenApiSchema pSchema)
                            FixSchemaEnumWithoutType(pSchema, new HashSet<OpenApiSchema>());
                    }
                }

                // operations
                if (path.Operations != null)
                {
                    foreach (var op in path.Operations.Values)
                    {
                        if (op == null)
                            continue;
                        if (op is IOpenApiExtensible opExt)
                            ScrubEnumsInExtensions(opExt);

                        if (op.Parameters != null)
                            foreach (var p in op.Parameters)
                            {
                                if (p is IOpenApiExtensible pExt)
                                    ScrubEnumsInExtensions(pExt);
                                if (p?.Schema is OpenApiSchema pSchema)
                                    FixSchemaEnumWithoutType(pSchema, new HashSet<OpenApiSchema>());
                            }

                        if (op.RequestBody is OpenApiRequestBody rb && rb.Content != null)
                        {
                            if (rb is IOpenApiExtensible rbExt)
                                ScrubEnumsInExtensions(rbExt);
                            foreach (OpenApiMediaType media in rb.Content.Values.OfType<OpenApiMediaType>())
                                if (media.Schema is OpenApiSchema mtSchema)
                                {
                                    // Optional hardening: wrap primitive/enum request bodies
                                    bool isBareEnum = mtSchema.Enum is { Count: > 0 } && (mtSchema.Type == null || mtSchema.Type != JsonSchemaType.Object) &&
                                                      (mtSchema.Properties == null || mtSchema.Properties.Count == 0);
                                    bool isPrimitive = mtSchema.Type == JsonSchemaType.String || mtSchema.Type == JsonSchemaType.Integer ||
                                                       mtSchema.Type == JsonSchemaType.Number || mtSchema.Type == JsonSchemaType.Boolean;
                                    if (isBareEnum || isPrimitive)
                                    {
                                        media.Schema = new OpenApiSchema
                                        {
                                            Type = JsonSchemaType.Object,
                                            Properties = new Dictionary<string, IOpenApiSchema> { ["value"] = mtSchema },
                                            Required = new HashSet<string> { "value" }
                                        };
                                    }

                                    FixSchemaEnumWithoutType(mtSchema, new HashSet<OpenApiSchema>());
                                }
                        }

                        if (op.Responses != null)
                            foreach (var resp in op.Responses.Values)
                            {
                                if (resp == null)
                                    continue;
                                if (resp is IOpenApiExtensible respExt)
                                    ScrubEnumsInExtensions(respExt);
                                if (resp.Content != null)
                                    foreach (OpenApiMediaType media in resp.Content.Values.OfType<OpenApiMediaType>())
                                        if (media.Schema is OpenApiSchema mtSchema)
                                            FixSchemaEnumWithoutType(mtSchema, new HashSet<OpenApiSchema>());
                                if (resp.Headers != null)
                                    foreach (var h in resp.Headers.Values)
                                    {
                                        if (h is IOpenApiExtensible hExt)
                                            ScrubEnumsInExtensions(hExt);
                                        if (h?.Schema is OpenApiSchema hSchema)
                                            FixSchemaEnumWithoutType(hSchema, new HashSet<OpenApiSchema>());
                                    }
                            }
                    }
                }
            }
        }

        // components
        if (doc.Components != null)
        {
            if (doc.Components is IOpenApiExtensible compExt)
                ScrubEnumsInExtensions(compExt);

            if (doc.Components.Schemas != null)
                foreach (IOpenApiSchema s in doc.Components.Schemas.Values)
                    if (s is OpenApiSchema os)
                        FixSchemaEnumWithoutType(os, new HashSet<OpenApiSchema>());

            if (doc.Components.Parameters != null)
                foreach (var p in doc.Components.Parameters.Values)
                {
                    if (p is IOpenApiExtensible pExt)
                        ScrubEnumsInExtensions(pExt);
                    if (p?.Schema is OpenApiSchema pSchema)
                        FixSchemaEnumWithoutType(pSchema, new HashSet<OpenApiSchema>());
                }

            if (doc.Components.RequestBodies != null)
                foreach (var rb in doc.Components.RequestBodies.Values)
                {
                    if (rb is IOpenApiExtensible rbExt)
                        ScrubEnumsInExtensions(rbExt);
                    if (rb?.Content != null)
                        foreach (var mt in rb.Content.Values)
                            if (mt?.Schema is OpenApiSchema mtSchema)
                                FixSchemaEnumWithoutType(mtSchema, new HashSet<OpenApiSchema>());
                }

            if (doc.Components.Responses != null)
                foreach (var r in doc.Components.Responses.Values)
                {
                    if (r is IOpenApiExtensible rExt)
                        ScrubEnumsInExtensions(rExt);
                    if (r?.Content != null)
                        foreach (var mt in r.Content.Values)
                            if (mt?.Schema is OpenApiSchema mtSchema)
                                FixSchemaEnumWithoutType(mtSchema, new HashSet<OpenApiSchema>());
                }

            if (doc.Components.Headers != null)
                foreach (var h in doc.Components.Headers.Values)
                {
                    if (h is IOpenApiExtensible hExt)
                        ScrubEnumsInExtensions(hExt);
                    if (h?.Schema is OpenApiSchema hSchema)
                        FixSchemaEnumWithoutType(hSchema, new HashSet<OpenApiSchema>());
                }
        }
    }

    private static void ScrubEnumsInExtensions(IOpenApiExtensible? target)
    {
        if (target?.Extensions == null || target.Extensions.Count == 0)
            return;

        // Create a list of keys to remove to avoid modification during enumeration
        var keysToRemove = new List<string>();

        foreach (KeyValuePair<string, IOpenApiExtension> kvp in target.Extensions)
        {
            string key = kvp.Key;
            if (!key.StartsWith("x-", StringComparison.Ordinal))
                continue;

            // Mark this extension for removal to avoid enum confusion
            keysToRemove.Add(key);
        }

        // Remove the marked extensions after enumeration is complete
        foreach (string key in keysToRemove)
        {
            target.Extensions.Remove(key);
        }
    }

    private static void FixSchemaEnumWithoutType(OpenApiSchema schema, HashSet<OpenApiSchema> visited)
    {
        if (schema == null || !visited.Add(schema))
            return;

        if (schema.Enum != null && schema.Enum.Count > 0)
        {
            // Never object-ify enums. Prefer to keep primitive type; infer and override if currently object
            bool allStrings = schema.Enum.All(e => e is JsonValue jv && jv.TryGetValue<string>(out _));
            bool allNumbers = schema.Enum.All(e => e is JsonValue jv && (jv.GetValueKind() == JsonValueKind.Number));
            bool allBools = schema.Enum.All(e => e is JsonValue jv && (jv.GetValueKind() == JsonValueKind.True || jv.GetValueKind() == JsonValueKind.False));

            JsonSchemaType desired = JsonSchemaType.String;
            if (allNumbers)
                desired = JsonSchemaType.Number;
            else if (allBools)
                desired = JsonSchemaType.Boolean;

            if (schema.Type == null || schema.Type == JsonSchemaType.Object || schema.Type == JsonSchemaType.Array)
                schema.Type = desired;

            // Ensure no accidental object facets remain on an enum schema
            schema.Properties = null;
            schema.AdditionalProperties = null;
            schema.AdditionalPropertiesAllowed = false;
        }

        if (schema.Properties != null)
            foreach (IOpenApiSchema p in schema.Properties.Values)
                if (p is OpenApiSchema ps)
                    FixSchemaEnumWithoutType(ps, visited);

        if (schema.Items != null && schema.Items is OpenApiSchema items)
            FixSchemaEnumWithoutType(items, visited);

        if (schema.AllOf != null)
            foreach (IOpenApiSchema s in schema.AllOf)
                if (s is OpenApiSchema os)
                    FixSchemaEnumWithoutType(os, visited);

        if (schema.AnyOf != null)
            foreach (IOpenApiSchema s in schema.AnyOf)
                if (s is OpenApiSchema os)
                    FixSchemaEnumWithoutType(os, visited);

        if (schema.OneOf != null)
            foreach (IOpenApiSchema s in schema.OneOf)
                if (s is OpenApiSchema os)
                    FixSchemaEnumWithoutType(os, visited);

        if (schema.AdditionalProperties != null && schema.AdditionalProperties is OpenApiSchema ap)
            FixSchemaEnumWithoutType(ap, visited);
    }

    /// <summary>
    /// Final validation pass to ensure all schema names are valid according to OpenAPI specification.
    /// </summary>
    private void RemoveRedundantDerivedValue(OpenApiDocument doc)
    {
        if (doc.Components?.Schemas == null)
            return;
        IDictionary<string, IOpenApiSchema>? pool = doc.Components.Schemas;

        // ------------- local helpers ------------------------------------------
        static IOpenApiSchema Resolve(IOpenApiSchema s, IDictionary<string, IOpenApiSchema> p) =>
            (s is OpenApiSchemaReference schemaRef && schemaRef.Reference.Id != null && p.TryGetValue(schemaRef.Reference.Id, out IOpenApiSchema? t)) ? t : s;

        static bool IsWellDefined(IOpenApiSchema s) =>
            s.Type != null || s is OpenApiSchemaReference || (s.Enum?.Count ?? 0) > 0 || (s.Items != null) || (s.AllOf?.Count ?? 0) > 0 ||
            (s.OneOf?.Count ?? 0) > 0 || (s.AnyOf?.Count ?? 0) > 0;

        // ------------- main pass ----------------------------------------------
        foreach (IOpenApiSchema container in pool.Values)
        {
            if (container.AllOf is not { Count: > 1 })
                continue;

            // find the FIRST fragment (base or earlier override) that has a well-defined `value`
            IOpenApiSchema? firstValueOwner = null;
            foreach (IOpenApiSchema? frag in container.AllOf.Select(f => Resolve(f, pool)))
            {
                if (frag?.Properties != null && frag.Properties.TryGetValue("value", out IOpenApiSchema? prop) && IsWellDefined(prop))
                {
                    firstValueOwner = frag;
                    break;
                }
            }

            if (firstValueOwner == null)
                continue; // nobody defines `value` in a useful way

            // remove *every* later override of `value`
            foreach (IOpenApiSchema? frag in container.AllOf.Select(f => Resolve(f, pool)))
            {
                if (frag == firstValueOwner)
                    continue; // skip the first one

                if (frag?.Properties?.ContainsKey("value") == true)
                {
                    frag.Properties.Remove("value");
                    frag.Required?.Remove("value");
                    _logger.LogInformation("Removed redundant 'value' property override in schema fragment");
                }
            }
        }
    }

    private void RemoveShadowingUntypedProperties(OpenApiDocument doc)
    {
        if (doc.Components?.Schemas == null)
            return;
        IDictionary<string, IOpenApiSchema>? pool = doc.Components.Schemas;

        static IOpenApiSchema Resolve(IOpenApiSchema s, IDictionary<string, IOpenApiSchema> p) =>
            (s is OpenApiSchemaReference schemaRef && schemaRef.Reference.Id != null && p.TryGetValue(schemaRef.Reference.Id, out IOpenApiSchema? t)) ? t : s;

        static bool IsUntyped(IOpenApiSchema s) =>
            s.Type == null && s is not OpenApiSchemaReference && (s.Enum?.Count ?? 0) == 0 && (s.Items == null) && (s.AllOf?.Count ?? 0) == 0 &&
            (s.OneOf?.Count ?? 0) == 0 && (s.AnyOf?.Count ?? 0) == 0;

        foreach (IOpenApiSchema container in pool.Values)
        {
            // Need: at least one $ref fragment  +  one inline fragment with properties
            if (container.AllOf == null)
                continue;

            IOpenApiSchema? baseFrag = container.AllOf.FirstOrDefault(f => f is OpenApiSchemaReference);
            IOpenApiSchema? overrideFrag = container.AllOf.FirstOrDefault(f => f.Properties?.Count > 0);

            if (baseFrag == null || overrideFrag == null)
                continue;

            IOpenApiSchema? baseSchema = Resolve(baseFrag, pool);
            if (baseSchema?.Properties == null)
                continue;

            foreach ((string? propName, IOpenApiSchema? childProp) in overrideFrag.Properties)
            {
                if (!baseSchema.Properties.TryGetValue(propName, out IOpenApiSchema? baseProp))
                    continue;

                bool childConcrete = !IsUntyped(childProp);
                bool baseIsBare = IsUntyped(baseProp);

                if (baseIsBare)
                {
                    baseSchema.Properties.Remove(propName);
                    baseSchema.Required?.Remove(propName);
                    _logger.LogInformation("Removed untyped shadowed property '{Prop}' from base schema '{Base}' (overridden in '{Child}')", propName,
                        baseSchema.Title ?? "(unnamed)", container.Title ?? "(unnamed)");
                }
            }
        }
    }

    private void DisambiguateMultiContentRequestSchemas(OpenApiDocument document)
    {
        if (document.Paths == null || document.Components?.Schemas == null)
            return;

        IDictionary<string, IOpenApiSchema>? schemas = document.Components.Schemas;
        var renameMap = new Dictionary<string, string>();

        foreach (OpenApiOperation operation in document.Paths.Values.Where(p => p?.Operations != null)
                                                       .SelectMany(p => p.Operations.Values))
        {
            if (operation.RequestBody is OpenApiRequestBodyReference || (operation.RequestBody?.Content?.Count ?? 0) <= 1)
            {
                continue;
            }

            _logger.LogInformation("Found multi-content requestBody in operation '{OperationId}'. Checking for schema renaming.",
                operation.OperationId ?? "unnamed");

            // We must materialize the list to modify it during iteration
            foreach ((string mediaType, IOpenApiMediaType mediaInterface) in operation.RequestBody.Content!.ToList())
            {
                if (mediaInterface is not OpenApiMediaType media)
                    continue;

                if (media.Schema == null)
                    continue;

                // --- THIS IS THE NEW, CORRECT LOGIC ---
                // If the schema is inline (no reference), we must extract it into a component first.
                if (media.Schema is not OpenApiSchemaReference && !_schemaFixer.IsSchemaEmpty(media.Schema))
                {
                    // Create a name for our new component.
                    string mediaName = OpenApiNameNormalizer.NormalizeMediaTypeName(mediaType);
                    string newSchemaName = ReserveUniqueSchemaName(schemas, $"{operation.OperationId ?? "UnnamedOperation"} {mediaName} Request", "RequestBody");

                    _logger.LogInformation("Extracting inline request body schema for '{MediaType}' in operation '{OpId}' to new component '{NewSchemaName}'.",
                        mediaType, operation.OperationId ?? "unnamed", newSchemaName);

                    // Add the inline schema to the components dictionary.
                    if (media.Schema is OpenApiSchema extractedSchema)
                    {
                        // In v2.3, Title is read-only, so we can't modify it directly
                        schemas.Add(newSchemaName, extractedSchema);

                        // Replace the inline schema with a reference to our new component.
                        media.Schema = new OpenApiSchemaReference(newSchemaName);
                    }
                }
                // --- END OF NEW LOGIC ---

                // Now that we can be certain we have a reference, we can check for the name collision.
                if (media.Schema is not OpenApiSchemaReference schemaRef)
                    continue;

                string? originalSchemaName = schemaRef.Reference.Id;

                if (originalSchemaName != null && string.Equals(originalSchemaName, operation.OperationId, StringComparison.OrdinalIgnoreCase))
                {
                    if (renameMap.TryGetValue(originalSchemaName, out string? newName))
                    {
                        // Create a new reference with the updated ID
                        media.Schema = new OpenApiSchemaReference(newName);
                        _logger.LogInformation("Updated reference from '{OldId}' to '{NewId}'", originalSchemaName, newName);
                        continue;
                    }

                    newName = ReserveUniqueSchemaName(schemas, $"{originalSchemaName}Body", "Dto");

                    _logger.LogWarning("CRITICAL COLLISION: Schema '{Original}' (used in {OpId}) matches OperationId. Renaming to '{New}'.", originalSchemaName,
                        operation.OperationId ?? "unnamed", newName);

                    if (schemas.TryGetValue(originalSchemaName, out IOpenApiSchema? schemaToRename))
                    {
                        schemas.Remove(originalSchemaName);
                        schemas.Add(newName, schemaToRename);

                        // Create a new reference with the updated ID
                        media.Schema = new OpenApiSchemaReference(newName);
                        _logger.LogInformation("Updated reference from '{OldId}' to '{NewId}'", originalSchemaName, newName);
                        renameMap[originalSchemaName] = newName;
                    }
                }
            }
        }

        if (renameMap.Any())
        {
            _logger.LogInformation("Applying global reference updates for request body schema collisions...");
            _referenceFixer.UpdateAllReferences(document, renameMap);
        }
    }

    private static string TrimQuotes(string value)
    {
        if (value.Length >= 2 && ((value.StartsWith("\"") && value.EndsWith("\"")) || (value.StartsWith("'") && value.EndsWith("'"))))
        {
            return value.Substring(1, value.Length - 2);
        }

        return value;
    }

    private static bool LooksLikeMalformedStructuredEnumValue(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return false;

        string trimmed = value.Trim();

        if ((trimmed.StartsWith('{') && trimmed.EndsWith('}')) || (trimmed.StartsWith('[') && trimmed.EndsWith(']')))
        {
            try
            {
                JsonNode? parsed = JsonNode.Parse(trimmed);

                if (parsed is JsonObject or JsonArray)
                    return true;
            }
            catch (JsonException)
            {
                // Not actually JSON, continue with heuristic checks below.
            }
        }

        bool hasJsonDelimiters = trimmed.IndexOfAny(['{', '}', '[', ']']) >= 0;
        bool hasQuotedPropertyPattern = trimmed.Contains("\":", StringComparison.Ordinal) || trimmed.Contains("\": ", StringComparison.Ordinal);
        bool hasJsonFragmentMarkers = trimmed.Contains("{{", StringComparison.Ordinal) || trimmed.Contains("}}", StringComparison.Ordinal) ||
                                      trimmed.Contains('\n') || trimmed.Contains('\r');

        return hasQuotedPropertyPattern || (hasJsonDelimiters && hasJsonFragmentMarkers);
    }


    private void RemoveDeprecatedOperationsAndSchemas(OpenApiDocument document)
    {
        if (document == null)
            return;

        int removedOperations = 0;
        int removedPaths = 0;
        int removedSchemas = 0;

        if (document.Paths != null)
        {
            var emptyPathKeys = new List<string>();

            foreach (KeyValuePair<string, IOpenApiPathItem> pathEntry in document.Paths.ToList())
            {
                IOpenApiPathItem? pathItem = pathEntry.Value;

                if (pathItem?.Operations == null || pathItem.Operations.Count == 0)
                    continue;

                List<HttpMethod> deprecatedOperations = pathItem.Operations.Where(op => op.Value?.Deprecated == true)
                                                                .Select(op => op.Key)
                                                                .ToList();

                if (deprecatedOperations.Count == 0)
                    continue;

                foreach (HttpMethod operationType in deprecatedOperations)
                {
                    pathItem.Operations.Remove(operationType);
                    removedOperations++;
                }

                if (pathItem.Operations.Count == 0)
                    emptyPathKeys.Add(pathEntry.Key);
            }

            foreach (string pathKey in emptyPathKeys)
            {
                document.Paths.Remove(pathKey);
                removedPaths++;
            }
        }

        if (document.Components?.Schemas != null)
        {
            HashSet<string> referencedSchemaIds = CollectReferencedSchemaIds(document);

            List<string> deprecatedSchemaKeys = document.Components.Schemas.Where(kvp =>
                                                            kvp.Value is OpenApiSchema schema && schema.Deprecated &&
                                                            !referencedSchemaIds.Contains(kvp.Key))
                                                        .Select(kvp => kvp.Key)
                                                        .ToList();

            foreach (string schemaKey in deprecatedSchemaKeys)
            {
                document.Components.Schemas.Remove(schemaKey);
                removedSchemas++;
            }
        }

        if (removedOperations > 0 || removedPaths > 0 || removedSchemas > 0)
        {
            _logger.LogInformation("Removed deprecated elements. Operations: {OperationCount}, Paths: {PathCount}, Schemas: {SchemaCount}.", removedOperations,
                removedPaths, removedSchemas);
        }
    }

    private static HashSet<string> CollectReferencedSchemaIds(OpenApiDocument document)
    {
        var referencedSchemaIds = new HashSet<string>(StringComparer.Ordinal);
        var visited = new HashSet<IOpenApiSchema>(ReferenceEqualityComparer<IOpenApiSchema>.Instance);

        void VisitSchema(IOpenApiSchema? schema)
        {
            if (schema == null)
                return;

            if (TryGetSchemaRefId(schema, out string? refId) && !string.IsNullOrWhiteSpace(refId))
            {
                referencedSchemaIds.Add(refId);
                return;
            }

            if (schema is not OpenApiSchema concreteSchema || !visited.Add(concreteSchema))
                return;

            if (concreteSchema.Properties != null)
                foreach (IOpenApiSchema child in concreteSchema.Properties.Values)
                    VisitSchema(child);

            if (concreteSchema.Items != null)
                VisitSchema(concreteSchema.Items);

            if (concreteSchema.AdditionalProperties != null)
                VisitSchema(concreteSchema.AdditionalProperties);

            if (concreteSchema.AllOf != null)
                foreach (IOpenApiSchema child in concreteSchema.AllOf)
                    VisitSchema(child);

            if (concreteSchema.AnyOf != null)
                foreach (IOpenApiSchema child in concreteSchema.AnyOf)
                    VisitSchema(child);

            if (concreteSchema.OneOf != null)
                foreach (IOpenApiSchema child in concreteSchema.OneOf)
                    VisitSchema(child);

            if (concreteSchema.Not != null)
                VisitSchema(concreteSchema.Not);
        }

        void VisitContent(IDictionary<string, IOpenApiMediaType>? content)
        {
            if (content == null)
                return;

            foreach (IOpenApiMediaType mediaType in content.Values)
                VisitSchema(mediaType?.Schema);
        }

        void VisitParameter(IOpenApiParameter? parameter)
        {
            if (parameter is not OpenApiParameter concreteParameter)
                return;

            VisitSchema(concreteParameter.Schema);
            VisitContent(concreteParameter.Content);
        }

        void VisitHeader(IOpenApiHeader? header)
        {
            if (header is not OpenApiHeader concreteHeader)
                return;

            VisitSchema(concreteHeader.Schema);
            VisitContent(concreteHeader.Content);
        }

        void VisitRequestBody(IOpenApiRequestBody? requestBody)
        {
            if (requestBody is OpenApiRequestBody concreteRequestBody)
                VisitContent(concreteRequestBody.Content);
        }

        void VisitResponse(IOpenApiResponse? response)
        {
            if (response is not OpenApiResponse concreteResponse)
                return;

            VisitContent(concreteResponse.Content);

            if (concreteResponse.Headers != null)
                foreach (IOpenApiHeader header in concreteResponse.Headers.Values)
                    VisitHeader(header);
        }

        if (document.Components?.Schemas != null)
            foreach (IOpenApiSchema schema in document.Components.Schemas.Values)
                VisitSchema(schema);

        if (document.Components?.Parameters != null)
            foreach (IOpenApiParameter parameter in document.Components.Parameters.Values)
                VisitParameter(parameter);

        if (document.Components?.Headers != null)
            foreach (IOpenApiHeader header in document.Components.Headers.Values)
                VisitHeader(header);

        if (document.Components?.RequestBodies != null)
            foreach (IOpenApiRequestBody requestBody in document.Components.RequestBodies.Values)
                VisitRequestBody(requestBody);

        if (document.Components?.Responses != null)
            foreach (IOpenApiResponse response in document.Components.Responses.Values)
                VisitResponse(response);

        if (document.Paths == null)
            return referencedSchemaIds;

        foreach (IOpenApiPathItem pathItem in document.Paths.Values)
        {
            if (pathItem?.Parameters != null)
                foreach (IOpenApiParameter parameter in pathItem.Parameters)
                    VisitParameter(parameter);

            if (pathItem?.Operations == null)
                continue;

            foreach (OpenApiOperation operation in pathItem.Operations.Values)
            {
                if (operation?.Parameters != null)
                    foreach (IOpenApiParameter parameter in operation.Parameters)
                        VisitParameter(parameter);

                VisitRequestBody(operation?.RequestBody);

                if (operation?.Responses != null)
                    foreach (IOpenApiResponse response in operation.Responses.Values)
                        VisitResponse(response);
            }
        }

        return referencedSchemaIds;
    }


    private void LogState(string stage, OpenApiDocument document)
    {
        // LogState is disabled by default (_logState = false)
        // Uncomment the following code if debugging is needed:
        /*
        if (document?.Components?.Schemas?.TryGetValue("CreateDocument", out IOpenApiSchema? schema) == true)
        {
            _logger.LogWarning("DEBUG >>> STAGE: CreateDocument is FOUND");
        }
        else
        {
            _logger.LogWarning("DEBUG >>> STAGE: CreateDocument value not found. Stage: {Stage}", stage);
        }
        */
    }


    private static IList<IOpenApiSchema>? RemoveRedundantEmptyEnums(IList<IOpenApiSchema>? list, Func<OpenApiSchema, bool> isRedundant)
    {
        if (list == null || list.Count == 0)
            return list;

        List<IOpenApiSchema> kept = list.Where(b => b is not OpenApiSchema concreteB || !isRedundant(concreteB))
                                        .ToList();
        return kept.Count == 0 ? null : kept;
    }

    private void StripEmptyEnumBranches(OpenApiDocument document)
    {
        if (document.Components?.Schemas == null)
            return;

        var visited = new HashSet<OpenApiSchema>();
        var queue = new Queue<OpenApiSchema>(document.Components.Schemas.Values.OfType<OpenApiSchema>());

        static bool IsTrulyRedundantEmptyEnum(OpenApiSchema s) =>
            s.Enum != null && s.Enum.Count == 0 && s.Type == null && (s.Properties == null || s.Properties.Count == 0) && s.Items == null &&
            s.AdditionalProperties == null && s.OneOf == null && s.AnyOf == null && s.AllOf == null;

        while (queue.Count > 0)
        {
            OpenApiSchema? schema = queue.Dequeue();
            if (schema == null || !visited.Add(schema))
                continue;

            schema.OneOf = RemoveRedundantEmptyEnums(schema.OneOf?.ToList(), IsTrulyRedundantEmptyEnum)
                ?.ToList();
            schema.AnyOf = RemoveRedundantEmptyEnums(schema.AnyOf?.ToList(), IsTrulyRedundantEmptyEnum)
                ?.ToList();
            schema.AllOf = RemoveRedundantEmptyEnums(schema.AllOf?.ToList(), IsTrulyRedundantEmptyEnum)
                ?.ToList();

            if (schema.Properties != null)
                foreach (IOpenApiSchema? p in schema.Properties.Values)
                    if (p is OpenApiSchema concreteP)
                        queue.Enqueue(concreteP);
            if (schema.Items is OpenApiSchema concreteItems)
                queue.Enqueue(concreteItems);
            if (schema.AllOf != null)
                foreach (IOpenApiSchema? b in schema.AllOf)
                    if (b is OpenApiSchema concreteB)
                        queue.Enqueue(concreteB);
            if (schema.OneOf != null)
                foreach (IOpenApiSchema? b in schema.OneOf)
                    if (b is OpenApiSchema concreteB)
                        queue.Enqueue(concreteB);
            if (schema.AnyOf != null)
                foreach (IOpenApiSchema? b in schema.AnyOf)
                    if (b is OpenApiSchema concreteB)
                        queue.Enqueue(concreteB);
            if (schema.AdditionalProperties is OpenApiSchema concreteAdditional)
                queue.Enqueue(concreteAdditional);
        }
    }

    private async ValueTask<MemoryStream> PreprocessSpecFile(string path, OpenApiFixerOptions? options = null, CancellationToken cancellationToken = default)
    {
        string raw = await _fileUtil.Read(path, cancellationToken: cancellationToken);

        //raw = Regex.Replace(raw, @"\{\s*""\$ref""\s*:\s*""(?<id>[^""#/][^""]*)""\s*\}",
        //    m => $"{{ \"$ref\": \"#/components/schemas/{m.Groups["id"].Value}\" }}");

        raw = _preprocessingFixer.Fix(raw, options);

        return new MemoryStream(Encoding.UTF8.GetBytes(raw));
    }

    private async ValueTask<OpenApiSpecVersion> DetectSpecVersion(string path, CancellationToken cancellationToken)
    {
        string raw = await _fileUtil.Read(path, cancellationToken: cancellationToken);
        string? version = null;

        try
        {
            if (JsonNode.Parse(raw) is JsonObject root)
                version = root["openapi"]?.GetValue<string>() ?? root["swagger"]?.GetValue<string>();
        }
        catch (JsonException)
        {
            Match match = Regex.Match(raw, @"(?m)^\s*(?:openapi|swagger)\s*:\s*['\""']?(?<version>\d+\.\d+(?:\.\d+)?)");

            if (match.Success)
                version = match.Groups["version"].Value;
        }

        if (!Version.TryParse(version, out Version? parsed))
            throw new InvalidOperationException($"Unable to determine the OpenAPI version of '{path}'.");

        return (parsed.Major, parsed.Minor) switch
        {
            (2, _) => OpenApiSpecVersion.OpenApi2_0,
            (3, 0) => OpenApiSpecVersion.OpenApi3_0,
            (3, 1) => OpenApiSpecVersion.OpenApi3_1,
            (3, 2) => OpenApiSpecVersion.OpenApi3_2,
            _ => throw new NotSupportedException($"OpenAPI version '{version}' is not supported.")
        };
    }

    private static Dictionary<string, string> AttachWebhooksToPaths(OpenApiDocument document)
    {
        var attached = new Dictionary<string, string>(StringComparer.Ordinal);

        if (document.Webhooks is not { Count: > 0 })
            return attached;

        document.Paths ??= new OpenApiPaths();
        var index = 0;

        foreach ((string webhookName, IOpenApiPathItem pathItem) in document.Webhooks)
        {
            string syntheticPath;

            do
            {
                syntheticPath = $"/__openapi_fixer_webhooks/event-{index++}";
            } while (document.Paths.ContainsKey(syntheticPath));

            document.Paths.Add(syntheticPath, pathItem);
            attached.Add(syntheticPath, webhookName);
        }

        return attached;
    }

    private static void DetachWebhooksFromPaths(OpenApiDocument document, IReadOnlyDictionary<string, string> attachedWebhooks)
    {
        if (attachedWebhooks.Count == 0 || document.Paths == null)
            return;

        document.Webhooks ??= new Dictionary<string, IOpenApiPathItem>();

        foreach ((string syntheticPath, string webhookName) in attachedWebhooks)
        {
            if (!document.Paths.Remove(syntheticPath, out IOpenApiPathItem? pathItem))
            {
                document.Webhooks.Remove(webhookName);
                continue;
            }

            document.Webhooks[webhookName] = pathItem;
        }
    }

}
