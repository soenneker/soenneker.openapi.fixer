using Microsoft.Extensions.Logging;
using Microsoft.OpenApi;
using Microsoft.OpenApi.Reader;
using Soenneker.Extensions.ValueTask;
using Soenneker.OpenApi.Fixer.Abstract;
using Soenneker.Utils.Process.Abstract;
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
using System.Runtime.CompilerServices;
using Soenneker.Extensions.Task;

namespace Soenneker.OpenApi.Fixer;

///<inheritdoc cref="IOpenApiFixer"/>
public sealed class OpenApiFixer : IOpenApiFixer
{
    private readonly ILogger<OpenApiFixer> _logger;

    private readonly IProcessUtil _processUtil;

    public OpenApiFixer(ILogger<OpenApiFixer> logger, IProcessUtil processUtil)
    {
        _logger = logger;
        _processUtil = processUtil;
    }

    public async ValueTask Fix(string sourceFilePath, string targetFilePath, CancellationToken cancellationToken = default)
    {
        try
        {
            // STAGE 0: DOCUMENT LOADING & INITIAL PARSING
            await ReadAndValidateOpenApi(sourceFilePath, cancellationToken)
                .NoSync();
            await using MemoryStream pre = await PreprocessSpecFile(sourceFilePath, cancellationToken);
            (OpenApiDocument? document, OpenApiDiagnostic? diagnostics) = await OpenApiDocument.LoadAsync(pre, cancellationToken: cancellationToken)
                                                                                               .NoSync();

            if (diagnostics?.Errors?.Any() == true)
            {
                string msgs = string.Join("; ", diagnostics.Errors.Select(e => e.Message));
                _logger.LogWarning($"OpenAPI parsing errors during loading: {msgs}");
            }

            LogState("After STAGE 0: Initial Load", document!);

            // STAGE 1: IDENTIFIERS, NAMING, AND SECURITY
            _logger.LogInformation("Running initial cleanup on identifiers, paths, and security schemes...");
            FixYamlUnsafeDescriptions(document!);
            EnsureSecuritySchemes(document!);
            RenameConflictingPaths(document!);

            RenameInvalidComponentSchemas(document!);

            _logger.LogInformation("Resolving collisions between operation IDs and schema names...");
            ResolveSchemaOperationNameCollisions(document!);

            _logger.LogInformation("Ensuring unique operation IDs...");
            _logger.LogInformation("Normalizing operation IDs...");
            NormalizeOperationIds(document!);

            EnsureUniqueOperationIds(document!);

            // STAGE 2: REFERENCE INTEGRITY & SCRUBBING
            _logger.LogInformation("Scrubbing all component references to fix broken links...");
            ScrubComponentRefs(document!, cancellationToken);
            LogState("After STAGE 2: Ref Scrubbing", document!);

            // STAGE 3: STRUCTURAL TRANSFORMATIONS
            _logger.LogInformation("Performing major structural transformations (inlining, extraction)...");
            InlinePrimitiveComponents(document!);
            DisambiguateMultiContentRequestSchemas(document!);

            FixContentTypeWrapperCollisions(document!);

            _logger.LogInformation("Removing deprecated operations and schemas...");
            RemoveDeprecatedOperationsAndSchemas(document!);

            // Harden primitive/enum request bodies by wrapping into small objects to avoid Kiota regressions
            WrapPrimitiveRequestBodies(document!);

            ExtractInlineArrayItemSchemas(document!);
            ExtractInlineSchemas(document!, cancellationToken);
            LogState("After STAGE 3A: Transformations", document!);

            EnsureDiscriminatorForOneOf(document!);

            _logger.LogInformation("Removing shadowed untyped properties…");
            RemoveShadowingUntypedProperties(document!);
            RemoveRedundantDerivedValue(document!);

            _logger.LogInformation("Re-scrubbing references after extraction...");
            ScrubComponentRefs(document!, cancellationToken);
            LogState("After STAGE 3B: Re-Scrubbing", document!);

            // STAGE 4: DEEP SCHEMA NORMALIZATION & CLEANING
            _logger.LogInformation("Applying deep schema normalizations and cleaning...");

            // MergeAmbiguousOneOfSchemas(document);
            LogState("After STAGE 4A: MergeAmbiguousOneOfSchemas", document!);

            ApplySchemaNormalizations(document!, cancellationToken);
            LogState("After STAGE 4B: ApplySchemaNormalizations", document!);

            FixErrorMessageArrayCollision(document!);
            LogState("FixErrorMessageArrayCollision", document!);

            //SetExplicitNullabilityOnAllSchemas(document); // This now contains the robust fix
            // LogState("After STAGE 4C: SetExplicitNullability", document!);

            if (document!.Components?.Schemas != null)
            {
                foreach (IOpenApiSchema schema in document.Components.Schemas.Values)
                {
                    if (schema is OpenApiSchema concreteSchema)
                    {
                        DeepCleanSchema(concreteSchema, []);
                    }
                }
            }

            LogState("After STAGE 4D: Deep Cleaning", document);

            FixMalformedEnumValues(document);
            LogState("After STAGE 4E.1: FixMalformedEnumValues", document);

            StripEmptyEnumBranches(document);
            LogState("After STAGE 4E: StripEmptyEnumBranches", document);

            FixInvalidDefaults(document);
            LogState("After STAGE 4F: FixInvalidDefaults", document);

            FixAllInlineValueEnums(document);
            LogState("After STAGE 4G: FixAllInlineValueEnums", document);

            PromoteEnumBranchesUnderDiscriminator(document);
            LogState("After STAGE 4H: PromoteEnumBranchesUnderDiscriminator", document);

            WrapEnumBranchesInCompositions(document);
            LogState("After STAGE 4H.1: WrapEnumBranchesInCompositions", document);

            // Re-scrub references after creating new wrapper components
            ScrubComponentRefs(document, cancellationToken);
            LogState("After STAGE 4I: Re-Scrub After Enum Promotion", document);

            // STAGE 5: FINAL CLEANUP
            _logger.LogInformation("Performing final cleanup of empty keys and invalid structures...");
            RemoveEmptyInlineSchemas(document);
            RemoveInvalidDefaults(document);

            LogState("After STAGE 5: Final Cleanup", document);

            // STAGE 6: FINAL VALIDATION AND CLEANUP
            _logger.LogInformation("Final validation and cleanup process started...");

            // Scrub bogus enums under vendor extensions and harden enum schemas missing type
            FixBadEnums(document);

            // Normalize boolean enums (enum: [true|false]) into plain booleans, set default for singletons
            NormalizeBooleanEnums(document);

            // Fix discriminator mappings that reference non-existent or enum schemas
            FixDiscriminatorMappingsForEnums(document);

            // Fix properties declared as object that actually allOf an enum schema
            FixEnumAllOfObjectPropertyMismatch(document);

            // Blanket safety: wrap any enum-like or primitive branches in unions so Kiota always sees classes
            ComprehensiveEnumWrapperFix(document);

            // Ensure discriminator holders (including nested additionalProperties) declare and require the discriminator property
            _logger.LogInformation("Ensuring discriminator property is required on all schemas with a discriminator...");
            EnsureDiscriminatorRequiredEverywhere(document);

            // Replace $refs that drill into #/paths/.../examples/... with component schema refs
            FixRefsPointingIntoPathsExamples(document);

            // Remove empty-string enum values that cause empty child names in generators
            StripEmptyStringEnumValues(document);

            // Final safety net: ensure no union branch is a non-object (enums, primitives, arrays)
            WrapNonObjectUnionBranchesEverywhere(document);
            NormalizeAllOfWrappers(document);

            InlinePrimitivePropertyRefs(document);
            EnsureInlineObjectTypes(document!);

            CleanDocumentForSerialization(document);

            LogDanglingOrPrimitivePropertyRefs(document!);

            // Run discriminator-required pass one last time to guarantee required flags are present
            EnsureDiscriminatorRequiredEverywhere(document);

            // Final validation: ensure all schema names are valid
            ValidateAndFixSchemaNames(document);

            string json = await document.SerializeAsync(OpenApiSpecVersion.OpenApi3_0, OpenApiConstants.Json, cancellationToken: cancellationToken);

            // Fix JSON boolean values (convert Python-style True/False to JSON true/false)
            json = FixJsonBooleanValues(json);

            await File.WriteAllTextAsync(targetFilePath, json, cancellationToken);

            _logger.LogInformation($"Cleaned OpenAPI spec saved to {targetFilePath}");
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("OpenAPI fix was canceled.");
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during OpenAPI fix");
            throw;
        }

        await ReadAndValidateOpenApi(targetFilePath, cancellationToken)
            .NoSync();
    }

    private void LogDanglingOrPrimitivePropertyRefs(OpenApiDocument doc)
    {
        IDictionary<string, IOpenApiSchema> comps = doc.Components?.Schemas ?? new Dictionary<string, IOpenApiSchema>();

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

            if (s is OpenApiSchema os && os.Properties != null)
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
                var expectedWrapperName = $"{op.OperationId!}{media.Replace('/', '_')}";
                if (doc.Components.Schemas.ContainsKey(expectedWrapperName))
                {
                    string newName = ReserveUniqueSchemaName(doc.Components.Schemas, expectedWrapperName, "Body");
                    _logger.LogWarning("Schema '{Old}' collides with Kiota wrapper in operation '{Op}'. Renaming to '{New}'.", expectedWrapperName,
                        op.OperationId!, newName);

                    renameMap[expectedWrapperName] = newName;
                }
            }
        }

        if (renameMap.Count > 0)
            UpdateAllReferences(doc, renameMap); // you already have this helper
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
                foreach (var media in op.RequestBody.Content.Values.OfType<OpenApiMediaType>())
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
                            foreach (var media in rb.Content.Values.OfType<OpenApiMediaType>())
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
                                    foreach (var media in resp.Content.Values.OfType<OpenApiMediaType>())
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
    /// This catches any schemas that might have been added after the initial renaming pass.
    /// </summary>
    private void ValidateAndFixSchemaNames(OpenApiDocument doc)
    {
        IDictionary<string, IOpenApiSchema>? schemas = doc.Components?.Schemas;
        if (schemas == null)
            return;

        var mapping = new Dictionary<string, string>();
        var existingKeys = new HashSet<string>(schemas.Keys, StringComparer.OrdinalIgnoreCase);

        foreach (string key in schemas.Keys.ToList())
        {
            if (!IsValidIdentifier(key))
            {
                string baseName = SanitizeName(key);
                if (string.IsNullOrWhiteSpace(baseName))
                    baseName = "Schema";

                string newKey = baseName;
                var i = 1;
                while (existingKeys.Contains(newKey))
                {
                    newKey = $"{baseName}_{i++}";
                }

                mapping[key] = newKey;
                existingKeys.Add(newKey);
            }
        }

        if (mapping.Count > 0)
        {
            foreach ((string oldKey, string newKey) in mapping)
            {
                IOpenApiSchema schema = schemas[oldKey];
                schemas.Remove(oldKey);
                schemas[newKey] = schema;
            }

            UpdateAllReferences(doc, mapping);
        }
    }

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
            foreach (var (mediaType, mediaInterface) in operation.RequestBody.Content!.ToList())
            {
                if (mediaInterface is not OpenApiMediaType media)
                    continue;

                if (media.Schema == null)
                    continue;

                // --- THIS IS THE NEW, CORRECT LOGIC ---
                // If the schema is inline (no reference), we must extract it into a component first.
                if (media.Schema is not OpenApiSchemaReference && !IsSchemaEmpty(media.Schema))
                {
                    // Create a name for our new component.
                    string newSchemaName =
                        ReserveUniqueSchemaName(schemas, $"{operation.OperationId ?? "unnamed"}{mediaType.Replace("/", "_")}", "RequestBody");

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
            UpdateAllReferences(document, renameMap);
        }
    }

    private void ReplaceAllRefs(OpenApiDocument document, string oldKey, string newKey)
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


    private static string TrimQuotes(string value)
    {
        if (value.Length >= 2 && ((value.StartsWith("\"") && value.EndsWith("\"")) || (value.StartsWith("'") && value.EndsWith("'"))))
        {
            return value.Substring(1, value.Length - 2);
        }

        return value;
    }

    private void RemoveInvalidDefaults(OpenApiDocument document)
    {
        if (document.Components?.Schemas == null)
            return;

        foreach (IOpenApiSchema? schema in document.Components.Schemas.Values)
        {
            if (schema is OpenApiSchema concreteSchema && concreteSchema.Type == JsonSchemaType.Object && concreteSchema.Default != null &&
                concreteSchema.Default is not JsonObject)
            {
                _logger.LogWarning("Removing invalid default ({Default}) from object schema '{Schema}'", concreteSchema.Default,
                    concreteSchema.Title ?? "(no title)");
                concreteSchema.Default = null;
            }
        }
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

            foreach (var pathEntry in document.Paths.ToList())
            {
                var pathItem = pathEntry.Value;

                if (pathItem?.Operations == null || pathItem.Operations.Count == 0)
                    continue;

                var deprecatedOperations = pathItem.Operations.Where(op => op.Value?.Deprecated == true)
                                                   .Select(op => op.Key)
                                                   .ToList();

                if (deprecatedOperations.Count == 0)
                    continue;

                foreach (var operationType in deprecatedOperations)
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
            var deprecatedSchemaKeys = document.Components.Schemas.Where(kvp => kvp.Value is OpenApiSchema schema && schema.Deprecated)
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

    private void RemoveEmptyInlineSchemas(OpenApiDocument document)
    {
        if (document.Components?.Schemas == null)
            return;

        var visited = new HashSet<OpenApiSchema>();
        foreach (IOpenApiSchema? schema in document.Components.Schemas.Values)
            if (schema is OpenApiSchema concreteSchema)
                Clean(concreteSchema, visited);
    }

    private void Clean(OpenApiSchema schema, HashSet<OpenApiSchema> visited)
    {
        if (schema == null || !visited.Add(schema))
            return;

        if (schema.AllOf != null)
        {
            schema.AllOf = schema.AllOf.Where(child => child != null && (child is OpenApiSchemaReference || !IsSchemaEmpty(child)))
                                 .ToList();
        }

        if (schema.OneOf != null)
        {
            schema.OneOf = schema.OneOf.Where(child => child != null && (child is OpenApiSchemaReference || !IsSchemaEmpty(child)))
                                 .ToList();
        }

        if (schema.AnyOf != null)
        {
            schema.AnyOf = schema.AnyOf.Where(child => child != null && (child is OpenApiSchemaReference || !IsSchemaEmpty(child)))
                                 .ToList();
        }

        if (schema.AllOf != null)
        {
            foreach (IOpenApiSchema? child in schema.AllOf)
                if (child is OpenApiSchema concreteChild)
                    Clean(concreteChild, visited);
        }

        if (schema.OneOf != null)
        {
            foreach (IOpenApiSchema? child in schema.OneOf)
                if (child is OpenApiSchema concreteChild)
                    Clean(concreteChild, visited);
        }

        if (schema.AnyOf != null)
        {
            foreach (IOpenApiSchema? child in schema.AnyOf)
                if (child is OpenApiSchema concreteChild)
                    Clean(concreteChild, visited);
        }

        if (schema.Properties != null)
        {
            foreach (IOpenApiSchema? prop in schema.Properties.Values)
                if (prop is OpenApiSchema concreteProp)
                    Clean(concreteProp, visited);
        }

        if (schema.Items is OpenApiSchema concreteItems)
        {
            Clean(concreteItems, visited);
        }

        if (schema.AdditionalProperties is OpenApiSchema concreteAdditional)
        {
            Clean(concreteAdditional, visited);
        }
    }

    private bool IsSchemaEmpty(IOpenApiSchema schema)
    {
        if (schema == null)
            return true;

        bool hasContent = schema is OpenApiSchemaReference || schema.Type != null || (schema.Properties?.Any() ?? false) || (schema.AllOf?.Any() ?? false) ||
                          (schema.OneOf?.Any() ?? false) || (schema.AnyOf?.Any() ?? false) || (schema.Enum?.Any() ?? false) || schema.Items != null ||
                          schema.AdditionalProperties != null || schema.AdditionalPropertiesAllowed;

        return !hasContent;
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

    private void DeepCleanSchema(OpenApiSchema? schema, HashSet<OpenApiSchema> visited)
    {
        if (schema == null || !visited.Add(schema))
        {
            return;
        }

        SanitizeExample(schema);

        if (schema.Default is JsonValue ds && ds.TryGetValue(out string? dsValue) && string.IsNullOrEmpty(dsValue))
        {
            schema.Default = null;
        }

        if (schema.Example is JsonValue es && es.TryGetValue(out string? esValue) && string.IsNullOrEmpty(esValue))
        {
            schema.Example = null;
        }

        if (schema.Enum != null && schema.Enum.Any())
        {
            List<JsonNode> cleanedEnum = schema.Enum.OfType<JsonValue>()
                                               .Where(s =>
                                               {
                                                   // Accept any non-null enum value (string, number, boolean, etc.)
                                                   return s.GetValueKind() != JsonValueKind.Null;
                                               })
                                               .Select(s =>
                                               {
                                                   // Preserve the original value type
                                                   return s;
                                               })
                                               .Cast<JsonNode>()
                                               .ToList();

            schema.Enum = cleanedEnum.Any() ? cleanedEnum : null;
        }

        if (schema.Properties != null)
        {
            foreach (IOpenApiSchema? p in schema.Properties.Values)
            {
                if (p is OpenApiSchema concreteP)
                    DeepCleanSchema(concreteP, visited);
            }
        }

        if (schema.Items != null)
        {
            if (schema.Items is OpenApiSchema concreteItems)
                DeepCleanSchema(concreteItems, visited);
        }

        if (schema.AdditionalProperties != null)
        {
            if (schema.AdditionalProperties is OpenApiSchema concreteAdditional)
                DeepCleanSchema(concreteAdditional, visited);
        }
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

    private async ValueTask<MemoryStream> PreprocessSpecFile(string path, CancellationToken cancellationToken = default)
    {
        string raw = await File.ReadAllTextAsync(path, Encoding.UTF8, cancellationToken);

        //raw = Regex.Replace(raw, @"\{\s*""\$ref""\s*:\s*""(?<id>[^""#/][^""]*)""\s*\}",
        //    m => $"{{ \"$ref\": \"#/components/schemas/{m.Groups["id"].Value}\" }}");

        return new MemoryStream(Encoding.UTF8.GetBytes(raw));
    }

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
            void InlineParameter(OpenApiParameter? param)
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
                foreach (OpenApiResponse? resp in document.Components.Responses.Values)
                {
                    if (resp?.Content != null)
                        foreach (OpenApiMediaType? mt in resp.Content.Values)
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
                foreach (OpenApiHeader? hdr in document.Components.Headers.Values)
                    if (hdr?.Schema is OpenApiSchema concreteSchema)
                        ReplaceRef(concreteSchema);

            // 6. Inline component‐level parameters
            if (document.Components.Parameters != null)
                foreach (OpenApiParameter? compParam in document.Components.Parameters.Values)
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
                    (rs.Type == null || (rs.Type != JsonSchemaType.Object && (rs.Properties == null || rs.Properties.Count == 0))))
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
                for (int i = 0; i < branches.Count; i++)
                {
                    IOpenApiSchema b = branches[i];

                    string? refId = (b as OpenApiSchemaReference)?.Reference.Id;
                    IOpenApiSchema resolved = b;
                    if (refId != null && comps.TryGetValue(refId, out IOpenApiSchema? compSchema))
                        resolved = compSchema;

                    if (resolved is OpenApiSchema rs && rs.Enum is { Count: > 0 } &&
                        (rs.Type == null || (rs.Type != JsonSchemaType.Object && (rs.Properties == null || rs.Properties.Count == 0))))
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
    /// Quotes descriptions (and similar text fields) that contain YAML-invalid
    /// mapping expressions like "Deprecated: something". These must be quoted
    /// because YAML interprets a colon + space as a new key-value mapping.
    /// </summary>
    private void FixYamlUnsafeDescriptions(OpenApiDocument document)
    {
        if (document == null)
            return;

        // Fix top-level descriptions
        document.Info.Description = FixYamlUnsafeString(document.Info.Description);

        // Fix all components (schemas, parameters, responses, etc.)
        if (document.Components != null)
        {
            foreach (var schema in document.Components.Schemas.Values)
                FixSchemaDescriptions(schema);

            foreach (var parameter in document.Components.Parameters.Values)
                FixParameterDescriptions(parameter);

            foreach (var response in document.Components.Responses.Values)
                FixResponseDescriptions(response);

            foreach (var requestBody in document.Components.RequestBodies.Values)
                FixRequestBodyDescriptions(requestBody);
        }

        // Fix all paths & operations
        foreach (var path in document.Paths.Values)
            FixPathItemDescriptions(path);
    }

    private static string? FixYamlUnsafeString(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return value;

        // YAML forbids unquoted "key: value" patterns in many scalar contexts
        // We detect colon+space OR leading/trailing colon situations.
        bool containsYamlMappingPattern = value.Contains(": ", StringComparison.Ordinal) || value.EndsWith(":", StringComparison.Ordinal) ||
                                          value.StartsWith(":", StringComparison.Ordinal);

        if (!containsYamlMappingPattern)
            return value;

        // Already quoted? Skip.
        if ((value.StartsWith("\"") && value.EndsWith("\"")) || (value.StartsWith("'") && value.EndsWith("'")))
            return value;

        // Escape internal quotes minimally but safely
        string escaped = value.Replace("\"", "\\\"");

        return $"\"{escaped}\"";
    }

    private void FixSchemaDescriptions(IOpenApiSchema schema)
    {
        if (schema == null)
            return;

        schema.Description = FixYamlUnsafeString(schema.Description);

        foreach (var prop in schema.Properties.Values)
        {
            prop.Description = FixYamlUnsafeString(prop.Description);
            FixSchemaDescriptions(prop); // recursive
        }

        // Handle `items`, `additionalProperties`
        FixSchemaDescriptions(schema.Items);
        if (schema.AdditionalProperties is OpenApiSchema addl)
            FixSchemaDescriptions(addl);

        // Handle oneOf/anyOf/allOf
        foreach (var s in schema.OneOf)
            FixSchemaDescriptions(s);
        foreach (var s in schema.AnyOf)
            FixSchemaDescriptions(s);
        foreach (var s in schema.AllOf)
            FixSchemaDescriptions(s);
    }

    private void FixPathItemDescriptions(IOpenApiPathItem item)
    {
        if (item == null)
            return;

        item.Description = FixYamlUnsafeString(item.Description);
        item.Summary = FixYamlUnsafeString(item.Summary);

        foreach (var op in item.Operations.Values)
            FixOperationDescriptions(op);
    }

    private void FixOperationDescriptions(OpenApiOperation op)
    {
        if (op == null)
            return;

        op.Description = FixYamlUnsafeString(op.Description);
        op.Summary = FixYamlUnsafeString(op.Summary);

        foreach (var param in op.Parameters)
            FixParameterDescriptions(param);

        FixRequestBodyDescriptions(op.RequestBody);

        foreach (var resp in op.Responses.Values)
            FixResponseDescriptions(resp);
    }

    private void FixParameterDescriptions(IOpenApiParameter parameter)
    {
        if (parameter == null)
            return;

        parameter.Description = FixYamlUnsafeString(parameter.Description);
        FixSchemaDescriptions(parameter.Schema);
    }

    private void FixRequestBodyDescriptions(IOpenApiRequestBody body)
    {
        if (body == null)
            return;

        body.Description = FixYamlUnsafeString(body.Description);

        foreach (var content in body.Content.Values)
            FixSchemaDescriptions(content.Schema);
    }

    private void FixResponseDescriptions(IOpenApiResponse response)
    {
        if (response == null)
            return;

        response.Description = FixYamlUnsafeString(response.Description);

        foreach (var content in response.Content.Values)
            FixSchemaDescriptions(content.Schema);
    }

    private void RenameInvalidComponentSchemas(OpenApiDocument document)
    {
        IDictionary<string, IOpenApiSchema>? schemas = document.Components?.Schemas;
        if (schemas == null)
            return;

        var mapping = new Dictionary<string, string>();
        var existingKeys = new HashSet<string>(schemas.Keys, StringComparer.OrdinalIgnoreCase);

        foreach (string key in schemas.Keys.ToList())
        {
            // Use strict validation suitable for Kiota: only [A-Za-z0-9_] and starts with a letter.
            // Also rename if our canonical validation would change the name.
            string validated = ValidateComponentName(key);
            bool needsRename = !IsValidIdentifier(key) || !string.Equals(validated, key, StringComparison.Ordinal);

            if (needsRename)
            {
                _logger.LogInformation("Found invalid schema name '{InvalidName}', sanitizing...", key);
                string baseName = validated;

                // Fallback to "Schema" if sanitization fails
                if (string.IsNullOrWhiteSpace(baseName))
                    baseName = "Schema";

                string newKey = baseName;
                var i = 1;

                // Ensure uniqueness deterministically
                while (existingKeys.Contains(newKey))
                {
                    newKey = $"{baseName}_{i++}";
                }

                mapping[key] = newKey;
                existingKeys.Add(newKey);
                _logger.LogInformation("Renamed schema '{OldName}' to '{NewName}'", key, newKey);
            }
        }

        foreach ((string oldKey, string newKey) in mapping)
        {
            IOpenApiSchema schema = schemas[oldKey];
            schemas.Remove(oldKey);

            // In v2.3, Title is read-only, so we can't modify it directly
            // The schema will keep its original title

            schemas[newKey] = schema;
        }

        if (mapping.Count > 0)
            UpdateAllReferences(document, mapping);
    }

    private void ApplySchemaNormalizations(OpenApiDocument document, CancellationToken cancellationToken)
    {
        if (document?.Components?.Schemas == null)
            return;

        IDictionary<string, IOpenApiSchema>? comps = document.Components.Schemas;

        // Helper to determine if a schema (or any of its referenced/composed branches) is object-like
        bool IsObjectLike(IOpenApiSchema s)
        {
            if (s is OpenApiSchemaReference sr && sr.Reference.Id != null && comps.TryGetValue(sr.Reference.Id, out IOpenApiSchema? resolved))
                return IsObjectLike(resolved);
            if (s is OpenApiSchema os)
            {
                if (os.Type == JsonSchemaType.Object)
                    return true;
                if (os.Properties?.Any() == true)
                    return true;
                if (os.AdditionalProperties != null || os.AdditionalPropertiesAllowed)
                    return true;
                if (os.AllOf != null && os.AllOf.Any(IsObjectLike))
                    return true;
                if (os.AnyOf != null && os.AnyOf.Any(IsObjectLike))
                    return true;
                if (os.OneOf != null && os.OneOf.Any(IsObjectLike))
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
                RemoveEmptyCompositionObjects(concreteSchema, visited);
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

            if ((schema.OneOf?.Any() == true || schema.AnyOf?.Any() == true) && schema.Discriminator == null)
            {
                const string discName = "type";
                if (schema is OpenApiSchema concreteSchema1)
                {
                    concreteSchema1.Discriminator = new OpenApiDiscriminator { PropertyName = discName };
                    concreteSchema1.Properties ??= new Dictionary<string, IOpenApiSchema>();
                    if (!concreteSchema1.Properties.ContainsKey(discName))
                    {
                        concreteSchema1.Properties[discName] = new OpenApiSchema
                            { Type = JsonSchemaType.String, Title = discName, Description = "Union discriminator" };
                    }

                    concreteSchema1.Required ??= new HashSet<string>();
                    if (!concreteSchema1.Required.Contains(discName))
                        concreteSchema1.Required.Add(discName);
                }
            }

            // ──────────────────────────────────────────────────────────────────
            // ENSURE THE DISCRIMINATOR PROPERTY EXISTS
            // ──────────────────────────────────────────────────────────────────
            if (schema.Discriminator is { PropertyName: { } discProp })
            {
                if (schema is OpenApiSchema concreteSchema2)
                {
                    concreteSchema2.Properties ??= new Dictionary<string, IOpenApiSchema>();

                    if (!concreteSchema2.Properties.ContainsKey(discProp))
                    {
                        concreteSchema2.Properties[discProp] = new OpenApiSchema
                        {
                            Type = JsonSchemaType.String,
                            Title = discProp,
                            Description = "Union discriminator"
                        };
                    }

                    concreteSchema2.Required ??= new HashSet<string>();
                    if (!concreteSchema2.Required.Contains(discProp))
                        concreteSchema2.Required.Add(discProp);
                }
            }

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

        var validPaths = new OpenApiPaths();
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

                var newResps = new OpenApiResponses();
                if (operation.Value.Responses != null)
                {
                    foreach (KeyValuePair<string, IOpenApiResponse> resp in operation.Value.Responses)
                    {
                        if (resp.Value == null)
                            continue;

                        if (resp.Value is OpenApiResponseReference)
                        {
                            newResps[resp.Key] = resp.Value;
                            continue;
                        }

                        if (resp.Value is OpenApiResponse concreteResp && resp.Value.Content != null)
                        {
                            concreteResp.Content = resp.Value.Content.Where(p => p.Key != null && p.Value != null)
                                                       .ToDictionary(p => NormalizeMediaType(p.Key), p => p.Value);
                        }

                        ScrubBrokenRefs(resp.Value.Content, document);

                        if (resp.Value.Content != null)
                        {
                            Dictionary<string, IOpenApiMediaType> valid = resp.Value.Content.Where(p =>
                                                                              {
                                                                                  if (p.Value == null)
                                                                                      return false;
                                                                                  IOpenApiMediaType? mt = p.Value;
                                                                                  if (mt.Schema == null)
                                                                                      return false;
                                                                                  IOpenApiSchema? sch = mt.Schema;
                                                                                  return sch is OpenApiSchemaReference || !IsSchemaEmpty(sch);
                                                                              })
                                                                              .ToDictionary(p => p.Key, p => p.Value);

                            if (valid.Any())
                            {
                                string status = resp.Key.Equals("4xx", StringComparison.OrdinalIgnoreCase) ? "4XX" : resp.Key;
                                newResps[status] = new OpenApiResponse
                                {
                                    Description = resp.Value.Description,
                                    Content = valid
                                };
                            }
                        }
                    }
                }

                if (newResps.Any())
                {
                    EnsureResponseDescriptions(newResps);
                    if (operation.Value is OpenApiOperation concreteOperation)
                    {
                        concreteOperation.Responses = newResps;
                    }
                }
                else
                {
                    if (operation.Value is OpenApiOperation concreteOperation)
                    {
                        concreteOperation.Responses = CreateFallbackResponses(operation.Key);
                    }
                }

                if (operation.Value.RequestBody != null && operation.Value.RequestBody is not OpenApiRequestBodyReference)
                {
                    OpenApiRequestBody? rb = (OpenApiRequestBody)operation.Value.RequestBody;
                    if (rb.Content != null)
                    {
                        // In v2.3, we can't modify the content directly, so we need to create a new request body
                        Dictionary<string, IOpenApiMediaType>? normalizedContent = rb.Content.Where(p => p.Key != null && p.Value != null)
                                                                                     .ToDictionary(p => NormalizeMediaType(p.Key), p => p.Value);

                        ScrubBrokenRefs(normalizedContent, document);
                        Dictionary<string, IOpenApiMediaType>? validRb = normalizedContent
                                                                         ?.Where(p => p.Value != null && (p.Value.Schema is OpenApiSchemaReference ||
                                                                             !IsMediaEmpty(p.Value)))
                                                                         .ToDictionary(p => p.Key, p => p.Value);

                        if (operation.Value is OpenApiOperation concreteOperation)
                        {
                            concreteOperation.RequestBody = (validRb != null && validRb.Any())
                                ? new OpenApiRequestBody { Description = rb.Description, Content = validRb }
                                : CreateFallbackRequestBody();
                        }
                    }
                }
            }

            if (path.Value is OpenApiPathItem concretePathItem)
            {
                validPaths.Add(path.Key, concretePathItem);
            }
        }

        document.Paths = validPaths;

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
                InjectTypeForNullable(concreteRoot, visitedSchemas);
        }

        // Ensure all schemas with discriminators have proper validation
        ValidateAndFixDiscriminators(document);
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

        bool IsMissing(OpenApiSchemaReference r) => string.IsNullOrWhiteSpace(r.Reference.Id) || !comps.ContainsKey(r.Reference.Id);

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
            if (s is OpenApiSchemaReference r && comps.TryGetValue(r.Reference.Id, out IOpenApiSchema? target))
                s = target;

            if (s is not OpenApiSchema os)
                return false;

            bool primitive = os.Type is JsonSchemaType.String or JsonSchemaType.Integer or JsonSchemaType.Number or JsonSchemaType.Boolean;
            bool noShape = (os.Properties?.Count ?? 0) == 0 && os.Items is null && (os.AllOf?.Count ?? 0) == 0 && (os.AnyOf?.Count ?? 0) == 0 &&
                           (os.OneOf?.Count ?? 0) == 0;
            bool noEnum = (os.Enum?.Count ?? 0) == 0;

            return primitive && noShape && noEnum;
        }

        IOpenApiSchema InlineTarget(OpenApiSchemaReference r)
        {
            // When missing, fall back to a string (your example is an ID)
            if (IsMissing(r))
            {
                JsonSchemaType inferred = InferPrimitiveTypeFromId(r.Reference.Id);
                return new OpenApiSchema { Type = inferred };
            }

            IOpenApiSchema target = comps[r.Reference.Id];
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
                    foreach (var media in rb.Content.Values.OfType<OpenApiMediaType>())
                        if (media.Schema is { } sch)
                        {
                            IOpenApiSchema? tmp = sch;
                            Visit(ref tmp);
                            media.Schema = tmp;
                        }

        if (doc.Components?.Responses != null)
            foreach (var resp in doc.Components.Responses.Values)
                if (resp?.Content != null)
                    foreach (var media in resp.Content.Values.OfType<OpenApiMediaType>())
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
                        foreach (var media in rb2.Content.Values.OfType<OpenApiMediaType>())
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
                                foreach (var media in r.Content.Values.OfType<OpenApiMediaType>())
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

    private void AddComponentSchema(OpenApiDocument doc, string compName, OpenApiSchema schema)
    {
        if (string.IsNullOrWhiteSpace(compName))
        {
            _logger.LogWarning("Skipped adding a component schema because its generated name was empty.");
            return;
        }

        string validatedName = ValidateComponentName(compName);

        if (!doc.Components.Schemas.ContainsKey(validatedName))
        {
            // In v2.3, Title is read-only, so we can't modify it directly
            doc.Components.Schemas[validatedName] = schema;
        }
    }

    private string GenerateSafePart(string? input, string fallback = "unnamed")
    {
        if (string.IsNullOrWhiteSpace(input))
        {
            return fallback;
        }

        string sanitized = NormalizeOperationId(input);
        return string.IsNullOrWhiteSpace(sanitized) ? fallback : sanitized;
    }

    private string ValidateComponentName(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            _logger.LogWarning("Component name was empty, using fallback name");
            return "UnnamedComponent";
        }

        string sanitized = Regex.Replace(name, @"[^a-zA-Z0-9_]", "_");

        if (!char.IsLetter(sanitized[0]))
        {
            sanitized = "C" + sanitized;
        }

        return sanitized;
    }

    private void ExtractInlineSchemas(OpenApiDocument document, CancellationToken cancellationToken)
    {
        static bool IsSimpleEnvelope(OpenApiSchema s) =>
            s.Properties?.Count == 1 && s.Properties.TryGetValue("data", out IOpenApiSchema? p) && p is OpenApiSchemaReference &&
            (s.Required == null || s.Required.Count <= 1);

        IDictionary<string, IOpenApiSchema>? comps = document.Components?.Schemas;
        if (comps == null)
            return;

        foreach (IOpenApiPathItem pathItem in document.Paths.Values)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (pathItem.Operations != null)
            {
                foreach ((HttpMethod opType, OpenApiOperation operation) in pathItem.Operations)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (operation == null)
                        continue;

                    string safeOpId = ValidateComponentName(GenerateSafePart(operation.OperationId ?? "unnamed", opType.ToString()));

                    if (operation.Parameters != null)
                    {
                        foreach (IOpenApiParameter? param in operation.Parameters.ToList())
                        {
                            // Skip parameter references as they don't need inline schema extraction
                            if (param is OpenApiParameterReference)
                                continue;

                            if (param is OpenApiParameter concreteParam && concreteParam.Content?.Any() == true)
                            {
                                IOpenApiMediaType? first = concreteParam.Content.Values.FirstOrDefault();
                                if (first?.Schema != null)
                                    concreteParam.Schema = first.Schema;

                                concreteParam.Content = null;
                            }
                        }
                    }

                    if (operation.RequestBody != null && operation.RequestBody is not OpenApiRequestBodyReference && operation.RequestBody.Content != null)
                    {
                        foreach (var (mediaType, mediaInterface) in operation.RequestBody.Content!.ToList())
                        {
                            if (mediaInterface is not OpenApiMediaType media)
                                continue;

                            IOpenApiSchema? schemaReq = media.Schema;
                            if (schemaReq == null || schemaReq is OpenApiSchemaReference)
                                continue;
                            if (schemaReq is OpenApiSchema concreteSchemaReq1 && IsSimpleEnvelope(concreteSchemaReq1))
                                continue;

                            string safeMedia;
                            string subtype = mediaType.Split(';')[0]
                                                      .Split('/')
                                                      .Last();
                            if (subtype.Equals("json", StringComparison.OrdinalIgnoreCase))
                                safeMedia = "";
                            else
                                safeMedia = ValidateComponentName(GenerateSafePart(subtype, "media"));

                            var baseName = $"{safeOpId}";
                            string compName = ReserveUniqueSchemaName(comps, baseName, $"RequestBody_{safeMedia}");

                            if (schemaReq is OpenApiSchema concreteSchemaReq2)
                                AddComponentSchema(document, compName, concreteSchemaReq2);
                            media.Schema = new OpenApiSchemaReference(compName);
                        }
                    }

                    if (operation.Responses != null)
                    {
                        foreach ((string statusCode, IOpenApiResponse response) in operation.Responses)
                        {
                            if (response == null || response is OpenApiResponseReference)
                            {
                                continue;
                            }

                            if (response.Content == null)
                                continue;

                            foreach (var (mediaType, mediaInterface) in response.Content!.ToList())
                            {
                                if (mediaInterface is not OpenApiMediaType media)
                                    continue;

                                IOpenApiSchema? schemaResp = media.Schema;
                                if (schemaResp == null || schemaResp is OpenApiSchemaReference)
                                    continue;
                                if (schemaResp is OpenApiSchema concreteSchemaResp1 && IsSimpleEnvelope(concreteSchemaResp1))
                                    continue;

                                string safeMedia = ValidateComponentName(GenerateSafePart(mediaType, "media"));
                                var baseName = $"{safeOpId}_{statusCode}";
                                string compName = ReserveUniqueSchemaName(comps, baseName, $"Response_{safeMedia}");

                                if (schemaResp is OpenApiSchema concreteSchemaResp2)
                                    AddComponentSchema(document, compName, concreteSchemaResp2);
                                media.Schema = new OpenApiSchemaReference(compName);
                            }
                        }
                    }
                }
            }
        }
    }

    private static string ReserveUniqueSchemaName(IDictionary<string, IOpenApiSchema> comps, string baseName, string fallbackSuffix)
    {
        // Ensure a valid, non-empty component key baseline
        string Sanitize(string? name, string fallback)
        {
            if (string.IsNullOrWhiteSpace(name))
                return fallback;
            string sanitized = Regex.Replace(name, @"[^a-zA-Z0-9_]", "_");
            if (string.IsNullOrWhiteSpace(sanitized))
                sanitized = fallback;
            if (!char.IsLetter(sanitized[0]))
                sanitized = "C" + sanitized;
            return sanitized;
        }

        string candidate = Sanitize(baseName, "UnnamedComponent");
        if (!comps.ContainsKey(candidate))
            return candidate;

        string withSuffix = Sanitize($"{baseName}_{fallbackSuffix}", "UnnamedComponent_Wrapper");
        if (!comps.ContainsKey(withSuffix))
            return withSuffix;

        var i = 2;
        string numbered;
        do
        {
            numbered = Sanitize($"{baseName}_{fallbackSuffix}_{i++}", "UnnamedComponent_Wrapper");
        }
        while (comps.ContainsKey(numbered));

        return numbered;
    }

    private void EnsureUniqueOperationIds(OpenApiDocument doc)
    {
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach ((string pathKey, var pathItem) in doc.Paths)
        {
            if (pathItem?.Operations == null)
                continue;
            foreach ((HttpMethod method, OpenApiOperation operation) in pathItem.Operations)
            {
                if (string.IsNullOrWhiteSpace(operation.OperationId))
                {
                    // Deterministic base name: e.g., "get_/users/{id}"
                    string baseId = $"{method.ToString().ToLowerInvariant()}_{pathKey.Trim('/')}".Replace("/", "_")
                                                                                                 .Replace("{", "")
                                                                                                 .Replace("}", "")
                                                                                                 .Replace("-", "_");

                    string uniqueId = baseId;
                    var i = 1;

                    while (!seen.Add(uniqueId))
                    {
                        uniqueId = $"{baseId}_{i++}";
                    }

                    operation.OperationId = uniqueId;
                    _logger.LogDebug($"Assigned deterministic OperationId: {operation.OperationId}");
                }
                else
                {
                    string baseId = operation.OperationId;
                    string unique = baseId;
                    var i = 1;

                    while (!seen.Add(unique))
                    {
                        unique = $"{baseId}_{i++}";
                    }

                    if (operation.OperationId != unique)
                    {
                        _logger.LogDebug($"Renaming duplicate OperationId from '{operation.OperationId}' to '{unique}'");
                        operation.OperationId = unique;
                    }
                }
            }
        }
    }

    private void UpdateAllReferences(OpenApiDocument doc, Dictionary<string, string> mapping)
    {
        // Delegate to ReplaceAllRefs for each mapping pair
        foreach ((string oldKey, string newKey) in mapping)
        {
            ReplaceAllRefs(doc, oldKey, newKey);
        }
    }

    private static string SanitizeName(string input)
    {
        if (string.IsNullOrWhiteSpace(input))
            return string.Empty;
        var sb = new StringBuilder();
        foreach (char c in input)
        {
            if (char.IsLetterOrDigit(c) || c == '_')
                sb.Append(c);
            else
                sb.Append('_');
        }

        return sb.ToString();
    }

    /// <summary>
    /// Normalizes operationIds by removing parentheses and collapsing punctuation to single dashes/underscores.
    /// Ensures result is non-empty and starts with a letter.
    /// </summary>
    private static string NormalizeOperationId(string input)
    {
        if (string.IsNullOrWhiteSpace(input))
            return string.Empty;
        // Remove parentheses segments like (-deprecated)
        string noParens = Regex.Replace(input, "[()]+", string.Empty);
        // Replace any non-alphanumeric with '-'
        string collapsed = Regex.Replace(noParens, "[^a-zA-Z0-9]+", "-");
        // Collapse consecutive '-'
        collapsed = Regex.Replace(collapsed, "-+", "-")
                         .Trim('-');
        if (string.IsNullOrEmpty(collapsed))
            return "unnamed";
        // Ensure starts with a letter
        if (!char.IsLetter(collapsed[0]))
            collapsed = "op-" + collapsed;
        return collapsed;
    }

    /// <summary>
    /// Applies normalization to all existing operationIds.
    /// </summary>
    private void NormalizeOperationIds(OpenApiDocument doc)
    {
        if (doc.Paths == null)
            return;
        foreach (var path in doc.Paths.Values)
        {
            if (path?.Operations == null)
                continue;
            foreach (var op in path.Operations.Values)
            {
                if (op == null)
                    continue;
                if (!string.IsNullOrWhiteSpace(op.OperationId))
                {
                    string normalized = NormalizeOperationId(op.OperationId!);
                    if (!string.Equals(normalized, op.OperationId, StringComparison.Ordinal))
                        op.OperationId = normalized;
                }
            }
        }
    }

    private static bool IsValidIdentifier(string id) =>
        !string.IsNullOrWhiteSpace(id) && id.All(c => char.IsLetterOrDigit(c) || c == '_' || c == '-' || c == '.');

    private bool IsValidSchemaReference(OpenApiSchemaReference? reference, OpenApiDocument doc)
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

    private void ScrubBrokenRefs(IDictionary<string, IOpenApiMediaType>? contentDict, OpenApiDocument doc)
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

    /// <summary>
    /// Replaces "$ref" values that incorrectly point into path example chains (e.g., "#/paths/.../examples/...")
    /// with placeholder component schemas. This ensures all refs target valid component schemas.
    /// </summary>
    private void FixRefsPointingIntoPathsExamples(OpenApiDocument doc)
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

    private void ScrubAllRefs(IOpenApiSchema? schema, OpenApiDocument doc, HashSet<IOpenApiSchema> visited)
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

    private void ScrubComponentRefs(OpenApiDocument doc, CancellationToken cancellationToken)
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

    private void RenameConflictingPaths(OpenApiDocument doc)
    {
        if (doc.Paths == null || !doc.Paths.Any())
        {
            _logger.LogInformation("Document contains no paths to process in RenameConflictingPaths. Skipping.");
            return;
        }

        var newPaths = new OpenApiPaths();
        foreach (KeyValuePair<string, IOpenApiPathItem> kvp in doc.Paths)
        {
            string originalPath = kvp.Key;
            string newPath = originalPath;

            if (originalPath.Contains("/accounts/{account_id}/addressing/address_maps/{address_map_id}/accounts/{account_id}"))
            {
                newPath = originalPath.Replace("/accounts/{account_id}/addressing/address_maps/{address_map_id}/accounts/{account_id}",
                    "/accounts/{account_id}/addressing/address_maps/{address_map_id}/accounts/{member_account_id}");

                if (kvp.Value.Operations != null)
                {
                    foreach (OpenApiOperation? operation in kvp.Value.Operations.Values)
                    {
                        if (operation.Parameters == null)
                        {
                            operation.Parameters = new List<IOpenApiParameter>();
                        }

                        bool hasAccountId = operation.Parameters.Any(p => p?.Name == "account_id" && p?.In == ParameterLocation.Path);
                        bool hasMemberAccountId = operation.Parameters.Any(p => p?.Name == "member_account_id" && p?.In == ParameterLocation.Path);

                        if (!hasAccountId)
                        {
                            operation.Parameters.Add(new OpenApiParameter
                            {
                                Name = "account_id",
                                In = ParameterLocation.Path,
                                Required = true,
                                Schema = new OpenApiSchema
                                {
                                    Type = JsonSchemaType.String,
                                    MaxLength = 32,
                                    Description = "Identifier of a Cloudflare account."
                                }
                            });
                        }

                        if (!hasMemberAccountId)
                        {
                            operation.Parameters.Add(new OpenApiParameter
                            {
                                Name = "member_account_id",
                                In = ParameterLocation.Path,
                                Required = true,
                                Schema = new OpenApiSchema
                                {
                                    Type = JsonSchemaType.String,
                                    MaxLength = 32,
                                    Description = "Identifier of the member account to add/remove from the Address Map."
                                }
                            });
                        }

                        foreach (IOpenApiParameter? param in operation.Parameters)
                        {
                            if (param?.Name == "member_account_id" && param?.In == ParameterLocation.Path)
                            {
                                // In v2.3, Schema is read-only, so we can't modify it directly
                                // We'll handle this in a different way if needed
                                _logger.LogDebug("Would update schema description for parameter 'member_account_id'");
                            }
                        }
                    }
                }
            }
            else if (originalPath.EndsWith("/item", StringComparison.OrdinalIgnoreCase))
            {
                newPath = originalPath.Replace("/item", "/item_static");
            }
            else if (originalPath.Contains("/item/{", StringComparison.OrdinalIgnoreCase))
            {
                newPath = originalPath.Replace("/item", "/item_by_id");
            }

            newPaths.Add(newPath, kvp.Value);
        }

        doc.Paths = newPaths;
    }

    private static string NormalizeMediaType(string mediaType)
    {
        if (string.IsNullOrWhiteSpace(mediaType))
            return "application/json";
        string baseType = mediaType.Split(';')[0]
                                   .Trim();
        if (baseType.Contains('*') || !baseType.Contains('/'))
            return "application/json";
        return baseType;
    }

    private static bool IsMediaEmpty(IOpenApiMediaType media)
    {
        IOpenApiSchema? s = media.Schema;
        bool schemaEmpty = s == null || (s.Type == null && (s.Properties == null || !s.Properties.Any()) && s.Items == null &&
                                         (s.AllOf == null || !s.AllOf.Any()) && (s.AnyOf == null || !s.AnyOf.Any()) && (s.OneOf == null || !s.OneOf.Any()));
        bool hasExample = s?.Example != null || (media.Examples?.Any() == true);
        return schemaEmpty && !hasExample;
    }

    private void EnsureResponseDescriptions(OpenApiResponses responses)
    {
        foreach (KeyValuePair<string, IOpenApiResponse> kv in responses)
        {
            string code = kv.Key;
            IOpenApiResponse resp = kv.Value;
            if (resp is OpenApiResponse concreteResp && string.IsNullOrWhiteSpace(concreteResp.Description))
            {
                concreteResp.Description = code == "default" ? "Default response" : $"{code} response";
            }
        }
    }

    private async ValueTask ReadAndValidateOpenApi(string filePath, CancellationToken cancellationToken)
    {
        await using FileStream stream = File.OpenRead(filePath);

        var reader = new OpenApiJsonReader(); // force JSON
        ReadResult read = await reader.ReadAsync(stream, new Uri(filePath), // base URI for relative $refs
                                          new OpenApiReaderSettings(), cancellationToken)
                                      .NoSync();

        OpenApiDiagnostic? diagnostics = read.Diagnostic;
        if (diagnostics?.Errors?.Any() == true)
            _logger.LogWarning("OpenAPI parsing errors in {File}: {Msgs}", Path.GetFileName(filePath),
                string.Join("; ", diagnostics.Errors.Select(e => e.Message)));
    }

    private void CleanDocumentForSerialization(OpenApiDocument document)
    {
        if (document.Components?.Schemas == null)
            return;

        var visited = new HashSet<IOpenApiSchema>();
        foreach (IOpenApiSchema? schema in document.Components.Schemas.Values)
        {
            CleanSchemaForSerialization(schema, visited);
        }
    }

    /// <summary>
    /// Converts schemas that declare boolean type with enum constraints into plain booleans,
    /// and assigns a default when the enum contains a single boolean value.
    /// Example to normalize:
    ///   { "type": "boolean", "enum": [ true ] } -> { "type": "boolean", "default": true }
    ///   { "type": "boolean", "enum": [ true, false ] } -> { "type": "boolean" }
    /// This also handles cases where type is null but all enum values are booleans.
    /// </summary>
    private void NormalizeBooleanEnums(OpenApiDocument doc)
    {
        if (doc.Components?.Schemas == null)
            return;

        var visited = new HashSet<IOpenApiSchema>();

        void Visit(IOpenApiSchema? s)
        {
            if (s is not OpenApiSchema os)
                return;
            if (!visited.Add(os))
                return;

            if (os.Enum is { Count: > 0 })
            {
                bool allBoolean = os.Enum.All(e => e is JsonValue jv && (jv.GetValueKind() == JsonValueKind.True || jv.GetValueKind() == JsonValueKind.False));

                if (allBoolean)
                {
                    // Ensure type is boolean
                    os.Type = JsonSchemaType.Boolean;

                    // If only a single enum entry, set it as default
                    if (os.Enum.Count == 1)
                    {
                        os.Default = os.Enum[0];
                    }

                    // Drop enum to avoid CodeEnum generation on booleans
                    os.Enum = null;

                    // Make sure no object facets linger
                    os.Properties = null;
                    os.AdditionalProperties = null;
                    os.AdditionalPropertiesAllowed = false;
                }
            }

            if (os.Properties != null)
            {
                foreach (IOpenApiSchema child in os.Properties.Values)
                    Visit(child);
            }

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

        foreach (IOpenApiSchema s in doc.Components.Schemas.Values)
            Visit(s);
    }

    private void CleanSchemaForSerialization(IOpenApiSchema? schema, HashSet<IOpenApiSchema> visited)
    {
        if (schema == null || !visited.Add(schema))
            return;

        if (schema is not OpenApiSchema concreteSchema)
            return;

        // Cast to concrete type to modify read-only properties
        OpenApiSchema schemaToModify = concreteSchema;

        // Clean enum values
        if (schema.Enum != null && schema.Enum.Any())
        {
            var cleanedEnum = new List<JsonNode>();
            foreach (JsonNode enumValue in schema.Enum)
            {
                if (enumValue is JsonValue jsonValue)
                {
                    // Ensure the value is valid JSON
                    try
                    {
                        JsonValueKind valueKind = jsonValue.GetValueKind();
                        if (valueKind == JsonValueKind.String)
                        {
                            var stringValue = jsonValue.GetValue<string>();
                            if (stringValue != null)
                            {
                                // Remove any control characters that could cause JSON serialization issues
                                var cleanedString = new string(stringValue.Where(c => !char.IsControl(c) || c == '\n' || c == '\r' || c == '\t')
                                                                          .ToArray());
                                cleanedEnum.Add(JsonValue.Create(cleanedString));
                            }
                        }
                        else
                        {
                            cleanedEnum.Add(enumValue);
                        }
                    }
                    catch
                    {
                        // Skip invalid enum values
                        _logger.LogWarning("Removing invalid enum value from schema");
                    }
                }
                else
                {
                    cleanedEnum.Add(enumValue);
                }
            }

            schemaToModify.Enum = cleanedEnum.Any() ? cleanedEnum : null;
        }

        // Clean default values
        if (schema.Default != null)
        {
            try
            {
                if (schema.Default is JsonValue jsonValue)
                {
                    JsonValueKind valueKind = jsonValue.GetValueKind();
                    if (valueKind == JsonValueKind.String)
                    {
                        var stringValue = jsonValue.GetValue<string>();
                        if (stringValue != null)
                        {
                            // Remove any control characters that could cause JSON serialization issues
                            var cleanedString = new string(stringValue.Where(c => !char.IsControl(c) || c == '\n' || c == '\r' || c == '\t')
                                                                      .ToArray());
                            schemaToModify.Default = JsonValue.Create(cleanedString);
                        }
                    }
                }
            }
            catch
            {
                // Remove invalid default values
                _logger.LogWarning("Removing invalid default value from schema");
                schemaToModify.Default = null;
            }
        }

        // Clean example values
        if (schema.Example != null)
        {
            try
            {
                if (schema.Example is JsonValue jsonValue)
                {
                    JsonValueKind valueKind = jsonValue.GetValueKind();
                    if (valueKind == JsonValueKind.String)
                    {
                        var stringValue = jsonValue.GetValue<string>();
                        if (stringValue != null)
                        {
                            // Remove any control characters that could cause JSON serialization issues
                            var cleanedString = new string(stringValue.Where(c => !char.IsControl(c) || c == '\n' || c == '\r' || c == '\t')
                                                                      .ToArray());
                            schemaToModify.Example = JsonValue.Create(cleanedString);
                        }
                    }
                }
            }
            catch
            {
                // Remove invalid example values
                _logger.LogWarning("Removing invalid example value from schema");
                schemaToModify.Example = null;
            }
        }

        // Clean description
        if (!string.IsNullOrEmpty(schema.Description))
        {
            // Remove any control characters that could cause JSON serialization issues
            schemaToModify.Description = new string(schema.Description.Where(c => !char.IsControl(c) || c == '\n' || c == '\r' || c == '\t')
                                                          .ToArray());
        }

        // Clean title
        if (!string.IsNullOrEmpty(schema.Title))
        {
            // Remove any control characters that could cause JSON serialization issues
            schemaToModify.Title = new string(schema.Title.Where(c => !char.IsControl(c) || c == '\n' || c == '\r' || c == '\t')
                                                    .ToArray());
        }

        // Recursively clean nested schemas
        if (schema.Properties != null)
        {
            foreach (IOpenApiSchema property in schema.Properties.Values)
            {
                CleanSchemaForSerialization(property, visited);
            }
        }

        if (schema.Items != null)
        {
            CleanSchemaForSerialization(schema.Items, visited);
        }

        if (schema.AllOf != null)
        {
            foreach (IOpenApiSchema allOfSchema in schema.AllOf)
            {
                CleanSchemaForSerialization(allOfSchema, visited);
            }
        }

        if (schema.OneOf != null)
        {
            foreach (IOpenApiSchema oneOfSchema in schema.OneOf)
            {
                CleanSchemaForSerialization(oneOfSchema, visited);
            }
        }

        if (schema.AnyOf != null)
        {
            foreach (IOpenApiSchema anyOfSchema in schema.AnyOf)
            {
                CleanSchemaForSerialization(anyOfSchema, visited);
            }
        }

        if (schema.AdditionalProperties != null)
        {
            CleanSchemaForSerialization(schema.AdditionalProperties, visited);
        }
    }

    private string FixJsonBooleanValues(string json)
    {
        // Replace Python-style boolean values with JSON boolean values
        // This handles cases where the OpenAPI library produces True/False instead of true/false

        // Use regex to ensure we only replace boolean values, not strings that contain "True" or "False"
        // This prevents accidentally replacing values in strings like "description": "This is True"

        // Replace : True with : true (with space)
        json = System.Text.RegularExpressions.Regex.Replace(json, @":\s*True\b", ": true");
        // Replace : False with : false (with space)
        json = System.Text.RegularExpressions.Regex.Replace(json, @":\s*False\b", ": false");

        // Replace , True with , true (in arrays/objects)
        json = System.Text.RegularExpressions.Regex.Replace(json, @",\s*True\b", ", true");
        // Replace , False with , false (in arrays/objects)
        json = System.Text.RegularExpressions.Regex.Replace(json, @",\s*False\b", ", false");

        // Replace [True with [true (start of array)
        json = System.Text.RegularExpressions.Regex.Replace(json, @"\[\s*True\b", "[true");
        // Replace [False with [false (start of array)
        json = System.Text.RegularExpressions.Regex.Replace(json, @"\[\s*False\b", "[false");

        // Replace True] with true] (end of array)
        json = System.Text.RegularExpressions.Regex.Replace(json, @"\bTrue\s*\]", "true]");
        // Replace False] with false] (end of array)
        json = System.Text.RegularExpressions.Regex.Replace(json, @"\bFalse\s*\]", "false]");

        return json;
    }

    private void EnsureSecuritySchemes(OpenApiDocument document)
    {
        document.Components ??= new OpenApiComponents();

        IDictionary<string, IOpenApiSecurityScheme> schemes = document.Components.SecuritySchemes ??= new Dictionary<string, IOpenApiSecurityScheme>();

        if (!schemes.ContainsKey("assets_jwt"))
        {
            schemes["assets_jwt"] = new OpenApiSecurityScheme
            {
                Type = SecuritySchemeType.Http,
                Scheme = "bearer",
                BearerFormat = "JWT",
                Description = "JWT used for assets upload"
            };
        }

        foreach (IOpenApiPathItem? path in document.Paths.Values)
        {
            if (path?.Operations == null)
                continue;
            foreach (OpenApiOperation? op in path.Operations.Values)
            {
                if (op.Parameters == null)
                    continue;

                IOpenApiParameter? rogue = op.Parameters.FirstOrDefault(p =>
                    p?.In == ParameterLocation.Header && p?.Name?.StartsWith("authorization", StringComparison.OrdinalIgnoreCase) == true);

                if (rogue != null)
                {
                    op.Parameters.Remove(rogue);

                    op.Security ??= new List<OpenApiSecurityRequirement>();
                    op.Security.Add(new OpenApiSecurityRequirement
                    {
                        [new OpenApiSecuritySchemeReference("assets_jwt")] = new List<string>()
                    });
                }
            }
        }
    }

    private void FixInvalidDefaults(OpenApiDocument document)
    {
        if (document.Components?.Schemas == null)
            return;

        var visited = new HashSet<IOpenApiSchema>();
        foreach (IOpenApiSchema? schema in document.Components.Schemas.Values)
        {
            FixSchemaDefaults(schema, visited);
        }
    }

    private static string CanonicalSuccess(HttpMethod op) => op.Method switch
    {
        "POST" => "201",
        "DELETE" => "204",
        _ => "200"
    };

    private void FixSchemaDefaults(IOpenApiSchema? schema, HashSet<IOpenApiSchema> visited)
    {
        if (schema == null || !visited.Add(schema))
            return;

        if (schema is not OpenApiSchema concreteSchema)
            return;

        // --- ENUM DEFAULTS: robust matching & assignment ---
        if (schema.Enum is { Count: > 0 })
        {
            var enumByText = new Dictionary<string, JsonNode?>(StringComparer.OrdinalIgnoreCase);
            foreach (JsonNode e in schema.Enum)
            {
                if (e is JsonValue jv)
                {
                    if (jv.TryGetValue(out string? s))
                        enumByText[s] = e;
                    else if (jv.TryGetValue(out long l))
                        enumByText[l.ToString(System.Globalization.CultureInfo.InvariantCulture)] = e;
                    else if (jv.TryGetValue(out double d))
                        enumByText[d.ToString(System.Globalization.CultureInfo.InvariantCulture)] = e;
                }
                else
                {
                    enumByText[e.ToJsonString()] = e;
                }
            }

            if (schema.Default is not null)
            {
                string? defText = null;
                if (schema.Default is JsonValue dv)
                {
                    if (dv.TryGetValue(out string? ds))
                        defText = ds;
                    else if (dv.TryGetValue(out long dl))
                        defText = dl.ToString(System.Globalization.CultureInfo.InvariantCulture);
                    else if (dv.TryGetValue(out double dd))
                        defText = dd.ToString(System.Globalization.CultureInfo.InvariantCulture);
                    else
                        defText = schema.Default.ToJsonString();
                }
                else
                {
                    defText = schema.Default.ToJsonString();
                }

                if (defText is not null)
                {
                    if (enumByText.TryGetValue(defText, out JsonNode? matchingEnumElement))
                    {
                        if (!ReferenceEquals(schema.Default, matchingEnumElement))
                        {
                            concreteSchema.Default = matchingEnumElement;
                            _logger.LogWarning("Fixed enum default on '{SchemaTitle}' to '{NewDefault}'", schema.Title ?? "(no title)", defText);
                        }
                    }
                    else
                    {
                        JsonNode first = schema.Enum[0];
                        if (!ReferenceEquals(schema.Default, first))
                        {
                            concreteSchema.Default = first;
                            _logger.LogWarning("Replaced invalid enum default on '{SchemaTitle}' with first enum member", schema.Title ?? "(no title)");
                        }
                    }
                }
            }
        }

        if (schema.Default != null)
        {
            switch (schema.Type)
            {
                case JsonSchemaType.Boolean:
                {
                    bool? normalized = null;
                    if (schema.Default is JsonValue jv)
                    {
                        if (jv.GetValueKind() is JsonValueKind.True or JsonValueKind.False)
                            normalized = jv.GetValueKind() == JsonValueKind.True;
                        else if (jv.GetValueKind() == JsonValueKind.String && Boolean.TryParse(jv.GetValue<string>(), out bool b))
                            normalized = b;
                        else if (jv.GetValueKind() == JsonValueKind.Number && jv.TryGetValue(out int n) && (n == 0 || n == 1))
                            normalized = n == 1;
                    }

                    concreteSchema.Default = JsonValue.Create(normalized ?? false);
                    break;
                }

                case JsonSchemaType.Array:
                    if (schema.Default is not JsonArray)
                    {
                        concreteSchema.Default = new JsonArray();
                    }

                    break;

                case JsonSchemaType.String:
                    if (schema.Format == "date-time" && schema.Default is JsonValue dateStr)
                    {
                        if (dateStr.GetValue<string>() is string dateValue && !DateTimeOffset.TryParse(dateValue,
                                System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.RoundtripKind, out _))
                        {
                            concreteSchema.Default = null;
                        }
                    }

                    break;

                case JsonSchemaType.Integer:
                {
                    if (schema.Default is JsonValue jv)
                    {
                        if (jv.GetValueKind() == JsonValueKind.Number)
                        {
                            // ok
                        }
                        else if (jv.GetValueKind() == JsonValueKind.String && Int64.TryParse(jv.GetValue<string>(), System.Globalization.NumberStyles.Integer,
                                     System.Globalization.CultureInfo.InvariantCulture, out long parsed))
                        {
                            concreteSchema.Default = JsonValue.Create(parsed);
                        }
                        else
                        {
                            concreteSchema.Default = null;
                        }
                    }
                    else
                    {
                        concreteSchema.Default = null;
                    }

                    break;
                }

                case JsonSchemaType.Number:
                {
                    if (schema.Default is JsonValue jv)
                    {
                        if (jv.GetValueKind() == JsonValueKind.Number)
                        {
                            // ok
                        }
                        else if (jv.GetValueKind() == JsonValueKind.String && Double.TryParse(jv.GetValue<string>(), System.Globalization.NumberStyles.Float,
                                     System.Globalization.CultureInfo.InvariantCulture, out double parsed))
                        {
                            concreteSchema.Default = JsonValue.Create(parsed);
                        }
                        else
                        {
                            concreteSchema.Default = null;
                        }
                    }
                    else
                    {
                        concreteSchema.Default = null;
                    }

                    break;
                }
            }

            // --- FINAL GUARD: nuke bad string defaults on non-string/non-enum schemas ---
            if (schema.Default is JsonValue sVal && sVal.GetValueKind() == JsonValueKind.String && schema.Type is not JsonSchemaType.String &&
                !(schema.Enum?.Count > 0))
            {
                try
                {
                    string defText = sVal.GetValue<string>() ?? string.Empty;
                    string desc = schema.Description ?? string.Empty;
                    if (string.Equals(defText, desc, StringComparison.Ordinal) || schema.Type is JsonSchemaType.Object || schema.Type is JsonSchemaType.Array)
                    {
                        concreteSchema.Default = null;
                    }
                }
                catch
                {
                    concreteSchema.Default = null;
                }
            }
        }

        if (schema.Properties != null)
            foreach (IOpenApiSchema? prop in schema.Properties.Values)
                if (prop is OpenApiSchema concreteProp)
                    FixSchemaDefaults(concreteProp, visited);

        if (schema.Items is OpenApiSchema concreteItems)
            FixSchemaDefaults(concreteItems, visited);

        if (schema.AdditionalProperties is OpenApiSchema concreteAdditional)
            FixSchemaDefaults(concreteAdditional, visited);

        if (schema.AllOf != null)
            foreach (IOpenApiSchema? s in schema.AllOf)
                if (s is OpenApiSchema concreteS)
                    FixSchemaDefaults(concreteS, visited);

        if (schema.OneOf != null)
            foreach (IOpenApiSchema? s in schema.OneOf)
                if (s is OpenApiSchema concreteS)
                    FixSchemaDefaults(concreteS, visited);

        if (schema.AnyOf != null)
            foreach (IOpenApiSchema? s in schema.AnyOf)
                if (s is OpenApiSchema concreteS)
                    FixSchemaDefaults(concreteS, visited);
    }

    private void RemoveEmptyCompositionObjects(OpenApiSchema schema, HashSet<OpenApiSchema> visited)
    {
        if (schema == null || !visited.Add(schema))
            return;

        if (schema.Properties != null)
        {
            schema.Properties = schema.Properties.GroupBy(p => p.Key)
                                      .ToDictionary(g => g.Key, g => g.First()
                                                                      .Value);

            foreach (IOpenApiSchema? prop in schema.Properties.Values)
            {
                if (prop is OpenApiSchema concreteProp)
                {
                    RemoveEmptyCompositionObjects(concreteProp, visited);
                }
            }
        }

        if (schema.Items is OpenApiSchema concreteItems)
        {
            RemoveEmptyCompositionObjects(concreteItems, visited);
        }

        if (schema.AdditionalProperties is OpenApiSchema concreteAdditional)
        {
            RemoveEmptyCompositionObjects(concreteAdditional, visited);
        }

        if (schema.AllOf != null)
        {
            schema.AllOf = schema.AllOf.Where(s => s != null && !IsSchemaEmpty(s))
                                 .ToList();
            if (!schema.AllOf.Any())
            {
                schema.AllOf = null;
            }
        }

        if (schema.OneOf != null)
        {
            schema.OneOf = schema.OneOf.Where(s => s != null && !IsSchemaEmpty(s))
                                 .ToList();
            if (!schema.OneOf.Any())
            {
                schema.OneOf = null;
            }
        }

        if (schema.AnyOf != null)
        {
            schema.AnyOf = schema.AnyOf.Where(s => s != null && !IsSchemaEmpty(s))
                                 .ToList();
            if (!schema.AnyOf.Any())
            {
                schema.AnyOf = null;
            }
        }
    }

    private static void SanitizeExample(OpenApiSchema s)
    {
        if (s?.Example is JsonArray arr && arr.Count > 0)
        {
            if (s.Type == JsonSchemaType.String && arr.First() is JsonValue firstValue)
            {
                // Check if the value can be converted to string
                if (firstValue.TryGetValue(out string? stringValue) && stringValue != null)
                    s.Example = JsonValue.Create(stringValue);
                else
                    s.Example = null;
            }
            else
                s.Example = null;
        }

        if (s?.Example is JsonValue str)
        {
            // Check if the value can be converted to string
            if (str.TryGetValue(out string? strValue) && strValue != null && strValue.Length > 5_000)
                s.Example = null;
        }
    }

    private void InjectTypeForNullable(OpenApiSchema schema, HashSet<OpenApiSchema> visited)
    {
        if (schema == null || !visited.Add(schema))
            return;

        // In v2.3, nullability is handled through Type flags, not a separate Nullable property
        if (schema.Type == null)
        {
            if (schema is OpenApiSchema concreteSchema)
            {
                // Be conservative: only set object when this schema is object-like; otherwise leave null for primitives/enums
                bool looksObjectLike = (concreteSchema.Properties?.Any() == true) || concreteSchema.AdditionalProperties != null ||
                                       concreteSchema.AdditionalPropertiesAllowed ||
                                       (concreteSchema.AllOf?.Any(s =>
                                           s is OpenApiSchema os && (os.Properties?.Any() == true || os.Type == JsonSchemaType.Object)) == true) ||
                                       (concreteSchema.AnyOf?.Any(s =>
                                           s is OpenApiSchema os && (os.Properties?.Any() == true || os.Type == JsonSchemaType.Object)) == true) ||
                                       (concreteSchema.OneOf?.Any(s =>
                                           s is OpenApiSchema os && (os.Properties?.Any() == true || os.Type == JsonSchemaType.Object)) == true);
                if (!(concreteSchema.Enum?.Any() == true) && looksObjectLike)
                    concreteSchema.Type = JsonSchemaType.Object;
            }
        }

        if (schema.Properties != null)
            foreach (IOpenApiSchema? prop in schema.Properties.Values)
                if (prop is OpenApiSchema concreteProp)
                    InjectTypeForNullable(concreteProp, visited);

        if (schema.Items is OpenApiSchema concreteItems)
            InjectTypeForNullable(concreteItems, visited);

        if (schema.AdditionalProperties is OpenApiSchema concreteAdditional)
            InjectTypeForNullable(concreteAdditional, visited);

        if (schema.AllOf != null)
            foreach (IOpenApiSchema? s in schema.AllOf)
                if (s is OpenApiSchema concreteS)
                    InjectTypeForNullable(concreteS, visited);

        if (schema.OneOf != null)
            foreach (IOpenApiSchema? s in schema.OneOf)
                if (s is OpenApiSchema concreteS)
                    InjectTypeForNullable(concreteS, visited);

        if (schema.AnyOf != null)
            foreach (IOpenApiSchema? s in schema.AnyOf)
                if (s is OpenApiSchema concreteS)
                    InjectTypeForNullable(concreteS, visited);
    }

    private void ResolveSchemaOperationNameCollisions(OpenApiDocument doc)
    {
        if (doc.Components?.Schemas == null || doc.Paths == null)
            return;

        var operationIds = new HashSet<string>(doc.Paths.Values.Where(p => p?.Operations != null)
                                                  .SelectMany(p => p.Operations.Values)
                                                  .Where(op => op != null && !string.IsNullOrWhiteSpace(op.OperationId))
                                                  .Select(op => op.OperationId!), StringComparer.OrdinalIgnoreCase);

        if (!operationIds.Any())
            return;

        var mapping = new Dictionary<string, string>();

        // Use ToList() to create a copy of the keys, allowing modification of the collection during iteration.
        foreach (string key in doc.Components.Schemas.Keys.ToList())
        {
            // Check if the schema name (case-insensitive) collides with any operationId
            if (!operationIds.Contains(key))
                continue;

            // A collision exists. We must rename the schema.
            var newKey = $"{key}Body"; // A common and effective convention
            var i = 2;
            while (doc.Components.Schemas.ContainsKey(newKey) || mapping.ContainsKey(newKey))
            {
                newKey = $"{key}Body{i++}";
            }

            mapping[key] = newKey;
            _logger.LogWarning("Schema name '{OldKey}' conflicts with an operationId. Renaming schema to '{NewKey}'.", key, newKey);
        }

        if (mapping.Any())
        {
            foreach ((string oldKey, string newKey) in mapping)
            {
                if (doc.Components.Schemas.TryGetValue(oldKey, out IOpenApiSchema? schema))
                {
                    doc.Components.Schemas.Remove(oldKey);
                    // In v2.3, Title is read-only, so we can't modify it directly
                    doc.Components.Schemas[newKey] = schema;
                }
            }

            // After all renames are done, update all references throughout the entire document.
            _logger.LogInformation("Applying global reference updates for operationId/schema name collisions...");
            UpdateAllReferences(doc, mapping);
        }
    }

    private void ExtractInlineArrayItemSchemas(OpenApiDocument document)
    {
        if (document?.Components?.Schemas == null)
            return;

        var newSchemas = new Dictionary<string, OpenApiSchema>();
        var counter = 0;

        foreach ((string? schemaName, IOpenApiSchema? schema) in document.Components.Schemas.ToList())
        {
            if (schema.Type != JsonSchemaType.Array || schema.Items == null || schema.Items is OpenApiSchemaReference)
                continue;

            IOpenApiSchema? itemsSchema = schema.Items;

            if (itemsSchema.Type != JsonSchemaType.Object || (itemsSchema.Properties == null || !itemsSchema.Properties.Any()))
                continue;

            var itemName = $"{schemaName}_item";
            while (document.Components.Schemas.ContainsKey(itemName) || newSchemas.ContainsKey(itemName))
            {
                itemName = $"{schemaName}_item_{++counter}";
            }

            if (itemsSchema is OpenApiSchema concreteItemsSchema)
            {
                // In v2.3, Title is read-only, so we need to create a new schema
                var newSchema = new OpenApiSchema
                {
                    Type = concreteItemsSchema.Type,
                    Properties = concreteItemsSchema.Properties,
                    Required = concreteItemsSchema.Required,
                    Description = concreteItemsSchema.Description,
                    Format = concreteItemsSchema.Format,
                    Enum = concreteItemsSchema.Enum,
                    Items = concreteItemsSchema.Items,
                    AdditionalProperties = concreteItemsSchema.AdditionalProperties,
                    AllOf = concreteItemsSchema.AllOf,
                    OneOf = concreteItemsSchema.OneOf,
                    AnyOf = concreteItemsSchema.AnyOf,
                    Discriminator = concreteItemsSchema.Discriminator,
                    Title = itemName
                };
                newSchemas[itemName] = newSchema;
            }

            if (schema is OpenApiSchema concreteSchema)
            {
                concreteSchema.Items = new OpenApiSchemaReference(itemName);
            }

            _logger.LogInformation("Promoted inline array item schema from '{Parent}' to components schema '{ItemName}'", schemaName, itemName);
        }

        foreach ((string key, OpenApiSchema val) in newSchemas)
        {
            document.Components.Schemas[key] = val;
        }
    }

    public async ValueTask GenerateKiota(string fixedPath, string clientName, string libraryName, string targetDir,
        CancellationToken cancellationToken = default)
    {
        await _processUtil.Start("kiota", targetDir, $"generate -l CSharp -d \"{fixedPath}\" -o src -c {clientName} -n {libraryName} --ebc --cc",
                              waitForExit: true, cancellationToken: cancellationToken)
                          .NoSync();
    }

    private static bool TryGetSchemaRefId(IOpenApiSchema schema, out string? id)
    {
        id = null;

        switch (schema)
        {
            case OpenApiSchemaReference sr when sr.Reference.Id is { Length: > 0 }:
                id = sr.Reference.Id;
                return true;

            case OpenApiSchema os:
            {
                object? reference = os.GetType()
                                      .GetProperty("Reference")
                                      ?.GetValue(os);
                if (reference == null)
                    return false;

                string? typeValue = reference.GetType()
                                             .GetProperty("Type")
                                             ?.GetValue(reference)
                                             ?.ToString();
                string? idValue = reference.GetType()
                                           .GetProperty("Id")
                                           ?.GetValue(reference)
                                           ?.ToString();

                if (string.IsNullOrEmpty(idValue))
                    return false;

                if (typeValue is null || string.Equals(typeValue, "Schema", StringComparison.OrdinalIgnoreCase))
                {
                    id = idValue;
                    return true;
                }

                return false;
            }
        }

        return false;
    }

    private static bool IsSchemaRef(IOpenApiSchema s) => TryGetSchemaRefId(s, out _);

    private static string? GetSchemaRefId(IOpenApiSchema s) => TryGetSchemaRefId(s, out string? id) ? id : null;

    private static IOpenApiSchema MakeSchemaRef(string id) => new OpenApiSchemaReference(id);

    private static bool IsNonObjectLike(IOpenApiSchema? s)
    {
        if (s is null)
            return false;
        if (s is not OpenApiSchema os)
            return false;

        // Any non-object types should be wrapped (arrays, strings, numbers, booleans, integers)
        if (os.Type == JsonSchemaType.Array || os.Type == JsonSchemaType.String || os.Type == JsonSchemaType.Integer || os.Type == JsonSchemaType.Number ||
            os.Type == JsonSchemaType.Boolean)
            return true;

        // Enum-only or effectively-enum (no object properties)
        bool hasEnum = os.Enum is { Count: > 0 };
        bool isNotObject = os.Type != JsonSchemaType.Object;
        bool hasNoProps = os.Properties is null || os.Properties.Count == 0;
        if (hasEnum && (isNotObject || hasNoProps))
            return true;

        // Objects without properties but without enum can remain as objects
        return false;
    }


    /// <summary>
    /// Ensures parent has a string discriminator property and the property is required.
    /// </summary>
    private static void EnsureDiscriminatorProperty(OpenApiSchema parent)
    {
        if (parent.Discriminator is null)
            return;
        string disc = parent.Discriminator.PropertyName ?? "type";
        parent.Properties ??= new Dictionary<string, IOpenApiSchema>();
        if (!parent.Properties.ContainsKey(disc))
            parent.Properties[disc] = new OpenApiSchema { Type = JsonSchemaType.String };
        parent.Required ??= new HashSet<string>();
        parent.Required.Add(disc);
    }

    /// <summary>
    /// Recursively traverses all component schemas and ensures that wherever a discriminator is present,
    /// the discriminator property exists under properties and is marked as required.
    /// This covers nested locations like properties, items, allOf/anyOf/oneOf, and additionalProperties.
    /// </summary>
    private static void EnsureDiscriminatorRequiredEverywhere(OpenApiDocument doc)
    {
        if (doc.Components?.Schemas == null || doc.Components.Schemas.Count == 0)
            return;

        var visited = new HashSet<IOpenApiSchema>();

        void Visit(IOpenApiSchema? schema)
        {
            if (schema == null || !visited.Add(schema))
                return;

            if (schema is OpenApiSchema os && os.Discriminator != null)
            {
                EnsureDiscriminatorProperty(os);
            }

            if (schema.Properties != null)
            {
                foreach (IOpenApiSchema child in schema.Properties.Values)
                    Visit(child);
            }

            if (schema.Items != null)
                Visit(schema.Items);

            if (schema.AdditionalProperties != null)
                Visit(schema.AdditionalProperties);

            if (schema.AllOf != null)
            {
                foreach (IOpenApiSchema s in schema.AllOf)
                    Visit(s);
            }

            if (schema.AnyOf != null)
            {
                foreach (IOpenApiSchema s in schema.AnyOf)
                    Visit(s);
            }

            if (schema.OneOf != null)
            {
                foreach (IOpenApiSchema s in schema.OneOf)
                    Visit(s);
            }
        }

        foreach (IOpenApiSchema root in doc.Components.Schemas.Values)
        {
            Visit(root);
        }
    }

    /// <summary>
    /// Ensures that any discriminator mapping or union branch never points to an enum schema.
    /// If a target is an enum, we create an object wrapper and retarget both the branch and the mapping.
    /// This prevents Kiota CodeEnum→CodeClass cast crashes.
    /// </summary>
    private static void FixDiscriminatorMappingsForEnums(OpenApiDocument doc)
    {
        IDictionary<string, IOpenApiSchema>? comps = doc.Components?.Schemas;
        if (comps is null || comps.Count == 0)
            return;

        // Resolver to get a concrete schema for a possibly-ref branch
        IOpenApiSchema Resolve(IOpenApiSchema s)
        {
            if (IsSchemaRef(s) && GetSchemaRefId(s) is string id && comps.TryGetValue(id, out IOpenApiSchema? target))
                return target;
            return s;
        }

        foreach (KeyValuePair<string, IOpenApiSchema> kvp in comps.ToList())
        {
            string parentName = kvp.Key;
            IOpenApiSchema? parent = kvp.Value;
            if (parent?.Discriminator is null)
                continue;

            // Handle both oneOf and anyOf (mutate in place)
            IList<IOpenApiSchema>? branches = parent.OneOf ?? parent.AnyOf ?? parent.AllOf;
            if (branches is null || branches.Count == 0)
                continue;

            // Discriminator.mapping is string->OpenApiSchemaReference in v2.3
            parent.Discriminator.Mapping ??= new Dictionary<string, OpenApiSchemaReference>();

            bool changedBranches = false;

            for (int i = 0; i < branches.Count; i++)
            {
                IOpenApiSchema branch = branches[i];
                string? branchRefId = GetSchemaRefId(branch);
                IOpenApiSchema resolved = Resolve(branch);

                if (IsNonObjectLike(resolved))
                {
                    string baseName = branchRefId ?? $"{parentName}_Branch{i + 1}";
                    string wrapperName = ReserveUniqueSchemaName(comps, baseName, "Wrapper");

                    if (!comps.ContainsKey(wrapperName))
                    {
                        IOpenApiSchema valueSchema = branchRefId is not null ? MakeSchemaRef(branchRefId) : resolved;
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

                    // replace the branch with the wrapper ref
                    branches[i] = MakeSchemaRef(wrapperName);
                    changedBranches = true;

                    // --- NEW: fix mappings in all cases (ref OR inline) ---
                    // 1) if mapping targets a missing "ParentName_{i+1}" (your EnsureDiscriminatorForOneOf default)
                    string fallbackInlineId = $"{parentName}_{i + 1}";
                    foreach (string mapKey in parent.Discriminator.Mapping.Keys.ToList())
                    {
                        OpenApiSchemaReference val = parent.Discriminator.Mapping[mapKey];
                        string? valId = val.Reference.Id;

                        // retarget when the mapping pointed to the inline fallback OR the original branch ref id
                        if (string.Equals(valId, fallbackInlineId, StringComparison.Ordinal) ||
                            (branchRefId is not null && string.Equals(valId, branchRefId, StringComparison.Ordinal)))
                        {
                            parent.Discriminator.Mapping[mapKey] = new OpenApiSchemaReference(wrapperName);
                        }
                    }
                }
            }

            // No reassignment needed; branches were mutated in place

            // Extra safety: ensure discriminator property is defined on the parent
            if (parent is OpenApiSchema concreteParent)
            {
                EnsureDiscriminatorProperty(concreteParent);
            }

            // Also normalize mapping values so they ALWAYS reference components via JSON Pointer
            foreach (string k in parent.Discriminator.Mapping.Keys.ToList())
            {
                OpenApiSchemaReference v = parent.Discriminator.Mapping[k];
                string? id = v.Reference.Id;
                if (id is not null && comps.ContainsKey(id))
                    parent.Discriminator.Mapping[k] = new OpenApiSchemaReference(id);
                // If mapping points to an enum-like schema directly (not part of branches), wrap it too
                if (id is not null && comps.TryGetValue(id, out IOpenApiSchema? target) && IsNonObjectLike(target))
                {
                    string wrapperName = ReserveUniqueSchemaName(comps, id, "Wrapper");
                    if (!comps.ContainsKey(wrapperName))
                    {
                        comps[wrapperName] = new OpenApiSchema
                        {
                            Type = JsonSchemaType.Object,
                            Properties = new Dictionary<string, IOpenApiSchema> { ["value"] = new OpenApiSchemaReference(id) },
                            Required = new HashSet<string> { "value" }
                        };
                    }

                    parent.Discriminator.Mapping[k] = new OpenApiSchemaReference(wrapperName);
                }
            }
        }
    }

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

            bool changedBranches = false;

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
                    changedBranches = true;

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

            // Ensure discriminator property is defined on the parent
            if (parent is OpenApiSchema concreteParent)
            {
                EnsureDiscriminatorProperty(concreteParent);
            }
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
    /// Fixes malformed enum values that have quoted strings (e.g., "\"Latency\"" instead of "Latency").
    /// These malformed enums can cause Kiota to fail when trying to cast CodeEnum to CodeClass.
    /// </summary>
    private static void FixMalformedEnumValues(OpenApiDocument doc)
    {
        IDictionary<string, IOpenApiSchema>? comps = doc.Components?.Schemas;
        if (comps is null || comps.Count == 0)
            return;

        // Recursively fix malformed enum values in all schemas
        var visited = new HashSet<OpenApiSchema>();
        foreach (KeyValuePair<string, IOpenApiSchema> kvp in comps.ToList())
        {
            if (kvp.Value is OpenApiSchema schema)
            {
                FixMalformedEnumValuesRecursive(schema, visited, comps);
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

        void ProcessSchema(IOpenApiSchema? s, IDictionary<string, IOpenApiSchema>? comps)
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
                        if (rs.Type == JsonSchemaType.Object || (rs.Properties?.Any() == true) || rs.AdditionalProperties != null ||
                            rs.AdditionalPropertiesAllowed)
                            return true;
                    }
                }

                return false;
            }

            void ProcessUnion(IOpenApiSchema parent)
            {
                if (parent is not OpenApiSchema pos)
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
                            reverseLookup.TryGetValue(branchSchema, out string mappedId))
                        {
                            refId = mappedId;
                        }

                        if (refId != null && comps != null && comps.TryGetValue(refId, out IOpenApiSchema? target))
                            resolved = target;
                        bool isWrapperAlready = refId != null && refId.EndsWith("_Wrapper", StringComparison.Ordinal);

                        if (IsNonObjectLike(resolved) && !isWrapperAlready && (refId != null || allowInlineWrap))
                        {
                            string baseName = refId ?? (pos.Title ?? "UnionBranch");
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
                foreach (IOpenApiSchema child in os.Properties.Values)
                    ProcessSchema(child, comps);
            if (os.Items != null)
                ProcessSchema(os.Items, comps);
            if (os.AllOf != null)
                foreach (IOpenApiSchema c in os.AllOf)
                    ProcessSchema(c, comps);
            if (os.AnyOf != null)
                foreach (IOpenApiSchema c in os.AnyOf)
                    ProcessSchema(c, comps);
            if (os.OneOf != null)
                foreach (IOpenApiSchema c in os.OneOf)
                    ProcessSchema(c, comps);
            if (os.AdditionalProperties != null)
                ProcessSchema(os.AdditionalProperties, comps);
        }

        if (comps != null)
        {
            // Process all existing schemas first
            List<IOpenApiSchema> existingSchemas = comps.Values.ToList();
            foreach (IOpenApiSchema s in existingSchemas)
                ProcessSchema(s, comps);

            // Add new schemas after processing is complete
            foreach (KeyValuePair<string, IOpenApiSchema> kvp in newSchemas)
            {
                comps[kvp.Key] = kvp.Value;
                if (reverseLookup != null && kvp.Value != null && !reverseLookup.ContainsKey(kvp.Value))
                    reverseLookup[kvp.Value] = kvp.Key;
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
                            ProcessSchema(p.Schema, comps);

                if (path?.Operations != null)
                    foreach (var op in path.Operations.Values)
                    {
                        if (op?.Parameters != null)
                            foreach (var p in op.Parameters)
                                if (p?.Schema != null)
                                    ProcessSchema(p.Schema, comps);
                        if (op?.RequestBody is OpenApiRequestBody rb && rb.Content != null)
                            foreach (var mt in rb.Content.Values)
                                if (mt?.Schema != null)
                                    ProcessSchema(mt.Schema, comps);
                        if (op?.Responses != null)
                            foreach (var r in op.Responses.Values)
                            {
                                if (r?.Content != null)
                                    foreach (var mt in r.Content.Values)
                                        if (mt?.Schema != null)
                                            ProcessSchema(mt.Schema, comps);
                                if (r?.Headers != null)
                                    foreach (var h in r.Headers.Values)
                                        if (h?.Schema != null)
                                            ProcessSchema(h.Schema, comps);
                            }
                    }
            }
        }
    }

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
                if (propSchema.AllOf.Any(branch => GetSchemaRefId(branch) is string id && id.EndsWith("_Wrapper", StringComparison.Ordinal)))
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

                string wrapperName = $"{baseRefId}_Wrapper";
                if (!comps.ContainsKey(wrapperName))
                    wrapperName = ReserveUniqueSchemaName(comps, baseRefId, "Wrapper");

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

            foreach (JsonNode enumValue in schema.Enum)
            {
                if (enumValue is JsonValue jsonValue && jsonValue.TryGetValue(out string? stringValue))
                {
                    string trimmed = TrimQuotes(stringValue);
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
                // For inline schemas, we need to replace the enum values directly
                // Clear the existing enum and add the cleaned values
                schema.Enum.Clear();
                foreach (JsonNode cleanedValue in cleanedEnum)
                {
                    schema.Enum.Add(cleanedValue);
                }
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
                foreach (var (status, resp) in op.Responses)
                {
                    if (string.IsNullOrEmpty(status) || (status[0] != '4' && status[0] != '5'))
                        continue;
                    if (resp?.Content is null)
                        continue;

                    foreach (var (_, media) in resp.Content)
                    {
                        var s = media?.Schema;
                        if (s is OpenApiSchemaReference r && r.Reference.Type == ReferenceType.Schema && !string.IsNullOrEmpty(r.Reference.Id))
                        {
                            targetIds.Add(r.Reference.Id);
                        }
                        else if (s is OpenApiSchema inline)
                        {
                            // Inline error body: patch it locally.
                            CoerceMessageArrayToString(inline);
                        }
                    }
                }
            }
        }

        if (targetIds.Count == 0)
            return;

        // 2) Patch only those component schemas.
        foreach (var id in targetIds)
        {
            if (doc.Components.Schemas.TryGetValue(id, out var schema) && schema is OpenApiSchema os)
                CoerceMessageArrayToString(os);
        }

        // --- local helper ---
        static void CoerceMessageArrayToString(OpenApiSchema container)
        {
            if (container is null)
                return;

            // Depth-first, but very narrow: touch only the 'message' property at this level.
            if (container.Properties is { } props && props.TryGetValue("message", out var msg) && msg is OpenApiSchema m &&
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
                    foreach (var kv in m.Extensions)
                        replacement.Extensions[kv.Key] = kv.Value;

                container.Properties["message"] = replacement;
            }

            // No recursive rewrite: we don’t mutate nested children unless they are
            // themselves direct error bodies (handled when collected from responses).
        }
    }

    private sealed class ReferenceEqualityComparer<T> : IEqualityComparer<T> where T : class
    {
        public static ReferenceEqualityComparer<T> Instance { get; } = new();

        public bool Equals(T? x, T? y) => ReferenceEquals(x, y);

        public int GetHashCode(T obj) => RuntimeHelpers.GetHashCode(obj);
    }
}