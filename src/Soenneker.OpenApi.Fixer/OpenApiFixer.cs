using Microsoft.Extensions.Logging;
using Microsoft.OpenApi;
using Microsoft.OpenApi.Reader;
using Soenneker.Extensions.ValueTask;
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
using Soenneker.Utils.Directory.Abstract;
using Soenneker.Utils.File.Abstract;
using Soenneker.OpenApi.Fixer.Fixers.Abstract;
using Soenneker.OpenApi.Fixer.Abstract;

namespace Soenneker.OpenApi.Fixer;

///<inheritdoc cref="IOpenApiFixer"/>
public sealed class OpenApiFixer : IOpenApiFixer
{
    private static readonly Regex GeneratedEnumMemberRegex = new(
        @"(?<prefix>\[EnumMember\(Value = ""(?<value>(?:[^""\\]|\\.)*)""\)\]\s*#pragma warning disable CS1591\s*)(?<name>[^\r\n,]+)(?<suffix>,\s*#pragma warning restore CS1591)",
        RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private static readonly Dictionary<string, string> MultiCharacterEnumTokens = new(StringComparer.Ordinal)
    {
        ["!="] = "ExclamationEqual",
        ["!~"] = "ExclamationTilde",
        ["<="] = "LessThanOrEqual",
        [">="] = "GreaterThanOrEqual",
        ["=="] = "DoubleEqual"
    };

    private static readonly Dictionary<char, string> EnumSymbolTokens = new()
    {
        ['!'] = "Exclamation",
        ['"'] = "Quote",
        ['#'] = "Hash",
        ['$'] = "Dollar",
        ['%'] = "Percent",
        ['&'] = "Ampersand",
        ['\''] = "Apostrophe",
        ['('] = "LeftParenthesis",
        [')'] = "RightParenthesis",
        ['*'] = "Asterisk",
        ['+'] = "Plus",
        [','] = "Comma",
        ['-'] = "Minus",
        ['.'] = "Dot",
        ['/'] = "Slash",
        [':'] = "Colon",
        [';'] = "Semicolon",
        ['<'] = "LessThan",
        ['='] = "Equal",
        ['>'] = "GreaterThan",
        ['?'] = "QuestionMark",
        ['@'] = "At",
        ['['] = "LeftBracket",
        ['\\'] = "BackSlash",
        [']'] = "RightBracket",
        ['^'] = "Caret",
        ['{'] = "LeftBrace",
        ['|'] = "Pipe",
        ['}'] = "RightBrace",
        ['~'] = "Tilde"
    };

    private readonly ILogger<OpenApiFixer> _logger;

    private readonly IProcessUtil _processUtil;
    private readonly IOpenApiDescriptionFixer _descriptionFixer;
    private readonly IOpenApiReferenceFixer _referenceFixer;
    private readonly IOpenApiNamingFixer _namingFixer;
    private readonly IOpenApiSchemaFixer _schemaFixer;
    private readonly IDirectoryUtil _directoryUtil;
    private readonly IFileUtil _fileUtil;

    public OpenApiFixer(ILogger<OpenApiFixer> logger, IProcessUtil processUtil, IOpenApiDescriptionFixer descriptionFixer,
        IOpenApiReferenceFixer referenceFixer, IOpenApiNamingFixer namingFixer, IOpenApiSchemaFixer schemaFixer,
        IDirectoryUtil directoryUtil, IFileUtil fileUtil)
    {
        _logger = logger;
        _processUtil = processUtil;
        _descriptionFixer = descriptionFixer;
        _referenceFixer = referenceFixer;
        _namingFixer = namingFixer;
        _schemaFixer = schemaFixer;
        _directoryUtil = directoryUtil;
        _fileUtil = fileUtil;
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
            _descriptionFixer.FixYamlUnsafeDescriptions(document!);
            EnsureSecuritySchemes(document!);
            _namingFixer.RenameConflictingPaths(document!);

            _namingFixer.RenameInvalidComponentSchemas(document!);

            _logger.LogInformation("Resolving collisions between operation IDs and schema names...");
            _namingFixer.ResolveSchemaOperationNameCollisions(document!);

            _logger.LogInformation("Ensuring unique operation IDs...");
            _logger.LogInformation("Normalizing operation IDs...");
            _namingFixer.NormalizeOperationIds(document!);

            _namingFixer.EnsureUniqueOperationIds(document!);

            // STAGE 2: REFERENCE INTEGRITY & SCRUBBING
            _logger.LogInformation("Scrubbing all component references to fix broken links...");
            _referenceFixer.ScrubComponentRefs(document!, cancellationToken);
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
            _referenceFixer.ScrubComponentRefs(document!, cancellationToken);
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
                        _schemaFixer.DeepCleanSchema(concreteSchema, []);
                    }
                }
            }

            LogState("After STAGE 4D: Deep Cleaning", document);

            FixMalformedEnumValues(document);
            LogState("After STAGE 4E.1: FixMalformedEnumValues", document);

            StripEmptyEnumBranches(document);
            LogState("After STAGE 4E: StripEmptyEnumBranches", document);

            _schemaFixer.FixInvalidDefaults(document);
            LogState("After STAGE 4F: FixInvalidDefaults", document);

            FixAllInlineValueEnums(document);
            LogState("After STAGE 4G: FixAllInlineValueEnums", document);

            PromoteEnumBranchesUnderDiscriminator(document);
            LogState("After STAGE 4H: PromoteEnumBranchesUnderDiscriminator", document);

            WrapEnumBranchesInCompositions(document);
            LogState("After STAGE 4H.1: WrapEnumBranchesInCompositions", document);

            // Re-scrub references after creating new wrapper components
            _referenceFixer.ScrubComponentRefs(document, cancellationToken);
            LogState("After STAGE 4I: Re-Scrub After Enum Promotion", document);

            // STAGE 5: FINAL CLEANUP
            _logger.LogInformation("Performing final cleanup of empty keys and invalid structures...");
            _schemaFixer.RemoveEmptyInlineSchemas(document);
            _schemaFixer.RemoveInvalidDefaults(document);

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
            _referenceFixer.FixRefsPointingIntoPathsExamples(document);

            // Remove empty-string enum values that cause empty child names in generators
            StripEmptyStringEnumValues(document);

            // Final safety net: ensure no union branch is a non-object (enums, primitives, arrays)
            WrapNonObjectUnionBranchesEverywhere(document);
            FlattenMapAllOfCompositions(document);
            InlineMapOnlySchemaReferences(document);
            NormalizeAllOfWrappers(document);

            InlinePrimitivePropertyRefs(document);
            EnsureInlineObjectTypes(document!);
            EnsureNoNullSchemas(document);

            // Kiota (and some other generators) fail on duplicate branches in anyOf/oneOf/allOf (e.g. duplicated $ref entries).
            _schemaFixer.DeduplicateCompositionBranches(document);

            _schemaFixer.CleanDocumentForSerialization(document);

            LogDanglingOrPrimitivePropertyRefs(document!);

            // Run discriminator-required pass one last time to guarantee required flags are present
            EnsureDiscriminatorRequiredEverywhere(document);

            // Kiota can emit invalid assignments when discriminator enum properties carry string defaults.
            // Remove those defaults so generated C# compiles consistently.
            RemoveEnumDefaultsFromDiscriminatorLikeProperties(document);

            // Final validation: ensure all schema names are valid
            _namingFixer.ValidateAndFixSchemaNames(document);

            string json = await document.SerializeAsync(OpenApiSpecVersion.OpenApi3_0, OpenApiConstants.Json, cancellationToken: cancellationToken);

            // Fix JSON boolean values (convert Python-style True/False to JSON true/false)
            json = FixJsonBooleanValues(json);

            // Add enum member names for symbol-only values so Kiota can generate valid identifiers directly from the fixed spec.
            json = InjectKiotaEnumValueNames(json);

            await _fileUtil.Write(targetFilePath, json, cancellationToken: cancellationToken);

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
            List<string> deprecatedSchemaKeys = document.Components.Schemas.Where(kvp => kvp.Value is OpenApiSchema schema && schema.Deprecated)
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

    private async ValueTask<MemoryStream> PreprocessSpecFile(string path, CancellationToken cancellationToken = default)
    {
        string raw = await _fileUtil.Read(path, cancellationToken: cancellationToken);

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
    /// Applies schema normalizations to the document.
    /// </summary>
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

                        _referenceFixer.ScrubBrokenRefs(resp.Value.Content, document);

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
                                                                                  return sch is OpenApiSchemaReference || !_schemaFixer.IsSchemaEmpty(sch);
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

                        _referenceFixer.ScrubBrokenRefs(normalizedContent, document);
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
                _schemaFixer.InjectTypeForNullable(concreteRoot, visitedSchemas);
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

    private void AddComponentSchema(OpenApiDocument doc, string compName, OpenApiSchema schema)
    {
        if (string.IsNullOrWhiteSpace(compName))
        {
            _logger.LogWarning("Skipped adding a component schema because its generated name was empty.");
            return;
        }

        string validatedName = _namingFixer.ValidateComponentName(compName);

        if (!doc.Components.Schemas.ContainsKey(validatedName))
        {
            // In v2.3, Title is read-only, so we can't modify it directly
            doc.Components.Schemas[validatedName] = schema;
        }
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

                    string safeOpId = _namingFixer.ValidateComponentName(_namingFixer.GenerateSafePart(operation.OperationId ?? "unnamed", opType.ToString()));

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
                        foreach ((string mediaType, IOpenApiMediaType mediaInterface) in operation.RequestBody.Content!.ToList())
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
                                safeMedia = _namingFixer.ValidateComponentName(_namingFixer.GenerateSafePart(subtype, "media"));

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

                            foreach ((string mediaType, IOpenApiMediaType mediaInterface) in response.Content!.ToList())
                            {
                                if (mediaInterface is not OpenApiMediaType media)
                                    continue;

                                IOpenApiSchema? schemaResp = media.Schema;
                                if (schemaResp == null || schemaResp is OpenApiSchemaReference)
                                    continue;
                                if (schemaResp is OpenApiSchema concreteSchemaResp1 && IsSimpleEnvelope(concreteSchemaResp1))
                                    continue;

                                string safeMedia = _namingFixer.ValidateComponentName(_namingFixer.GenerateSafePart(mediaType, "media"));
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
        await using MemoryStream stream = await _fileUtil.ReadToMemoryStream(filePath, cancellationToken: cancellationToken);

        var reader = new OpenApiJsonReader(); // force JSON
        ReadResult read = await reader.ReadAsync(stream, new Uri(filePath), // base URI for relative $refs
                                          new OpenApiReaderSettings(), cancellationToken)
                                      .NoSync();

        OpenApiDiagnostic? diagnostics = read.Diagnostic;
        if (diagnostics?.Errors?.Any() == true)
            _logger.LogWarning("OpenAPI parsing errors in {File}: {Msgs}", Path.GetFileName(filePath),
                string.Join("; ", diagnostics.Errors.Select(e => e.Message)));
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

    private string InjectKiotaEnumValueNames(string json)
    {
        if (string.IsNullOrWhiteSpace(json))
            return json;

        JsonNode? root;

        try
        {
            root = JsonNode.Parse(json);
        }
        catch (JsonException ex)
        {
            _logger.LogDebug(ex, "Unable to parse serialized OpenAPI JSON when injecting Kiota enum names");
            return json;
        }

        if (root is null)
            return json;

        bool changed = InjectKiotaEnumValueNames(root, null);

        return changed ? root.ToJsonString(new JsonSerializerOptions { WriteIndented = true }) : json;
    }

    private static bool InjectKiotaEnumValueNames(JsonNode? node, string? suggestedName)
    {
        switch (node)
        {
            case JsonObject obj:
            {
                bool changed = TryInjectKiotaEnumValueNames(obj, suggestedName);

                foreach ((string key, JsonNode? child) in obj)
                {
                    switch (key)
                    {
                        case "schemas":
                        case "properties":
                            if (child is JsonObject namedChildren)
                            {
                                foreach ((string childName, JsonNode? namedChild) in namedChildren)
                                {
                                    changed |= InjectKiotaEnumValueNames(namedChild, childName);
                                }
                            }
                            break;
                        case "items":
                            changed |= InjectKiotaEnumValueNames(child, $"{suggestedName ?? "Item"}Item");
                            break;
                        case "additionalProperties":
                            changed |= InjectKiotaEnumValueNames(child, $"{suggestedName ?? "AdditionalProperty"}AdditionalProperty");
                            break;
                        default:
                            changed |= InjectKiotaEnumValueNames(child, suggestedName);
                            break;
                    }
                }

                return changed;
            }
            case JsonArray array:
            {
                bool changed = false;

                foreach (JsonNode? child in array)
                {
                    changed |= InjectKiotaEnumValueNames(child, suggestedName);
                }

                return changed;
            }
            default:
                return false;
        }
    }

    private static bool TryInjectKiotaEnumValueNames(JsonObject schemaObject, string? suggestedName)
    {
        if (schemaObject["enum"] is not JsonArray enumArray || enumArray.Count == 0)
            return false;

        var namesToInject = new Dictionary<string, string>(StringComparer.Ordinal);

        foreach (JsonNode? enumNode in enumArray)
        {
            if (enumNode is not JsonValue enumValue || !enumValue.TryGetValue(out string? enumText) || !ShouldInjectKiotaEnumName(enumText))
                continue;

            namesToInject[enumText] = BuildSafeEnumMemberName(enumText);
        }

        if (namesToInject.Count == 0)
            return false;

        bool changed = false;
        JsonObject xMsEnum = schemaObject["x-ms-enum"] as JsonObject ?? new JsonObject();

        if (schemaObject["x-ms-enum"] is null)
        {
            schemaObject["x-ms-enum"] = xMsEnum;
            changed = true;
        }

        if (xMsEnum["name"] is null)
        {
            xMsEnum["name"] = BuildSafeEnumMemberName(suggestedName ?? "GeneratedEnum");
            changed = true;
        }

        if (xMsEnum["modelAsString"] is null)
        {
            xMsEnum["modelAsString"] = false;
            changed = true;
        }

        JsonArray valuesArray = xMsEnum["values"] as JsonArray ?? new JsonArray();

        if (xMsEnum["values"] is null)
        {
            xMsEnum["values"] = valuesArray;
            changed = true;
        }

        foreach ((string enumValue, string enumName) in namesToInject)
        {
            JsonObject? existingValue = valuesArray
                                        .OfType<JsonObject>()
                                        .FirstOrDefault(valueObject =>
                                            valueObject["value"] is JsonValue value &&
                                            value.TryGetValue(out string? existingEnumValue) &&
                                            string.Equals(existingEnumValue, enumValue, StringComparison.Ordinal));

            if (existingValue is null)
            {
                valuesArray.Add(new JsonObject
                {
                    ["value"] = enumValue,
                    ["name"] = enumName
                });
                changed = true;
                continue;
            }

            if (existingValue["name"] is not JsonValue nameValue ||
                !nameValue.TryGetValue(out string? existingName) ||
                string.IsNullOrWhiteSpace(existingName))
            {
                existingValue["name"] = enumName;
                changed = true;
            }
        }

        return changed;
    }

    private static bool ShouldInjectKiotaEnumName(string enumValue)
    {
        if (string.IsNullOrWhiteSpace(enumValue))
            return false;

        bool hasNonWhitespace = false;

        foreach (char character in enumValue)
        {
            if (char.IsWhiteSpace(character))
                continue;

            hasNonWhitespace = true;

            if (char.IsLetterOrDigit(character))
                return false;
        }

        return hasNonWhitespace;
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


    private static string CanonicalSuccess(HttpMethod op) => op.Method switch
    {
        "POST" => "201",
        "DELETE" => "204",
        _ => "200"
    };


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
        await _directoryUtil.Create(targetDir, cancellationToken: cancellationToken).NoSync();

        string outputDir = $"src/{libraryName}";
        await _processUtil.Start("kiota", targetDir, $"generate -l CSharp -d \"{fixedPath}\" -o {outputDir} -c {clientName} -n {libraryName} --ebc --cc",
                              waitForExit: true, cancellationToken: cancellationToken)
                          .NoSync();

        await SanitizeGeneratedEnumMembers(targetDir, cancellationToken).NoSync();
    }

    public async ValueTask SanitizeGeneratedEnumMembers(string generatedRoot, CancellationToken cancellationToken = default)
    {
        if (!Directory.Exists(generatedRoot))
            return;

        foreach (string filePath in Directory.EnumerateFiles(generatedRoot, "*.cs", SearchOption.AllDirectories))
        {
            cancellationToken.ThrowIfCancellationRequested();

            string original = await _fileUtil.Read(filePath, cancellationToken: cancellationToken).NoSync();

            if (!original.Contains("GeneratedCode(\"Kiota\"", StringComparison.Ordinal) &&
                !original.Contains("<auto-generated/>", StringComparison.Ordinal))
            {
                continue;
            }

            string sanitized = SanitizeGeneratedEnumMembers(original);

            if (!string.Equals(original, sanitized, StringComparison.Ordinal))
                await _fileUtil.Write(filePath, sanitized, cancellationToken: cancellationToken).NoSync();
        }
    }

    private static string SanitizeGeneratedEnumMembers(string fileContents)
    {
        MatchCollection matches = GeneratedEnumMemberRegex.Matches(fileContents);

        if (matches.Count == 0)
            return fileContents;

        var usedNames = matches.Select(match => match.Groups["name"]
                                                     .Value
                                                     .Trim())
                               .Where(name => name.Length > 0)
                               .ToHashSet(StringComparer.Ordinal);

        return GeneratedEnumMemberRegex.Replace(fileContents, match =>
        {
            string currentName = match.Groups["name"].Value.Trim();

            if (IsValidEnumMemberIdentifier(currentName))
                return match.Value;

            usedNames.Remove(currentName);

            string enumValue = Regex.Unescape(match.Groups["value"].Value);
            string replacementName = BuildSafeEnumMemberName(enumValue);
            string uniqueName = MakeUniqueEnumMemberName(replacementName, usedNames);

            usedNames.Add(uniqueName);

            return $"{match.Groups["prefix"].Value}{uniqueName}{match.Groups["suffix"].Value}";
        });
    }

    private static bool IsValidEnumMemberIdentifier(string name) =>
        !string.IsNullOrWhiteSpace(name) && Regex.IsMatch(name, @"^[_\p{L}][_\p{L}\p{Nd}]*$");

    private static string MakeUniqueEnumMemberName(string candidate, HashSet<string> usedNames)
    {
        string safeCandidate = string.IsNullOrWhiteSpace(candidate) ? "EnumValue" : candidate;

        if (!usedNames.Contains(safeCandidate))
            return safeCandidate;

        int suffix = 1;
        string uniqueCandidate;

        do
        {
            uniqueCandidate = $"{safeCandidate}_{suffix++}";
        } while (usedNames.Contains(uniqueCandidate));

        return uniqueCandidate;
    }

    private static string BuildSafeEnumMemberName(string enumValue)
    {
        if (string.IsNullOrWhiteSpace(enumValue))
            return "EnumValue";

        if (MultiCharacterEnumTokens.TryGetValue(enumValue, out string? combinedToken))
            return combinedToken;

        var builder = new StringBuilder(enumValue.Length * 2);
        bool capitalizeNext = true;

        for (int i = 0; i < enumValue.Length; i++)
        {
            if (TryGetMultiCharacterEnumToken(enumValue, i, out string? multiCharacterToken, out int tokenLength))
            {
                builder.Append(multiCharacterToken);
                capitalizeNext = true;
                i += tokenLength - 1;
                continue;
            }

            char character = enumValue[i];

            if (char.IsLetterOrDigit(character))
            {
                if (builder.Length == 0 && char.IsDigit(character))
                    builder.Append("Value");

                builder.Append(capitalizeNext ? char.ToUpperInvariant(character) : character);
                capitalizeNext = false;
                continue;
            }

            if (character == '_' || char.IsWhiteSpace(character))
            {
                capitalizeNext = true;
                continue;
            }

            if (EnumSymbolTokens.TryGetValue(character, out string? symbolToken))
            {
                builder.Append(symbolToken);
                capitalizeNext = true;
            }
        }

        string sanitized = builder.ToString();

        if (string.IsNullOrWhiteSpace(sanitized))
            sanitized = "EnumValue";

        if (!char.IsLetter(sanitized[0]) && sanitized[0] != '_')
            sanitized = $"Value{sanitized}";

        return sanitized;
    }

    private static bool TryGetMultiCharacterEnumToken(string value, int startIndex, out string? token, out int tokenLength)
    {
        foreach ((string symbol, string mappedToken) in MultiCharacterEnumTokens)
        {
            if (startIndex + symbol.Length > value.Length)
                continue;

            if (string.CompareOrdinal(value, startIndex, symbol, 0, symbol.Length) == 0)
            {
                token = mappedToken;
                tokenLength = symbol.Length;
                return true;
            }
        }

        token = null;
        tokenLength = 0;
        return false;
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

    private static void RemoveEnumDefaultsFromDiscriminatorLikeProperties(OpenApiDocument document)
    {
        if (document.Components?.Schemas == null || document.Components.Schemas.Count == 0)
            return;

        var visited = new HashSet<IOpenApiSchema>();

        void Visit(IOpenApiSchema? schema)
        {
            if (schema == null || !visited.Add(schema))
                return;

            if (schema is not OpenApiSchema concrete)
                return;

            if (concrete.Properties != null)
            {
                foreach ((string propName, IOpenApiSchema propSchema) in concrete.Properties)
                {
                    if (propSchema is OpenApiSchema prop &&
                        prop.Enum is { Count: > 0 } &&
                        prop.Default is JsonValue defaultValue &&
                        defaultValue.GetValueKind() == JsonValueKind.String &&
                        string.Equals(propName, "type", StringComparison.OrdinalIgnoreCase))
                    {
                        prop.Default = null;
                    }

                    Visit(propSchema);
                }
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
        }

        foreach (IOpenApiSchema root in document.Components.Schemas.Values)
        {
            Visit(root);
        }
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

        if (schema is OpenApiSchemaReference sr && sr.Reference?.Id is { Length: > 0 } id &&
            comps.TryGetValue(id, out IOpenApiSchema? target) && target is OpenApiSchema targetSchema)
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
        bool isObjectOrUnset = schema.Type == null || schema.Type == JsonSchemaType.Object;
        bool hasMapSemantics = schema.AdditionalProperties != null || schema.AdditionalPropertiesAllowed;

        return hasNoCompositions && hasNoNamedProperties && hasNoItems && isObjectOrUnset && hasMapSemantics;
    }

    private static bool IsFlattenableOverlayObject(OpenApiSchema schema)
    {
        bool hasNoCompositions = (schema.AllOf?.Count ?? 0) == 0 && (schema.AnyOf?.Count ?? 0) == 0 && (schema.OneOf?.Count ?? 0) == 0;
        bool hasNoMapShape = schema.AdditionalProperties == null && !schema.AdditionalPropertiesAllowed;
        bool hasNoItems = schema.Items == null;
        bool isObjectOrUnset = schema.Type == null || schema.Type == JsonSchemaType.Object;

        return hasNoCompositions && hasNoMapShape && hasNoItems && isObjectOrUnset;
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
            if (schema is OpenApiSchemaReference sr && sr.Reference?.Id is { Length: > 0 } id &&
                comps.TryGetValue(id, out IOpenApiSchema? target) && target is OpenApiSchema targetSchema &&
                IsPureMapSchema(targetSchema))
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
                            CoerceMessageArrayToString(inline);
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
                CoerceMessageArrayToString(os);
        }

        // --- local helper ---
        static void CoerceMessageArrayToString(OpenApiSchema container)
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