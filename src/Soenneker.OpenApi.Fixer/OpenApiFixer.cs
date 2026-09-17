using Microsoft.Extensions.Logging;
using Microsoft.OpenApi;
using Microsoft.OpenApi.Reader;
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
using System.Runtime.CompilerServices;
using Soenneker.Extensions.Task;
using Soenneker.Utils.File.Abstract;
using Soenneker.OpenApi.Fixer.Fixers.Abstract;
using Soenneker.OpenApi.Fixer.Abstract;
using Soenneker.Utils.Directory.Abstract;
using Soenneker.Utils.MemoryStream.Abstract;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer : IOpenApiFixer
{
    private readonly ILogger<OpenApiFixer> _logger;

    private readonly IOpenApiDescriptionFixer _descriptionFixer;
    private readonly IOpenApiReferenceFixer _referenceFixer;
    private readonly IOpenApiNamingFixer _namingFixer;
    private readonly IOpenApiSchemaFixer _schemaFixer;
    private readonly IOpenApiInt32IdFixer _int32IdFixer;
    private readonly IOpenApiPreprocessingFixer _preprocessingFixer;
    private readonly IFileUtil _fileUtil;
    private readonly IDirectoryUtil _directoryUtil;
    private readonly IMemoryStreamUtil _memoryStreamUtil;

    public OpenApiFixer(ILogger<OpenApiFixer> logger, IOpenApiDescriptionFixer descriptionFixer,
        IOpenApiReferenceFixer referenceFixer, IOpenApiNamingFixer namingFixer, IOpenApiSchemaFixer schemaFixer,
        IOpenApiInt32IdFixer int32IdFixer, IOpenApiPreprocessingFixer preprocessingFixer, IFileUtil fileUtil,
        IDirectoryUtil directoryUtil, IMemoryStreamUtil memoryStreamUtil)
    {
        _logger = logger;
        _descriptionFixer = descriptionFixer;
        _referenceFixer = referenceFixer;
        _namingFixer = namingFixer;
        _schemaFixer = schemaFixer;
        _int32IdFixer = int32IdFixer;
        _preprocessingFixer = preprocessingFixer;
        _fileUtil = fileUtil;
        _directoryUtil = directoryUtil;
        _memoryStreamUtil = memoryStreamUtil;
    }

    public async ValueTask Fix(string sourceFilePath, string targetFilePath,
        CancellationToken cancellationToken = default)
    {
        await Fix(sourceFilePath, targetFilePath, null, cancellationToken).NoSync();
    }

    public async ValueTask Fix(string sourceFilePath, string targetFilePath, OpenApiFixerOptions? options,
        CancellationToken cancellationToken = default)
    {
        try
        {
            options ??= new OpenApiFixerOptions();
            // STAGE 0: DOCUMENT LOADING & INITIAL PARSING
            await using MemoryStream pre = await PreprocessSpecFile(sourceFilePath, options, cancellationToken);
            OpenApiSpecVersion sourceSpecVersion = DetectSpecVersion(pre);
            ReadResult read = await new OpenApiJsonReader().ReadAsync(pre, new Uri(Path.GetFullPath(sourceFilePath)),
                new OpenApiReaderSettings(), cancellationToken).NoSync();
            OpenApiDocument? document = read.Document;
            OpenApiDiagnostic? diagnostics = read.Diagnostic;

            if (diagnostics?.Errors?.Any() == true)
            {
                string msgs = string.Join("; ", diagnostics.Errors.Select(e => e.Message));
                _logger.LogWarning("OpenAPI parsing errors during loading: {Messages}", msgs);
            }

            if (document is null)
                throw new InvalidOperationException($"Unable to load OpenAPI document from '{sourceFilePath}'.");

            NormalizeRequiredInfo(document);
            document.Paths ??= new OpenApiPaths();

            Dictionary<string, string> attachedWebhooks = AttachWebhooksToPaths(document!);

            // STAGE 1: IDENTIFIERS, NAMING, AND SECURITY
            _logger.LogDebug("Running initial cleanup on identifiers, paths, and security schemes...");
            _descriptionFixer.FixYamlUnsafeDescriptions(document!);
            _namingFixer.RenameConflictingPaths(document!);

            if (options.StripDateSuffixesFromGeneratedNames)
                _namingFixer.StripDateSuffixesFromGeneratedNames(document!);

            _namingFixer.RenameInvalidComponentSchemas(document!);

            _logger.LogDebug("Normalizing operation IDs...");
            _namingFixer.NormalizeOperationIds(document!);

            _logger.LogDebug("Ensuring unique operation IDs...");
            _namingFixer.EnsureUniqueOperationIds(document!);

            _logger.LogDebug("Resolving collisions between operation IDs and schema names...");
            _namingFixer.ResolveSchemaOperationNameCollisions(document!);

            // STAGE 2: REFERENCE INTEGRITY & SCRUBBING
            _logger.LogDebug("Scrubbing all component references to fix broken links...");
            _referenceFixer.ScrubComponentRefs(document!, cancellationToken);

            // STAGE 3: STRUCTURAL TRANSFORMATIONS
            _logger.LogDebug("Performing major structural transformations (inlining, extraction)...");
            InlinePrimitiveComponents(document!);
            DisambiguateMultiContentRequestSchemas(document!);

            FixContentTypeWrapperCollisions(document!);

            EnsureInlineSchemaTypes(document!);
            ExtractInlineArrayItemSchemas(document!);
            ExtractInlineComponentContentSchemas(document!);
            _schemaFixer.NormalizeNullablePrimitiveCompositions(document!);
            ExtractInlineComposedSchemas(document!);
            ExtractInlineObjectPropertySchemas(document!);
            ExtractInlineSchemas(document!, cancellationToken);

            _logger.LogDebug("Removing shadowed untyped properties…");
            RemoveShadowingUntypedProperties(document!);
            RemoveRedundantDerivedValue(document!);

            _logger.LogDebug("Re-scrubbing references after extraction...");
            _referenceFixer.ScrubComponentRefs(document!, cancellationToken);

            // STAGE 4: DEEP SCHEMA NORMALIZATION & CLEANING
            _logger.LogDebug("Applying deep schema normalizations and cleaning...");

            RewriteCombinedUnionsAsIntersection(document);

            ApplySchemaNormalizations(document!, cancellationToken);
            RemoveDiscriminatorsFromNonObjectSchemas(document!);

            FixErrorMessageArrayCollision(document!);

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

            FixMalformedEnumValues(document);

            StripEmptyEnumBranches(document);

            _schemaFixer.FixInvalidDefaults(document);
            RemoveStringDefaultsFromUuidSchemas(document);

            FixAllInlineValueEnums(document);

            PromoteEnumBranchesUnderDiscriminator(document);

            WrapEnumBranchesInCompositions(document);

            // Re-scrub references after creating new wrapper components
            _referenceFixer.ScrubComponentRefs(document, cancellationToken);

            // STAGE 5: FINAL CLEANUP
            _logger.LogDebug("Performing final cleanup of empty keys and invalid structures...");
            _schemaFixer.RemoveEmptyInlineSchemas(document);
            _schemaFixer.RemoveInvalidDefaults(document);

            // STAGE 6: FINAL VALIDATION AND CLEANUP
            _logger.LogDebug("Final validation and cleanup process started...");

            // Scrub bogus enums under vendor extensions and harden enum schemas missing type
            FixBadEnums(document);

            // Fix discriminator mappings that reference non-existent or enum schemas
            FixDiscriminatorMappingsForEnums(document);

            // Fix properties declared as object that actually allOf an enum schema
            FixEnumAllOfObjectPropertyMismatch(document);

            // Discriminators are only valid for object polymorphism. Drop any carried by primitive convenience unions before enum wrapper passes.
            RemoveDiscriminatorsFromNonObjectSchemas(document);

            // Blanket safety: wrap any enum-like or primitive branches in unions so Kiota always sees classes
            ComprehensiveEnumWrapperFix(document);

            // Replace $refs that drill into #/paths/.../examples/... with component schema refs
            _referenceFixer.FixRefsPointingIntoPathsExamples(document);

            // Final safety net: ensure no union branch is a non-object (enums, primitives, arrays)
            WrapNonObjectUnionBranchesEverywhere(document);
            NormalizeNonObjectAllOfCompositions(document);
            FlattenMapAllOfCompositions(document);
            InlineMapOnlySchemaReferences(document);
            NormalizeAllOfWrappers(document);
            FlattenObjectAllOfCompositions(document);
            RemoveMetadataOnlyAllOfBranches(document);
            FixEnumAllOfObjectPropertyMismatch(document);

            InlinePrimitivePropertyRefs(document);
            NormalizeNonObjectAllOfCompositions(document);
            WrapNonObjectUnionBranchesEverywhere(document);
            CollapseNonDiscriminatedInlineObjectUnions(document);
            EnsureInlineSchemaTypes(document!);
            ExtractInlineSchemasCore(document!, cancellationToken, false);
            ExtractInlineComponentContentSchemas(document!);
            ExtractInlineComposedSchemas(document!);
            ExtractInlineObjectPropertySchemas(document!);
            _schemaFixer.NormalizeNullablePrimitiveCompositions(document!);
            NormalizeSingletonStringConstsAsEnums(document!);
            ExtractInlineEnumSchemas(document!);
            RemoveMetadataOnlyAllOfBranches(document);
            EnsureNoNullSchemas(document);

            if (options.Int32IdTransform)
                _int32IdFixer.Transform(document);

            // Kiota (and some other generators) fail on duplicate branches in anyOf/oneOf/allOf (e.g. duplicated $ref entries).
            _schemaFixer.DeduplicateCompositionBranches(document);

            _schemaFixer.CleanDocumentForSerialization(document);
            StripEmptyPropertyNames(document);

            LogDanglingOrPrimitivePropertyRefs(document!);

            RemoveDiscriminatorsFromNonObjectSchemas(document);

            // Kiota can emit invalid assignments when string enum defaults use wire values that differ from generated member names.
            // Remove those defaults so generated C# compiles consistently.
            RemoveStringDefaultsFromEnumOrConstSchemas(document);

            PromoteNestedDiscriminatorUnions(document);

            // Give generators concrete access to the real wire properties of composed object models without
            // inventing discriminator fields or replacing the source oneOf/anyOf constraints.
            ExposeComposedObjectPropertiesForGenerators(document);

            // Composition exposure can make nested error messages visible only after the earlier
            // error-response pass. Re-run it so Kiota never dereferences an array as an error item.
            FixErrorMessageArrayCollision(document);

            // Structural passes above can introduce new discriminators and operations after the earlier cleanup stages.
            // Repair the final document shape before strict validation of the serialized output.
            EnsureDiscriminatorRequiredEverywhere(document);
            EnsureValidResponses(document);

            // Final validation: ensure all schema names are valid
            _namingFixer.ValidateAndFixSchemaNames(document);

            DetachWebhooksFromPaths(document, attachedWebhooks);

            OpenApiSpecVersion outputSpecVersion = options.OutputSpecVersion ?? sourceSpecVersion;
            string json = await document.SerializeAsync(outputSpecVersion, OpenApiConstants.Json,
                cancellationToken: cancellationToken);

            // Fix JSON boolean values (convert Python-style True/False to JSON true/false)
            json = FixJsonBooleanValues(json);

            // The OpenAPI serializer can write decoded control characters back into string values.
            // Canonicalize once more before the JSON-based post-processing passes.
            json = _preprocessingFixer.Fix(json, options);

            // Microsoft.OpenApi 3.10 preserves JSON Schema multi-type arrays. Kiota recursively treats unions with
            // multiple non-null types as polymorphic models, so express the same constraint as an explicit anyOf.
            json = NormalizeKiotaIncompatibleMultiTypes(json);

            // Add enum member names for symbol-only values so Kiota can generate valid identifiers directly from the fixed spec.
            json = InjectKiotaEnumValueNames(json);

            string fullTargetPath = Path.GetFullPath(targetFilePath);
            string temporaryTargetPath = $"{fullTargetPath}.{Guid.NewGuid():N}.tmp";

            try
            {
                await _fileUtil.Write(temporaryTargetPath, json, cancellationToken: cancellationToken);
                await ReadAndValidateOpenApi(temporaryTargetPath, cancellationToken).NoSync();
                await _fileUtil.Move(temporaryTargetPath, fullTargetPath, log: false, cancellationToken).NoSync();
            }
            finally
            {
                await _fileUtil.TryDelete(temporaryTargetPath, log: false, CancellationToken.None).NoSync();
            }

            _logger.LogInformation("Cleaned OpenAPI spec saved to {TargetFilePath}", fullTargetPath);
        }
        catch (OperationCanceledException)
        {
            _logger.LogDebug("OpenAPI fix was canceled.");
            throw;
        }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Error during OpenAPI fix");
            throw;
        }
    }

    private void NormalizeRequiredInfo(OpenApiDocument document)
    {
        document.Info ??= new OpenApiInfo { Title = "OpenAPI", Version = "1.0.0" };

        if (string.IsNullOrWhiteSpace(document.Info.Title))
        {
            document.Info.Title = "OpenAPI";
            _logger.LogDebug("Injected fallback OpenAPI info title");
        }

        if (string.IsNullOrWhiteSpace(document.Info.Version))
        {
            document.Info.Version = "1.0.0";
            _logger.LogDebug("Injected fallback OpenAPI info version");
        }
    }
}