using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi;
using Microsoft.OpenApi.Reader;
using System.Text.Json.Nodes;
using Soenneker.OpenApi.Fixer.Abstract;
using Soenneker.OpenApi.Fixer.Compatibility;

namespace Soenneker.OpenApi.Fixer;

///<inheritdoc cref="IOpenApiFixer"/>
public sealed class OpenApiFixer : IOpenApiFixer
{
    private readonly ILogger<OpenApiFixer> _logger;

    private const bool _logState = false;

    public OpenApiFixer(ILogger<OpenApiFixer> logger)
    {
        _logger = logger;
    }

    public async ValueTask Fix(string sourceFilePath, string targetFilePath, CancellationToken cancellationToken = default)
    {
        try
        {
            // STAGE 0: DOCUMENT LOADING & INITIAL PARSING
            await ReadAndValidateOpenApi(sourceFilePath);
            await using MemoryStream pre = await PreprocessSpecFile(sourceFilePath, cancellationToken);
            var (document, _) = await OpenApiDocument.LoadAsync(pre);

            LogState("After STAGE 0: Initial Load", document);

            // STAGE 1: IDENTIFIERS, NAMING, AND SECURITY
            _logger.LogInformation("Running initial cleanup on identifiers, paths, and security schemes...");
            EnsureSecuritySchemes(document);
            RenameConflictingPaths(document);

            RenameInvalidComponentSchemas(document);

            _logger.LogInformation("Resolving collisions between operation IDs and schema names...");
            ResolveSchemaOperationNameCollisions(document);

            _logger.LogInformation("Ensuring unique operation IDs...");

            EnsureUniqueOperationIds(document);

            // STAGE 2: REFERENCE INTEGRITY & SCRUBBING
            _logger.LogInformation("Scrubbing all component references to fix broken links...");
            ScrubComponentRefs(document, cancellationToken);
            LogState("After STAGE 2: Ref Scrubbing", document);

            // STAGE 3: STRUCTURAL TRANSFORMATIONS
            _logger.LogInformation("Performing major structural transformations (inlining, extraction)...");
            InlinePrimitiveComponents(document);
            DisambiguateMultiContentRequestSchemas(document);

            FixContentTypeWrapperCollisions(document);

            ExtractInlineArrayItemSchemas(document);
            ExtractInlineSchemas(document, cancellationToken);
            LogState("After STAGE 3A: Transformations", document);

            EnsureDiscriminatorForOneOf(document);

            _logger.LogInformation("Removing shadowed untyped properties…");
            RemoveShadowingUntypedProperties(document);
            RemoveRedundantDerivedValue(document);

            _logger.LogInformation("Re-scrubbing references after extraction...");
            ScrubComponentRefs(document, cancellationToken);
            LogState("After STAGE 3B: Re-Scrubbing", document);

            // STAGE 4: DEEP SCHEMA NORMALIZATION & CLEANING
            _logger.LogInformation("Applying deep schema normalizations and cleaning...");

            // MergeAmbiguousOneOfSchemas(document);
            LogState("After STAGE 4A: MergeAmbiguousOneOfSchemas", document);

            ApplySchemaNormalizations(document, cancellationToken);
            LogState("After STAGE 4B: ApplySchemaNormalizations", document);

            //SetExplicitNullabilityOnAllSchemas(document); // This now contains the robust fix
            LogState("After STAGE 4C: SetExplicitNullability", document);

            if (document.Components?.Schemas != null)
            {
                foreach (IOpenApiSchema? schema in document.Components.Schemas.Values)
                {
                    if (schema is OpenApiSchema concreteSchema)
                    {
                        DeepCleanSchema(concreteSchema, new HashSet<OpenApiSchema>());
                    }
                }
            }

            LogState("After STAGE 4D: Deep Cleaning", document);

            StripEmptyEnumBranches(document);
            LogState("After STAGE 4E: StripEmptyEnumBranches", document);

            FixInvalidDefaults(document);
            LogState("After STAGE 4F: FixInvalidDefaults", document);

            FixAllInlineValueEnums(document);
            LogState("After STAGE 4G: FixAllInlineValueEnums", document);

            // STAGE 5: FINAL CLEANUP
            _logger.LogInformation("Performing final cleanup of empty keys and invalid structures...");
            RemoveEmptyInlineSchemas(document);
            RemoveInvalidDefaults(document);

            LogState("After STAGE 5: Final Cleanup", document);

            // STAGE 6: SERIALIZATION
            _logger.LogInformation("Serialization process started...");
            string json = await document.SerializeAsync(OpenApiSpecVersion.OpenApi3_0, OpenApiConstants.Json);
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

        await ReadAndValidateOpenApi(targetFilePath);
    }

    private void FixContentTypeWrapperCollisions(OpenApiDocument doc)
    {
        if (doc.Components?.Schemas == null || doc.Paths == null) return;
        var renameMap = new Dictionary<string, string>();

        foreach (var op in doc.Paths.Values.Where(p => p != null && p.Operations != null).SelectMany(p => p.Operations.Values))
        {
            if (op == null)
                continue;

            if (op.RequestBody?.Content == null || op.OperationId == null) continue;

            foreach (var (media, mt) in op.RequestBody.Content)
            {
                var expectedWrapperName = $"{op.OperationId}{media.Replace('/', '_')}";
                if (doc.Components.Schemas.ContainsKey(expectedWrapperName))
                {
                    var newName = ReserveUniqueSchemaName(doc.Components.Schemas, expectedWrapperName, "Body");
                    _logger.LogWarning("Schema '{Old}' collides with Kiota wrapper in operation '{Op}'. Renaming to '{New}'.", expectedWrapperName,
                        op.OperationId, newName);

                    renameMap[expectedWrapperName] = newName;
                }
            }
        }

        if (renameMap.Count > 0)
            UpdateAllReferences(doc, renameMap); // you already have this helper
    }

    private void EnsureDiscriminatorForOneOf(OpenApiDocument doc)
    {
        if (doc.Components?.Schemas == null) return;

        foreach (var (schemaName, schema) in doc.Components.Schemas)
        {
            if (schema is not OpenApiSchema concreteSchema) continue;
            EnsureDiscriminatorForSchema(concreteSchema, schemaName);
        }
    }

    private void NormalizeSchemaFormats(OpenApiSchema schema)
    {
        // Normalize formats for Kiota compatibility
        if (string.Equals(schema.Format, "datetime", StringComparison.OrdinalIgnoreCase)) schema.Format = "date-time";
        if (string.Equals(schema.Format, "uuid4", StringComparison.OrdinalIgnoreCase)) schema.Format = "uuid";
        if (string.Equals(schema.Format, "uint64", StringComparison.OrdinalIgnoreCase)) schema.Format = "int64";
        if (string.Equals(schema.Format, "uint32", StringComparison.OrdinalIgnoreCase)) schema.Format = "int32";
        if (string.Equals(schema.Format, "uint16", StringComparison.OrdinalIgnoreCase)) schema.Format = "int32";
        if (string.Equals(schema.Format, "uint8", StringComparison.OrdinalIgnoreCase)) schema.Format = "int32";

        // Recursively process nested schemas
        if (schema.Properties != null)
        {
            foreach (var (_, propSchema) in schema.Properties)
            {
                if (propSchema is OpenApiSchema concretePropSchema)
                {
                    NormalizeSchemaFormats(concretePropSchema);
                }
            }
        }

        if (schema.AdditionalProperties is OpenApiSchema concreteAdditionalProps)
        {
            NormalizeSchemaFormats(concreteAdditionalProps);
        }

        if (schema.Items is OpenApiSchema concreteItems)
        {
            NormalizeSchemaFormats(concreteItems);
        }

        if (schema.AllOf != null)
        {
            foreach (var allOfSchema in schema.AllOf)
            {
                if (allOfSchema is OpenApiSchema concreteAllOfSchema)
                {
                    NormalizeSchemaFormats(concreteAllOfSchema);
                }
            }
        }

        if (schema.OneOf != null)
        {
            foreach (var oneOfSchema in schema.OneOf)
            {
                if (oneOfSchema is OpenApiSchema concreteOneOfSchema)
                {
                    NormalizeSchemaFormats(concreteOneOfSchema);
                }
            }
        }

        if (schema.AnyOf != null)
        {
            foreach (var anyOfSchema in schema.AnyOf)
            {
                if (anyOfSchema is OpenApiSchema concreteAnyOfSchema)
                {
                    NormalizeSchemaFormats(concreteAnyOfSchema);
                }
            }
        }
    }

    private void EnsureDiscriminatorForSchema(OpenApiSchema schema, string schemaName)
    {
        // Process the current schema
        IList<IOpenApiSchema>? poly = schema.OneOf ?? schema.AnyOf;
        if (poly is {Count: > 1} && schema.Discriminator == null)
        {
            const string discProp = "type";
            schema.SetDiscriminator(new OpenApiDiscriminator
            {
                PropertyName = discProp,
                Mapping = new Dictionary<string, OpenApiSchemaReference>()
            });

            // In v2, Properties and Required are read-only, so we need to handle this differently
            // We'll use the compatibility extension methods to set these properties
            // Add the discriminator property to the schema
            var properties = new Dictionary<string, IOpenApiSchema>
            {
                [discProp] = new OpenApiSchema {Type = JsonSchemaType.String}
            };
            schema.SetProperties(properties);

            // Add the property to required
            var required = new List<string> {discProp};
            schema.SetRequired(required);

            _logger.LogInformation("Added discriminator property '{Prop}' to schema '{Schema}'.", discProp, schemaName);

            // build mapping
            for (int i = 0; i < poly.Count; i++)
            {
                if (poly[i] is not OpenApiSchema branch) continue;
                string refId;

                if (branch.GetReferenceId() is { } id) // referenced branch
                {
                    refId = id;
                }
                else // inline branch
                {
                    // should already have been promoted by PromoteInlinePolymorphs
                    refId = $"{schemaName}_{i + 1}";
                }

                schema.Discriminator.Mapping.TryAdd(refId, new OpenApiSchemaReference(refId));
            }

            _logger.LogInformation("Added discriminator mapping for polymorphic schema '{Schema}'.", schemaName);
        }

        // Recursively process nested schemas
        if (schema.Properties != null)
        {
            foreach (var (propName, propSchema) in schema.Properties)
            {
                if (propSchema is OpenApiSchema concretePropSchema)
                {
                    EnsureDiscriminatorForSchema(concretePropSchema, $"{schemaName}.{propName}");
                }
            }
        }

        if (schema.AdditionalProperties is OpenApiSchema concreteAdditionalProps)
        {
            EnsureDiscriminatorForSchema(concreteAdditionalProps, $"{schemaName}.additionalProperties");
        }

        if (schema.Items is OpenApiSchema concreteItems)
        {
            EnsureDiscriminatorForSchema(concreteItems, $"{schemaName}.items");
        }

        if (schema.AllOf != null)
        {
            foreach (var allOfSchema in schema.AllOf)
            {
                if (allOfSchema is OpenApiSchema concreteAllOfSchema)
                {
                    EnsureDiscriminatorForSchema(concreteAllOfSchema, $"{schemaName}.allOf");
                }
            }
        }

        if (schema.OneOf != null)
        {
            foreach (var oneOfSchema in schema.OneOf)
            {
                if (oneOfSchema is OpenApiSchema concreteOneOfSchema)
                {
                    EnsureDiscriminatorForSchema(concreteOneOfSchema, $"{schemaName}.oneOf");
                }
            }
        }

        if (schema.AnyOf != null)
        {
            foreach (var anyOfSchema in schema.AnyOf)
            {
                if (anyOfSchema is OpenApiSchema concreteAnyOfSchema)
                {
                    EnsureDiscriminatorForSchema(concreteAnyOfSchema, $"{schemaName}.anyOf");
                }
            }
        }
    }

    private void RemoveRedundantDerivedValue(OpenApiDocument doc)
    {
        if (doc.Components?.Schemas == null) return;
        IDictionary<string, IOpenApiSchema>? pool = doc.Components.Schemas;

        // ------------- local helpers ------------------------------------------
        static IOpenApiSchema Resolve(IOpenApiSchema s, IDictionary<string, IOpenApiSchema> p) =>
            (s.GetReferenceType() == Compatibility.ReferenceType.Schema && p.TryGetValue(s.GetReferenceId()!, out IOpenApiSchema? t)) ? t : s;

        static bool IsWellDefined(IOpenApiSchema s) =>
            s.Type != null || s.IsReference() || (s.Enum?.Count ?? 0) > 0 || (s.Items != null) || (s.AllOf?.Count ?? 0) > 0 || (s.OneOf?.Count ?? 0) > 0 ||
            (s.AnyOf?.Count ?? 0) > 0;

        // ------------- main pass ----------------------------------------------
        foreach (IOpenApiSchema container in pool.Values)
        {
            if (container.AllOf is not {Count: > 1}) continue;

            // find the FIRST fragment (base or earlier override) that has a well-defined `value`
            IOpenApiSchema? firstValueOwner = null;
            foreach (IOpenApiSchema? frag in container.AllOf.Select(f => Resolve(f, pool)))
            {
                if (frag.Properties != null && frag.Properties.TryGetValue("value", out IOpenApiSchema? prop) && IsWellDefined(prop))
                {
                    firstValueOwner = frag;
                    break;
                }
            }

            if (firstValueOwner == null) continue; // nobody defines `value` in a useful way

            // remove *every* later override of `value`
            bool afterFirst = false;
            foreach (IOpenApiSchema? frag in container.AllOf.Select(f => Resolve(f, pool)))
            {
                if (frag == firstValueOwner)
                {
                    afterFirst = true; // start skipping after this fragment
                    continue;
                }

                if (!afterFirst) continue;

                if (frag.Properties != null && frag.Properties.Remove("value"))
                {
                    frag.Required?.Remove("value");
                    _logger.LogInformation("Removed redundant derived 'value' from schema '{Derived}' (base already defines it)",
                        container.Title ?? container.GetReferenceId() ?? "(unnamed)");
                }
            }
        }
    }

    private void RemoveShadowingUntypedProperties(OpenApiDocument doc)
    {
        if (doc.Components?.Schemas == null) return;
        IDictionary<string, IOpenApiSchema>? pool = doc.Components.Schemas;

        static IOpenApiSchema Resolve(IOpenApiSchema s, IDictionary<string, IOpenApiSchema> p) =>
            (s.GetReferenceType() == Compatibility.ReferenceType.Schema && p.TryGetValue(s.GetReferenceId()!, out IOpenApiSchema? t)) ? t : s;

        static bool IsUntyped(IOpenApiSchema s) =>
            s.Type == null && !s.IsReference() && (s.Enum?.Count ?? 0) == 0 && (s.Items == null) && (s.AllOf?.Count ?? 0) == 0 && (s.OneOf?.Count ?? 0) == 0 &&
            (s.AnyOf?.Count ?? 0) == 0;

        foreach (IOpenApiSchema container in pool.Values)
        {
            // Need: at least one $ref fragment  +  one inline fragment with properties
            if (container.AllOf == null) continue;

            IOpenApiSchema? baseFrag = container.AllOf.FirstOrDefault(f => f.GetReferenceType() == Compatibility.ReferenceType.Schema);
            IOpenApiSchema? overrideFrag = container.AllOf.FirstOrDefault(f => f.Properties?.Count > 0);

            if (baseFrag == null || overrideFrag == null) continue;

            IOpenApiSchema baseSchema = Resolve(baseFrag, pool);
            if (baseSchema.Properties == null) continue;

            foreach ((string? propName, IOpenApiSchema? childProp) in overrideFrag.Properties)
            {
                if (childProp == null) continue;

                IOpenApiSchema? baseProp = baseSchema.Properties.TryGetValue(propName, out var prop) ? prop : null;
                if (baseProp == null) continue;

                if (IsUntyped(childProp) && !IsUntyped(baseProp))
                {
                    overrideFrag.Properties.Remove(propName);
                    overrideFrag.Required?.Remove(propName);
                    _logger.LogInformation("Removed shadowing untyped property '{Prop}' from schema '{Schema}' (base defines it as typed)", propName,
                        container.Title ?? container.GetReferenceId() ?? "(unnamed)");
                }
            }
        }
    }

    private void DisambiguateMultiContentRequestSchemas(OpenApiDocument document)
    {
        if (document.Paths == null || document.Components?.Schemas == null) return;

        var schemas = document.Components.Schemas;
        var renameMap = new Dictionary<string, string>();

        foreach (OpenApiOperation operation in document.Paths.Values.Where(p => p != null && p.Operations != null).SelectMany(p => p.Operations.Values))
        {
            if (operation == null)
                continue;

            if (operation.RequestBody?.GetReference() != null || (operation.RequestBody?.Content?.Count ?? 0) <= 1)
            {
                continue;
            }

            _logger.LogInformation("Found multi-content requestBody in operation '{OperationId}'. Checking for schema renaming.", operation.OperationId);

            // We must materialize the list to modify it during iteration
            foreach (var (mediaType, media) in operation.RequestBody.Content.ToList())
            {
                if (media.Schema == null) continue;

                // --- THIS IS THE NEW, CORRECT LOGIC ---
                // If the schema is inline (no reference), we must extract it into a component first.
                if (media.Schema.GetReference() == null && !IsSchemaEmpty(media.Schema))
                {
                    // Create a name for our new component.
                    string newSchemaName = ReserveUniqueSchemaName(schemas, $"{operation.OperationId}{mediaType.Replace("/", "_")}", "RequestBody");

                    _logger.LogInformation("Extracting inline request body schema for '{MediaType}' in operation '{OpId}' to new component '{NewSchemaName}'.",
                        mediaType, operation.OperationId, newSchemaName);

                    // Add the inline schema to the components dictionary.
                    if (media.Schema is OpenApiSchema extractedSchema)
                    {
                        // In v2, Title is read-only, so we need to use the compatibility extension
                        extractedSchema.SetTitle(newSchemaName);
                        schemas.Add(newSchemaName, extractedSchema);
                    }

                    // Replace the inline schema with a reference to our new component.
                    // In v2, Reference is read-only, so we need to create a new schema with the reference
                    media.Schema = new OpenApiSchemaReference(newSchemaName);
                }
                // --- END OF NEW LOGIC ---

                // Now that we can be certain we have a reference, we can check for the name collision.
                if (media.Schema.GetReference() == null) continue;

                string originalSchemaName = media.Schema.GetReferenceId()!;

                if (string.Equals(originalSchemaName, operation.OperationId, StringComparison.OrdinalIgnoreCase))
                {
                    if (renameMap.TryGetValue(originalSchemaName, out var newName))
                    {
                        // In v2, we can't directly set reference ID, need to create new reference
                        media.Schema = new OpenApiSchemaReference(newName);
                        continue;
                    }

                    newName = ReserveUniqueSchemaName(schemas, $"{originalSchemaName}Body", "Dto");

                    _logger.LogWarning("CRITICAL COLLISION: Schema '{Original}' (used in {OpId}) matches OperationId. Renaming to '{New}'.", originalSchemaName,
                        operation.OperationId, newName);

                    if (schemas.TryGetValue(originalSchemaName, out var schemaToRename))
                    {
                        schemas.Remove(originalSchemaName);
                        // In v2, Title is read-only, so we need to use the compatibility extension
                        if (schemaToRename is OpenApiSchema concreteSchema)
                        {
                            concreteSchema.SetTitle(newName);
                        }

                        schemas.Add(newName, schemaToRename);

                        // In v2, we can't directly set reference ID, need to create new reference
                        media.Schema = new OpenApiSchemaReference(newName);
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
        string oldRef = $"#/components/schemas/{oldKey}";
        string newRef = $"#/components/schemas/{newKey}";

        var visited = new HashSet<OpenApiSchema>();

        void Recurse(OpenApiSchema? schema, string? context = null)
        {
            if (schema == null || !visited.Add(schema))
                return;

            if (schema.GetReference()?.ReferenceV3 == oldRef)
            {
                _logger.LogInformation("Would rewrite $ref from '{OldRef}' to '{NewRef}' at {Context} (read-only in v2)", oldRef, newRef,
                    context ?? "unknown location");
            }

            if (schema.Properties != null)
            {
                foreach (var kvp in schema.Properties)
                    if (kvp.Value is OpenApiSchema concreteValue)
                        Recurse(concreteValue, $"{context ?? "schema"}.properties.{kvp.Key}");
            }

            if (schema.Items != null)
                if (schema.Items is OpenApiSchema concreteItems)
                    Recurse(concreteItems, $"{context ?? "schema"}.items");

            foreach (var s in schema.AllOf ?? Enumerable.Empty<IOpenApiSchema>())
                if (s is OpenApiSchema concreteS)
                    Recurse(concreteS, $"{context ?? "schema"}.allOf");
            foreach (var s in schema.AnyOf ?? Enumerable.Empty<IOpenApiSchema>())
                if (s is OpenApiSchema concreteS)
                    Recurse(concreteS, $"{context ?? "schema"}.anyOf");
            foreach (var s in schema.OneOf ?? Enumerable.Empty<IOpenApiSchema>())
                if (s is OpenApiSchema concreteS)
                    Recurse(concreteS, $"{context ?? "schema"}.oneOf");
        }

        foreach (var (key, schema) in document.Components.Schemas)
            if (schema is OpenApiSchema concreteSchema)
                Recurse(concreteSchema, $"components.schemas.{key}");

        foreach (var (pathKey, pathItem) in document.Paths)
        foreach (var (method, operation) in pathItem.Operations)
        {
            var operationContext = $"paths.{pathKey}.{method}";

            if (operation.RequestBody?.Content != null)
            {
                foreach (var (mediaType, media) in operation.RequestBody.Content)
                    if (media.Schema is OpenApiSchema concreteSchema)
                        Recurse(concreteSchema, $"{operationContext}.requestBody.{mediaType}");
            }

            foreach (var (responseCode, response) in operation.Responses)
            {
                foreach (var (mediaType, media) in response.Content)
                    if (media.Schema is OpenApiSchema concreteSchema)
                        Recurse(concreteSchema, $"{operationContext}.responses[{responseCode}].{mediaType}");
            }

            foreach (var param in operation.Parameters)
                if (param.Schema is OpenApiSchema concreteSchema)
                    Recurse(concreteSchema, $"{operationContext}.parameters[{param.Name}]");
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
        if (document.Components?.Schemas == null) return;

        foreach (IOpenApiSchema? schema in document.Components.Schemas.Values)
        {
            if (schema is not OpenApiSchema concreteSchema) continue;
            if (concreteSchema.Type == JsonSchemaType.Object && concreteSchema.Default != null && !(concreteSchema.Default is JsonObject))
            {
                _logger.LogWarning("Removing invalid default ({Default}) from object schema '{Schema}'", concreteSchema.Default,
                    concreteSchema.Title ?? "(no title)");
                concreteSchema.Default = null;
            }
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
            schema.AllOf = schema.AllOf.Where(child => child != null && (child.GetReference() != null || !IsSchemaEmpty(child))).ToList();
        }

        if (schema.OneOf != null)
        {
            schema.OneOf = schema.OneOf.Where(child => child != null && (child.GetReference() != null || !IsSchemaEmpty(child))).ToList();
        }

        if (schema.AnyOf != null)
        {
            schema.AnyOf = schema.AnyOf.Where(child => child != null && (child.GetReference() != null || !IsSchemaEmpty(child))).ToList();
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

        if (schema.Items != null)
        {
            if (schema.Items is OpenApiSchema concreteItems)
                Clean(concreteItems, visited);
        }

        if (schema.AdditionalProperties != null)
        {
            if (schema.AdditionalProperties is OpenApiSchema concreteAdditionalProps)
                Clean(concreteAdditionalProps, visited);
        }
    }

    private bool IsSchemaEmpty(IOpenApiSchema schema)
    {
        if (schema == null) return true;

        bool hasContent = schema.GetReference() != null || schema.Type != null || (schema.Properties?.Any() ?? false) || (schema.AllOf?.Any() ?? false) ||
                          (schema.OneOf?.Any() ?? false) || (schema.AnyOf?.Any() ?? false) || (schema.Enum?.Any() ?? false) || schema.Items != null ||
                          schema.AdditionalProperties != null || schema.AdditionalPropertiesAllowed;

        return !hasContent;
    }

    private void LogState(string stage, OpenApiDocument document)
    {
        if (!_logState) return;

        if (document.Components.Schemas.TryGetValue("CreateDocument", out IOpenApiSchema? schema))
        {
            _logger.LogWarning("DEBUG >>> STAGE: CreateDocument is FOUND");
        }
        else
        {
            _logger.LogWarning("DEBUG >>> STAGE: CreateDocument value not found.", stage);
        }
    }

    private void DeepCleanSchema(OpenApiSchema? schema, HashSet<OpenApiSchema> visited)
    {
        if (schema == null || !visited.Add(schema))
        {
            return;
        }

        SanitizeExample(schema);

        if (schema.Default is JsonValue ds && ds.TryGetValue<string>(out var defaultStr) && string.IsNullOrEmpty(defaultStr))
        {
            schema.Default = null;
        }

        if (schema.Example is JsonValue es && es.TryGetValue<string>(out var exampleStr) && string.IsNullOrEmpty(exampleStr))
        {
            schema.Example = null;
        }

        if (schema.Enum != null && schema.Enum.Any())
        {
            List<JsonNode> cleanedEnum = schema.Enum.OfType<JsonValue>()
                                               .Where(s => s.TryGetValue<string>(out var enumStr) && !string.IsNullOrEmpty(enumStr))
                                               .Select(s => (JsonNode) TrimQuotes(s.GetValue<string>()))
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
            if (schema.AdditionalProperties is OpenApiSchema concreteAdditionalProps)
                DeepCleanSchema(concreteAdditionalProps, visited);
        }
    }

    private static IList<IOpenApiSchema>? RemoveRedundantEmptyEnums(IList<IOpenApiSchema>? list, Func<IOpenApiSchema, bool> isRedundant)
    {
        if (list == null || list.Count == 0)
            return list;

        List<IOpenApiSchema> kept = list.Where(b => !isRedundant(b)).ToList();
        return kept.Count == 0 ? null : kept;
    }

    private void StripEmptyEnumBranches(OpenApiDocument document)
    {
        if (document.Components?.Schemas == null)
            return;

        var visited = new HashSet<OpenApiSchema>();
        var queue = new Queue<OpenApiSchema>(document.Components.Schemas.Values.OfType<OpenApiSchema>());

        static bool IsTrulyRedundantEmptyEnum(IOpenApiSchema s) =>
            s.Enum != null && s.Enum.Count == 0 && s.Type == null && (s.Properties == null || s.Properties.Count == 0) && s.Items == null &&
            s.AdditionalProperties == null && s.OneOf == null && s.AnyOf == null && s.AllOf == null;

        while (queue.Count > 0)
        {
            OpenApiSchema? schema = queue.Dequeue();
            if (schema == null || !visited.Add(schema))
                continue;

            schema.OneOf = RemoveRedundantEmptyEnums(schema.OneOf, IsTrulyRedundantEmptyEnum);
            schema.AnyOf = RemoveRedundantEmptyEnums(schema.AnyOf, IsTrulyRedundantEmptyEnum);
            schema.AllOf = RemoveRedundantEmptyEnums(schema.AllOf, IsTrulyRedundantEmptyEnum);

            if (schema.Properties != null)
                foreach (IOpenApiSchema? p in schema.Properties.Values)
                    if (p is OpenApiSchema concreteP)
                        queue.Enqueue(concreteP);
            if (schema.Items is OpenApiSchema concreteItems) queue.Enqueue(concreteItems);
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
            if (schema.AdditionalProperties is OpenApiSchema concreteAdditional) queue.Enqueue(concreteAdditional);
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
        List<string> primitives = comps.Where(kv =>
                                           kv.Value.Type != null &&
                                           (kv.Value.Type == JsonSchemaType.String || kv.Value.Type == JsonSchemaType.Integer ||
                                            kv.Value.Type == JsonSchemaType.Boolean ||
                                            kv.Value.Type == JsonSchemaType.Number) && (kv.Value.Properties?.Count ?? 0) == 0 &&
                                           (kv.Value.Enum?.Count ?? 0) == 0 &&
                                           (kv.Value.OneOf?.Count ?? 0) == 0 && (kv.Value.AnyOf?.Count ?? 0) == 0 && (kv.Value.AllOf?.Count ?? 0) == 0 &&
                                           kv.Value.Items == null)
                                       .Select(kv => kv.Key)
                                       .ToList();

        if (!primitives.Any())
            return;

        foreach (string primKey in primitives)
        {
            if (comps[primKey] is not OpenApiSchema primitiveSchema) continue;

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

            var visited = new HashSet<OpenApiSchema>();

            // Recursively replace schema.$ref → inlineSchema
            void ReplaceRef(OpenApiSchema? schema)
            {
                if (schema == null || !visited.Add(schema)) return;

                if (schema.GetReference() != null && schema.GetReferenceType() == Compatibility.ReferenceType.Schema && schema.GetReferenceId() == primKey)
                {
                    // remove the ref and copy inline constraints
                    // In v2, we can't directly set reference to null, but we can copy the properties
                    // Create a new schema with the inline constraints and copy properties
                    var newSchema = new OpenApiSchema
                    {
                        Type = inlineSchema.Type,
                        Format = inlineSchema.Format,
                        Description = inlineSchema.Description,
                        MaxLength = inlineSchema.MaxLength,
                        Pattern = inlineSchema.Pattern,
                        Minimum = inlineSchema.Minimum,
                        Maximum = inlineSchema.Maximum,
                        Properties = schema.Properties,
                        Required = schema.Required,
                        Enum = schema.Enum,
                        AllOf = schema.AllOf,
                        OneOf = schema.OneOf,
                        AnyOf = schema.AnyOf,
                        Items = schema.Items,
                        AdditionalProperties = schema.AdditionalProperties,
                        AdditionalPropertiesAllowed = schema.AdditionalPropertiesAllowed,
                        Discriminator = schema.Discriminator
                    };

                    // Copy the new schema's properties to the original
                    schema.Type = newSchema.Type;
                    schema.Format = newSchema.Format;
                    schema.Description = newSchema.Description;
                    schema.MaxLength = newSchema.MaxLength;
                    schema.Pattern = newSchema.Pattern;
                    schema.Minimum = newSchema.Minimum;
                    schema.Maximum = newSchema.Maximum;
                    return;
                }

                // dive into composite schemas
                if (schema.AllOf != null)
                    foreach (IOpenApiSchema? c in schema.AllOf)
                        if (c is OpenApiSchema concreteC)
                            ReplaceRef(concreteC);
                if (schema.OneOf != null)
                    foreach (IOpenApiSchema? c in schema.OneOf)
                        if (c is OpenApiSchema concreteC)
                            ReplaceRef(concreteC);
                if (schema.AnyOf != null)
                    foreach (IOpenApiSchema? c in schema.AnyOf)
                        if (c is OpenApiSchema concreteC)
                            ReplaceRef(concreteC);
                if (schema.Properties != null)
                    foreach (IOpenApiSchema? prop in schema.Properties.Values)
                        if (prop is OpenApiSchema concreteProp)
                            ReplaceRef(concreteProp);
                if (schema.Items is OpenApiSchema concreteItems) ReplaceRef(concreteItems);
                if (schema.AdditionalProperties is OpenApiSchema concreteAdditional) ReplaceRef(concreteAdditional);
            }

            // Handle inlining a parameter $ref → copy its fields, then ReplaceRef(schema)
            void InlineParameter(OpenApiParameter param)
            {
                if (param.GetReference() != null && param.GetReferenceType() == Compatibility.ReferenceType.Parameter &&
                    document.Components.Parameters.TryGetValue(param.GetReferenceId()!, out IOpenApiParameter? compParam))
                {
                    // inline the parameter definition
                    // In v2, we can't directly set reference to null, but we can copy the properties
                    if (compParam is OpenApiParameter concreteCompParam)
                    {
                        param.Name = concreteCompParam.Name;
                        param.In = concreteCompParam.In;
                        param.Required = concreteCompParam.Required;
                        param.Description = param.Description ?? concreteCompParam.Description;
                        param.Schema = concreteCompParam.Schema is not null ? concreteCompParam.Schema : null;
                    }
                }

                if (param.Schema != null)
                    if (param.Schema is OpenApiSchema concreteSchema)
                        ReplaceRef(concreteSchema);
            }

            // 2. Replace refs in component schemas
            foreach (IOpenApiSchema cs in comps.Values.ToList())
                if (cs is OpenApiSchema concreteCs)
                    ReplaceRef(concreteCs);

            // 3. Replace refs in request‐bodies
            if (document.Components.RequestBodies != null)
                foreach (OpenApiRequestBody? rb in document.Components.RequestBodies.Values)
                    if (rb?.Content != null)
                        foreach (OpenApiMediaType? mt in rb.Content.Values)
                            if (mt.Schema is OpenApiSchema concreteSchema)
                                ReplaceRef(concreteSchema);

            // 4. Replace refs in responses
            if (document.Components.Responses != null)
                foreach (IOpenApiResponse? resp in document.Components.Responses.Values)
                    if (resp is OpenApiResponse concreteResp && concreteResp.Content != null)
                        foreach (OpenApiMediaType? mt in concreteResp.Content.Values)
                            if (mt.Schema is OpenApiSchema concreteSchema)
                                ReplaceRef(concreteSchema);

            // 5. Replace refs in headers
            if (document.Components.Headers != null)
                foreach (OpenApiHeader? hdr in document.Components.Headers.Values)
                    if (hdr.Schema is OpenApiSchema concreteSchema)
                        ReplaceRef(concreteSchema);

            // 6. Inline component‐level parameters
            if (document.Components.Parameters != null)
                foreach (IOpenApiParameter? compParam in document.Components.Parameters.Values)
                    if (compParam is OpenApiParameter concreteParam)
                        InlineParameter(concreteParam);

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
                    foreach (OpenApiOperation? op in pathItem.Operations.Values)
                    {
                        if (op == null)
                            continue;

                        if (op.Parameters != null)
                            foreach (IOpenApiParameter? p in op.Parameters)
                                if (p is OpenApiParameter concreteP)
                                    InlineParameter(concreteP);

                        if (op.RequestBody?.Content != null)
                            foreach (OpenApiMediaType? mt in op.RequestBody.Content.Values)
                                if (mt.Schema is OpenApiSchema concreteSchema)
                                    ReplaceRef(concreteSchema);

                        if (op.Responses != null)
                            foreach (IOpenApiResponse? resp in op.Responses.Values)
                                if (resp is OpenApiResponse concreteResp && concreteResp.Content != null)
                                    foreach (OpenApiMediaType? mt in concreteResp.Content.Values)
                                        if (mt.Schema is OpenApiSchema concreteSchema)
                                            ReplaceRef(concreteSchema);
                    }
            }

            // 8. Finally, remove the now‐inlined component
            comps.Remove(primKey);
        }
    }

    private void FixAllInlineValueEnums(OpenApiDocument document)
    {
        IDictionary<string, IOpenApiSchema>? comps = document.Components?.Schemas;
        if (comps == null) return;

        foreach (KeyValuePair<string, IOpenApiSchema> kv in comps.ToList())
        {
            string key = kv.Key;
            if (kv.Value is not OpenApiSchema schema) continue;
            OpenApiSchema? wrapperSegment = null;

            if (schema.Properties?.ContainsKey("value") == true)
                wrapperSegment = schema;
            else if (schema.AllOf?.Count == 2 && schema.AllOf[1] is OpenApiSchema allOfSchema && allOfSchema.Properties?.ContainsKey("value") == true)
                wrapperSegment = allOfSchema;
            else
                continue;
            var valueSchema = wrapperSegment.Properties["value"] as OpenApiSchema;
            if (valueSchema == null) continue;
            var inline = valueSchema;
            if (inline.Enum == null || inline.Enum.Count == 0) continue;

            var enumKey = $"{key}_value";
            if (!comps.ContainsKey(enumKey))
            {
                comps[enumKey] = new OpenApiSchema
                {
                    Type = inline.Type,
                    Title = enumKey,
                    Enum = inline.Enum.ToList()
                } as IOpenApiSchema;
            }

            // In v2, we can't directly set properties, need to handle this differently
            // We'll use the compatibility extension methods to set the reference
            if (wrapperSegment is OpenApiSchema concreteWrapperSegment)
            {
                var properties = new Dictionary<string, IOpenApiSchema>
                {
                    ["value"] = new OpenApiSchemaReference(enumKey)
                };
                concreteWrapperSegment.SetProperties(properties);

                _logger.LogInformation("Set reference to enum schema '{EnumKey}' for property 'value' in schema '{Key}'", enumKey, key);
            }
        }
    }

    private void RenameInvalidComponentSchemas(OpenApiDocument document)
    {
        var schemas = document.Components?.Schemas;
        if (schemas == null)
            return;

        var mapping = new Dictionary<string, string>();
        var existingKeys = new HashSet<string>(schemas.Keys, StringComparer.OrdinalIgnoreCase);

        foreach (var key in schemas.Keys.ToList())
        {
            if (!IsValidIdentifier(key))
            {
                string baseName = SanitizeName(key);

                // Fallback to "Schema" if sanitization fails
                if (string.IsNullOrWhiteSpace(baseName))
                    baseName = "Schema";

                string newKey = baseName;
                int i = 1;

                // Ensure uniqueness deterministically
                while (existingKeys.Contains(newKey))
                {
                    newKey = $"{baseName}_{i++}";
                }

                mapping[key] = newKey;
                existingKeys.Add(newKey);
            }
        }

        foreach ((string oldKey, string newKey) in mapping)
        {
            var schema = schemas[oldKey];
            schemas.Remove(oldKey);

            // In v2, Title is read-only, so we need to use the compatibility extension
            if (schema is OpenApiSchema concreteSchema)
            {
                concreteSchema.SetTitle(newKey);
            }

            schemas[newKey] = schema;
        }

        if (mapping.Count > 0)
            UpdateAllReferences(document, mapping);
    }

    private void ApplySchemaNormalizations(OpenApiDocument document, CancellationToken cancellationToken)
    {
        if (document?.Components?.Schemas == null) return;

        IDictionary<string, IOpenApiSchema>? comps = document.Components.Schemas;

        foreach (KeyValuePair<string, IOpenApiSchema> kv in comps)
        {
            if (kv.Value is OpenApiSchema schema && string.IsNullOrWhiteSpace(schema.Title))
            {
                // In v2, Title is read-only, so we need to use the compatibility extension
                schema.SetTitle(kv.Key);
            }
        }

        var visited = new HashSet<OpenApiSchema>();
        foreach (var schema in comps.Values)
        {
            if (schema is OpenApiSchema concreteSchema) RemoveEmptyCompositionObjects(concreteSchema, visited);
        }

        foreach (KeyValuePair<string, IOpenApiSchema> kv in comps.ToList())
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (kv.Value is not OpenApiSchema schema) continue;

            NormalizeSchemaFormats(schema);

            bool hasComposition = (schema.OneOf?.Any() == true) || (schema.AnyOf?.Any() == true) || (schema.AllOf?.Any() == true);
            if (schema.Type == null && hasComposition)
            {
                schema.Type = JsonSchemaType.Object;
            }

            if ((schema.OneOf?.Any() == true || schema.AnyOf?.Any() == true) && schema.Discriminator == null)
            {
                const string discName = "type";
                schema.SetDiscriminator(new OpenApiDiscriminator {PropertyName = discName});
                // In v2, Properties and Required are read-only, so we need to handle this differently
                // We'll use the compatibility extension methods to set these properties
                var properties = new Dictionary<string, IOpenApiSchema>
                {
                    [discName] = new OpenApiSchema {Type = JsonSchemaType.String}
                };
                schema.SetProperties(properties);

                var required = new List<string> {discName};
                schema.SetRequired(required);

                _logger.LogInformation("Set discriminator property '{Prop}' for schema '{Key}'", discName, kv.Key);
            }

            // ──────────────────────────────────────────────────────────────────
            // ENSURE THE DISCRIMINATOR PROPERTY EXISTS
            // ──────────────────────────────────────────────────────────────────
            if (schema.Discriminator is {PropertyName: { } discProp})
            {
                // In v2, Properties and Required are read-only, so we need to handle this differently
                // We'll use the compatibility extension methods to ensure the property exists
                if (schema.Properties == null || !schema.Properties.ContainsKey(discProp))
                {
                    var properties = new Dictionary<string, IOpenApiSchema>
                    {
                        [discProp] = new OpenApiSchema {Type = JsonSchemaType.String}
                    };
                    schema.SetProperties(properties);
                }

                if (schema.Required == null || !schema.Required.Contains(discProp))
                {
                    var required = new List<string> {discProp};
                    schema.SetRequired(required);
                }

                _logger.LogInformation("Ensured discriminator property '{Prop}' exists for schema '{Key}'", discProp, kv.Key);
            }

            IList<IOpenApiSchema>? compositionList = schema.OneOf ?? schema.AnyOf;
            if (compositionList?.Any() == true && schema.Discriminator != null)
            {
                schema.Discriminator.Mapping ??= new Dictionary<string, OpenApiSchemaReference>();
                foreach (IOpenApiSchema branch in compositionList)
                {
                    if (branch is not OpenApiSchema concreteBranch) continue;
                    if (concreteBranch.GetReferenceId() != null && !schema.Discriminator.Mapping.ContainsKey(concreteBranch.GetReferenceId()!))
                    {
                        string? mappingKey = GetMappingKeyFromRef(concreteBranch.GetReferenceId()!);
                        if (!string.IsNullOrEmpty(mappingKey))
                        {
                            schema.Discriminator.Mapping[mappingKey] = new OpenApiSchemaReference(concreteBranch.GetReferenceId()!);
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
            if (schema is OpenApiSchema concreteSchema)
            {
                bool hasProps = (concreteSchema.Properties?.Any() == true) || concreteSchema.AdditionalProperties != null ||
                                concreteSchema.AdditionalPropertiesAllowed;
                if (hasProps && concreteSchema.Type == null)
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
                if (operation.Value == null) continue;

                var newResps = new OpenApiResponses();
                foreach (KeyValuePair<string, IOpenApiResponse> resp in operation.Value.Responses)
                {
                    if (resp.Value == null) continue;

                    if (resp.Value.GetReference() != null)
                    {
                        newResps[resp.Key] = resp.Value as OpenApiResponse ?? resp.Value;
                        continue;
                    }

                    if (resp.Value.Content != null)
                    {
                        // In v2, Content is read-only, so we can't assign to it directly
                        // We'll handle content normalization by creating a new response with normalized content
                        var normalizedContent = new Dictionary<string, OpenApiMediaType>();
                        foreach (var kvp in resp.Value.Content)
                        {
                            var normalizedKey = NormalizeMediaType(kvp.Key);
                            normalizedContent[normalizedKey] = kvp.Value;
                        }

                        // Create a new response with normalized content
                        var newResponse = new OpenApiResponse
                        {
                            Description = resp.Value.Description,
                            Content = normalizedContent
                        };

                        // Replace the original response
                        operation.Value.Responses[resp.Key] = newResponse;
                    }

                    ScrubBrokenRefs(resp.Value.Content, document);

                    if (resp.Value.Content != null)
                    {
                        Dictionary<string, OpenApiMediaType> valid = resp.Value.Content.Where(p =>
                                                                         {
                                                                             if (p.Value == null) return false;
                                                                             if (p.Value is not OpenApiMediaType mt) return false;
                                                                             if (mt.Schema == null) return false;
                                                                             if (mt.Schema is not OpenApiSchema sch) return false;
                                                                             return sch.GetReference() != null || !IsSchemaEmpty(sch);
                                                                         })
                                                                         .ToDictionary(p => p.Key, p => p.Value as OpenApiMediaType);

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

                if (newResps.Any())
                {
                    EnsureResponseDescriptions(newResps);
                    operation.Value.Responses = newResps;
                }
                else
                {
                    operation.Value.Responses = CreateFallbackResponses(operation.Key);
                }

                if (operation.Value.RequestBody != null && operation.Value.RequestBody.GetReference() == null)
                {
                    IOpenApiRequestBody? rb = operation.Value.RequestBody;
                    if (rb.Content != null)
                    {
                        // In v2, Content is read-only, so we can't assign to it directly
                        // We'll handle content normalization by creating a new request body with normalized content
                        var normalizedContent = new Dictionary<string, OpenApiMediaType>();
                        foreach (var kvp in rb.Content)
                        {
                            var normalizedKey = NormalizeMediaType(kvp.Key);
                            normalizedContent[normalizedKey] = kvp.Value;
                        }

                        // Create a new request body with normalized content
                        var newRequestBody = new OpenApiRequestBody
                        {
                            Description = rb.Description,
                            Content = normalizedContent
                        };

                        // Replace the original request body
                        operation.Value.RequestBody = newRequestBody;
                    }

                    ScrubBrokenRefs(rb.Content, document);
                    Dictionary<string, OpenApiMediaType>? validRb = rb.Content?.Where(p =>
                                                                      {
                                                                          if (p.Value == null) return false;
                                                                          if (p.Value is not OpenApiMediaType mt) return false;
                                                                          return mt.Schema?.GetReference() != null || !IsMediaEmpty(mt);
                                                                      })
                                                                      .ToDictionary(p => p.Key, p => p.Value as OpenApiMediaType);

                    operation.Value.RequestBody = (validRb != null && validRb.Any())
                        ? new OpenApiRequestBody {Description = rb.Description, Content = validRb}
                        : CreateFallbackRequestBody();
                }
            }

            validPaths.Add(path.Key, path.Value as OpenApiPathItem ?? new OpenApiPathItem());
        }

        document.Paths = validPaths;

        foreach (KeyValuePair<string, IOpenApiSchema> kv in comps)
        {
            if (kv.Value is not OpenApiSchema schema) continue;

            bool onlyHasRequired = schema.Type == JsonSchemaType.Object && (schema.Properties == null || !schema.Properties.Any()) && schema.Items == null &&
                                   (schema.AllOf?.Any() != true) && (schema.AnyOf?.Any() != true) && (schema.OneOf?.Any() != true) &&
                                   schema.AdditionalProperties == null && (schema.Required?.Any() == true);

            if (onlyHasRequired)
            {
                List<string> reqs = schema.Required?.Where(r => !string.IsNullOrWhiteSpace(r)).ToList() ?? new List<string>();
                if (reqs.Any())
                {
                    // In v2, Properties is read-only, so we need to use the compatibility extension
                    var properties = new Dictionary<string, IOpenApiSchema>();
                    foreach (string req in reqs)
                    {
                        properties[req] = new OpenApiSchema {Type = JsonSchemaType.Object};
                    }

                    schema.SetProperties(properties);
                }

                schema.AdditionalProperties = new OpenApiSchema {Type = JsonSchemaType.Object};
                schema.AdditionalPropertiesAllowed = true;
                schema.Required = new HashSet<string>();
            }

            bool isTrulyEmpty = schema.Type == JsonSchemaType.Object && (schema.Properties == null || !schema.Properties.Any()) && schema.Items == null &&
                                (schema.AllOf?.Any() != true) && (schema.AnyOf?.Any() != true) && (schema.OneOf?.Any() != true) &&
                                schema.AdditionalProperties == null;

            if (isTrulyEmpty)
            {
                schema.SetProperties(new Dictionary<string, OpenApiSchema>().ToDictionary(kvp => kvp.Key, kvp => kvp.Value as IOpenApiSchema));
                schema.AdditionalProperties = new OpenApiSchema {Type = JsonSchemaType.Object};
                schema.AdditionalPropertiesAllowed = true;
                schema.Required = new HashSet<string>();
            }
        }

        foreach (var schema in comps.Values)
        {
            if (schema is OpenApiSchema concreteSchema && concreteSchema.Enum != null && concreteSchema.Enum.Any())
            {
                if (concreteSchema.Enum.All(x => x is JsonValue))
                {
                    concreteSchema.Type = JsonSchemaType.String;
                }
            }
        }

        var visitedSchemas = new HashSet<OpenApiSchema>();
        foreach (var root in comps.Values)
        {
            if (root is OpenApiSchema concreteRoot) InjectTypeForNullable(concreteRoot, visitedSchemas);
        }
    }

    private string? GetMappingKeyFromRef(string refId)
    {
        if (string.IsNullOrEmpty(refId)) return null;

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
                Content = new Dictionary<string, OpenApiMediaType>
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

    private static OpenApiRequestBody CreateFallbackRequestBody()
    {
        return new OpenApiRequestBody
        {
            Description = "Fallback request body",
            Content = new Dictionary<string, OpenApiMediaType>
            {
                ["application/json"] = new OpenApiMediaType
                {
                    Schema = new OpenApiSchema
                    {
                        Type = JsonSchemaType.Object,
                        Title = "FallbackRequestBody",
                        Description = "Fallback request body schema"
                    }
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
            // In v2, Title is read-only, so we need to use the compatibility extension
            schema.SetTitle(validatedName);
            doc.Components.Schemas[validatedName] = schema;
        }
    }

    private string GenerateSafePart(string? input, string fallback = "unnamed")
    {
        if (string.IsNullOrWhiteSpace(input))
        {
            return fallback;
        }

        string sanitized = SanitizeName(input);
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
            s.Properties?.Count == 1 && s.Properties.TryGetValue("data", out IOpenApiSchema? p) && p?.GetReference() != null &&
            (s.Required == null || s.Required.Count <= 1);

        IDictionary<string, IOpenApiSchema>? comps = document.Components?.Schemas;
        if (comps == null) return;

        foreach (OpenApiPathItem pathItem in document.Paths.Values)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (pathItem?.Operations == null)
                continue;

            foreach ((HttpMethod opType, OpenApiOperation operation) in pathItem.Operations)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (operation == null) continue;

                string safeOpId = ValidateComponentName(GenerateSafePart(operation.OperationId, opType.ToString()));

                if (operation.Parameters != null)
                {
                    foreach (IOpenApiParameter? param in operation.Parameters.ToList())
                    {
                        if (param is OpenApiParameter concreteParam && concreteParam.Content?.Any() == true)
                        {
                            OpenApiMediaType? first = concreteParam.Content.Values.FirstOrDefault();
                            if (first?.Schema != null)
                                concreteParam.Schema = first.Schema;

                            concreteParam.Content = null;
                        }
                    }
                }

                if (operation.RequestBody != null && operation.RequestBody.GetReference() == null && operation.RequestBody.Content != null)
                {
                    foreach ((string mediaType, OpenApiMediaType media) in operation.RequestBody.Content.ToList())
                    {
                        if (media?.Schema is not OpenApiSchema schema) continue;
                        if (schema.GetReference() != null) continue;
                        if (IsSimpleEnvelope(schema)) continue;

                        string safeMedia;
                        string subtype = mediaType.Split(';')[0].Split('/').Last();
                        if (subtype.Equals("json", StringComparison.OrdinalIgnoreCase))
                            safeMedia = "";
                        else
                            safeMedia = ValidateComponentName(GenerateSafePart(subtype, "media"));

                        string baseName = $"{safeOpId}";
                        string compName = ReserveUniqueSchemaName(comps, baseName, $"RequestBody_{safeMedia}");

                        AddComponentSchema(document, compName, schema);
                        media.Schema = new OpenApiSchemaReference(compName);
                    }
                }

                foreach ((string statusCode, IOpenApiResponse response) in operation.Responses)
                {
                    if (response == null || response.GetReference() != null)
                    {
                        continue;
                    }

                    if (response.Content == null) continue;

                    foreach ((string mediaType, OpenApiMediaType media) in response.Content.ToList())
                    {
                        if (media?.Schema is not OpenApiSchema schema) continue;
                        if (schema.GetReference() != null) continue;
                        if (IsSimpleEnvelope(schema)) continue;

                        string safeMedia = ValidateComponentName(GenerateSafePart(mediaType, "media"));
                        string baseName = $"{safeOpId}_{statusCode}";
                        string compName = ReserveUniqueSchemaName(comps, baseName, $"Response_{safeMedia}");

                        AddComponentSchema(document, compName, schema);
                        media.Schema = new OpenApiSchemaReference(compName);
                    }
                }
            }
        }
    }

    private string ReserveUniqueSchemaName(IDictionary<string, IOpenApiSchema> comps, string baseName, string fallbackSuffix)
    {
        if (!comps.ContainsKey(baseName))
            return baseName;

        string withSuffix = $"{baseName}_{fallbackSuffix}";
        if (!comps.ContainsKey(withSuffix))
            return withSuffix;

        int i = 2;
        string numbered;
        do
        {
            numbered = $"{withSuffix}_{i++}";
        }
        while (comps.ContainsKey(numbered));

        return numbered;
    }

    private void EnsureUniqueOperationIds(OpenApiDocument doc)
    {
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var (pathKey, pathItem) in doc.Paths)
        {
            if (pathItem?.Operations == null)
                continue;

            foreach (var (method, operation) in pathItem.Operations)
            {
                if (operation == null)
                    continue;

                if (string.IsNullOrWhiteSpace(operation.OperationId))
                {
                    // Deterministic base name: e.g., "get_/users/{id}"
                    string baseId = $"{method.ToString().ToLowerInvariant()}_{pathKey.Trim('/')}".Replace("/", "_")
                                                                                                 .Replace("{", "")
                                                                                                 .Replace("}", "")
                                                                                                 .Replace("-", "_");

                    string uniqueId = baseId;
                    int i = 1;

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
                    int i = 1;

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

    private static void UpdateAllReferences(OpenApiDocument doc, Dictionary<string, string> mapping)
    {
        void RewriteRef(OpenApiReference? r)
        {
            if (r == null) return;
            if (r.Type != Compatibility.ReferenceType.Schema) return;
            if (string.IsNullOrEmpty(r.Id)) return;

            if (mapping.TryGetValue(r.Id, out string? newId))
                r.Id = newId;
        }

        var visited = new HashSet<OpenApiSchema>();

        void WalkSchema(OpenApiSchema? s)
        {
            if (s == null || !visited.Add(s)) return;

            RewriteRef(s.GetReference());

            if (s.Properties != null)
                foreach (IOpenApiSchema? child in s.Properties.Values)
                    if (child is OpenApiSchema concreteChild)
                        WalkSchema(concreteChild);

            if (s.Items != null)
                if (s.Items is OpenApiSchema concreteItems)
                    WalkSchema(concreteItems);

            foreach (IOpenApiSchema a in s.AllOf ?? Enumerable.Empty<IOpenApiSchema>())
                if (a is OpenApiSchema concreteA)
                    WalkSchema(concreteA);
            foreach (IOpenApiSchema o in s.OneOf ?? Enumerable.Empty<IOpenApiSchema>())
                if (o is OpenApiSchema concreteO)
                    WalkSchema(concreteO);
            foreach (IOpenApiSchema a in s.AnyOf ?? Enumerable.Empty<IOpenApiSchema>())
                if (a is OpenApiSchema concreteA)
                    WalkSchema(concreteA);

            if (s.AdditionalProperties != null)
                if (s.AdditionalProperties is OpenApiSchema concreteAdditional)
                    WalkSchema(concreteAdditional);
        }

        if (doc.Components?.Schemas != null)
            foreach (IOpenApiSchema? schema in doc.Components.Schemas.Values)
                if (schema is OpenApiSchema concreteSchema)
                    WalkSchema(concreteSchema);

        if (doc.Components?.Parameters != null)
            foreach (IOpenApiParameter? p in doc.Components.Parameters.Values)
            {
                if (p is OpenApiParameter concreteP)
                {
                    RewriteRef(concreteP.GetReference());
                    if (concreteP.Schema is OpenApiSchema concreteSchema) WalkSchema(concreteSchema);
                }
            }

        if (doc.Components?.Headers != null)
            foreach (IOpenApiHeader? h in doc.Components.Headers.Values)
            {
                if (h is OpenApiHeader concreteH)
                {
                    RewriteRef(concreteH.GetReference());
                    if (concreteH.Schema is OpenApiSchema concreteSchema) WalkSchema(concreteSchema);
                }
            }

        if (doc.Components?.RequestBodies != null)
            foreach (IOpenApiRequestBody? rb in doc.Components.RequestBodies.Values)
            {
                if (rb is OpenApiRequestBody concreteRb)
                {
                    RewriteRef(concreteRb.GetReference());
                    if (concreteRb.Content != null)
                        foreach (OpenApiMediaType? mt in concreteRb.Content.Values)
                            if (mt.Schema is OpenApiSchema concreteSchema)
                                WalkSchema(concreteSchema);
                }
            }

        if (doc.Components?.Responses != null)
            foreach (IOpenApiResponse? resp in doc.Components.Responses.Values)
            {
                if (resp is OpenApiResponse concreteResp)
                {
                    RewriteRef(concreteResp.GetReference());
                    if (concreteResp.Content != null)
                        foreach (OpenApiMediaType? mt in concreteResp.Content.Values)
                            if (mt.Schema is OpenApiSchema concreteSchema)
                                WalkSchema(concreteSchema);
                }
            }

        foreach (OpenApiPathItem? path in doc.Paths.Values)
        {
            if (path?.Operations == null)
                continue;

            foreach (OpenApiOperation? op in path.Operations.Values)
            {
                if (op == null)
                    continue;

                RewriteRef(op.RequestBody?.GetReference());
                if (op.RequestBody?.Content != null)
                    foreach (OpenApiMediaType? mt in op.RequestBody.Content.Values)
                        if (mt.Schema is OpenApiSchema concreteSchema)
                            WalkSchema(concreteSchema);

                if (op.Parameters != null)
                    foreach (IOpenApiParameter? p in op.Parameters)
                    {
                        if (p is OpenApiParameter concreteP)
                        {
                            RewriteRef(concreteP.GetReference());
                            if (concreteP.Schema is OpenApiSchema concreteSchema) WalkSchema(concreteSchema);
                        }
                    }

                if (op.Responses != null)
                    foreach (IOpenApiResponse? resp in op.Responses.Values)
                    {
                        if (resp is OpenApiResponse concreteResp)
                        {
                            RewriteRef(concreteResp.GetReference());
                            if (concreteResp.Content != null)
                                foreach (OpenApiMediaType? mt in concreteResp.Content.Values)
                                    if (mt.Schema is OpenApiSchema concreteSchema)
                                        WalkSchema(concreteSchema);
                        }
                    }
            }
        }
    }

    private static string SanitizeName(string input)
    {
        if (string.IsNullOrWhiteSpace(input)) return string.Empty;
        var sb = new StringBuilder();
        foreach (char c in input)
        {
            if (char.IsLetterOrDigit(c) || c == '_') sb.Append(c);
            else sb.Append('_');
        }

        return sb.ToString();
    }

    private static bool IsValidIdentifier(string id) => !string.IsNullOrWhiteSpace(id) && id.All(c => char.IsLetterOrDigit(c) || c == '_' || c == '-');

    private bool IsValidSchemaReference(BaseOpenApiReference? reference, OpenApiDocument doc)
    {
        if (reference == null)
        {
            _logger.LogTrace("IsValidSchemaReference check: Reference object is null.");
            return false;
        }

        if (string.IsNullOrWhiteSpace(reference.Id))
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

        bool keyExists;
        switch (reference.Type)
        {
            case Microsoft.OpenApi.ReferenceType.Schema:
                keyExists = comps.Schemas?.ContainsKey(reference.Id) ?? false;
                break;
            case Microsoft.OpenApi.ReferenceType.Response:
                keyExists = comps.Responses?.ContainsKey(reference.Id) ?? false;
                break;
            case Microsoft.OpenApi.ReferenceType.Parameter:
                keyExists = comps.Parameters?.ContainsKey(reference.Id) ?? false;
                break;
            case Microsoft.OpenApi.ReferenceType.RequestBody:
                keyExists = comps.RequestBodies?.ContainsKey(reference.Id) ?? false;
                break;
            case Microsoft.OpenApi.ReferenceType.Header:
                keyExists = comps.Headers?.ContainsKey(reference.Id) ?? false;
                break;
            case Microsoft.OpenApi.ReferenceType.SecurityScheme:
                keyExists = comps.SecuritySchemes?.ContainsKey(reference.Id) ?? false;
                break;
            case Microsoft.OpenApi.ReferenceType.Link:
                keyExists = comps.Links?.ContainsKey(reference.Id) ?? false;
                break;
            case Microsoft.OpenApi.ReferenceType.Callback:
                keyExists = comps.Callbacks?.ContainsKey(reference.Id) ?? false;
                break;
            case Microsoft.OpenApi.ReferenceType.Example:
                keyExists = comps.Examples?.ContainsKey(reference.Id) ?? false;
                break;
            default:
                _logger.LogWarning("IsValidSchemaReference check: Unhandled reference type '{RefType}' for ID '{RefId}'.", reference.Type, reference.Id);
                return false;
        }

        if (!keyExists)
        {
            _logger.LogWarning(
                "IsValidSchemaReference failed for Ref ID '{RefId}' of Type '{RefType}'. Key does not exist in the corresponding component dictionary.",
                reference.Id, reference.Type);
        }

        return keyExists;
    }

    private void ScrubBrokenRefs(IDictionary<string, OpenApiMediaType>? contentDict, OpenApiDocument doc)
    {
        if (contentDict == null) return;

        var visited = new HashSet<OpenApiSchema>();

        foreach (string key in contentDict.Keys.ToList())
        {
            OpenApiMediaType media = contentDict[key];
            if (media.Schema is OpenApiSchema schema)
            {
                if (schema.GetBaseReference() != null && !IsValidSchemaReference(schema.GetBaseReference(), doc))
                {
                    // In v2, Reference is read-only, so we can't set it to null directly
                    // We'll create a new schema without the reference and replace it
                    var newSchema = new OpenApiSchema
                    {
                        Type = schema.Type,
                        Format = schema.Format,
                        Description = schema.Description,
                        Title = schema.Title,
                        Default = schema.Default,
                        Example = schema.Example,
                        Enum = schema.Enum,
                        AllOf = schema.AllOf,
                        OneOf = schema.OneOf,
                        AnyOf = schema.AnyOf,
                        Items = schema.Items,
                        AdditionalProperties = schema.AdditionalProperties,
                        AdditionalPropertiesAllowed = schema.AdditionalPropertiesAllowed,
                        Properties = schema.Properties,
                        Required = schema.Required,
                        Discriminator = schema.Discriminator
                    };

                    // Replace the schema in the media type
                    media.Schema = newSchema;
                    _logger.LogWarning("Removed broken media-type ref @ {Key}", key);
                }

                ScrubAllRefs(schema, doc, visited);
            }
        }
    }

    private void ScrubAllRefs(OpenApiSchema? schema, OpenApiDocument doc, HashSet<OpenApiSchema> visited)
    {
        if (schema == null || !visited.Add(schema)) return;

        if (schema.GetBaseReference() != null && !IsValidSchemaReference(schema.GetBaseReference(), doc))
        {
            // In v2, Reference is read-only, so we can't set it to null directly
            // We'll create a new schema without the reference and copy all properties
            var newSchema = new OpenApiSchema
            {
                Type = schema.Type,
                Format = schema.Format,
                Description = schema.Description,
                Title = schema.Title,
                Default = schema.Default,
                Example = schema.Example,
                Enum = schema.Enum,
                AllOf = schema.AllOf,
                OneOf = schema.OneOf,
                AnyOf = schema.AnyOf,
                Items = schema.Items,
                AdditionalProperties = schema.AdditionalProperties,
                AdditionalPropertiesAllowed = schema.AdditionalPropertiesAllowed,
                Properties = schema.Properties,
                Required = schema.Required,
                Discriminator = schema.Discriminator
            };

            // Copy the new schema's properties to the original
            schema.Type = newSchema.Type;
            schema.Format = newSchema.Format;
            schema.Description = newSchema.Description;
            schema.Title = newSchema.Title;
            schema.Default = newSchema.Default;
            schema.Example = newSchema.Example;
            schema.Enum = newSchema.Enum;
            schema.AllOf = newSchema.AllOf;
            schema.OneOf = newSchema.OneOf;
            schema.AnyOf = newSchema.AnyOf;
            schema.Items = newSchema.Items;
            schema.AdditionalProperties = newSchema.AdditionalProperties;
            schema.AdditionalPropertiesAllowed = newSchema.AdditionalPropertiesAllowed;
            schema.Properties = newSchema.Properties;
            schema.Required = newSchema.Required;
            schema.Discriminator = newSchema.Discriminator;

            _logger.LogWarning("Cleared nested broken ref for schema {Schema}", schema.Title ?? "(no title)");
        }

        if (schema.AllOf != null)
            foreach (IOpenApiSchema? s in schema.AllOf)
                if (s is OpenApiSchema concreteS)
                    ScrubAllRefs(concreteS, doc, visited);

        if (schema.OneOf != null)
            foreach (IOpenApiSchema? s in schema.OneOf)
                if (s is OpenApiSchema concreteS)
                    ScrubAllRefs(concreteS, doc, visited);

        if (schema.AnyOf != null)
            foreach (IOpenApiSchema? s in schema.AnyOf)
                if (s is OpenApiSchema concreteS)
                    ScrubAllRefs(concreteS, doc, visited);

        if (schema.Properties != null)
            foreach (IOpenApiSchema? p in schema.Properties.Values)
                if (p is OpenApiSchema concreteP)
                    ScrubAllRefs(concreteP, doc, visited);

        if (schema.Items != null)
            if (schema.Items is OpenApiSchema concreteItems)
                ScrubAllRefs(concreteItems, doc, visited);

        if (schema.AdditionalProperties != null)
            if (schema.AdditionalProperties is OpenApiSchema concreteAdditional)
                ScrubAllRefs(concreteAdditional, doc, visited);
    }

    private void ScrubComponentRefs(OpenApiDocument doc, CancellationToken cancellationToken)
    {
        var visited = new HashSet<OpenApiSchema>();

        void PatchSchema(OpenApiSchema? sch)
        {
            if (sch != null)
            {
                ScrubAllRefs(sch, doc, visited);
            }
        }

        void PatchContent(IDictionary<string, OpenApiMediaType>? content)
        {
            if (content == null) return;
            foreach (OpenApiMediaType media in content.Values)
            {
                PatchSchema(media.Schema as OpenApiSchema);
            }
        }

        if (doc.Components == null) return;

        if (doc.Components.RequestBodies != null)
        {
            foreach (KeyValuePair<string, IOpenApiRequestBody> kv in doc.Components.RequestBodies)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (kv.Value is OpenApiRequestBody concreteRb)
                    PatchContent(concreteRb.Content);
            }
        }

        if (doc.Components.Responses != null)
        {
            foreach (KeyValuePair<string, IOpenApiResponse> kv in doc.Components.Responses)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (kv.Value is OpenApiResponse concreteResp)
                    PatchContent(concreteResp.Content);
            }
        }

        if (doc.Components.Parameters != null)
        {
            foreach (KeyValuePair<string, IOpenApiParameter> kv in doc.Components.Parameters)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (kv.Value is OpenApiParameter concreteParam)
                    PatchSchema(concreteParam.Schema as OpenApiSchema);
            }
        }

        if (doc.Components.Headers != null)
        {
            foreach (KeyValuePair<string, IOpenApiHeader> kv in doc.Components.Headers)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (kv.Value is OpenApiHeader concreteHeader)
                    PatchSchema(concreteHeader.Schema as OpenApiSchema);
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

                if (kvp.Value?.Operations != null)
                {
                    foreach (OpenApiOperation? operation in kvp.Value.Operations.Values)
                    {
                        if (operation == null)
                            continue;

                        if (operation.Parameters == null)
                        {
                            operation.Parameters = new List<OpenApiParameter>().Cast<IOpenApiParameter>().ToList();
                        }

                        bool hasAccountId = operation.Parameters.Any(p => p.Name == "account_id" && p.In == ParameterLocation.Path);
                        bool hasMemberAccountId = operation.Parameters.Any(p => p.Name == "member_account_id" && p.In == ParameterLocation.Path);

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

                        foreach (OpenApiParameter? param in operation.Parameters)
                        {
                            if (param.Name == "member_account_id" && param.In == ParameterLocation.Path)
                            {
                                param.Schema ??= new OpenApiSchema();
                                param.Schema.Description = "Identifier of the member account to add/remove from the Address Map.";
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

            newPaths.Add(newPath, kvp.Value as OpenApiPathItem ?? new OpenApiPathItem());
        }

        doc.Paths = newPaths;
    }

    private static string NormalizeMediaType(string mediaType)
    {
        if (string.IsNullOrWhiteSpace(mediaType))
            return "application/json";
        string baseType = mediaType.Split(';')[0].Trim();
        if (baseType.Contains('*') || !baseType.Contains('/'))
            return "application/json";
        return baseType;
    }

    private static bool IsMediaEmpty(OpenApiMediaType media)
    {
        OpenApiSchema? s = media.Schema as OpenApiSchema;
        bool schemaEmpty = s == null || (s.Type == null && (s.Properties == null || !s.Properties.Any()) && s.Items == null && !s.AllOf.Any() &&
                                         !s.AnyOf.Any() && !s.OneOf.Any());
        bool hasExample = s?.Example != null || (media.Examples?.Any() == true);
        return schemaEmpty && !hasExample;
    }

    private void EnsureResponseDescriptions(OpenApiResponses responses)
    {
        foreach (KeyValuePair<string, IOpenApiResponse> kv in responses)
        {
            string code = kv.Key;
            OpenApiResponse resp = kv.Value as OpenApiResponse ?? new OpenApiResponse();
            if (string.IsNullOrWhiteSpace(resp.Description))
            {
                resp.Description = code == "default" ? "Default response" : $"{code} response";
            }
        }
    }

    private async ValueTask<OpenApiDiagnostic> ReadAndValidateOpenApi(string filePath)
    {
        await using FileStream stream = File.OpenRead(filePath);

        var (_, diagnostic) = await OpenApiDocument.LoadAsync(stream);

        if (diagnostic.Errors?.Any() == true)
        {
            string msgs = string.Join("; ", diagnostic.Errors.Select(e => e.Message));
            _logger.LogWarning($"OpenAPI parsing errors in {Path.GetFileName(filePath)}: {msgs}");
        }

        return diagnostic;
    }

    private void EnsureSecuritySchemes(OpenApiDocument document)
    {
        document.Components ??= new OpenApiComponents();

        IDictionary<string, IOpenApiSecurityScheme> schemes = document.Components.SecuritySchemes ??=
            new Dictionary<string, OpenApiSecurityScheme>().ToDictionary(kvp => kvp.Key, kvp => kvp.Value as IOpenApiSecurityScheme);

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

        foreach (OpenApiPathItem? path in document.Paths.Values)
        {
            if (path?.Operations == null)
                continue;

            foreach (OpenApiOperation? op in path.Operations.Values)
            {
                if (op.Parameters == null)
                    continue;

                IOpenApiParameter? rogue = op.Parameters.FirstOrDefault(p =>
                    p.In == ParameterLocation.Header && p.Name.StartsWith("authorization", StringComparison.OrdinalIgnoreCase));

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
        if (document.Components?.Schemas == null) return;

        var visited = new HashSet<OpenApiSchema>();
        foreach (IOpenApiSchema? schema in document.Components.Schemas.Values)
        {
            if (schema is OpenApiSchema concreteSchema)
                FixSchemaDefaults(concreteSchema, visited);
        }
    }

    private static string CanonicalSuccess(HttpMethod op) => op.Method switch
    {
        "POST" => "201",
        "DELETE" => "204",
        _ => "200"
    };

    private void FixSchemaDefaults(OpenApiSchema schema, HashSet<OpenApiSchema> visited)
    {
        if (schema == null || !visited.Add(schema)) return;

        if (schema.Enum != null && schema.Enum.Any())
        {
            List<string?> enumValues = schema.Enum.Select(e => e.ToString()).ToList();

            if (schema.Default != null)
            {
                var defaultValue = schema.Default.ToString();
                if (!enumValues.Contains(defaultValue))
                {
                    string? matchingValue = enumValues.FirstOrDefault(v => v.Equals(defaultValue, StringComparison.OrdinalIgnoreCase));

                    if (matchingValue != null)
                    {
                        schema.Default = matchingValue;
                    }
                    else
                    {
                        schema.Default = schema.Enum.First();
                    }

                    _logger.LogWarning("Fixed invalid default value '{OldDefault}' to '{NewDefault}' in schema '{SchemaTitle}'", defaultValue, schema.Default,
                        schema.Title ?? "(no title)");
                }
            }
        }

        if (schema.Default != null)
        {
            switch (schema.Type?.ToString().ToLowerInvariant())
            {
                case "boolean":
                    if (!(schema.Default is JsonValue))
                    {
                        schema.Default = false;
                    }

                    break;

                case "array":
                    if (!(schema.Default is JsonArray))
                    {
                        schema.Default = new JsonArray();
                    }

                    break;

                case "string":
                    if (schema.Format == "date-time" && schema.Default is JsonValue dateStr && dateStr.TryGetValue<string>(out var dateValue))
                    {
                        if (!DateTime.TryParse(dateValue, out _))
                        {
                            schema.Default = null;
                        }
                    }

                    break;
            }
        }

        if (schema.Properties != null)
            foreach (IOpenApiSchema? prop in schema.Properties.Values)
                if (prop is OpenApiSchema concreteProp)
                    FixSchemaDefaults(concreteProp, visited);

        if (schema.Items != null)
            if (schema.Items is OpenApiSchema concreteItems)
                FixSchemaDefaults(concreteItems, visited);

        if (schema.AdditionalProperties != null)
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
        if (schema == null || !visited.Add(schema)) return;

        if (schema.Properties != null)
        {
            schema.Properties = schema.Properties.GroupBy(p => p.Key).ToDictionary(g => g.Key, g => g.First().Value);

            foreach (IOpenApiSchema? prop in schema.Properties.Values)
            {
                if (prop is OpenApiSchema concreteProp)
                    RemoveEmptyCompositionObjects(concreteProp, visited);
            }
        }

        if (schema.Items != null)
        {
            if (schema.Items is OpenApiSchema concreteItems)
                RemoveEmptyCompositionObjects(concreteItems, visited);
        }

        if (schema.AdditionalProperties != null)
        {
            if (schema.AdditionalProperties is OpenApiSchema concreteAdditional)
                RemoveEmptyCompositionObjects(concreteAdditional, visited);
        }

        if (schema.AllOf != null)
        {
            schema.AllOf = schema.AllOf.Where(s => s != null && !IsSchemaEmpty(s)).ToList();
            if (!schema.AllOf.Any())
            {
                schema.AllOf = null;
            }
        }

        if (schema.OneOf != null)
        {
            schema.OneOf = schema.OneOf.Where(s => s != null && !IsSchemaEmpty(s)).ToList();
            if (!schema.OneOf.Any())
            {
                schema.OneOf = null;
            }
        }

        if (schema.AnyOf != null)
        {
            schema.AnyOf = schema.AnyOf.Where(s => s != null && !IsSchemaEmpty(s)).ToList();
            if (!schema.AnyOf.Any())
            {
                schema.AnyOf = null;
            }
        }
    }

    private static void SanitizeExample(OpenApiSchema s)
    {
        if (s?.Example is JsonArray arr && arr.Any())
        {
            if (s.Type == JsonSchemaType.String && arr.First() is JsonValue os && os.TryGetValue<string>(out var strValue))
                s.Example = strValue;
            else
                s.Example = null;
        }

        if (s?.Example is JsonValue str && str.TryGetValue<string>(out var value) && value?.Length > 5_000)
            s.Example = null;
    }

    private void InjectTypeForNullable(OpenApiSchema schema, HashSet<OpenApiSchema> visited)
    {
        if (schema == null || !visited.Add(schema))
            return;

        // Note: Nullable property is not available in v2, so we skip this check
        if (schema.Type == null)
        {
            schema.Type = JsonSchemaType.Object;
            _logger.LogWarning("Injected default type='object' for nullable schema '{SchemaTitle}'", schema.Title ?? "(no title)");
        }

        // Ensure discriminator properties are properly set for this schema
        EnsureDiscriminatorForSchema(schema, schema.Title ?? "(no title)");

        if (schema.Properties != null)
            foreach (IOpenApiSchema? prop in schema.Properties.Values)
                if (prop is OpenApiSchema concreteProp)
                    InjectTypeForNullable(concreteProp, visited);

        if (schema.Items != null && schema.Items is OpenApiSchema itemsSchema)
            InjectTypeForNullable(itemsSchema, visited);

        if (schema.AdditionalProperties != null && schema.AdditionalProperties is OpenApiSchema additionalSchema)
            InjectTypeForNullable(additionalSchema, visited);

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

        var operationIds = new HashSet<string>(
            doc.Paths.Values.Where(p => p != null && p.Operations != null)
               .SelectMany(p => p.Operations.Values)
               .Where(op => op != null && !string.IsNullOrWhiteSpace(op.OperationId))
               .Select(op => op.OperationId), StringComparer.OrdinalIgnoreCase);

        if (!operationIds.Any()) return;

        var mapping = new Dictionary<string, string>();

        // Use ToList() to create a copy of the keys, allowing modification of the collection during iteration.
        foreach (var key in doc.Components.Schemas.Keys.ToList())
        {
            // Check if the schema name (case-insensitive) collides with any operationId
            if (!operationIds.Contains(key)) continue;

            // A collision exists. We must rename the schema.
            string newKey = $"{key}Body"; // A common and effective convention
            int i = 2;
            while (doc.Components.Schemas.ContainsKey(newKey) || mapping.ContainsKey(newKey))
            {
                newKey = $"{key}Body{i++}";
            }

            mapping[key] = newKey;
            _logger.LogWarning("Schema name '{OldKey}' conflicts with an operationId. Renaming schema to '{NewKey}'.", key, newKey);
        }

        if (mapping.Any())
        {
            foreach (var (oldKey, newKey) in mapping)
            {
                if (doc.Components.Schemas.TryGetValue(oldKey, out var schema))
                {
                    doc.Components.Schemas.Remove(oldKey);
                    // In v2, Title is read-only, so we need to use the compatibility extension
                    if (schema is OpenApiSchema concreteSchema)
                    {
                        concreteSchema.SetTitle(newKey);
                    }

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
        int counter = 0;

        foreach ((string? schemaName, IOpenApiSchema? schema) in document.Components.Schemas.ToList())
        {
            if (schema is not OpenApiSchema concreteSchema) continue;
            if (concreteSchema.Type != JsonSchemaType.Array || concreteSchema.Items == null || concreteSchema.Items.GetReference() != null)
                continue;

            OpenApiSchema? itemsSchema = concreteSchema.Items as OpenApiSchema;

            if (itemsSchema.Type != JsonSchemaType.Object || (itemsSchema.Properties == null || !itemsSchema.Properties.Any()))
                continue;

            string itemName = $"{schemaName}_item";
            while (document.Components.Schemas.ContainsKey(itemName) || newSchemas.ContainsKey(itemName))
            {
                itemName = $"{schemaName}_item_{++counter}";
            }

            itemsSchema.Title ??= itemName;
            newSchemas[itemName] = itemsSchema;

            // In v2, we can't directly set Items to a new schema with reference
            // We'll create a new schema with the reference and replace the original
            var newSchema = new OpenApiSchema
            {
                Type = concreteSchema.Type,
                Format = concreteSchema.Format,
                Description = concreteSchema.Description,
                Title = concreteSchema.Title,
                Default = concreteSchema.Default,
                Example = concreteSchema.Example,
                Enum = concreteSchema.Enum,
                AllOf = concreteSchema.AllOf,
                OneOf = concreteSchema.OneOf,
                AnyOf = concreteSchema.AnyOf,
                Items = new OpenApiSchemaReference(itemName),
                AdditionalProperties = concreteSchema.AdditionalProperties,
                AdditionalPropertiesAllowed = concreteSchema.AdditionalPropertiesAllowed,
                Properties = concreteSchema.Properties,
                Required = concreteSchema.Required,
                Discriminator = concreteSchema.Discriminator
            };

            // Replace the original schema in the document
            document.Components.Schemas[schemaName] = newSchema;

            _logger.LogInformation("Set reference to item schema '{ItemName}' for schema '{SchemaName}'", itemName, schemaName);

            _logger.LogInformation("Promoted inline array item schema from '{Parent}' to components schema '{ItemName}'", schemaName, itemName);
        }

        foreach ((string key, OpenApiSchema val) in newSchemas)
        {
            document.Components.Schemas[key] = val;
        }
    }
}

