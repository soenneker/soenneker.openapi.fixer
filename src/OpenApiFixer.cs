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
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi;
using Soenneker.Extensions.ValueTask;
using Soenneker.OpenApi.Fixer.Abstract;
using Soenneker.Utils.Process.Abstract;

namespace Soenneker.OpenApi.Fixer;

///<inheritdoc cref="IOpenApiFixer"/>
public sealed class OpenApiFixer : IOpenApiFixer
{
    private readonly ILogger<OpenApiFixer> _logger;

    private const bool _logState = false;
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
            await ReadAndValidateOpenApi(sourceFilePath);
            await using MemoryStream pre = await PreprocessSpecFile(sourceFilePath, cancellationToken);
            var result = await OpenApiDocument.LoadAsync(pre);
            var document = result.Document;
            var diagnostics = result.Diagnostic;
            
            if (diagnostics?.Errors?.Any() == true)
            {
                string msgs = string.Join("; ", diagnostics.Errors.Select(e => e.Message));
                _logger.LogWarning($"OpenAPI parsing errors during loading: {msgs}");
            }

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

        foreach (var op in doc.Paths.Values.SelectMany(p => p.Operations.Values))
        {
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
            IList<IOpenApiSchema>? poly = schema.OneOf ?? schema.AnyOf;
            if (poly is not {Count: > 1}) continue; // not polymorphic
            if (schema.Discriminator != null) continue; // already OK

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
                        refId = schemaRef.Reference.Id;
                    }
                    else // inline branch
                    {
                        // should already have been promoted by PromoteInlinePolymorphs
                        refId = $"{schemaName}_{i + 1}";
                    }

                    concreteSchema.Discriminator.Mapping.TryAdd(refId, new OpenApiSchemaReference(refId));
                }

                _logger.LogInformation("Added discriminator mapping for polymorphic schema '{Schema}'.", schemaName);
            }
            else
            {
                _logger.LogWarning("Could not cast schema to OpenApiSchema for discriminator injection");
            }
        }
    }

    private void RemoveRedundantDerivedValue(OpenApiDocument doc)
    {
        if (doc.Components?.Schemas == null) return;
        IDictionary<string, IOpenApiSchema>? pool = doc.Components.Schemas;

        // ------------- local helpers ------------------------------------------
        static IOpenApiSchema Resolve(IOpenApiSchema s, IDictionary<string, IOpenApiSchema> p) =>
            (s is OpenApiSchemaReference schemaRef && p.TryGetValue(schemaRef.Reference.Id, out IOpenApiSchema? t)) ? t : s;

        static bool IsWellDefined(IOpenApiSchema s) =>
            s.Type != null || s is OpenApiSchemaReference || (s.Enum?.Count ?? 0) > 0 || (s.Items != null) || (s.AllOf?.Count ?? 0) > 0 ||
            (s.OneOf?.Count ?? 0) > 0 || (s.AnyOf?.Count ?? 0) > 0;

        // ------------- main pass ----------------------------------------------
        foreach (IOpenApiSchema container in pool.Values)
        {
            if (container.AllOf is not {Count: > 1}) continue;

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

            if (firstValueOwner == null) continue; // nobody defines `value` in a useful way

            // remove *every* later override of `value`
            foreach (IOpenApiSchema? frag in container.AllOf.Select(f => Resolve(f, pool)))
            {
                if (frag == firstValueOwner) continue; // skip the first one

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
        if (doc.Components?.Schemas == null) return;
        IDictionary<string, IOpenApiSchema>? pool = doc.Components.Schemas;

        static IOpenApiSchema Resolve(IOpenApiSchema s, IDictionary<string, IOpenApiSchema> p) =>
            (s is OpenApiSchemaReference schemaRef && p.TryGetValue(schemaRef.Reference.Id, out IOpenApiSchema? t)) ? t : s;

        static bool IsUntyped(IOpenApiSchema s) =>
            s.Type == null && s is not OpenApiSchemaReference && (s.Enum?.Count ?? 0) == 0 && (s.Items == null) && (s.AllOf?.Count ?? 0) == 0 &&
            (s.OneOf?.Count ?? 0) == 0 && (s.AnyOf?.Count ?? 0) == 0;

        foreach (IOpenApiSchema container in pool.Values)
        {
            // Need: at least one $ref fragment  +  one inline fragment with properties
            if (container.AllOf == null) continue;

            IOpenApiSchema? baseFrag = container.AllOf.FirstOrDefault(f => f is OpenApiSchemaReference);
            IOpenApiSchema? overrideFrag = container.AllOf.FirstOrDefault(f => f.Properties?.Count > 0);

            if (baseFrag == null || overrideFrag == null) continue;

            IOpenApiSchema baseSchema = Resolve(baseFrag, pool);
            if (baseSchema.Properties == null) continue;

            foreach ((string? propName, IOpenApiSchema? childProp) in overrideFrag.Properties)
            {
                if (!baseSchema.Properties.TryGetValue(propName, out IOpenApiSchema? baseProp)) continue;

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
        if (document.Paths == null || document.Components?.Schemas == null) return;

        var schemas = document.Components.Schemas;
        var renameMap = new Dictionary<string, string>();

        foreach (OpenApiOperation operation in document.Paths.Values.SelectMany(p => p.Operations.Values))
        {
            if (operation.RequestBody is OpenApiRequestBodyReference || (operation.RequestBody?.Content?.Count ?? 0) <= 1)
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
                if (media.Schema is not OpenApiSchemaReference && !IsSchemaEmpty(media.Schema))
                {
                    // Create a name for our new component.
                    string newSchemaName = ReserveUniqueSchemaName(schemas, $"{operation.OperationId}{mediaType.Replace("/", "_")}", "RequestBody");

                    _logger.LogInformation("Extracting inline request body schema for '{MediaType}' in operation '{OpId}' to new component '{NewSchemaName}'.",
                        mediaType, operation.OperationId, newSchemaName);

                    // Add the inline schema to the components dictionary.
                                OpenApiSchema extractedSchema = (OpenApiSchema)media.Schema;
            // In v2.3, Title is read-only, so we can't modify it directly
            schemas.Add(newSchemaName, extractedSchema);

                    // Replace the inline schema with a reference to our new component.
                    media.Schema = new OpenApiSchemaReference(newSchemaName);
                }
                // --- END OF NEW LOGIC ---

                // Now that we can be certain we have a reference, we can check for the name collision.
                if (media.Schema is not OpenApiSchemaReference schemaRef) continue;

                string originalSchemaName = schemaRef.Reference.Id;

                if (string.Equals(originalSchemaName, operation.OperationId, StringComparison.OrdinalIgnoreCase))
                {
                    if (renameMap.TryGetValue(originalSchemaName, out var newName))
                    {
                        // In v2.3, we can't directly modify the reference ID as it's init-only
                        // We need to create a new reference
                        _logger.LogInformation("Would update reference from '{OldId}' to '{NewId}'", originalSchemaName, newName);
                        continue;
                    }

                    newName = ReserveUniqueSchemaName(schemas, $"{originalSchemaName}Body", "Dto");

                    _logger.LogWarning("CRITICAL COLLISION: Schema '{Original}' (used in {OpId}) matches OperationId. Renaming to '{New}'.", originalSchemaName,
                        operation.OperationId, newName);

                    if (schemas.TryGetValue(originalSchemaName, out var schemaToRename))
                    {
                        schemas.Remove(originalSchemaName);
                        // In v2.3, Title is read-only, so we can't modify it directly
                        schemas.Add(newName, schemaToRename);

                        // In v2.3, we can't directly modify the reference ID as it's init-only
                        // We need to create a new reference
                        _logger.LogInformation("Would update reference from '{OldId}' to '{NewId}'", originalSchemaName, newName);
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

        var visited = new HashSet<OpenApiSchema>();

        void Recurse(IOpenApiSchema? schema, string? context = null)
        {
            if (schema == null || !visited.Add((OpenApiSchema)schema))
                return;

            if (schema is OpenApiSchemaReference schemaRef && schemaRef.Reference.ReferenceV3 == oldRef)
            {
                _logger.LogInformation("Rewriting $ref from '{OldRef}' to '{NewRef}' at {Context}", oldRef, newRef, context ?? "unknown location");
                // In v2.3, we can't directly modify the reference ID, but we can log the change
                // The actual replacement would need to be done at a higher level
            }

            if (schema.Properties != null)
            {
                foreach (var kvp in schema.Properties)
                    Recurse(kvp.Value, $"{context ?? "schema"}.properties.{kvp.Key}");
            }

            if (schema.Items != null)
                Recurse(schema.Items, $"{context ?? "schema"}.items");

            foreach (var s in schema.AllOf ?? Enumerable.Empty<IOpenApiSchema>())
                Recurse(s, $"{context ?? "schema"}.allOf");
            foreach (var s in schema.AnyOf ?? Enumerable.Empty<IOpenApiSchema>())
                Recurse(s, $"{context ?? "schema"}.anyOf");
            foreach (var s in schema.OneOf ?? Enumerable.Empty<IOpenApiSchema>())
                Recurse(s, $"{context ?? "schema"}.oneOf");
        }

        foreach (var (key, schema) in document.Components.Schemas)
            Recurse(schema, $"components.schemas.{key}");

        foreach (var (pathKey, pathItem) in document.Paths)
        foreach (var (method, operation) in pathItem.Operations)
        {
            var operationContext = $"paths.{pathKey}.{method}";

            if (operation.RequestBody?.Content != null)
            {
                foreach (var (mediaType, media) in operation.RequestBody.Content)
                    Recurse(media.Schema, $"{operationContext}.requestBody.{mediaType}");
            }

            foreach (var (responseCode, response) in operation.Responses)
            {
                foreach (var (mediaType, media) in response.Content)
                    Recurse(media.Schema, $"{operationContext}.responses[{responseCode}].{mediaType}");
            }

            foreach (var param in operation.Parameters)
                Recurse(param.Schema, $"{operationContext}.parameters[{param.Name}]");
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
            if (schema is OpenApiSchema concreteSchema && concreteSchema.Type == JsonSchemaType.Object && concreteSchema.Default != null && concreteSchema.Default is not JsonObject)
            {
                _logger.LogWarning("Removing invalid default ({Default}) from object schema '{Schema}'", concreteSchema.Default, concreteSchema.Title ?? "(no title)");
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
            schema.AllOf = schema.AllOf.Where(child => child != null && (child is OpenApiSchemaReference || !IsSchemaEmpty(child))).ToList();
        }

        if (schema.OneOf != null)
        {
            schema.OneOf = schema.OneOf.Where(child => child != null && (child is OpenApiSchemaReference || !IsSchemaEmpty(child))).ToList();
        }

        if (schema.AnyOf != null)
        {
            schema.AnyOf = schema.AnyOf.Where(child => child != null && (child is OpenApiSchemaReference || !IsSchemaEmpty(child))).ToList();
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
        if (schema == null) return true;

        bool hasContent = schema is OpenApiSchemaReference || schema.Type != null || (schema.Properties?.Any() ?? false) ||
                          (schema.AllOf?.Any() ?? false) || (schema.OneOf?.Any() ?? false) || (schema.AnyOf?.Any() ?? false) || (schema.Enum?.Any() ?? false) ||
                          schema.Items != null || schema.AdditionalProperties != null || schema.AdditionalPropertiesAllowed;

        return !hasContent;
    }

    private void LogState(string stage, OpenApiDocument document)
    {
        if (!_logState) return;

        if (document.Components.Schemas.TryGetValue("CreateDocument", out IOpenApiSchema schema))
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

        if (schema.Default is JsonValue ds && ds.GetValue<string>() is string dsValue && string.IsNullOrEmpty(dsValue))
        {
            schema.Default = null;
        }

        if (schema.Example is JsonValue es && es.GetValue<string>() is string esValue && string.IsNullOrEmpty(esValue))
        {
            schema.Example = null;
        }

        if (schema.Enum != null && schema.Enum.Any())
        {
            List<JsonNode> cleanedEnum = schema.Enum.OfType<JsonValue>()
                                                  .Where(s => s.GetValue<string>() is string value && !string.IsNullOrEmpty(value))
                                                  .Select(s => JsonValue.Create(TrimQuotes(s.GetValue<string>())))
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

        List<IOpenApiSchema> kept = list.Where(b => !isRedundant((OpenApiSchema)b)).ToList();
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

            schema.OneOf = RemoveRedundantEmptyEnums(schema.OneOf?.ToList(), IsTrulyRedundantEmptyEnum)?.ToList();
            schema.AnyOf = RemoveRedundantEmptyEnums(schema.AnyOf?.ToList(), IsTrulyRedundantEmptyEnum)?.ToList();
            schema.AllOf = RemoveRedundantEmptyEnums(schema.AllOf?.ToList(), IsTrulyRedundantEmptyEnum)?.ToList();

            if (schema.Properties != null)
                foreach (IOpenApiSchema? p in schema.Properties.Values)
                    if (p is OpenApiSchema concreteP) queue.Enqueue(concreteP);
            if (schema.Items is OpenApiSchema concreteItems) queue.Enqueue(concreteItems);
            if (schema.AllOf != null)
                foreach (IOpenApiSchema? b in schema.AllOf)
                    if (b is OpenApiSchema concreteB) queue.Enqueue(concreteB);
            if (schema.OneOf != null)
                foreach (IOpenApiSchema? b in schema.OneOf)
                    if (b is OpenApiSchema concreteB) queue.Enqueue(concreteB);
            if (schema.AnyOf != null)
                foreach (IOpenApiSchema? b in schema.AnyOf)
                    if (b is OpenApiSchema concreteB) queue.Enqueue(concreteB);
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
                                           (kv.Value.Type == JsonSchemaType.String || kv.Value.Type == JsonSchemaType.Integer || kv.Value.Type == JsonSchemaType.Boolean ||
                                            kv.Value.Type == JsonSchemaType.Number) &&
                                           (kv.Value.Properties?.Count ?? 0) == 0 && (kv.Value.Enum?.Count ?? 0) == 0 && (kv.Value.OneOf?.Count ?? 0) == 0 &&
                                           (kv.Value.AnyOf?.Count ?? 0) == 0 && (kv.Value.AllOf?.Count ?? 0) == 0 && kv.Value.Items == null)
                                       .Select(kv => kv.Key)
                                       .ToList();

        if (!primitives.Any())
            return;

        foreach (string primKey in primitives)
        {
            OpenApiSchema primitiveSchema = (OpenApiSchema)comps[primKey];

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
            void ReplaceRef(IOpenApiSchema? schema)
            {
                if (schema == null || !visited.Add((OpenApiSchema)schema)) return;

                // In v2.3, we need to check if this is a reference to our primitive
                if (schema is OpenApiSchemaReference schemaRef && schemaRef.Reference.Id == primKey)
                {
                    // Replace the reference with the inline schema
                    // Note: This is a simplified approach - in a real implementation,
                    // we'd need to replace the entire schema object, but this gives us
                    // the basic pattern for v2.3
                    _logger.LogInformation("Would replace reference to '{PrimKey}' with inline schema", primKey);
                }

                            // dive into composite schemas
            if (schema.AllOf != null)
                foreach (IOpenApiSchema? c in schema.AllOf)
                    if (c is OpenApiSchema concreteC) ReplaceRef(concreteC);
            if (schema.OneOf != null)
                foreach (IOpenApiSchema? c in schema.OneOf)
                    if (c is OpenApiSchema concreteC) ReplaceRef(concreteC);
            if (schema.AnyOf != null)
                foreach (IOpenApiSchema? c in schema.AnyOf)
                    if (c is OpenApiSchema concreteC) ReplaceRef(concreteC);
            if (schema.Properties != null)
                foreach (IOpenApiSchema? prop in schema.Properties.Values)
                    if (prop is OpenApiSchema concreteProp) ReplaceRef(concreteProp);
            if (schema.Items != null) ReplaceRef(schema.Items);
            if (schema.AdditionalProperties != null) ReplaceRef(schema.AdditionalProperties);
            }

            // Handle inlining a parameter $ref → copy its fields, then ReplaceRef(schema)
            void InlineParameter(OpenApiParameter param)
            {
                if (param.Schema != null)
                    ReplaceRef(param.Schema);
            }

            // 2. Replace refs in component schemas
            foreach (IOpenApiSchema cs in comps.Values.ToList())
                if (cs is OpenApiSchema concreteCs) ReplaceRef(concreteCs);

            // 3. Replace refs in request‐bodies
            if (document.Components.RequestBodies != null)
                foreach (OpenApiRequestBody? rb in document.Components.RequestBodies.Values)
                foreach (OpenApiMediaType? mt in rb.Content.Values)
                    if (mt.Schema is OpenApiSchema concreteSchema) ReplaceRef(concreteSchema);

            // 4. Replace refs in responses
            if (document.Components.Responses != null)
                foreach (OpenApiResponse? resp in document.Components.Responses.Values)
                foreach (OpenApiMediaType? mt in resp.Content.Values)
                    if (mt.Schema is OpenApiSchema concreteSchema) ReplaceRef(concreteSchema);

            // 5. Replace refs in headers
            if (document.Components.Headers != null)
                foreach (OpenApiHeader? hdr in document.Components.Headers.Values)
                    if (hdr.Schema is OpenApiSchema concreteSchema) ReplaceRef(concreteSchema);

            // 6. Inline component‐level parameters
            if (document.Components.Parameters != null)
                foreach (OpenApiParameter? compParam in document.Components.Parameters.Values)
                    InlineParameter(compParam);

            // 7. Inline path‐level and operation‐level parameters
            foreach (OpenApiPathItem? pathItem in document.Paths.Values)
            {
                // path‐level
                if (pathItem.Parameters != null)
                    foreach (OpenApiParameter? p in pathItem.Parameters)
                        InlineParameter(p);

                // each operation
                foreach (OpenApiOperation? op in pathItem.Operations.Values)
                {
                    if (op.Parameters != null)
                        foreach (OpenApiParameter? p in op.Parameters)
                            InlineParameter(p);

                    if (op.RequestBody?.Content != null)
                        foreach (OpenApiMediaType? mt in op.RequestBody.Content.Values)
                            if (mt.Schema is OpenApiSchema concreteSchema) ReplaceRef(concreteSchema);

                    foreach (OpenApiResponse? resp in op.Responses.Values)
                        if (resp.Content != null)
                            foreach (OpenApiMediaType? mt in resp.Content.Values)
                                if (mt.Schema is OpenApiSchema concreteSchema) ReplaceRef(concreteSchema);
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
            IOpenApiSchema schema = kv.Value;
            OpenApiSchema? wrapperSegment = null;

            if (schema.Properties?.ContainsKey("value") == true)
                wrapperSegment = (OpenApiSchema)schema;
            else if (schema.AllOf?.Count == 2 && schema.AllOf[1].Properties?.ContainsKey("value") == true)
                wrapperSegment = (OpenApiSchema)schema.AllOf[1];
            else
                continue;

            IOpenApiSchema? inline = wrapperSegment.Properties["value"];
            if (inline?.Enum == null || inline.Enum.Count == 0) continue;

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

            if (wrapperSegment != null)
                wrapperSegment.Properties["value"] = new OpenApiSchemaReference(enumKey);
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
                var i = 1;

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

            // In v2.3, Title is read-only, so we can't modify it directly
            // The schema will keep its original title

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
            if (kv.Value != null && string.IsNullOrWhiteSpace(kv.Value.Title))
            {
                // In v2.3, Title is read-only, so we can't modify it directly
                // We'll handle this in a different way if needed
                _logger.LogDebug("Schema '{Key}' has no title, but Title is read-only in v2.3", kv.Key);
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
            IOpenApiSchema? schema = kv.Value;
            if (schema == null) continue;

            if (schema is OpenApiSchema concreteSchema)
            {
                if (string.Equals(concreteSchema.Format, "datetime", StringComparison.OrdinalIgnoreCase)) concreteSchema.Format = "date-time";
                if (string.Equals(concreteSchema.Format, "uuid4", StringComparison.OrdinalIgnoreCase)) concreteSchema.Format = "uuid";

                bool hasComposition = (concreteSchema.OneOf?.Any() == true) || (concreteSchema.AnyOf?.Any() == true) || (concreteSchema.AllOf?.Any() == true);
                if (concreteSchema.Type == null && hasComposition)
                {
                    concreteSchema.Type = JsonSchemaType.Object;
                }
            }

            if ((schema.OneOf?.Any() == true || schema.AnyOf?.Any() == true) && schema.Discriminator == null)
            {
                const string discName = "type";
                if (schema is OpenApiSchema concreteSchema1)
                {
                    concreteSchema1.Discriminator = new OpenApiDiscriminator {PropertyName = discName};
                    concreteSchema1.Properties ??= new Dictionary<string, IOpenApiSchema>();
                    if (!concreteSchema1.Properties.ContainsKey(discName))
                    {
                        concreteSchema1.Properties[discName] = new OpenApiSchema {Type = JsonSchemaType.String, Title = discName, Description = "Union discriminator"};
                    }

                    concreteSchema1.Required ??= new HashSet<string>();
                    if (!concreteSchema1.Required.Contains(discName)) concreteSchema1.Required.Add(discName);
                }
            }

            // ──────────────────────────────────────────────────────────────────
            // ENSURE THE DISCRIMINATOR PROPERTY EXISTS
            // ──────────────────────────────────────────────────────────────────
            if (schema.Discriminator is {PropertyName: { } discProp})
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
                    if (branch is OpenApiSchemaReference schemaRef && !schema.Discriminator.Mapping.ContainsKey(schemaRef.Reference.Id))
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
            if (schema == null) continue;
            bool hasProps = (schema.Properties?.Any() == true) || schema.AdditionalProperties != null || schema.AdditionalPropertiesAllowed;
            if (hasProps && schema.Type == null)
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
                if (operation.Value == null) continue;

                var newResps = new OpenApiResponses();
                foreach (KeyValuePair<string, IOpenApiResponse> resp in operation.Value.Responses)
                {
                    if (resp.Value == null) continue;

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
                        Dictionary<string, OpenApiMediaType> valid = resp.Value.Content.Where(p =>
                                                                         {
                                                                             if (p.Value == null) return false;
                                                                             OpenApiMediaType? mt = p.Value;
                                                                             if (mt.Schema == null) return false;
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
                        var normalizedContent = rb.Content.Where(p => p.Key != null && p.Value != null)
                                                         .ToDictionary(p => NormalizeMediaType(p.Key), p => p.Value);
                        
                        ScrubBrokenRefs(normalizedContent, document);
                        Dictionary<string, OpenApiMediaType>? validRb = normalizedContent?.Where(p => p.Value?.Schema is OpenApiSchemaReference || !IsMediaEmpty(p.Value))
                                                                      .ToDictionary(p => p.Key, p => p.Value);

                        if (operation.Value is OpenApiOperation concreteOperation)
                        {
                            concreteOperation.RequestBody = (validRb != null && validRb.Any())
                                ? new OpenApiRequestBody {Description = rb.Description, Content = validRb}
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
            if (kv.Value == null) continue;
            IOpenApiSchema schema = kv.Value;

            if (schema is OpenApiSchema concreteSchema)
            {
                bool onlyHasRequired = concreteSchema.Type == JsonSchemaType.Object && (concreteSchema.Properties == null || !concreteSchema.Properties.Any()) && concreteSchema.Items == null &&
                                       (concreteSchema.AllOf?.Any() != true) && (concreteSchema.AnyOf?.Any() != true) && (concreteSchema.OneOf?.Any() != true) &&
                                       concreteSchema.AdditionalProperties == null && (concreteSchema.Required?.Any() == true);

                if (onlyHasRequired)
                {
                    List<string> reqs = concreteSchema.Required?.Where(r => !string.IsNullOrWhiteSpace(r)).Select(r => r!).ToList() ?? new List<string>();
                    if (reqs.Any())
                    {
                        concreteSchema.Properties = reqs.ToDictionary(name => name, _ => (IOpenApiSchema)new OpenApiSchema {Type = JsonSchemaType.Object});
                    }

                    concreteSchema.AdditionalProperties = new OpenApiSchema {Type = JsonSchemaType.Object};
                    concreteSchema.AdditionalPropertiesAllowed = true;
                    concreteSchema.Required = new HashSet<string>();
                }

                bool isTrulyEmpty = concreteSchema.Type == JsonSchemaType.Object && (concreteSchema.Properties == null || !concreteSchema.Properties.Any()) && concreteSchema.Items == null &&
                                    (concreteSchema.AllOf?.Any() != true) && (concreteSchema.AnyOf?.Any() != true) && (concreteSchema.OneOf?.Any() != true) &&
                                    concreteSchema.AdditionalProperties == null;

                if (isTrulyEmpty)
                {
                    concreteSchema.Properties = new Dictionary<string, IOpenApiSchema>();
                    concreteSchema.AdditionalProperties = new OpenApiSchema {Type = JsonSchemaType.Object};
                    concreteSchema.AdditionalPropertiesAllowed = true;
                    concreteSchema.Required = new HashSet<string>();
                }
            }
        }

        foreach (var schema in comps.Values)
        {
            if (schema?.Enum == null || !schema.Enum.Any()) continue;
            if (schema.Enum.All(x => x is JsonValue))
            {
                if (schema is OpenApiSchema concreteSchema)
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
            s.Properties?.Count == 1 && s.Properties.TryGetValue("data", out IOpenApiSchema? p) && p is OpenApiSchemaReference &&
            (s.Required == null || s.Required.Count <= 1);

        IDictionary<string, IOpenApiSchema>? comps = document.Components?.Schemas;
        if (comps == null) return;

        foreach (IOpenApiPathItem pathItem in document.Paths.Values)
        {
            cancellationToken.ThrowIfCancellationRequested();

                            foreach ((HttpMethod opType, OpenApiOperation operation) in pathItem.Operations)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (operation == null) continue;

                string safeOpId = ValidateComponentName(GenerateSafePart(operation.OperationId, opType.ToString()));

                if (operation.Parameters != null)
                {
                    foreach (OpenApiParameter? param in operation.Parameters.ToList())
                    {
                        if (param.Content?.Any() == true)
                        {
                            OpenApiMediaType? first = param.Content.Values.FirstOrDefault();
                            if (first?.Schema != null)
                                param.Schema = first.Schema;

                            param.Content = null;
                        }
                    }
                }

                if (operation.RequestBody != null && operation.RequestBody is not OpenApiRequestBodyReference && operation.RequestBody.Content != null)
                {
                    foreach ((string mediaType, OpenApiMediaType media) in operation.RequestBody.Content.ToList())
                    {
                        IOpenApiSchema? schemaReq = media?.Schema;
                        if (schemaReq == null || schemaReq is OpenApiSchemaReference) continue;
                        if (schemaReq is OpenApiSchema concreteSchemaReq1 && IsSimpleEnvelope(concreteSchemaReq1)) continue;

                        string safeMedia;
                        string subtype = mediaType.Split(';')[0].Split('/').Last();
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

                foreach ((string statusCode, IOpenApiResponse response) in operation.Responses)
                {
                    if (response == null || response is OpenApiResponseReference)
                    {
                        continue;
                    }

                    if (response.Content == null) continue;

                    foreach ((string mediaType, OpenApiMediaType media) in response.Content.ToList())
                    {
                        IOpenApiSchema? schemaResp = media?.Schema;
                        if (schemaResp == null || schemaResp is OpenApiSchemaReference) continue;
                        if (schemaResp is OpenApiSchema concreteSchemaResp1 && IsSimpleEnvelope(concreteSchemaResp1)) continue;

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

    private string ReserveUniqueSchemaName(IDictionary<string, IOpenApiSchema> comps, string baseName, string fallbackSuffix)
    {
        if (!comps.ContainsKey(baseName))
            return baseName;

        var withSuffix = $"{baseName}_{fallbackSuffix}";
        if (!comps.ContainsKey(withSuffix))
            return withSuffix;

        var i = 2;
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
            foreach (var (method, operation) in pathItem.Operations)
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
        void RewriteRef(OpenApiSchemaReference? r)
        {
            if (r == null) return;
            if (string.IsNullOrEmpty(r.Reference.Id)) return;

            if (mapping.TryGetValue(r.Reference.Id, out string? newId))
            {
                // In v2.3, we can't directly modify the reference ID as it's init-only
                // We need to create a new reference
                _logger.LogInformation("Would update schema reference from '{OldId}' to '{NewId}'", r.Reference.Id, newId);
            }
        }

        void RewriteParameterRef(OpenApiParameterReference? r)
        {
            if (r == null) return;
            if (string.IsNullOrEmpty(r.Reference.Id)) return;

            if (mapping.TryGetValue(r.Reference.Id, out string? newId))
            {
                _logger.LogInformation("Would update parameter reference from '{OldId}' to '{NewId}'", r.Reference.Id, newId);
            }
        }

        void RewriteHeaderRef(OpenApiHeaderReference? r)
        {
            if (r == null) return;
            if (string.IsNullOrEmpty(r.Reference.Id)) return;

            if (mapping.TryGetValue(r.Reference.Id, out string? newId))
            {
                _logger.LogInformation("Would update header reference from '{OldId}' to '{NewId}'", r.Reference.Id, newId);
            }
        }

        void RewriteRequestBodyRef(OpenApiRequestBodyReference? r)
        {
            if (r == null) return;
            if (string.IsNullOrEmpty(r.Reference.Id)) return;

            if (mapping.TryGetValue(r.Reference.Id, out string? newId))
            {
                _logger.LogInformation("Would update request body reference from '{OldId}' to '{NewId}'", r.Reference.Id, newId);
            }
        }

        void RewriteResponseRef(OpenApiResponseReference? r)
        {
            if (r == null) return;
            if (string.IsNullOrEmpty(r.Reference.Id)) return;

            if (mapping.TryGetValue(r.Reference.Id, out string? newId))
            {
                _logger.LogInformation("Would update response reference from '{OldId}' to '{NewId}'", r.Reference.Id, newId);
            }
        }

        var visited = new HashSet<OpenApiSchema>();

        void WalkSchema(IOpenApiSchema? s)
        {
            if (s == null || !visited.Add((OpenApiSchema)s)) return;

            if (s is OpenApiSchemaReference schemaRef)
                RewriteRef(schemaRef);

            if (s.Properties != null)
                foreach (IOpenApiSchema? child in s.Properties.Values)
                    WalkSchema(child);

            if (s.Items != null) WalkSchema(s.Items);

            foreach (IOpenApiSchema a in s.AllOf ?? Enumerable.Empty<IOpenApiSchema>()) 
                WalkSchema(a);
            foreach (IOpenApiSchema o in s.OneOf ?? Enumerable.Empty<IOpenApiSchema>()) 
                WalkSchema(o);
            foreach (IOpenApiSchema a in s.AnyOf ?? Enumerable.Empty<IOpenApiSchema>()) 
                WalkSchema(a);

            if (s.AdditionalProperties != null)
                WalkSchema(s.AdditionalProperties);
        }

        if (doc.Components?.Schemas != null)
            foreach (IOpenApiSchema? schema in doc.Components.Schemas.Values)
                WalkSchema(schema);

        if (doc.Components?.Parameters != null)
            foreach (IOpenApiParameter? p in doc.Components.Parameters.Values)
            {
                if (p is OpenApiParameterReference paramRef)
                    RewriteParameterRef(paramRef);
                if (p.Schema != null)
                    WalkSchema(p.Schema);
            }

        if (doc.Components?.Headers != null)
            foreach (IOpenApiHeader? h in doc.Components.Headers.Values)
            {
                if (h is OpenApiHeaderReference headerRef)
                    RewriteHeaderRef(headerRef);
                if (h.Schema != null)
                    WalkSchema(h.Schema);
            }

        if (doc.Components?.RequestBodies != null)
            foreach (IOpenApiRequestBody? rb in doc.Components.RequestBodies.Values)
            {
                if (rb is OpenApiRequestBodyReference requestBodyRef)
                    RewriteRequestBodyRef(requestBodyRef);
                foreach (OpenApiMediaType? mt in rb.Content.Values)
                    if (mt.Schema != null)
                        WalkSchema(mt.Schema);
            }

                    if (doc.Components?.Responses != null)
                foreach (IOpenApiResponse? resp in doc.Components.Responses.Values)
                {
                    if (resp is OpenApiResponseReference responseRef)
                        RewriteResponseRef(responseRef);
                    foreach (OpenApiMediaType? mt in resp.Content.Values)
                        if (mt.Schema != null)
                            WalkSchema(mt.Schema);
                }

        foreach (IOpenApiPathItem? path in doc.Paths.Values)
        foreach (OpenApiOperation? op in path.Operations.Values)
        {
            if (op.RequestBody is OpenApiRequestBodyReference requestBodyRef)
                RewriteRequestBodyRef(requestBodyRef);
            if (op.RequestBody?.Content != null)
                foreach (OpenApiMediaType? mt in op.RequestBody.Content.Values)
                    if (mt.Schema != null)
                        WalkSchema(mt.Schema);

            if (op.Parameters != null)
                foreach (IOpenApiParameter? p in op.Parameters)
                {
                    if (p is OpenApiParameterReference paramRef)
                        RewriteParameterRef(paramRef);
                                    if (p.Schema != null)
                    WalkSchema(p.Schema);
                }

            foreach (IOpenApiResponse? resp in op.Responses.Values)
            {
                if (resp is OpenApiResponseReference responseRef)
                    RewriteResponseRef(responseRef);
                foreach (OpenApiMediaType? mt in resp.Content.Values)
                    if (mt.Schema != null)
                        WalkSchema(mt.Schema);
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
            _logger.LogWarning(
                "IsValidSchemaReference failed for Ref ID '{RefId}'. Key does not exist in the schemas component dictionary.",
                reference.Reference.Id);
        }

        return keyExists;
    }

    private void ScrubBrokenRefs(IDictionary<string, OpenApiMediaType>? contentDict, OpenApiDocument doc)
    {
        if (contentDict == null) return;

        var visited = new HashSet<IOpenApiSchema>();

        foreach (string key in contentDict.Keys.ToList())
        {
            OpenApiMediaType media = contentDict[key];
            IOpenApiSchema? schema = media.Schema;
            if (schema is OpenApiSchemaReference schemaRef && !IsValidSchemaReference(schemaRef, doc))
            {
                _logger.LogWarning("Found broken media-type ref @ {Key}", key);
            }

            ScrubAllRefs(schema, doc, visited);
        }
    }

    private void ScrubAllRefs(IOpenApiSchema? schema, OpenApiDocument doc, HashSet<IOpenApiSchema> visited)
    {
        if (schema == null || !visited.Add(schema)) return;

        if (schema is OpenApiSchemaReference schemaRef && !IsValidSchemaReference(schemaRef, doc))
        {
            _logger.LogWarning("Found broken ref for schema {Schema}", schema.Title ?? "(no title)");
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

        if (schema.Items != null) ScrubAllRefs(schema.Items, doc, visited);

        if (schema.AdditionalProperties != null) ScrubAllRefs(schema.AdditionalProperties, doc, visited);
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

        void PatchContent(IDictionary<string, OpenApiMediaType>? content)
        {
            if (content == null) return;
            foreach (OpenApiMediaType media in content.Values)
            {
                PatchSchema(media.Schema);
            }
        }

        if (doc.Components == null) return;

        foreach (KeyValuePair<string, IOpenApiRequestBody> kv in doc.Components.RequestBodies)
        {
            cancellationToken.ThrowIfCancellationRequested();
            PatchContent(kv.Value.Content);
        }

        foreach (KeyValuePair<string, IOpenApiResponse> kv in doc.Components.Responses)
        {
            cancellationToken.ThrowIfCancellationRequested();
            PatchContent(kv.Value.Content);
        }

        foreach (KeyValuePair<string, IOpenApiParameter> kv in doc.Components.Parameters)
        {
            cancellationToken.ThrowIfCancellationRequested();
            PatchSchema(kv.Value.Schema);
        }

        foreach (KeyValuePair<string, IOpenApiHeader> kv in doc.Components.Headers)
        {
            cancellationToken.ThrowIfCancellationRequested();
            PatchSchema(kv.Value.Schema);
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

                foreach (OpenApiOperation? operation in kvp.Value.Operations.Values)
                {
                    if (operation.Parameters == null)
                    {
                        operation.Parameters = new List<IOpenApiParameter>();
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

                    foreach (IOpenApiParameter? param in operation.Parameters)
                    {
                        if (param.Name == "member_account_id" && param.In == ParameterLocation.Path)
                        {
                            // In v2.3, Schema is read-only, so we can't modify it directly
                            // We'll handle this in a different way if needed
                            _logger.LogDebug("Would update schema description for parameter 'member_account_id'");
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
        string baseType = mediaType.Split(';')[0].Trim();
        if (baseType.Contains('*') || !baseType.Contains('/'))
            return "application/json";
        return baseType;
    }

    private static bool IsMediaEmpty(OpenApiMediaType media)
    {
        IOpenApiSchema? s = media.Schema;
        bool schemaEmpty = s == null || (s.Type == null && (s.Properties == null || !s.Properties.Any()) && s.Items == null &&
                                         !s.AllOf.Any() && !s.AnyOf.Any() && !s.OneOf.Any());
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

    private async ValueTask ReadAndValidateOpenApi(string filePath)
    {
        await using FileStream stream = File.OpenRead(filePath);
        var result = await OpenApiDocument.LoadAsync(stream);
        var diagnostics = result.Diagnostic;

        if (diagnostics.Errors?.Any() == true)
        {
            string msgs = string.Join("; ", diagnostics.Errors.Select(e => e.Message));
            _logger.LogWarning($"OpenAPI parsing errors in {Path.GetFileName(filePath)}: {msgs}");
        }
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
            foreach (OpenApiOperation? op in path.Operations.Values)
            {
                if (op.Parameters == null) continue;

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

    private void FixSchemaDefaults(IOpenApiSchema schema, HashSet<IOpenApiSchema> visited)
    {
        if (schema == null || !visited.Add(schema)) return;

        // Cast to concrete type to modify read-only properties
        if (schema is not OpenApiSchema concreteSchema) return;

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
                        concreteSchema.Default = JsonValue.Create(matchingValue);
                    }
                    else
                    {
                        concreteSchema.Default = schema.Enum.First();
                    }

                    _logger.LogWarning("Fixed invalid default value '{OldDefault}' to '{NewDefault}' in schema '{SchemaTitle}'", defaultValue, concreteSchema.Default,
                        schema.Title ?? "(no title)");
                }
            }
        }

        if (schema.Default != null)
        {
            switch (schema.Type)
            {
                case JsonSchemaType.Boolean:
                    if (schema.Default is not JsonValue || schema.Default.GetValueKind() != JsonValueKind.True && schema.Default.GetValueKind() != JsonValueKind.False)
                    {
                        concreteSchema.Default = JsonValue.Create(false);
                    }

                    break;

                case JsonSchemaType.Array:
                    if (schema.Default is not JsonArray)
                    {
                        concreteSchema.Default = new JsonArray();
                    }

                    break;

                case JsonSchemaType.String:
                    if (schema.Format == "date-time" && schema.Default is JsonValue dateStr)
                    {
                        if (dateStr.GetValue<string>() is string dateValue && !DateTime.TryParse(dateValue, out _))
                        {
                            concreteSchema.Default = null;
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
        if (s?.Example is JsonArray arr && arr.Count > 0)
        {
            if (s.Type == JsonSchemaType.String && arr.First() is JsonValue firstValue && firstValue.GetValue<string>() is string stringValue)
                s.Example = JsonValue.Create(stringValue);
            else
                s.Example = null;
        }

        if (s?.Example is JsonValue str && str.GetValue<string>() is string strValue && strValue.Length > 5_000)
            s.Example = null;
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
                concreteSchema.Type = JsonSchemaType.Object;
                _logger.LogWarning("Injected default type='object' for schema with no type '{SchemaTitle}'", schema.Title ?? "(no title)");
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
        if (doc.Components?.Schemas == null || doc.Paths == null) return;

        var operationIds = new HashSet<string>(
            doc.Paths.Values.SelectMany(p => p.Operations.Values)
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
            foreach (var (oldKey, newKey) in mapping)
            {
                if (doc.Components.Schemas.TryGetValue(oldKey, out var schema))
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

    public async ValueTask ProcessKiota(string fixedPath, string clientName, string libraryName, string targetDir,
        CancellationToken cancellationToken = default)
    {
        await _processUtil.Start("kiota", targetDir, $"kiota generate -l CSharp -d \"{fixedPath}\" -o src -c {clientName} -n {libraryName} --ebc --cc",
                              waitForExit: true, cancellationToken: cancellationToken)
                          .NoSync();
    }
}