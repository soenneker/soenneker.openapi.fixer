using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Any;
using Microsoft.OpenApi.Models;
using Microsoft.OpenApi.Readers;
using Soenneker.OpenApi.Fixer.Abstract;

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
            var reader = new OpenApiStreamReader();
            OpenApiDocument? document = reader.Read(pre, out _);

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
                foreach (OpenApiSchema? schema in document.Components.Schemas.Values)
                {
                    DeepCleanSchema(schema, new HashSet<OpenApiSchema>());
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
            await using var outFs = new FileStream(targetFilePath, FileMode.Create);
            await using var tw = new StreamWriter(outFs);
            var jw = new Microsoft.OpenApi.Writers.OpenApiJsonWriter(tw);
            document.SerializeAsV3(jw);
            await tw.FlushAsync(cancellationToken);

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
                    _logger.LogWarning(
                        "Schema '{Old}' collides with Kiota wrapper in operation '{Op}'. Renaming to '{New}'.",
                        expectedWrapperName, op.OperationId, newName);

                    renameMap[expectedWrapperName] = newName;
                }
            }
        }

        if (renameMap.Count > 0)
            UpdateAllReferences(doc, renameMap);   // you already have this helper
    }

    private void EnsureDiscriminatorForOneOf(OpenApiDocument doc)
    {
        if (doc.Components?.Schemas == null) return;

        foreach (var (schemaName, schema) in doc.Components.Schemas)
        {
            IList<OpenApiSchema>? poly = schema.OneOf ?? schema.AnyOf;
            if (poly is not { Count: > 1 }) continue;          // not polymorphic
            if (schema.Discriminator != null) continue;      // already OK

            const string discProp = "type";
            schema.Discriminator = new OpenApiDiscriminator
            {
                PropertyName = discProp,
                Mapping = new Dictionary<string, string>()
            };

            schema.Properties ??= new Dictionary<string, OpenApiSchema>();
            if (!schema.Properties.ContainsKey(discProp))
            {
                schema.Properties[discProp] = new OpenApiSchema
                {
                    Type = "string",
                    Description = "Union discriminator"
                };
                _logger.LogInformation("Injected discriminator property '{Prop}' into schema '{Schema}'.",
                                       discProp, schemaName);
            }

            schema.Required ??= new HashSet<string>();
            schema.Required.Add(discProp);

            // build mapping
            for (int i = 0; i < poly.Count; i++)
            {
                OpenApiSchema branch = poly[i];
                string refId;

                if (branch.Reference?.Id is { } id)            // referenced branch
                {
                    refId = id;
                }
                else                                           // inline branch
                {
                    // should already have been promoted by PromoteInlinePolymorphs
                    refId = $"{schemaName}_{i + 1}";
                }

                schema.Discriminator.Mapping.TryAdd(refId, $"#/components/schemas/{refId}");
            }

            _logger.LogInformation("Added discriminator mapping for polymorphic schema '{Schema}'.",
                                   schemaName);
        }
    }

    private void RemoveRedundantDerivedValue(OpenApiDocument doc)
    {
        if (doc.Components?.Schemas == null) return;
        IDictionary<string, OpenApiSchema>? pool = doc.Components.Schemas;

        // ------------- local helpers ------------------------------------------
        static OpenApiSchema Resolve(OpenApiSchema s, IDictionary<string, OpenApiSchema> p)
            => (s.Reference?.Type == ReferenceType.Schema && p.TryGetValue(s.Reference.Id, out OpenApiSchema? t)) ? t : s;

        static bool IsWellDefined(OpenApiSchema s) =>
            !string.IsNullOrWhiteSpace(s.Type) ||
            s.Reference != null ||
            (s.Enum?.Count ?? 0) > 0 ||
            (s.Items != null) ||
            (s.AllOf?.Count ?? 0) > 0 ||
            (s.OneOf?.Count ?? 0) > 0 ||
            (s.AnyOf?.Count ?? 0) > 0;

        // ------------- main pass ----------------------------------------------
        foreach (OpenApiSchema container in pool.Values)
        {
            if (container.AllOf is not { Count: > 1 }) continue;

            // find the FIRST fragment (base or earlier override) that has a well-defined `value`
            OpenApiSchema? firstValueOwner = null;
            foreach (OpenApiSchema? frag in container.AllOf.Select(f => Resolve(f, pool)))
            {
                if (frag.Properties != null &&
                    frag.Properties.TryGetValue("value", out OpenApiSchema? prop) &&
                    IsWellDefined(prop))
                {
                    firstValueOwner = frag;
                    break;
                }
            }
            if (firstValueOwner == null) continue;   // nobody defines `value` in a useful way

            // remove *every* later override of `value`
            bool afterFirst = false;
            foreach (OpenApiSchema? frag in container.AllOf.Select(f => Resolve(f, pool)))
            {
                if (frag == firstValueOwner)
                {
                    afterFirst = true;   // start skipping after this fragment
                    continue;
                }

                if (!afterFirst) continue;

                if (frag.Properties != null && frag.Properties.Remove("value"))
                {
                    frag.Required?.Remove("value");
                    _logger.LogInformation(
                        "Removed redundant derived 'value' from schema '{Derived}' (base already defines it)",
                        container.Title ?? container.Reference?.Id ?? "(unnamed)");
                }
            }
        }
    }
    private void RemoveShadowingUntypedProperties(OpenApiDocument doc)
    {
        if (doc.Components?.Schemas == null) return;
        IDictionary<string, OpenApiSchema>? pool = doc.Components.Schemas;

        static OpenApiSchema Resolve(OpenApiSchema s, IDictionary<string, OpenApiSchema> p)
            => (s.Reference?.Type == ReferenceType.Schema && p.TryGetValue(s.Reference.Id, out OpenApiSchema? t)) ? t : s;

        static bool IsUntyped(OpenApiSchema s) =>
            string.IsNullOrWhiteSpace(s.Type) &&
            s.Reference == null &&
            (s.Enum?.Count ?? 0) == 0 &&
            (s.Items == null) &&
            (s.AllOf?.Count ?? 0) == 0 &&
            (s.OneOf?.Count ?? 0) == 0 &&
            (s.AnyOf?.Count ?? 0) == 0;

        foreach (OpenApiSchema container in pool.Values)
        {

            // Need: at least one $ref fragment  +  one inline fragment with properties
            if (container.AllOf == null) continue;

            OpenApiSchema? baseFrag = container.AllOf.FirstOrDefault(f => f.Reference?.Type == ReferenceType.Schema);
            OpenApiSchema? overrideFrag = container.AllOf.FirstOrDefault(f => f.Properties?.Count > 0);

            if (baseFrag == null || overrideFrag == null) continue;

            OpenApiSchema baseSchema = Resolve(baseFrag, pool);
            if (baseSchema.Properties == null) continue;

            foreach ((string? propName, OpenApiSchema? childProp) in overrideFrag.Properties)
            {
                if (!baseSchema.Properties.TryGetValue(propName, out OpenApiSchema? baseProp)) continue;

                bool childConcrete = !IsUntyped(childProp);
                bool baseIsBare = IsUntyped(baseProp);

                if (baseIsBare)
                {
                    baseSchema.Properties.Remove(propName);
                    baseSchema.Required?.Remove(propName);
                    _logger.LogInformation(
                        "Removed untyped shadowed property '{Prop}' from base schema '{Base}' (overridden in '{Child}')",
                        propName,
                        baseSchema.Title ?? baseSchema.Reference?.Id ?? "(unnamed)",
                        container.Title ?? "(unnamed)");
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
            if (operation.RequestBody?.Reference != null || (operation.RequestBody?.Content?.Count ?? 0) <= 1)
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
                if (media.Schema.Reference == null && !IsSchemaEmpty(media.Schema))
                {
                    // Create a name for our new component.
                    string newSchemaName = ReserveUniqueSchemaName(schemas, $"{operation.OperationId}{mediaType.Replace("/", "_")}", "RequestBody");

                    _logger.LogInformation("Extracting inline request body schema for '{MediaType}' in operation '{OpId}' to new component '{NewSchemaName}'.", mediaType, operation.OperationId, newSchemaName);

                    // Add the inline schema to the components dictionary.
                    OpenApiSchema extractedSchema = media.Schema;
                    extractedSchema.Title ??= newSchemaName;
                    schemas.Add(newSchemaName, extractedSchema);

                    // Replace the inline schema with a reference to our new component.
                    media.Schema = new OpenApiSchema
                    {
                        Reference = new OpenApiReference { Type = ReferenceType.Schema, Id = newSchemaName }
                    };
                }
                // --- END OF NEW LOGIC ---

                // Now that we can be certain we have a reference, we can check for the name collision.
                if (media.Schema.Reference == null) continue;

                string originalSchemaName = media.Schema.Reference.Id;

                if (string.Equals(originalSchemaName, operation.OperationId, StringComparison.OrdinalIgnoreCase))
                {
                    if (renameMap.TryGetValue(originalSchemaName, out var newName))
                    {
                        media.Schema.Reference.Id = newName;
                        continue;
                    }

                    newName = ReserveUniqueSchemaName(schemas, $"{originalSchemaName}Body", "Dto");

                    _logger.LogWarning("CRITICAL COLLISION: Schema '{Original}' (used in {OpId}) matches OperationId. Renaming to '{New}'.", originalSchemaName, operation.OperationId, newName);

                    if (schemas.TryGetValue(originalSchemaName, out var schemaToRename))
                    {
                        schemas.Remove(originalSchemaName);
                        schemaToRename.Title ??= newName;
                        schemas.Add(newName, schemaToRename);

                        media.Schema.Reference.Id = newName;
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

            if (schema.Reference?.ReferenceV3 == oldRef)
            {
                _logger.LogInformation("Rewriting $ref from '{OldRef}' to '{NewRef}' at {Context}", oldRef, newRef, context ?? "unknown location");
                schema.Reference = new OpenApiReference
                {
                    Type = ReferenceType.Schema,
                    Id = newKey
                };
            }

            if (schema.Properties != null)
            {
                foreach (var kvp in schema.Properties)
                    Recurse(kvp.Value, $"{context ?? "schema"}.properties.{kvp.Key}");
            }

            if (schema.Items != null)
                Recurse(schema.Items, $"{context ?? "schema"}.items");

            foreach (var (s, type) in schema.AllOf?.Select(s => (s, "allOf")) ?? Enumerable.Empty<(OpenApiSchema, string)>())
                Recurse(s, $"{context ?? "schema"}.{type}");
            foreach (var (s, type) in schema.AnyOf?.Select(s => (s, "anyOf")) ?? Enumerable.Empty<(OpenApiSchema, string)>())
                Recurse(s, $"{context ?? "schema"}.{type}");
            foreach (var (s, type) in schema.OneOf?.Select(s => (s, "oneOf")) ?? Enumerable.Empty<(OpenApiSchema, string)>())
                Recurse(s, $"{context ?? "schema"}.{type}");
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

        foreach (OpenApiSchema? schema in document.Components.Schemas.Values)
        {
            if (schema.Type == "object" && schema.Default != null && !(schema.Default is OpenApiObject))
            {
                _logger.LogWarning("Removing invalid default ({Default}) from object schema '{Schema}'", schema.Default, schema.Title ?? "(no title)");
                schema.Default = null;
            }
        }
    }

    private void RemoveEmptyInlineSchemas(OpenApiDocument document)
    {
        if (document.Components?.Schemas == null)
            return;

        var visited = new HashSet<OpenApiSchema>();
        foreach (OpenApiSchema? schema in document.Components.Schemas.Values)
            Clean(schema, visited);
    }

    private void Clean(OpenApiSchema schema, HashSet<OpenApiSchema> visited)
    {
        if (schema == null || !visited.Add(schema))
            return;

        if (schema.AllOf != null)
        {
            schema.AllOf = schema.AllOf.Where(child => child != null && (child.Reference != null || !IsSchemaEmpty(child))).ToList();
        }

        if (schema.OneOf != null)
        {
            schema.OneOf = schema.OneOf.Where(child => child != null && (child.Reference != null || !IsSchemaEmpty(child))).ToList();
        }

        if (schema.AnyOf != null)
        {
            schema.AnyOf = schema.AnyOf.Where(child => child != null && (child.Reference != null || !IsSchemaEmpty(child))).ToList();
        }

        if (schema.AllOf != null)
        {
            foreach (OpenApiSchema? child in schema.AllOf)
                if (child != null)
                    Clean(child, visited);
        }

        if (schema.OneOf != null)
        {
            foreach (OpenApiSchema? child in schema.OneOf)
                if (child != null)
                    Clean(child, visited);
        }

        if (schema.AnyOf != null)
        {
            foreach (OpenApiSchema? child in schema.AnyOf)
                if (child != null)
                    Clean(child, visited);
        }

        if (schema.Properties != null)
        {
            foreach (OpenApiSchema? prop in schema.Properties.Values)
                if (prop != null)
                    Clean(prop, visited);
        }

        if (schema.Items != null)
        {
            Clean(schema.Items, visited);
        }

        if (schema.AdditionalProperties != null)
        {
            Clean(schema.AdditionalProperties, visited);
        }
    }

    private bool IsSchemaEmpty(OpenApiSchema schema)
    {
        if (schema == null) return true;

        bool hasContent = schema.Reference != null || !string.IsNullOrWhiteSpace(schema.Type) || (schema.Properties?.Any() ?? false) ||
                          (schema.AllOf?.Any() ?? false) || (schema.OneOf?.Any() ?? false) || (schema.AnyOf?.Any() ?? false) || (schema.Enum?.Any() ?? false) ||
                          schema.Items != null || schema.AdditionalProperties != null || schema.AdditionalPropertiesAllowed;

        return !hasContent;
    }

    private void LogState(string stage, OpenApiDocument document)
    {
         if (!_logState) return;

        if (document.Components.Schemas.TryGetValue("CreateDocument", out OpenApiSchema schema))
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

        if (schema.Default is OpenApiString ds && string.IsNullOrEmpty(ds.Value))
        {
            schema.Default = null;
        }

        if (schema.Example is OpenApiString es && string.IsNullOrEmpty(es.Value))
        {
            schema.Example = null;
        }

        if (schema.Enum != null && schema.Enum.Any())
        {
            List<IOpenApiAny> cleanedEnum = schema.Enum.OfType<OpenApiString>()
                                                  .Where(s => !string.IsNullOrEmpty(s.Value))
                                                  .Select(s => new OpenApiString(TrimQuotes(s.Value)))
                                                  .Cast<IOpenApiAny>()
                                                  .ToList();

            schema.Enum = cleanedEnum.Any() ? cleanedEnum : null;
        }

        if (schema.Properties != null)
        {
            foreach (OpenApiSchema? p in schema.Properties.Values)
            {
                DeepCleanSchema(p, visited);
            }
        }

        if (schema.Items != null)
        {
            DeepCleanSchema(schema.Items, visited);
        }

        if (schema.AdditionalProperties != null)
        {
            DeepCleanSchema(schema.AdditionalProperties, visited);
        }
    }

    private static IList<OpenApiSchema>? RemoveRedundantEmptyEnums(IList<OpenApiSchema>? list, Func<OpenApiSchema, bool> isRedundant)
    {
        if (list == null || list.Count == 0)
            return list;

        List<OpenApiSchema> kept = list.Where(b => !isRedundant(b)).ToList();
        return kept.Count == 0 ? null : kept;
    }

    private void StripEmptyEnumBranches(OpenApiDocument document)
    {
        if (document.Components?.Schemas == null)
            return;

        var visited = new HashSet<OpenApiSchema>();
        var queue = new Queue<OpenApiSchema>(document.Components.Schemas.Values);

        static bool IsTrulyRedundantEmptyEnum(OpenApiSchema s) =>
            s.Enum != null && s.Enum.Count == 0 && string.IsNullOrWhiteSpace(s.Type) && (s.Properties == null || s.Properties.Count == 0) && s.Items == null &&
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
                foreach (OpenApiSchema? p in schema.Properties.Values)
                    queue.Enqueue(p);
            if (schema.Items != null) queue.Enqueue(schema.Items);
            if (schema.AllOf != null)
                foreach (OpenApiSchema? b in schema.AllOf)
                    queue.Enqueue(b);
            if (schema.OneOf != null)
                foreach (OpenApiSchema? b in schema.OneOf)
                    queue.Enqueue(b);
            if (schema.AnyOf != null)
                foreach (OpenApiSchema? b in schema.AnyOf)
                    queue.Enqueue(b);
            if (schema.AdditionalProperties != null) queue.Enqueue(schema.AdditionalProperties);
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
        if (document.Components?.Schemas is not IDictionary<string, OpenApiSchema> comps)
            return;

        // 1. Identify pure‐primitive schemas
        List<string> primitives = comps
                                  .Where(kv =>
                                      !string.IsNullOrWhiteSpace(kv.Value.Type)
                                      && (kv.Value.Type == "string" || kv.Value.Type == "integer"
                                                                    || kv.Value.Type == "boolean" || kv.Value.Type == "number")
                                      && (kv.Value.Properties?.Count ?? 0) == 0
                                      && (kv.Value.Enum?.Count ?? 0) == 0
                                      && (kv.Value.OneOf?.Count ?? 0) == 0
                                      && (kv.Value.AnyOf?.Count ?? 0) == 0
                                      && (kv.Value.AllOf?.Count ?? 0) == 0
                                      && kv.Value.Items == null
                                  )
                                  .Select(kv => kv.Key)
                                  .ToList();

        if (!primitives.Any())
            return;

        foreach (string primKey in primitives)
        {
            OpenApiSchema primitiveSchema = comps[primKey];

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

                if (schema.Reference != null
                 && schema.Reference.Type == ReferenceType.Schema
                 && schema.Reference.Id == primKey)
                {
                    // remove the ref and copy inline constraints
                    schema.Reference = null;
                    schema.Type = inlineSchema.Type;
                    schema.Format = inlineSchema.Format;
                    schema.Description = inlineSchema.Description;
                    schema.MaxLength = inlineSchema.MaxLength;
                    schema.Pattern = inlineSchema.Pattern;
                    schema.Minimum = inlineSchema.Minimum;
                    schema.Maximum = inlineSchema.Maximum;
                    return;
                }

                // dive into composite schemas
                if (schema.AllOf != null) foreach (OpenApiSchema? c in schema.AllOf) ReplaceRef(c);
                if (schema.OneOf != null) foreach (OpenApiSchema? c in schema.OneOf) ReplaceRef(c);
                if (schema.AnyOf != null) foreach (OpenApiSchema? c in schema.AnyOf) ReplaceRef(c);
                if (schema.Properties != null)
                    foreach (OpenApiSchema? prop in schema.Properties.Values)
                        ReplaceRef(prop);
                if (schema.Items != null) ReplaceRef(schema.Items);
                if (schema.AdditionalProperties != null) ReplaceRef(schema.AdditionalProperties);
            }

            // Handle inlining a parameter $ref → copy its fields, then ReplaceRef(schema)
            void InlineParameter(OpenApiParameter param)
            {
                if (param.Reference != null
                 && param.Reference.Type == ReferenceType.Parameter
                 && document.Components.Parameters.TryGetValue(param.Reference.Id, out OpenApiParameter? compParam))
                {
                    // inline the parameter definition
                    param.Reference = null;
                    param.Name = compParam.Name;
                    param.In = compParam.In;
                    param.Required = compParam.Required;
                    param.Description = param.Description ?? compParam.Description;
                    param.Schema = compParam.Schema is not null
                                        ? new OpenApiSchema(compParam.Schema)
                                        : null;
                }

                if (param.Schema != null)
                    ReplaceRef(param.Schema);
            }

            // 2. Replace refs in component schemas
            foreach (OpenApiSchema cs in comps.Values.ToList())
                ReplaceRef(cs);

            // 3. Replace refs in request‐bodies
            if (document.Components.RequestBodies != null)
                foreach (OpenApiRequestBody? rb in document.Components.RequestBodies.Values)
                    foreach (OpenApiMediaType? mt in rb.Content.Values)
                        ReplaceRef(mt.Schema);

            // 4. Replace refs in responses
            if (document.Components.Responses != null)
                foreach (OpenApiResponse? resp in document.Components.Responses.Values)
                    foreach (OpenApiMediaType? mt in resp.Content.Values)
                        ReplaceRef(mt.Schema);

            // 5. Replace refs in headers
            if (document.Components.Headers != null)
                foreach (OpenApiHeader? hdr in document.Components.Headers.Values)
                    ReplaceRef(hdr.Schema);

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
                            ReplaceRef(mt.Schema);

                    foreach (OpenApiResponse? resp in op.Responses.Values)
                        if (resp.Content != null)
                            foreach (OpenApiMediaType? mt in resp.Content.Values)
                                ReplaceRef(mt.Schema);
                }
            }

            // 8. Finally, remove the now‐inlined component
            comps.Remove(primKey);
        }
    }

    private void FixAllInlineValueEnums(OpenApiDocument document)
    {
        IDictionary<string, OpenApiSchema>? comps = document.Components?.Schemas;
        if (comps == null) return;

        foreach (KeyValuePair<string, OpenApiSchema> kv in comps.ToList())
        {
            string key = kv.Key;
            OpenApiSchema schema = kv.Value;
            OpenApiSchema? wrapperSegment = null;

            if (schema.Properties?.ContainsKey("value") == true)
                wrapperSegment = schema;
            else if (schema.AllOf?.Count == 2 && schema.AllOf[1].Properties?.ContainsKey("value") == true)
                wrapperSegment = schema.AllOf[1];
            else
                continue;

            OpenApiSchema? inline = wrapperSegment.Properties["value"];
            if (inline.Enum == null || inline.Enum.Count == 0) continue;

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

            wrapperSegment.Properties["value"] = new OpenApiSchema
            {
                Reference = new OpenApiReference
                {
                    Type = ReferenceType.Schema,
                    Id = enumKey
                }
            };
        }
    }

    private void RenameInvalidComponentSchemas(OpenApiDocument document)
    {
        IDictionary<string, OpenApiSchema>? schemas = document.Components?.Schemas;
        if (schemas == null) return;

        var mapping = new Dictionary<string, string>();
        foreach (string key in schemas.Keys.ToList())
        {
            if (!IsValidIdentifier(key))
            {
                string newKey = SanitizeName(key);
                if (string.IsNullOrWhiteSpace(newKey) || schemas.ContainsKey(newKey))
                    newKey = $"{newKey}_{Guid.NewGuid():N}";
                mapping[key] = newKey;
            }
        }

        foreach ((string oldKey, string newKey) in mapping)
        {
            OpenApiSchema schema = schemas[oldKey];
            schemas.Remove(oldKey);
            if (string.IsNullOrWhiteSpace(schema.Title))
                schema.Title = newKey;
            schemas[newKey] = schema;
        }

        if (mapping.Any())
            UpdateAllReferences(document, mapping);
    }

    private void ApplySchemaNormalizations(OpenApiDocument document, CancellationToken cancellationToken)
    {
        if (document?.Components?.Schemas == null) return;

        IDictionary<string, OpenApiSchema>? comps = document.Components.Schemas;

        foreach (KeyValuePair<string, OpenApiSchema> kv in comps)
        {
            if (kv.Value != null && string.IsNullOrWhiteSpace(kv.Value.Title))
            {
                kv.Value.Title = kv.Key;
            }
        }

        var visited = new HashSet<OpenApiSchema>();
        foreach (var schema in comps.Values)
        {
            if (schema != null) RemoveEmptyCompositionObjects(schema, visited);
        }

        foreach (KeyValuePair<string, OpenApiSchema> kv in comps.ToList())
        {
            cancellationToken.ThrowIfCancellationRequested();
            OpenApiSchema? schema = kv.Value;
            if (schema == null) continue;

            if (string.Equals(schema.Format, "datetime", StringComparison.OrdinalIgnoreCase)) schema.Format = "date-time";
            if (string.Equals(schema.Format, "uuid4", StringComparison.OrdinalIgnoreCase)) schema.Format = "uuid";

            bool hasComposition = (schema.OneOf?.Any() == true) || (schema.AnyOf?.Any() == true) || (schema.AllOf?.Any() == true);
            if (string.IsNullOrWhiteSpace(schema.Type) && hasComposition)
            {
                schema.Type = "object";
            }

            if ((schema.OneOf?.Any() == true || schema.AnyOf?.Any() == true) && schema.Discriminator == null)
            {
                const string discName = "type";
                schema.Discriminator = new OpenApiDiscriminator { PropertyName = discName };
                schema.Properties ??= new Dictionary<string, OpenApiSchema>();
                if (!schema.Properties.ContainsKey(discName))
                {
                    schema.Properties[discName] = new OpenApiSchema { Type = "string", Title = discName, Description = "Union discriminator" };
                }

                schema.Required ??= new HashSet<string>();
                if (!schema.Required.Contains(discName)) schema.Required.Add(discName);
            }

            // ──────────────────────────────────────────────────────────────────
            // ENSURE THE DISCRIMINATOR PROPERTY EXISTS
            // ──────────────────────────────────────────────────────────────────
            if (schema.Discriminator is { PropertyName: { } discProp })
            {
                schema.Properties ??= new Dictionary<string, OpenApiSchema>();

                if (!schema.Properties.ContainsKey(discProp))
                {
                    schema.Properties[discProp] = new OpenApiSchema
                    {
                        Type = "string",
                        Title = discProp,
                        Description = "Union discriminator"
                    };
                }

                schema.Required ??= new HashSet<string>();
                if (!schema.Required.Contains(discProp))
                    schema.Required.Add(discProp);
            }

            IList<OpenApiSchema>? compositionList = schema.OneOf ?? schema.AnyOf;
            if (compositionList?.Any() == true && schema.Discriminator != null)
            {
                schema.Discriminator.Mapping ??= new Dictionary<string, string>();
                foreach (OpenApiSchema branch in compositionList)
                {
                    if (branch.Reference?.Id != null && !schema.Discriminator.Mapping.ContainsKey(branch.Reference.Id))
                    {
                        string? mappingKey = GetMappingKeyFromRef(branch.Reference.Id);
                        if (!string.IsNullOrEmpty(mappingKey))
                        {
                            schema.Discriminator.Mapping[mappingKey] = $"#/components/schemas/{branch.Reference.Id}";
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
            if (hasProps && string.IsNullOrWhiteSpace(schema.Type))
            {
                schema.Type = "object";
            }
        }

        var validPaths = new OpenApiPaths();
        foreach (KeyValuePair<string, OpenApiPathItem> path in document.Paths)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (path.Value?.Operations == null || !path.Value.Operations.Any())
                continue;

            foreach (KeyValuePair<OperationType, OpenApiOperation> operation in path.Value.Operations)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (operation.Value == null) continue;

                var newResps = new OpenApiResponses();
                foreach (KeyValuePair<string, OpenApiResponse> resp in operation.Value.Responses)
                {
                    if (resp.Value == null) continue;

                    if (resp.Value.Reference != null)
                    {
                        newResps[resp.Key] = resp.Value;
                        continue;
                    }

                    if (resp.Value.Content != null)
                    {
                        resp.Value.Content = resp.Value.Content.Where(p => p.Key != null && p.Value != null)
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
                            OpenApiSchema? sch = mt.Schema;
                            return sch.Reference != null || !IsSchemaEmpty(sch);
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
                    operation.Value.Responses = newResps;
                }
                else
                {
                    operation.Value.Responses = CreateFallbackResponses(operation.Key);
                }

                if (operation.Value.RequestBody != null && operation.Value.RequestBody.Reference == null)
                {
                    OpenApiRequestBody? rb = operation.Value.RequestBody;
                    if (rb.Content != null)
                    {
                        rb.Content = rb.Content.Where(p => p.Key != null && p.Value != null).ToDictionary(p => NormalizeMediaType(p.Key), p => p.Value);
                    }

                    ScrubBrokenRefs(rb.Content, document);
                    Dictionary<string, OpenApiMediaType>? validRb = rb.Content?.Where(p => p.Value?.Schema?.Reference != null || !IsMediaEmpty(p.Value)).ToDictionary(p => p.Key, p => p.Value);

                    operation.Value.RequestBody = (validRb != null && validRb.Any())
                        ? new OpenApiRequestBody { Description = rb.Description, Content = validRb }
                        : CreateFallbackRequestBody();
                }
            }

            validPaths.Add(path.Key, path.Value);
        }

        document.Paths = validPaths;

        foreach (KeyValuePair<string, OpenApiSchema> kv in comps)
        {
            if (kv.Value == null) continue;
            OpenApiSchema schema = kv.Value;

            bool onlyHasRequired = schema.Type == "object" && (schema.Properties == null || !schema.Properties.Any()) && schema.Items == null &&
                                   (schema.AllOf?.Any() != true) && (schema.AnyOf?.Any() != true) && (schema.OneOf?.Any() != true) &&
                                   schema.AdditionalProperties == null && (schema.Required?.Any() == true);

            if (onlyHasRequired)
            {
                List<string> reqs = schema.Required?.Where(r => !string.IsNullOrWhiteSpace(r)).ToList() ?? new List<string>();
                if (reqs.Any())
                {
                    schema.Properties = reqs.ToDictionary(name => name, _ => new OpenApiSchema { Type = "object" });
                }

                schema.AdditionalProperties = new OpenApiSchema { Type = "object" };
                schema.AdditionalPropertiesAllowed = true;
                schema.Required = new HashSet<string>();
            }

            bool isTrulyEmpty = schema.Type == "object" && (schema.Properties == null || !schema.Properties.Any()) && schema.Items == null &&
                                (schema.AllOf?.Any() != true) && (schema.AnyOf?.Any() != true) && (schema.OneOf?.Any() != true) &&
                                schema.AdditionalProperties == null;

            if (isTrulyEmpty)
            {
                schema.Properties = new Dictionary<string, OpenApiSchema>();
                schema.AdditionalProperties = new OpenApiSchema { Type = "object" };
                schema.AdditionalPropertiesAllowed = true;
                schema.Required = new HashSet<string>();
            }
        }

        foreach (var schema in comps.Values)
        {
            if (schema?.Enum == null || !schema.Enum.Any()) continue;
            if (schema.Enum.All(x => x is OpenApiString))
            {
                schema.Type = "string";
            }
        }

        var visitedSchemas = new HashSet<OpenApiSchema>();
        foreach (var root in comps.Values)
        {
            if (root != null) InjectTypeForNullable(root, visitedSchemas);
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


    private static OpenApiResponses CreateFallbackResponses(OperationType op)
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
                            Type = "object",
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
                        Type = "object",
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
            if (string.IsNullOrWhiteSpace(schema.Title))
                schema.Title = validatedName;
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
            s.Properties?.Count == 1 && s.Properties.TryGetValue("data", out OpenApiSchema? p) && p?.Reference != null && (s.Required == null || s.Required.Count <= 1);

        IDictionary<string, OpenApiSchema>? comps = document.Components?.Schemas;
        if (comps == null) return;

        foreach (OpenApiPathItem pathItem in document.Paths.Values)
        {
            cancellationToken.ThrowIfCancellationRequested();

            foreach ((OperationType opType, OpenApiOperation operation) in pathItem.Operations)
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

                if (operation.RequestBody != null && operation.RequestBody.Reference == null && operation.RequestBody.Content != null)
                {
                    foreach ((string mediaType, OpenApiMediaType media) in operation.RequestBody.Content.ToList())
                    {
                        OpenApiSchema? schema = media?.Schema;
                        if (schema == null || schema.Reference != null) continue;
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
                        media.Schema = new OpenApiSchema
                        {
                            Reference = new OpenApiReference
                            {
                                Type = ReferenceType.Schema,
                                Id = compName
                            }
                        };
                    }
                }

                foreach ((string statusCode, OpenApiResponse response) in operation.Responses)
                {
                    if (response == null || response.Reference != null)
                    {
                        continue;
                    }

                    if (response.Content == null) continue;

                    foreach ((string mediaType, OpenApiMediaType media) in response.Content.ToList())
                    {
                        OpenApiSchema? schema = media?.Schema;
                        if (schema == null || schema.Reference != null) continue;
                        if (IsSimpleEnvelope(schema)) continue;

                        string safeMedia = ValidateComponentName(GenerateSafePart(mediaType, "media"));
                        string baseName = $"{safeOpId}_{statusCode}";
                        string compName = ReserveUniqueSchemaName(comps, baseName, $"Response_{safeMedia}");

                        AddComponentSchema(document, compName, schema);
                        media.Schema = new OpenApiSchema
                        {
                            Reference = new OpenApiReference
                            {
                                Type = ReferenceType.Schema,
                                Id = compName
                            }
                        };
                    }
                }
            }
        }
    }

    private string ReserveUniqueSchemaName(IDictionary<string, OpenApiSchema> comps, string baseName, string fallbackSuffix)
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
        } while (comps.ContainsKey(numbered));

        return numbered;
    }

    private void EnsureUniqueOperationIds(OpenApiDocument doc)
    {
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (OpenApiPathItem? path in doc.Paths.Values)
        {
            foreach (KeyValuePair<OperationType, OpenApiOperation> kvp in path.Operations)
            {
                OpenApiOperation op = kvp.Value;

                if (string.IsNullOrWhiteSpace(op.OperationId))
                {
                    op.OperationId = $"{kvp.Key}{Guid.NewGuid():N}";
                    _logger.LogDebug($"Generated missing OperationId: {op.OperationId}");
                }

                string baseId = op.OperationId;
                string unique = baseId;
                int i = 1;

                while (!seen.Add(unique))
                {
                    unique = $"{baseId}_{i++}";
                }

                if (op.OperationId != unique)
                {
                    _logger.LogDebug($"Renaming duplicate OperationId from '{op.OperationId}' to '{unique}'");
                    op.OperationId = unique;
                }
            }
        }
    }

    private static void UpdateAllReferences(OpenApiDocument doc, Dictionary<string, string> mapping)
    {
        void RewriteRef(OpenApiReference? r)
        {
            if (r == null) return;
            if (r.Type != ReferenceType.Schema) return;
            if (string.IsNullOrEmpty(r.Id)) return;

            if (mapping.TryGetValue(r.Id, out string? newId))
                r.Id = newId;
        }

        var visited = new HashSet<OpenApiSchema>();

        void WalkSchema(OpenApiSchema? s)
        {
            if (s == null || !visited.Add(s)) return;

            RewriteRef(s.Reference);

            if (s.Properties != null)
                foreach (OpenApiSchema? child in s.Properties.Values)
                    WalkSchema(child);

            if (s.Items != null) WalkSchema(s.Items);

            foreach (OpenApiSchema a in s.AllOf ?? Enumerable.Empty<OpenApiSchema>()) WalkSchema(a);
            foreach (OpenApiSchema o in s.OneOf ?? Enumerable.Empty<OpenApiSchema>()) WalkSchema(o);
            foreach (OpenApiSchema a in s.AnyOf ?? Enumerable.Empty<OpenApiSchema>()) WalkSchema(a);

            if (s.AdditionalProperties != null)
                WalkSchema(s.AdditionalProperties);
        }

        if (doc.Components?.Schemas != null)
            foreach (OpenApiSchema? schema in doc.Components.Schemas.Values)
                WalkSchema(schema);

        if (doc.Components?.Parameters != null)
            foreach (OpenApiParameter? p in doc.Components.Parameters.Values)
            {
                RewriteRef(p.Reference);
                WalkSchema(p.Schema);
            }

        if (doc.Components?.Headers != null)
            foreach (OpenApiHeader? h in doc.Components.Headers.Values)
            {
                RewriteRef(h.Reference);
                WalkSchema(h.Schema);
            }

        if (doc.Components?.RequestBodies != null)
            foreach (OpenApiRequestBody? rb in doc.Components.RequestBodies.Values)
            {
                RewriteRef(rb.Reference);
                foreach (OpenApiMediaType? mt in rb.Content.Values)
                    WalkSchema(mt.Schema);
            }

        if (doc.Components?.Responses != null)
            foreach (OpenApiResponse? resp in doc.Components.Responses.Values)
            {
                RewriteRef(resp.Reference);
                foreach (OpenApiMediaType? mt in resp.Content.Values)
                    WalkSchema(mt.Schema);
            }

        foreach (OpenApiPathItem? path in doc.Paths.Values)
            foreach (OpenApiOperation? op in path.Operations.Values)
            {
                RewriteRef(op.RequestBody?.Reference);
                if (op.RequestBody?.Content != null)
                    foreach (OpenApiMediaType? mt in op.RequestBody.Content.Values)
                        WalkSchema(mt.Schema);

                if (op.Parameters != null)
                    foreach (OpenApiParameter? p in op.Parameters)
                    {
                        RewriteRef(p.Reference);
                        WalkSchema(p.Schema);
                    }

                foreach (OpenApiResponse? resp in op.Responses.Values)
                {
                    RewriteRef(resp.Reference);
                    foreach (OpenApiMediaType? mt in resp.Content.Values)
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

    private bool IsValidSchemaReference(OpenApiReference? reference, OpenApiDocument doc)
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
            case ReferenceType.Schema:
                keyExists = comps.Schemas?.ContainsKey(reference.Id) ?? false;
                break;
            case ReferenceType.Response:
                keyExists = comps.Responses?.ContainsKey(reference.Id) ?? false;
                break;
            case ReferenceType.Parameter:
                keyExists = comps.Parameters?.ContainsKey(reference.Id) ?? false;
                break;
            case ReferenceType.RequestBody:
                keyExists = comps.RequestBodies?.ContainsKey(reference.Id) ?? false;
                break;
            case ReferenceType.Header:
                keyExists = comps.Headers?.ContainsKey(reference.Id) ?? false;
                break;
            case ReferenceType.SecurityScheme:
                keyExists = comps.SecuritySchemes?.ContainsKey(reference.Id) ?? false;
                break;
            case ReferenceType.Link:
                keyExists = comps.Links?.ContainsKey(reference.Id) ?? false;
                break;
            case ReferenceType.Callback:
                keyExists = comps.Callbacks?.ContainsKey(reference.Id) ?? false;
                break;
            case ReferenceType.Example:
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
            OpenApiSchema? schema = media.Schema;
            if (schema?.Reference != null && !IsValidSchemaReference(schema.Reference, doc))
            {
                schema.Reference = null;
                _logger.LogWarning("Removed broken media-type ref @ {Key}", key);
            }

            ScrubAllRefs(schema, doc, visited);
        }
    }

    private void ScrubAllRefs(OpenApiSchema? schema, OpenApiDocument doc, HashSet<OpenApiSchema> visited)
    {
        if (schema == null || !visited.Add(schema)) return;

        if (schema.Reference != null && !IsValidSchemaReference(schema.Reference, doc))
        {
            schema.Reference = null;
            _logger.LogWarning("Cleared nested broken ref for schema {Schema}", schema.Title ?? "(no title)");
        }

        if (schema.AllOf != null)
            foreach (OpenApiSchema? s in schema.AllOf)
                ScrubAllRefs(s, doc, visited);

        if (schema.OneOf != null)
            foreach (OpenApiSchema? s in schema.OneOf)
                ScrubAllRefs(s, doc, visited);

        if (schema.AnyOf != null)
            foreach (OpenApiSchema? s in schema.AnyOf)
                ScrubAllRefs(s, doc, visited);

        if (schema.Properties != null)
            foreach (OpenApiSchema? p in schema.Properties.Values)
                ScrubAllRefs(p, doc, visited);

        if (schema.Items != null) ScrubAllRefs(schema.Items, doc, visited);

        if (schema.AdditionalProperties != null) ScrubAllRefs(schema.AdditionalProperties, doc, visited);
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
                PatchSchema(media.Schema);
            }
        }

        if (doc.Components == null) return;

        foreach (KeyValuePair<string, OpenApiRequestBody> kv in doc.Components.RequestBodies)
        {
            cancellationToken.ThrowIfCancellationRequested();
            PatchContent(kv.Value.Content);
        }

        foreach (KeyValuePair<string, OpenApiResponse> kv in doc.Components.Responses)
        {
            cancellationToken.ThrowIfCancellationRequested();
            PatchContent(kv.Value.Content);
        }

        foreach (KeyValuePair<string, OpenApiParameter> kv in doc.Components.Parameters)
        {
            cancellationToken.ThrowIfCancellationRequested();
            PatchSchema(kv.Value.Schema);
        }

        foreach (KeyValuePair<string, OpenApiHeader> kv in doc.Components.Headers)
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
        foreach (KeyValuePair<string, OpenApiPathItem> kvp in doc.Paths)
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
                        operation.Parameters = new List<OpenApiParameter>();
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
                                Type = "string",
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
                                Type = "string",
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
        OpenApiSchema? s = media.Schema;
        bool schemaEmpty = s == null || (string.IsNullOrWhiteSpace(s.Type) && (s.Properties == null || !s.Properties.Any()) && s.Items == null &&
                                         !s.AllOf.Any() && !s.AnyOf.Any() && !s.OneOf.Any());
        bool hasExample = s?.Example != null || (media.Examples?.Any() == true);
        return schemaEmpty && !hasExample;
    }

    private void EnsureResponseDescriptions(OpenApiResponses responses)
    {
        foreach (KeyValuePair<string, OpenApiResponse> kv in responses)
        {
            string code = kv.Key;
            OpenApiResponse resp = kv.Value;
            if (string.IsNullOrWhiteSpace(resp.Description))
            {
                resp.Description = code == "default" ? "Default response" : $"{code} response";
            }
        }
    }

    private async ValueTask<OpenApiDiagnostic> ReadAndValidateOpenApi(string filePath)
    {
        await using FileStream stream = File.OpenRead(filePath);

        var reader = new OpenApiStreamReader();
        var diagnostic = new OpenApiDiagnostic();
        reader.Read(stream, out diagnostic);

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

        IDictionary<string, OpenApiSecurityScheme> schemes = document.Components.SecuritySchemes ??= new Dictionary<string, OpenApiSecurityScheme>();

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
            foreach (OpenApiOperation? op in path.Operations.Values)
            {
                if (op.Parameters == null) continue;

                OpenApiParameter? rogue = op.Parameters.FirstOrDefault(p =>
                    p.In == ParameterLocation.Header && p.Name.StartsWith("authorization", StringComparison.OrdinalIgnoreCase));

                if (rogue != null)
                {
                    op.Parameters.Remove(rogue);

                    op.Security ??= new List<OpenApiSecurityRequirement>();
                    op.Security.Add(new OpenApiSecurityRequirement
                    {
                        [schemes["assets_jwt"]] = new List<string>()
                    });
                }
            }
        }
    }

    private void FixInvalidDefaults(OpenApiDocument document)
    {
        if (document.Components?.Schemas == null) return;

        var visited = new HashSet<OpenApiSchema>();
        foreach (OpenApiSchema? schema in document.Components.Schemas.Values)
        {
            FixSchemaDefaults(schema, visited);
        }
    }

    private static string CanonicalSuccess(OperationType op) => op switch
    {
        OperationType.Post => "201",
        OperationType.Delete => "204",
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
                        schema.Default = new OpenApiString(matchingValue);
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
            switch (schema.Type?.ToLower())
            {
                case "boolean":
                    if (!(schema.Default is OpenApiBoolean))
                    {
                        schema.Default = new OpenApiBoolean(false);
                    }

                    break;

                case "array":
                    if (!(schema.Default is OpenApiArray))
                    {
                        schema.Default = new OpenApiArray();
                    }

                    break;

                case "string":
                    if (schema.Format == "date-time" && schema.Default is OpenApiString dateStr)
                    {
                        if (!DateTime.TryParse(dateStr.Value, out _))
                        {
                            schema.Default = null;
                        }
                    }

                    break;
            }
        }

        if (schema.Properties != null)
            foreach (OpenApiSchema? prop in schema.Properties.Values)
                FixSchemaDefaults(prop, visited);

        if (schema.Items != null)
            FixSchemaDefaults(schema.Items, visited);

        if (schema.AdditionalProperties != null)
            FixSchemaDefaults(schema.AdditionalProperties, visited);

        if (schema.AllOf != null)
            foreach (OpenApiSchema? s in schema.AllOf)
                FixSchemaDefaults(s, visited);

        if (schema.OneOf != null)
            foreach (OpenApiSchema? s in schema.OneOf)
                FixSchemaDefaults(s, visited);

        if (schema.AnyOf != null)
            foreach (OpenApiSchema? s in schema.AnyOf)
                FixSchemaDefaults(s, visited);
    }
    private void RemoveEmptyCompositionObjects(OpenApiSchema schema, HashSet<OpenApiSchema> visited)
    {
        if (schema == null || !visited.Add(schema)) return;

        if (schema.Properties != null)
        {
            schema.Properties = schema.Properties.GroupBy(p => p.Key).ToDictionary(g => g.Key, g => g.First().Value);

            foreach (OpenApiSchema? prop in schema.Properties.Values)
            {
                RemoveEmptyCompositionObjects(prop, visited);
            }
        }

        if (schema.Items != null)
        {
            RemoveEmptyCompositionObjects(schema.Items, visited);
        }

        if (schema.AdditionalProperties != null)
        {
            RemoveEmptyCompositionObjects(schema.AdditionalProperties, visited);
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
        if (s?.Example is OpenApiArray arr && arr.Any())
        {
            if (s.Type == "string" && arr.First() is OpenApiString os)
                s.Example = new OpenApiString(os.Value);
            else
                s.Example = null;
        }

        if (s?.Example is OpenApiString str && str.Value?.Length > 5_000)
            s.Example = null;
    }

    private void InjectTypeForNullable(OpenApiSchema schema, HashSet<OpenApiSchema> visited)
    {
        if (schema == null || !visited.Add(schema))
            return;

        if (schema.Nullable && string.IsNullOrWhiteSpace(schema.Type))
        {
            schema.Type = "object";
            _logger.LogWarning("Injected default type='object' for nullable schema '{SchemaTitle}'", schema.Title ?? "(no title)");
        }

        if (schema.Properties != null)
            foreach (OpenApiSchema? prop in schema.Properties.Values)
                InjectTypeForNullable(prop, visited);

        if (schema.Items != null)
            InjectTypeForNullable(schema.Items, visited);

        if (schema.AdditionalProperties != null)
            InjectTypeForNullable(schema.AdditionalProperties, visited);

        if (schema.AllOf != null)
            foreach (OpenApiSchema? s in schema.AllOf)
                InjectTypeForNullable(s, visited);

        if (schema.OneOf != null)
            foreach (OpenApiSchema? s in schema.OneOf)
                InjectTypeForNullable(s, visited);

        if (schema.AnyOf != null)
            foreach (OpenApiSchema? s in schema.AnyOf)
                InjectTypeForNullable(s, visited);
    }

    private void ResolveSchemaOperationNameCollisions(OpenApiDocument doc)
    {
        if (doc.Components?.Schemas == null || doc.Paths == null) return;

        var operationIds = new HashSet<string>(
            doc.Paths.Values.SelectMany(p => p.Operations.Values)
               .Where(op => op != null && !string.IsNullOrWhiteSpace(op.OperationId))
               .Select(op => op.OperationId),
            StringComparer.OrdinalIgnoreCase);

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
                    schema.Title ??= newKey; // Set title if it's empty, helps with debugging and generated code.
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

        foreach ((string? schemaName, OpenApiSchema? schema) in document.Components.Schemas.ToList())
        {
            if (schema.Type != "array" || schema.Items == null || schema.Items.Reference != null)
                continue;

            OpenApiSchema? itemsSchema = schema.Items;

            if (itemsSchema.Type != "object" || (itemsSchema.Properties == null || !itemsSchema.Properties.Any()))
                continue;

            string itemName = $"{schemaName}_item";
            while (document.Components.Schemas.ContainsKey(itemName) || newSchemas.ContainsKey(itemName))
            {
                itemName = $"{schemaName}_item_{++counter}";
            }

            itemsSchema.Title ??= itemName;
            newSchemas[itemName] = itemsSchema;

            schema.Items = new OpenApiSchema
            {
                Reference = new OpenApiReference
                {
                    Type = ReferenceType.Schema,
                    Id = itemName
                }
            };

            _logger.LogInformation("Promoted inline array item schema from '{Parent}' to components schema '{ItemName}'", schemaName, itemName);
        }

        foreach ((string key, OpenApiSchema val) in newSchemas)
        {
            document.Components.Schemas[key] = val;
        }
    }

}