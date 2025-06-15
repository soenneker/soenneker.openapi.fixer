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
            // =================================================================
            // STAGE 0: DOCUMENT LOADING & INITIAL PARSING
            // =================================================================
            await ReadAndValidateOpenApi(sourceFilePath);

            await using MemoryStream pre = PreprocessSpecFile(sourceFilePath);

            var reader = new OpenApiStreamReader();
            var diagnostic = new OpenApiDiagnostic();

            OpenApiDocument? document = reader.Read(pre, out diagnostic);
            // ...

            LogState("After STAGE 0: Initial Load", document);

            // =================================================================
            // STAGE 1: IDENTIFIERS, NAMING, AND SECURITY (SAFE, EARLY FIXES)
            // =================================================================
            _logger.LogInformation("Running initial cleanup on identifiers, paths, and security schemes...");
            EnsureSecuritySchemes(document);
            RenameConflictingPaths(document);
            EnsureUniqueOperationIds(document);
            RenameInvalidComponentSchemas(document);

            LogState("After STAGE 1: Naming", document);

            // =================================================================
            // STAGE 2: REFERENCE INTEGRITY & SCRUBBING (CRITICAL EARLY STEP)
            // =================================================================
            _logger.LogInformation("Scrubbing all component references to fix broken links...");
            ScrubComponentRefs(document, cancellationToken);

            LogState("After STAGE 2: Ref Scrubbing", document);

            // =================================================================
            // STAGE 3: STRUCTURAL TRANSFORMATIONS
            // =================================================================
            _logger.LogInformation("Performing major structural transformations (inlining, extraction)...");
            InlinePrimitiveComponents(document);
            ExtractInlineArrayItemSchemas(document);
            ExtractInlineSchemas(document, cancellationToken);
            LogState("After STAGE 3A: Transformations", document);

            _logger.LogInformation("Re-scrubbing references after extraction...");
            ScrubComponentRefs(document, cancellationToken);

            LogState("After STAGE 3B: Re-Scrubbing", document);

            // =================================================================
            // STAGE 4: DEEP SCHEMA NORMALIZATION & CLEANING
            // =================================================================
            _logger.LogInformation("Applying deep schema normalizations and cleaning...");
            ApplySchemaNormalizations(document, cancellationToken);
            LogState("After STAGE 4A: ApplySchemaNormalizations", document);

            if (document.Components?.Schemas != null)
            {
                foreach (var schema in document.Components.Schemas.Values)
                {
                    DeepCleanSchema(schema, new HashSet<OpenApiSchema>());
                }
            }

            LogState("After STAGE 4B: Deep Cleaning", document);

            // ... (rest of stage 4)
            StripEmptyEnumBranches(document);

            LogState("After STAGE 4D: StripEmptyEnumBranches", document);

            FixInvalidDefaults(document);

            LogState("After STAGE 4E:FixInvalidDefaults", document);

            FixAllInlineValueEnums(document);

            LogState("After STAGE 4F: FixAllInlineValueEnums", document);

            // =================================================================
            // STAGE 5: FINAL CLEANUP OF EMPTY OR INVALID ITEMS
            // =================================================================
            _logger.LogInformation("Performing final cleanup of empty keys and invalid structures...");
            RemoveEmptyInlineSchemas(document);
            RemoveInvalidDefaults(document);

            LogState("After STAGE 5: Final Cleanup", document);

            // =================================================================
            // STAGE 6: SERIALIZATION
            // =================================================================
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

    private static string TrimQuotes(string value)
    {
        if (value.Length >= 2 && ((value.StartsWith("\"") && value.EndsWith("\"")) || (value.StartsWith("'") && value.EndsWith("'"))))
        {
            return value.Substring(1, value.Length - 2);
        }

        return value;
    }

    /// <summary>
    /// Remove any default values whose CLR type doesn't match the schema.Type.
    /// </summary>
    private void RemoveInvalidDefaults(OpenApiDocument document)
    {
        if (document.Components?.Schemas == null) return;

        foreach (var schema in document.Components.Schemas.Values)
        {
            // If this schema is typed as "object", its Default must be an OpenApiObject
            if (schema.Type == "object" && schema.Default != null && !(schema.Default is OpenApiObject))
            {
                _logger.LogWarning("Removing invalid default ({Default}) from object schema '{Schema}'", schema.Default, schema.Title ?? "(no title)");
                schema.Default = null;
            }
        }
    }

    /// <summary>
    /// Remove any inline schemas that are completely empty (no type, no props, no ref, etc.)
    /// from allOf / oneOf / anyOf arrays throughout the document.
    /// </summary>
    private void RemoveEmptyInlineSchemas(OpenApiDocument document)
    {
        if (document.Components?.Schemas == null)
            return;

        var visited = new HashSet<OpenApiSchema>();
        foreach (var schema in document.Components.Schemas.Values)
            Clean(schema, visited);
    }

    private void Clean(OpenApiSchema schema, HashSet<OpenApiSchema> visited)
    {
        if (schema == null || !visited.Add(schema))
            return;

        // Filter each composition array
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

        // Recurse into any remaining branches
        if (schema.AllOf != null)
        {
            foreach (var child in schema.AllOf)
            {
                if (child != null)
                {
                    Clean(child, visited);
                }
            }
        }

        if (schema.OneOf != null)
        {
            foreach (var child in schema.OneOf)
            {
                if (child != null)
                {
                    Clean(child, visited);
                }
            }
        }

        if (schema.AnyOf != null)
        {
            foreach (var child in schema.AnyOf)
            {
                if (child != null)
                {
                    Clean(child, visited);
                }
            }
        }

        // And recurse into normal properties/items
        if (schema.Properties != null)
        {
            foreach (var prop in schema.Properties.Values)
            {
                if (prop != null)
                {
                    Clean(prop, visited);
                }
            }
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

    // Replace your IsSchemaEmpty with this correct version.
    private bool IsSchemaEmpty(OpenApiSchema schema)
    {
        if (schema == null) return true;

        // A schema is NOT empty if it has ANY of these defining characteristics.
        bool hasContent = schema.Reference != null || // <-- The critical missing check
                          !string.IsNullOrWhiteSpace(schema.Type) || (schema.Properties?.Any() ?? false) || (schema.AllOf?.Any() ?? false) ||
                          (schema.OneOf?.Any() ?? false) || (schema.AnyOf?.Any() ?? false) || (schema.Enum?.Any() ?? false) || schema.Items != null ||
                          schema.AdditionalProperties != null || schema.AdditionalPropertiesAllowed;

        return !hasContent;
    }

    private void LogState(string stage, OpenApiDocument document)
    {
        if (!_logState)
        {
            return;
        }

        if (document.Components.Schemas.TryGetValue("CallRequest", out var schema) && schema.Properties.TryGetValue("to", out var voiceSettingsSchema))
        {
            int count = voiceSettingsSchema.OneOf?.Count ?? 0;
            _logger.LogWarning("DEBUG >>> STAGE: {Stage} -- voice_settings.oneOf count is: {Count}", stage, count);
        }
        else
        {
            _logger.LogWarning("DEBUG >>> STAGE: {Stage} -- AIAssistantStartRequest or voice_settings not found.", stage);
        }
    }

    private void DeepCleanSchema(OpenApiSchema? schema, HashSet<OpenApiSchema> visited)
    {
        if (schema == null || !visited.Add(schema))
        {
            return;
        }

        SanitizeExample(schema);

        // 1. Clean default: ""
        if (schema.Default is OpenApiString ds && string.IsNullOrEmpty(ds.Value))
        {
            schema.Default = null;
        }

        // 2. Clean example: ""
        if (schema.Example is OpenApiString es && string.IsNullOrEmpty(es.Value))
        {
            schema.Example = null;
        }

        // 3. Clean the Enum list of any empty or null strings.
        // This logic only runs if an Enum property already exists.
        if (schema.Enum != null && schema.Enum.Any())
        {
            var cleanedEnum = schema.Enum.OfType<OpenApiString>()
                                    .Where(s => !string.IsNullOrEmpty(s.Value))
                                    .Select(s => new OpenApiString(TrimQuotes(s.Value)))
                                    .Cast<IOpenApiAny>()
                                    .ToList();

            // If the enum list becomes empty after cleaning, set it to null.
            schema.Enum = cleanedEnum.Any() ? cleanedEnum : null;
        }

        // 4. Recurse into STANDARD nested schemas ONLY.
        // DO NOT recurse into allOf, oneOf, or anyOf.
        // The branches of those compositions are handled by the main loop in Fix().
        if (schema.Properties != null)
        {
            foreach (var p in schema.Properties.Values)
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

        var kept = list.Where(b => !isRedundant(b)).ToList();
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
            var schema = queue.Dequeue();
            if (schema == null || !visited.Add(schema))
                continue;

            // ---- clean compositions ----
            schema.OneOf = RemoveRedundantEmptyEnums(schema.OneOf, IsTrulyRedundantEmptyEnum);
            schema.AnyOf = RemoveRedundantEmptyEnums(schema.AnyOf, IsTrulyRedundantEmptyEnum);
            schema.AllOf = RemoveRedundantEmptyEnums(schema.AllOf, IsTrulyRedundantEmptyEnum);

            // ---- enqueue children ----
            if (schema.Properties != null)
                foreach (var p in schema.Properties.Values)
                    queue.Enqueue(p);
            if (schema.Items != null) queue.Enqueue(schema.Items);
            if (schema.AllOf != null)
                foreach (var b in schema.AllOf)
                    queue.Enqueue(b);
            if (schema.OneOf != null)
                foreach (var b in schema.OneOf)
                    queue.Enqueue(b);
            if (schema.AnyOf != null)
                foreach (var b in schema.AnyOf)
                    queue.Enqueue(b);
            if (schema.AdditionalProperties != null) queue.Enqueue(schema.AdditionalProperties);
        }
    }

    private MemoryStream PreprocessSpecFile(string path)
    {
        var raw = File.ReadAllText(path, Encoding.UTF8);

        // Your existing ref cleanup. This should run *after* the JSON syntax fix.
        raw = Regex.Replace(raw, @"\{\s*""\$ref""\s*:\s*""(?<id>[^""#/][^""]*)""\s*\}",
            m => $"{{ \"$ref\": \"#/components/schemas/{m.Groups["id"].Value}\" }}");

        return new MemoryStream(Encoding.UTF8.GetBytes(raw));
    }

    private void InlinePrimitiveComponents(OpenApiDocument document)
    {
        if (document.Components?.Schemas is not IDictionary<string, OpenApiSchema> comps)
            return;

        // 1) Find all schemas that are pure primitives (string/int/boolean/number) with no props/enum/oneOf/etc.
        List<string> primitives = comps.Where(kv =>
                                           !string.IsNullOrWhiteSpace(kv.Value.Type) &&
                                           (kv.Value.Type == "string" || kv.Value.Type == "integer" || kv.Value.Type == "boolean" ||
                                            kv.Value.Type == "number") &&
                                           (kv.Value.Properties == null || kv.Value.Properties.Count == 0) &&
                                           (kv.Value.Enum == null || kv.Value.Enum.Count == 0) &&
                                           (kv.Value.OneOf == null || kv.Value.OneOf.Count == 0) && (kv.Value.AnyOf == null || kv.Value.AnyOf.Count == 0) &&
                                           (kv.Value.AllOf == null || kv.Value.AllOf.Count == 0) && kv.Value.Items == null)
                                       .Select(kv => kv.Key)
                                       .ToList();

        if (!primitives.Any())
            return;

        foreach (string primKey in primitives)
        {
            OpenApiSchema primitiveSchema = comps[primKey];

            // Make a shallow "inline" copy of the primitive's constraints
            var inlineSchema = new OpenApiSchema
            {
                Type = primitiveSchema.Type,
                Format = primitiveSchema.Format,
                Description = primitiveSchema.Description,
                // Example = primitiveSchema.Example,
                MaxLength = primitiveSchema.MaxLength,
                Pattern = primitiveSchema.Pattern,
                Minimum = primitiveSchema.Minimum,
                Maximum = primitiveSchema.Maximum,
                // (copy any other primitive constraints you need)
            };

            // We use this HashSet to avoid revisiting the same OpenApiSchema node multiple times
            var visited = new HashSet<OpenApiSchema>();

            void ReplaceRef(OpenApiSchema? schema)
            {
                if (schema == null)
                    return;

                // If we've already been here, bail out immediately
                if (!visited.Add(schema))
                    return;

                // 1) If this node is a $ref pointing at primKey, inline it
                if (schema.Reference != null && schema.Reference.Type == ReferenceType.Schema && schema.Reference.Id == primKey)
                {
                    schema.Reference = null;
                    schema.Type = inlineSchema.Type;
                    schema.Format = inlineSchema.Format;
                    schema.Description = inlineSchema.Description;
                    //schema.Example = inlineSchema.Example;
                    schema.MaxLength = inlineSchema.MaxLength;
                    schema.Pattern = inlineSchema.Pattern;
                    schema.Minimum = inlineSchema.Minimum;
                    schema.Maximum = inlineSchema.Maximum;
                    // No further recursion needed here
                    return;
                }

                // 2) If it's a $ref to some OTHER component, fetch that component's schema and recurse
                if (schema.Reference != null && schema.Reference.Type == ReferenceType.Schema)
                {
                    string? targetId = schema.Reference.Id;
                    if (document.Components.Schemas.TryGetValue(targetId, out OpenApiSchema? targetSchema))
                    {
                        ReplaceRef(targetSchema);
                    }

                    return;
                }

                // 3) Otherwise, descend into children (anyOf/allOf/OneOf → Properties → Items → AdditionalProperties)
                if (schema.AllOf != null)
                    foreach (OpenApiSchema? child in schema.AllOf)
                        ReplaceRef(child);

                if (schema.OneOf != null)
                    foreach (OpenApiSchema? child in schema.OneOf)
                        ReplaceRef(child);

                if (schema.AnyOf != null)
                    foreach (OpenApiSchema? child in schema.AnyOf)
                        ReplaceRef(child);

                if (schema.Properties != null)
                    foreach (OpenApiSchema? prop in schema.Properties.Values)
                        ReplaceRef(prop);

                if (schema.Items != null)
                    ReplaceRef(schema.Items);

                if (schema.AdditionalProperties != null)
                    ReplaceRef(schema.AdditionalProperties);
            }

            // —— NEW: Walk every component schema itself first. 
            //          That way, references buried inside other component definitions get inlined:
            foreach (OpenApiSchema componentSchema in comps.Values.ToList())
            {
                ReplaceRef(componentSchema);
            }

            // 2a) Walk through Component RequestBody → Content → schema
            if (document.Components.RequestBodies != null)
                foreach (OpenApiRequestBody? rb in document.Components.RequestBodies.Values)
                foreach (OpenApiMediaType? mt in rb.Content.Values)
                    ReplaceRef(mt.Schema);

            // 2b) Walk through Component Response → Content → schema
            if (document.Components.Responses != null)
                foreach (OpenApiResponse? resp in document.Components.Responses.Values)
                foreach (OpenApiMediaType? mt in resp.Content.Values)
                    ReplaceRef(mt.Schema);

            // 2c) Walk through Component Parameter → schema
            if (document.Components.Parameters != null)
                foreach (OpenApiParameter? param in document.Components.Parameters.Values)
                    ReplaceRef(param.Schema);

            // 2d) Walk through Component Header → schema
            if (document.Components.Headers != null)
                foreach (OpenApiHeader? header in document.Components.Headers.Values)
                    ReplaceRef(header.Schema);

            // 3) Walk all Paths → Operations → (Parameters → schema), (RequestBody → Content → schema), (Responses → Content → schema)
            foreach (OpenApiPathItem? pathItem in document.Paths.Values)
            {
                foreach (OpenApiOperation? op in pathItem.Operations.Values)
                {
                    if (op.Parameters != null)
                    {
                        foreach (OpenApiParameter? p in op.Parameters)
                            ReplaceRef(p.Schema);
                    }

                    if (op.RequestBody?.Content != null)
                    {
                        foreach (OpenApiMediaType? mt in op.RequestBody.Content.Values)
                            ReplaceRef(mt.Schema);
                    }

                    foreach (OpenApiResponse? resp in op.Responses.Values)
                    {
                        if (resp.Content != null)
                        {
                            foreach (OpenApiMediaType? mt in resp.Content.Values)
                                ReplaceRef(mt.Schema);
                        }
                    }
                }
            }

            // Having inlined every reference to primKey, we can safely remove it from Components.Schemas
            comps.Remove(primKey);
        }
    }


    /// <summary>
    /// For every component schema that has an inline 'value' object,
    /// if there is a sibling schema named '{SchemaName}_value' that is
    /// actually an enum, replace the inline 'value' with a reference
    /// to that enum schema.
    /// </summary>
    private void FixAllInlineValueEnums(OpenApiDocument document)
    {
        IDictionary<string, OpenApiSchema>? comps = document.Components?.Schemas;
        if (comps == null) return;

        foreach (KeyValuePair<string, OpenApiSchema> kv in comps.ToList())
        {
            string key = kv.Key;
            OpenApiSchema schema = kv.Value;
            OpenApiSchema? wrapperSegment = null;

            // A) inline value property
            if (schema.Properties?.ContainsKey("value") == true)
                wrapperSegment = schema;
            // B) allOf wrapper
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
        if (document == null) return;
        if (document.Components?.Schemas == null) return;

        var comps = document.Components.Schemas;

        // Ensure each schema has a Title
        foreach (var kv in comps)
        {
            if (kv.Value == null) continue;
            if (string.IsNullOrWhiteSpace(kv.Value.Title))
            {
                kv.Value.Title = kv.Key;
            }
        }

        // Remove empty objects from composition arrays
        var visited = new HashSet<OpenApiSchema>();
        foreach (var schema in comps.Values)
        {
            if (schema == null) continue;
            RemoveEmptyCompositionObjects(schema, visited);
        }

        // Union types: explicit object
        foreach (var schema in comps.Values)
        {
            if (schema == null) continue;

            if (string.Equals(schema.Format, "datetime", StringComparison.OrdinalIgnoreCase))
                schema.Format = "date-time";

            if (string.Equals(schema.Format, "uuid4", StringComparison.OrdinalIgnoreCase))
                schema.Format = "uuid";

            bool hasComposition = (schema.OneOf?.Any() == true) || (schema.AnyOf?.Any() == true) || (schema.AllOf?.Any() == true);

            if (string.IsNullOrWhiteSpace(schema.Type) && hasComposition)
            {
                schema.Type = "object";
            }
        }

        // Add discriminator for oneOf unions
        foreach (var kv in comps.ToList())
        {
            if (kv.Value == null) continue;

            var schema = kv.Value;
            if (schema.OneOf?.Any() != true) continue;

            schema.Type = "object";
            const string discName = "type";
            schema.Discriminator ??= new OpenApiDiscriminator
            {
                PropertyName = discName,
                Mapping = new Dictionary<string, string>()
            };

            schema.Properties ??= new Dictionary<string, OpenApiSchema>();
            if (!schema.Properties.ContainsKey(discName))
            {
                schema.Properties[discName] = new OpenApiSchema
                {
                    Type = "string",
                    Title = discName,
                    Description = "Union discriminator"
                };
            }

            schema.Required ??= new HashSet<string>();
            if (!schema.Required.Contains(discName))
                schema.Required.Add(discName);

            foreach (var branch in schema.OneOf)
            {
                if (branch?.Reference?.Id == null) continue;
                schema.Discriminator.Mapping[branch.Reference.Id] = $"#/components/schemas/{branch.Reference.Id}";
            }
        }

        // Add discriminator for anyOf unions
        foreach (var kv in comps.ToList())
        {
            if (kv.Value == null) continue;

            var schema = kv.Value;
            if (schema.AnyOf?.Any() != true) continue;

            schema.Type = "object";
            const string discName = "type";
            schema.Discriminator ??= new OpenApiDiscriminator
            {
                PropertyName = discName,
                Mapping = new Dictionary<string, string>()
            };

            schema.Properties ??= new Dictionary<string, OpenApiSchema>();
            if (!schema.Properties.ContainsKey(discName))
            {
                schema.Properties[discName] = new OpenApiSchema
                {
                    Type = "string",
                    Title = discName,
                    Description = "Union discriminator"
                };
            }

            schema.Required ??= new HashSet<string>();
            if (!schema.Required.Contains(discName))
                schema.Required.Add(discName);

            foreach (var branch in schema.AnyOf)
            {
                if (branch?.Reference?.Id == null) continue;
                schema.Discriminator.Mapping[branch.Reference.Id] = $"#/components/schemas/{branch.Reference.Id}";
            }
        }

        // Schemas with properties or additionalProperties need explicit object type
        foreach (var schema in comps.Values)
        {
            if (schema == null) continue;
            bool hasProps = (schema.Properties?.Any() == true) || schema.AdditionalProperties != null || schema.AdditionalPropertiesAllowed;
            if (hasProps && string.IsNullOrWhiteSpace(schema.Type))
            {
                schema.Type = "object";
            }
        }

        // Process paths
        var validPaths = new OpenApiPaths();
        foreach (var path in document.Paths)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (path.Value?.Operations == null || !path.Value.Operations.Any())
                continue;

            foreach (var operation in path.Value.Operations)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (operation.Value == null) continue;

                var newResps = new OpenApiResponses();
                foreach (var resp in operation.Value.Responses)
                {
                    if (resp.Value == null) continue;

                    // *** THE CRITICAL FIX ***
                    // If the response itself is a reference, we just keep it and do not process its content here.
                    // The component it refers to will be processed when the component dictionary is processed.
                    if (resp.Value.Reference != null)
                    {
                        newResps[resp.Key] = resp.Value;
                        continue;
                    }

                    // --- The rest of the logic now only runs for INLINE responses ---

                    if (resp.Value.Content != null)
                    {
                        resp.Value.Content = resp.Value.Content.Where(p => p.Key != null && p.Value != null)
                                                 .ToDictionary(p => NormalizeMediaType(p.Key), p => p.Value);
                    }

                    ScrubBrokenRefs(resp.Value.Content, document); // Now safe, as it only scrubs inline content

                    if (resp.Value.Content != null)
                    {
                        var valid = resp.Value.Content.Where(p =>
                                        {
                                            if (p.Value == null) return false;
                                            var mt = p.Value;
                                            if (mt.Schema == null) return false;
                                            var sch = mt.Schema;
                                            // Use the corrected IsSchemaEmpty logic here to be safe
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

                // ================== START OF FIX ==================
                // RequestBody
                // ** CHECK IF THE REQUEST BODY IS A REFERENCE. IF IT IS, DO NOT TOUCH IT. **
                if (operation.Value.RequestBody != null && operation.Value.RequestBody.Reference == null)
                {
                    // This logic will now only run for truly inline request bodies,
                    // leaving referenced ones like AIAssistantStartRequest alone.
                    var rb = operation.Value.RequestBody;
                    if (rb.Content != null)
                    {
                        rb.Content = rb.Content.Where(p => p.Key != null && p.Value != null).ToDictionary(p => NormalizeMediaType(p.Key), p => p.Value);
                    }

                    ScrubBrokenRefs(rb.Content, document);
                    var validRb = rb.Content?.Where(p => p.Value?.Schema?.Reference != null || !IsMediaEmpty(p.Value)).ToDictionary(p => p.Key, p => p.Value);

                    operation.Value.RequestBody = (validRb != null && validRb.Any())
                        ? new OpenApiRequestBody {Description = rb.Description, Content = validRb}
                        : CreateFallbackRequestBody();
                }
                // =================== END OF FIX ===================
            }

            validPaths.Add(path.Key, path.Value);
        }

        document.Paths = validPaths;

        // Process remaining schemas
        foreach (var kv in comps)
        {
            if (kv.Value == null) continue;
            var schema = kv.Value;

            bool onlyHasRequired = schema.Type == "object" && (schema.Properties == null || !schema.Properties.Any()) && schema.Items == null &&
                                   (schema.AllOf?.Any() != true) && (schema.AnyOf?.Any() != true) && (schema.OneOf?.Any() != true) &&
                                   schema.AdditionalProperties == null && (schema.Required?.Any() == true);

            if (onlyHasRequired)
            {
                var reqs = schema.Required?.Where(r => !string.IsNullOrWhiteSpace(r)).ToList() ?? new List<string>();
                if (reqs.Any())
                {
                    schema.Properties = reqs.ToDictionary(name => name, _ => new OpenApiSchema {Type = "object"});
                }

                schema.AdditionalProperties = new OpenApiSchema {Type = "object"};
                schema.AdditionalPropertiesAllowed = true;
                continue;
            }

            bool isTrulyEmpty = schema.Type == "object" && (schema.Properties == null || !schema.Properties.Any()) && schema.Items == null &&
                                (schema.AllOf?.Any() != true) && (schema.AnyOf?.Any() != true) && (schema.OneOf?.Any() != true) &&
                                schema.AdditionalProperties == null;

            if (isTrulyEmpty)
            {
                schema.Properties = new Dictionary<string, OpenApiSchema>();
                schema.AdditionalProperties = new OpenApiSchema {Type = "object"};
                schema.AdditionalPropertiesAllowed = true;
                schema.Required = new HashSet<string>();
            }
        }

        // Process enum types
        foreach (var schema in comps.Values)
        {
            if (schema?.Enum == null || !schema.Enum.Any()) continue;
            if (schema.Enum.All(x => x is OpenApiString))
            {
                schema.Type = "string";
            }
        }

        // Process nullable types
        var visitedSchemas = new HashSet<OpenApiSchema>();
        foreach (var root in comps.Values)
        {
            if (root == null) continue;
            InjectTypeForNullable(root, visitedSchemas);
        }
    }

    private static OpenApiResponses CreateFallbackResponses(OperationType op)
    {
        var code = CanonicalSuccess(op);

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

    // *** MODIFIED: Added guard for empty component name ***
    private void AddComponentSchema(OpenApiDocument doc, string compName, OpenApiSchema schema)
    {
        if (string.IsNullOrWhiteSpace(compName))
        {
            _logger.LogWarning("Skipped adding a component schema because its generated name was empty.");
            return;
        }

        var validatedName = ValidateComponentName(compName);

        if (!doc.Components.Schemas.ContainsKey(validatedName))
        {
            if (string.IsNullOrWhiteSpace(schema.Title))
                schema.Title = validatedName;
            doc.Components.Schemas[validatedName] = schema;
        }
    }

    // *** NEW HELPER METHOD ***
    private string GenerateSafePart(string? input, string fallback = "unnamed")
    {
        if (string.IsNullOrWhiteSpace(input))
        {
            return fallback;
        }

        var sanitized = SanitizeName(input);
        return string.IsNullOrWhiteSpace(sanitized) ? fallback : sanitized;
    }

    private string ValidateComponentName(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            _logger.LogWarning("Component name was empty, using fallback name");
            return "UnnamedComponent";
        }

        // Remove any invalid characters that might cause issues
        var sanitized = Regex.Replace(name, @"[^a-zA-Z0-9_]", "_");

        // Ensure it starts with a letter
        if (!char.IsLetter(sanitized[0]))
        {
            sanitized = "C" + sanitized;
        }

        return sanitized;
    }

    // *** MODIFIED: Hardened name generation ***
    /// <summary>
    /// Walk every path/operation, pull inline request- and response-schemas up
    /// into <c>#/components/schemas</c>, and normalise FastAPI-style parameters
    /// that still use <c>"content": {"*/*": { "schema": …}}</c>.
    /// </summary>
    private void ExtractInlineSchemas(OpenApiDocument document, CancellationToken cancellationToken)
    {
        // Helper: treat simple data envelopes as “already okay” – don’t extract them.
        static bool IsSimpleEnvelope(OpenApiSchema s) =>
            s.Properties?.Count == 1 && s.Properties.TryGetValue("data", out var p) && p?.Reference != null && (s.Required == null || s.Required.Count <= 1);

        IDictionary<string, OpenApiSchema>? comps = document.Components?.Schemas;
        if (comps == null) return;

        foreach (OpenApiPathItem pathItem in document.Paths.Values)
        {
            cancellationToken.ThrowIfCancellationRequested();

            foreach ((OperationType opType, OpenApiOperation operation) in pathItem.Operations)
            {
                cancellationToken.ThrowIfCancellationRequested();
                if (operation == null) continue;

                // build a safe prefix for any component names we generate
                string safeOpId = ValidateComponentName(GenerateSafePart(operation.OperationId, opType.ToString()));

                // ─────────────────────────────────────────────────────────────
                // 1) NORMALISE PARAMETERS  (content → schema)
                // ─────────────────────────────────────────────────────────────
                if (operation.Parameters != null)
                {
                    foreach (var param in operation.Parameters.ToList())
                    {
                        if (param.Content?.Any() == true)
                        {
                            var first = param.Content.Values.FirstOrDefault();
                            if (first?.Schema != null)
                                param.Schema = first.Schema;

                            param.Content = null; // Kiota ignores it anyway
                        }
                    }
                }

                // ─────────────────────────────────────────────────────────────
                // 2) REQUEST BODIES
                // ─────────────────────────────────────────────────────────────
                // *** START OF FIX ***
                // Only process request bodies that are defined inline and are not references.
                if (operation.RequestBody != null && operation.RequestBody.Reference == null && operation.RequestBody.Content != null)
                {
                    foreach ((string mediaType, OpenApiMediaType media) in operation.RequestBody.Content.ToList())
                    {
                        OpenApiSchema? schema = media?.Schema;
                        if (schema == null || schema.Reference != null) continue;
                        if (IsSimpleEnvelope(schema)) continue;

                        string safeMedia;
                        var subtype = mediaType.Split(';')[0].Split('/').Last();
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
                // *** END OF FIX ***

                // ─────────────────────────────────────────────────────────────
                // 3) RESPONSES
                // ─────────────────────────────────────────────────────────────
                foreach ((string statusCode, OpenApiResponse response) in operation.Responses)
                {
                    // *** START OF FIX ***
                    // Only process responses that are defined inline and are not references.
                    if (response == null || response.Reference != null)
                    {
                        continue;
                    }
                    // *** END OF FIX ***

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

        // final fallback: add numeric counter
        int i = 2;
        string numbered;
        do
        {
            numbered = $"{withSuffix}_{i++}";
        } while (comps.ContainsKey(numbered));

        return numbered;
    }

    private static void EnsureUniqueOperationIds(OpenApiDocument doc)
    {
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (var path in doc.Paths.Values)
        foreach (var kvp in path.Operations)
        {
            var op = kvp.Value;
            if (string.IsNullOrWhiteSpace(op.OperationId))
                op.OperationId = $"{kvp.Key}{Guid.NewGuid():N}";

            string baseId = op.OperationId;
            string unique = baseId;
            int i = 1;
            while (!seen.Add(unique))
                unique = $"{baseId}_{i++}";

            op.OperationId = unique;
        }
    }

    /// <summary>
    /// Rewrite <c>$ref</c> IDs everywhere in the document according to
    /// <paramref name="mapping"/> (old → new).  Walks the whole tree so
    /// references buried in <c>schema.Properties</c>, <c>items</c>, etc.
    /// can’t be missed.
    /// </summary>
    private static void UpdateAllReferences(OpenApiDocument doc, Dictionary<string, string> mapping)
    {
        /* ------------------------------------------------------------ */
        void RewriteRef(OpenApiReference? r)
            /* ------------------------------------------------------------ */
        {
            if (r == null) return;
            if (r.Type != ReferenceType.Schema) return; // only rewrite schema refs
            if (string.IsNullOrEmpty(r.Id)) return;

            if (mapping.TryGetValue(r.Id, out var newId))
                r.Id = newId;
        }

        /* ------------------------------------------------------------ */
        var visited = new HashSet<OpenApiSchema>();

        void WalkSchema(OpenApiSchema? s)
            /* ------------------------------------------------------------ */
        {
            if (s == null || !visited.Add(s)) return;

            RewriteRef(s.Reference);

            if (s.Properties != null)
                foreach (var child in s.Properties.Values)
                    WalkSchema(child);

            if (s.Items != null) WalkSchema(s.Items);

            foreach (var a in s.AllOf ?? Enumerable.Empty<OpenApiSchema>()) WalkSchema(a);
            foreach (var o in s.OneOf ?? Enumerable.Empty<OpenApiSchema>()) WalkSchema(o);
            foreach (var a in s.AnyOf ?? Enumerable.Empty<OpenApiSchema>()) WalkSchema(a);

            if (s.AdditionalProperties != null)
                WalkSchema(s.AdditionalProperties);
        }

        /* === 1. Components =================================================== */

        if (doc.Components?.Schemas != null)
            foreach (var schema in doc.Components.Schemas.Values)
                WalkSchema(schema);

        if (doc.Components?.Parameters != null)
            foreach (var p in doc.Components.Parameters.Values)
            {
                RewriteRef(p.Reference);
                WalkSchema(p.Schema);
            }

        if (doc.Components?.Headers != null)
            foreach (var h in doc.Components.Headers.Values)
            {
                RewriteRef(h.Reference);
                WalkSchema(h.Schema);
            }

        if (doc.Components?.RequestBodies != null)
            foreach (var rb in doc.Components.RequestBodies.Values)
            {
                RewriteRef(rb.Reference);
                foreach (var mt in rb.Content.Values)
                    WalkSchema(mt.Schema);
            }

        if (doc.Components?.Responses != null)
            foreach (var resp in doc.Components.Responses.Values)
            {
                RewriteRef(resp.Reference);
                foreach (var mt in resp.Content.Values)
                    WalkSchema(mt.Schema);
            }

        /* === 2. Paths/Operations ============================================= */

        foreach (var path in doc.Paths.Values)
        foreach (var op in path.Operations.Values)
        {
            /* request body */
            RewriteRef(op.RequestBody?.Reference);
            if (op.RequestBody?.Content != null)
                foreach (var mt in op.RequestBody.Content.Values)
                    WalkSchema(mt.Schema);

            /* parameters */
            if (op.Parameters != null)
                foreach (var p in op.Parameters)
                {
                    RewriteRef(p.Reference);
                    WalkSchema(p.Schema);
                }

            /* responses */
            foreach (var resp in op.Responses.Values)
            {
                RewriteRef(resp.Reference);
                foreach (var mt in resp.Content.Values)
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

    //private static bool IsValidSchemaReference(OpenApiReference? reference, OpenApiDocument doc)
    //{
    //    if (reference == null || string.IsNullOrWhiteSpace(reference.Id)) return false;
    //    OpenApiComponents? comps = doc.Components;
    //    if (comps == null) return false;
    //    return reference.Type switch
    //    {
    //        ReferenceType.Schema => comps.Schemas?.ContainsKey(reference.Id) ?? false,
    //        ReferenceType.RequestBody => comps.RequestBodies?.ContainsKey(reference.Id) ?? false,
    //        ReferenceType.Response => comps.Responses?.ContainsKey(reference.Id) ?? false,
    //        ReferenceType.Parameter => comps.Parameters?.ContainsKey(reference.Id) ?? false,
    //        ReferenceType.Header => comps.Headers?.ContainsKey(reference.Id) ?? false,
    //        _ => false
    //    };
    //}

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
        // Check the correct dictionary based on the reference type
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
                return false; // Or true if you want to be lenient with unknown types
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

        // *** FIX: Create a new visited set for this self-contained operation. ***
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

            // Pass the visited set to the recursive scrubber
            ScrubAllRefs(schema, doc, visited);
        }
    }

    /// <summary>
    /// Recursively removes broken $ref links from a schema tree, without infinite recursion.
    /// </summary>
    private void ScrubAllRefs(OpenApiSchema? schema, OpenApiDocument doc, HashSet<OpenApiSchema> visited)
    {
        // *** FIX: Use the shared 'visited' set. No local 'Recurse' function needed. ***
        if (schema == null || !visited.Add(schema)) return;

        if (schema.Reference != null && !IsValidSchemaReference(schema.Reference, doc))
        {
            schema.Reference = null;
            _logger.LogWarning("Cleared nested broken ref for schema {Schema}", schema.Title ?? "(no title)");
        }

        // Recurse through all possible child schemas
        if (schema.AllOf != null)
            foreach (var s in schema.AllOf)
                ScrubAllRefs(s, doc, visited);

        if (schema.OneOf != null)
            foreach (var s in schema.OneOf)
                ScrubAllRefs(s, doc, visited);

        if (schema.AnyOf != null)
            foreach (var s in schema.AnyOf)
                ScrubAllRefs(s, doc, visited);

        if (schema.Properties != null)
            foreach (var p in schema.Properties.Values)
                ScrubAllRefs(p, doc, visited);

        if (schema.Items != null) ScrubAllRefs(schema.Items, doc, visited);

        if (schema.AdditionalProperties != null) ScrubAllRefs(schema.AdditionalProperties, doc, visited);
    }

    private void ScrubComponentRefs(OpenApiDocument doc, CancellationToken cancellationToken)
    {
        // *** FIX: Create a single, shared 'visited' set for the entire scrubbing operation. ***
        var visited = new HashSet<OpenApiSchema>();

        void PatchSchema(OpenApiSchema? sch)
        {
            // This is now a pass-through to the recursive scrubber
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

        // ScrubTopLevelComponentRefs is no longer needed as the above logic handles it.
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

            // TODO: Move this specific stuff out
            // Handle duplicate account_id parameter
            if (originalPath.Contains("/accounts/{account_id}/addressing/address_maps/{address_map_id}/accounts/{account_id}"))
            {
                newPath = originalPath.Replace("/accounts/{account_id}/addressing/address_maps/{address_map_id}/accounts/{account_id}",
                    "/accounts/{account_id}/addressing/address_maps/{address_map_id}/accounts/{member_account_id}");

                // Update the parameter name in the operations
                foreach (var operation in kvp.Value.Operations.Values)
                {
                    if (operation.Parameters == null)
                    {
                        operation.Parameters = new List<OpenApiParameter>();
                    }

                    // Ensure both account_id parameters are present
                    var hasAccountId = operation.Parameters.Any(p => p.Name == "account_id" && p.In == ParameterLocation.Path);
                    var hasMemberAccountId = operation.Parameters.Any(p => p.Name == "member_account_id" && p.In == ParameterLocation.Path);

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

                    // Update existing member_account_id parameter if it exists
                    foreach (var param in operation.Parameters)
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
                                         !s.AllOf.Any() // ← don't treat allOf children as "empty"
                                         && !s.AnyOf.Any() && !s.OneOf.Any());
        bool hasExample = s?.Example != null || (media.Examples?.Any() == true);
        return schemaEmpty && !hasExample;
    }

    private void EnsureResponseDescriptions(OpenApiResponses responses)
    {
        foreach (var kv in responses)
        {
            var code = kv.Key;
            var resp = kv.Value;
            if (string.IsNullOrWhiteSpace(resp.Description))
            {
                resp.Description = code == "default" ? "Default response" : $"{code} response";
            }
        }
    }

    /// <summary>
    /// Reads an OpenAPI document from the given stream and logs any parsing errors.
    /// </summary>
    /// <param name="filePath">The path to the OpenAPI spec file.</param>
    /// <returns>The diagnostic object containing any parsing errors or warnings.</returns>
    private async ValueTask<OpenApiDiagnostic> ReadAndValidateOpenApi(string filePath)
    {
        await using FileStream stream = File.OpenRead(filePath);

        var reader = new OpenApiStreamReader();
        var diagnostic = new OpenApiDiagnostic();
        OpenApiDocument? document = reader.Read(stream, out diagnostic);

        if (diagnostic.Errors?.Any() == true)
        {
            string msgs = string.Join("; ", diagnostic.Errors.Select(e => e.Message));
            _logger.LogWarning($"OpenAPI parsing errors in {Path.GetFileName(filePath)}: {msgs}");
        }

        return diagnostic;
    }

    private void EnsureSecuritySchemes(OpenApiDocument document)
    {
        if (document.Components == null)
            document.Components = new OpenApiComponents();

        var schemes = document.Components.SecuritySchemes ??= new Dictionary<string, OpenApiSecurityScheme>();

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

        foreach (var path in document.Paths.Values)
        {
            foreach (var op in path.Operations.Values)
            {
                if (op.Parameters == null) continue;

                var rogue = op.Parameters.FirstOrDefault(p =>
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
        foreach (var schema in document.Components.Schemas.Values)
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

        // Fix enum defaults
        if (schema.Enum != null && schema.Enum.Any())
        {
            var enumValues = schema.Enum.Select(e => e.ToString()).ToList();

            // If there's a default value that's not in the enum, fix it
            if (schema.Default != null)
            {
                var defaultValue = schema.Default.ToString();
                if (!enumValues.Contains(defaultValue))
                {
                    // Try to find a matching value case-insensitively
                    var matchingValue = enumValues.FirstOrDefault(v => v.Equals(defaultValue, StringComparison.OrdinalIgnoreCase));

                    if (matchingValue != null)
                    {
                        // Create a new OpenApiString with the correct case
                        schema.Default = new OpenApiString(matchingValue);
                    }
                    else
                    {
                        // If no match found, use the first enum value
                        schema.Default = schema.Enum.First();
                    }

                    _logger.LogWarning("Fixed invalid default value '{OldDefault}' to '{NewDefault}' in schema '{SchemaTitle}'", defaultValue, schema.Default,
                        schema.Title ?? "(no title)");
                }
            }
        }

        // Fix type-specific defaults
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
                        // Remove invalid date-time defaults
                        if (!DateTime.TryParse(dateStr.Value, out _))
                        {
                            schema.Default = null;
                        }
                    }

                    break;
            }
        }

        // Recurse into nested schemas
        if (schema.Properties != null)
            foreach (var prop in schema.Properties.Values)
                FixSchemaDefaults(prop, visited);

        if (schema.Items != null)
            FixSchemaDefaults(schema.Items, visited);

        if (schema.AdditionalProperties != null)
            FixSchemaDefaults(schema.AdditionalProperties, visited);

        if (schema.AllOf != null)
            foreach (var s in schema.AllOf)
                FixSchemaDefaults(s, visited);

        if (schema.OneOf != null)
            foreach (var s in schema.OneOf)
                FixSchemaDefaults(s, visited);

        if (schema.AnyOf != null)
            foreach (var s in schema.AnyOf)
                FixSchemaDefaults(s, visited);
    }

    private void RemoveEmptyCompositionObjects(OpenApiSchema schema, HashSet<OpenApiSchema> visited)
    {
        if (schema == null || !visited.Add(schema)) return;

        // Recurse into nested schemas first
        if (schema.Properties != null)
        {
            // First, deduplicate any keys if they somehow got duplicated
            schema.Properties = schema.Properties.GroupBy(p => p.Key).ToDictionary(g => g.Key, g => g.First().Value);

            foreach (var prop in schema.Properties.Values)
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

        // Now, process the composition arrays on the current schema
        // using the corrected IsSchemaEmpty logic.

        if (schema.AllOf != null)
        {
            // Keep only schemas that are not empty
            schema.AllOf = schema.AllOf.Where(s => s != null && !IsSchemaEmpty(s)).ToList();
            // If the list is now empty, null it out
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
            // Keep the first primitive if it matches the schema type; otherwise nuke it
            if (s.Type == "string" && arr.First() is OpenApiString os)
                s.Example = new OpenApiString(os.Value);
            else
                s.Example = null;
        }

        // Drop multi-KB blobs (e.g., huge JWT samples)
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

        // recurse
        if (schema.Properties != null)
            foreach (var prop in schema.Properties.Values)
                InjectTypeForNullable(prop, visited);

        if (schema.Items != null)
            InjectTypeForNullable(schema.Items, visited);

        if (schema.AdditionalProperties != null)
            InjectTypeForNullable(schema.AdditionalProperties, visited);

        if (schema.AllOf != null)
            foreach (var s in schema.AllOf)
                InjectTypeForNullable(s, visited);

        if (schema.OneOf != null)
            foreach (var s in schema.OneOf)
                InjectTypeForNullable(s, visited);

        if (schema.AnyOf != null)
            foreach (var s in schema.AnyOf)
                InjectTypeForNullable(s, visited);
    }

    private void ExtractInlineArrayItemSchemas(OpenApiDocument document)
    {
        if (document?.Components?.Schemas == null)
            return;

        var newSchemas = new Dictionary<string, OpenApiSchema>();
        int counter = 0;

        foreach (var (schemaName, schema) in document.Components.Schemas.ToList())
        {
            if (schema.Type != "array" || schema.Items == null || schema.Items.Reference != null)
                continue;

            var itemsSchema = schema.Items;

            // Only promote if it's a non-empty inline object
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

        foreach (var (key, val) in newSchemas)
        {
            document.Components.Schemas[key] = val;
        }
    }
}