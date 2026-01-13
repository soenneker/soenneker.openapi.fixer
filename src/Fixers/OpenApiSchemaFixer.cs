using Microsoft.Extensions.Logging;
using Microsoft.OpenApi;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Nodes;
using Soenneker.OpenApi.Fixer.Fixers.Abstract;

namespace Soenneker.OpenApi.Fixer.Fixers;

/// <summary>
/// Provides functionality to clean, transform, and fix OpenAPI schemas, including removing empty schemas,
/// fixing defaults, cleaning for serialization, and injecting types for nullable schemas.
/// </summary>
public sealed class OpenApiSchemaFixer : IOpenApiSchemaFixer
{
    private readonly ILogger<OpenApiSchemaFixer> _logger;

    public OpenApiSchemaFixer(ILogger<OpenApiSchemaFixer> logger)
    {
        _logger = logger;
    }

    /// <inheritdoc />
    public void RemoveEmptyInlineSchemas(OpenApiDocument document)
    {
        if (document.Components?.Schemas == null)
            return;

        var visited = new HashSet<OpenApiSchema>();
        foreach (IOpenApiSchema? schema in document.Components.Schemas.Values)
            if (schema is OpenApiSchema concreteSchema)
                Clean(concreteSchema, visited);
    }

    /// <inheritdoc />
    public void Clean(OpenApiSchema schema, HashSet<OpenApiSchema> visited)
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

    /// <inheritdoc />
    public bool IsSchemaEmpty(IOpenApiSchema schema)
    {
        if (schema == null)
            return true;

        bool hasContent = schema is OpenApiSchemaReference || schema.Type != null || (schema.Properties?.Any() ?? false) || (schema.AllOf?.Any() ?? false) ||
                          (schema.OneOf?.Any() ?? false) || (schema.AnyOf?.Any() ?? false) || (schema.Enum?.Any() ?? false) || schema.Items != null ||
                          schema.AdditionalProperties != null || schema.AdditionalPropertiesAllowed;

        return !hasContent;
    }

    /// <inheritdoc />
    public void DeepCleanSchema(OpenApiSchema? schema, HashSet<OpenApiSchema> visited)
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

    /// <inheritdoc />
    public void CleanDocumentForSerialization(OpenApiDocument document)
    {
        if (document.Components?.Schemas == null)
            return;

        var visited = new HashSet<IOpenApiSchema>();
        foreach (IOpenApiSchema? schema in document.Components.Schemas.Values)
        {
            CleanSchemaForSerialization(schema, visited);
        }
    }

    /// <inheritdoc />
    public void CleanSchemaForSerialization(IOpenApiSchema? schema, HashSet<IOpenApiSchema> visited)
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

    /// <inheritdoc />
    public void FixInvalidDefaults(OpenApiDocument document)
    {
        if (document.Components?.Schemas == null)
            return;

        var visited = new HashSet<IOpenApiSchema>();
        foreach (IOpenApiSchema? schema in document.Components.Schemas.Values)
        {
            FixSchemaDefaults(schema, visited);
        }
    }

    /// <inheritdoc />
    public void FixSchemaDefaults(IOpenApiSchema? schema, HashSet<IOpenApiSchema> visited)
    {
        if (schema == null || !visited.Add(schema))
            return;

        // IMPORTANT:
        // Defaults can be present on $ref/composed schemas (type is often null). Generators (e.g. Kiota C#)
        // may emit invalid assignments like: Value = "lite"; for non-string model types.
        // We must visit and potentially clear defaults even when the schema is a reference schema.
        if (schema is OpenApiSchemaReference refSchema)
        {
            if (refSchema.Default is JsonValue dv && dv.GetValueKind() == JsonValueKind.String &&
                refSchema.Type == null && !(refSchema.Enum?.Any() ?? false))
            {
                refSchema.Default = null;
            }

            // No further traversal is typically needed for pure refs, but return defensively.
            return;
        }

        if (schema is not OpenApiSchema concreteSchema)
        {
            // Unknown schema implementation; still attempt to traverse children best-effort.
            if (schema.Properties != null)
                foreach (IOpenApiSchema? prop in schema.Properties.Values)
                    FixSchemaDefaults(prop, visited);

            if (schema.Items != null)
                FixSchemaDefaults(schema.Items, visited);

            if (schema.AdditionalProperties != null)
                FixSchemaDefaults(schema.AdditionalProperties, visited);

            if (schema.AllOf != null)
                foreach (IOpenApiSchema? s in schema.AllOf)
                    FixSchemaDefaults(s, visited);

            if (schema.OneOf != null)
                foreach (IOpenApiSchema? s in schema.OneOf)
                    FixSchemaDefaults(s, visited);

            if (schema.AnyOf != null)
                foreach (IOpenApiSchema? s in schema.AnyOf)
                    FixSchemaDefaults(s, visited);

            return;
        }

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

            // Extra safety: if the schema is typeless but looks like a $ref/composed schema, a string default is
            // almost certainly unsafe for code-gen (it can't infer conversion).
            if (schema.Default is JsonValue sVal2 && sVal2.GetValueKind() == JsonValueKind.String &&
                schema.Type == null && !(schema.Enum?.Any() ?? false) &&
                ((schema.AllOf?.Any() ?? false) || (schema.OneOf?.Any() ?? false) || (schema.AnyOf?.Any() ?? false)))
            {
                concreteSchema.Default = null;
            }
        }

        // Recurse through nested schemas (including $ref nodes) so we can clear bad defaults wherever they appear.
        if (schema.Properties != null)
            foreach (IOpenApiSchema? prop in schema.Properties.Values)
                FixSchemaDefaults(prop, visited);

        if (schema.Items != null)
            FixSchemaDefaults(schema.Items, visited);

        if (schema.AdditionalProperties != null)
            FixSchemaDefaults(schema.AdditionalProperties, visited);

        if (schema.AllOf != null)
            foreach (IOpenApiSchema? s in schema.AllOf)
                FixSchemaDefaults(s, visited);

        if (schema.OneOf != null)
            foreach (IOpenApiSchema? s in schema.OneOf)
                FixSchemaDefaults(s, visited);

        if (schema.AnyOf != null)
            foreach (IOpenApiSchema? s in schema.AnyOf)
                FixSchemaDefaults(s, visited);
    }

    /// <inheritdoc />
    public void RemoveInvalidDefaults(OpenApiDocument document)
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

    /// <inheritdoc />
    public void RemoveEmptyCompositionObjects(OpenApiSchema schema, HashSet<OpenApiSchema> visited)
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

    /// <inheritdoc />
    public void InjectTypeForNullable(OpenApiSchema schema, HashSet<OpenApiSchema> visited)
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

    /// <inheritdoc />
    public void DeduplicateCompositionBranches(OpenApiDocument document)
    {
        if (document == null)
            return;

        int removed = 0;
        var visited = new HashSet<OpenApiSchema>();

        OpenApiSchema? Resolve(IOpenApiSchema? s)
        {
            if (s == null)
                return null;

            if (s is OpenApiSchema os)
                return os;

            if (s is OpenApiSchemaReference r && document.Components?.Schemas != null &&
                document.Components.Schemas.TryGetValue(r.Reference.Id, out IOpenApiSchema? target) &&
                target is OpenApiSchema targetSchema)
                return targetSchema;

            return null;
        }

        void Visit(IOpenApiSchema? s)
        {
            OpenApiSchema? os = Resolve(s);
            if (os != null)
                DeduplicateCompositionBranches(os, visited, ref removed);
        }

        // Components/schemas
        if (document.Components?.Schemas != null)
        {
            foreach (IOpenApiSchema schema in document.Components.Schemas.Values)
                Visit(schema);
        }

        // Paths/operations
        if (document.Paths != null)
        {
            foreach (var (pathKey, pathItem) in document.Paths)
            {
                if (pathItem?.Operations == null)
                    continue;

                foreach (var (method, operation) in pathItem.Operations)
                {
                    // Request bodies
                    if (operation?.RequestBody?.Content != null)
                    {
                        foreach (var (mediaType, mediaInterface) in operation.RequestBody.Content)
                        {
                            if (mediaInterface is not OpenApiMediaType media)
                                continue;

                            Visit(media.Schema);
                        }
                    }

                    // Responses
                    if (operation?.Responses != null)
                    {
                        foreach (var (responseCode, response) in operation.Responses)
                        {
                            if (response?.Content == null)
                                continue;

                            foreach (var (mediaType, mediaInterface) in response.Content)
                            {
                                if (mediaInterface is not OpenApiMediaType media)
                                    continue;

                                Visit(media.Schema);
                            }
                        }
                    }

                    // Parameters
                    if (operation?.Parameters != null)
                    {
                        foreach (var param in operation.Parameters)
                        {
                            if (param is OpenApiParameter concreteParam)
                                Visit(concreteParam.Schema);
                        }
                    }

                    _ = method;
                    _ = pathKey;
                }
            }
        }

        // Components: requestBodies/responses/parameters/headers
        if (document.Components != null)
        {
            if (document.Components.RequestBodies != null)
            {
                foreach (IOpenApiRequestBody rb in document.Components.RequestBodies.Values)
                {
                    if (rb?.Content == null)
                        continue;

                    foreach (IOpenApiMediaType mt in rb.Content.Values)
                    {
                        if (mt is OpenApiMediaType concreteMt)
                            Visit(concreteMt.Schema);
                    }
                }
            }

            if (document.Components.Responses != null)
            {
                foreach (IOpenApiResponse resp in document.Components.Responses.Values)
                {
                    if (resp?.Content == null)
                        continue;

                    foreach (IOpenApiMediaType mt in resp.Content.Values)
                    {
                        if (mt is OpenApiMediaType concreteMt)
                            Visit(concreteMt.Schema);
                    }
                }
            }

            if (document.Components.Parameters != null)
            {
                foreach (IOpenApiParameter p in document.Components.Parameters.Values)
                {
                    if (p is OpenApiParameter concreteP)
                        Visit(concreteP.Schema);
                }
            }

            if (document.Components.Headers != null)
            {
                foreach (IOpenApiHeader h in document.Components.Headers.Values)
                {
                    if (h is OpenApiHeader concreteH)
                        Visit(concreteH.Schema);
                }
            }
        }

        if (removed > 0)
            _logger.LogInformation("Deduplicated {Count} duplicate composition branches (anyOf/oneOf/allOf) across the document", removed);
    }

    private static string? GetRefKey(IOpenApiSchema schema)
    {
        if (schema is OpenApiSchemaReference r)
            return r.Reference?.ReferenceV3 ?? r.Reference?.Id;

        return null;
    }

    private static List<IOpenApiSchema> DedupByRef(IList<IOpenApiSchema> list, ref int removed)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var result = new List<IOpenApiSchema>(list.Count);

        foreach (IOpenApiSchema? item in list)
        {
            if (item == null)
                continue;

            string? key = GetRefKey(item);
            if (key != null)
            {
                if (!seen.Add(key))
                {
                    removed++;
                    continue;
                }
            }

            result.Add(item);
        }

        return result;
    }

    private static void DeduplicateCompositionBranches(OpenApiSchema schema, HashSet<OpenApiSchema> visited, ref int removed)
    {
        if (schema == null || !visited.Add(schema))
            return;

        if (schema.AllOf is { Count: > 1 })
            schema.AllOf = DedupByRef(schema.AllOf, ref removed);

        if (schema.OneOf is { Count: > 1 })
            schema.OneOf = DedupByRef(schema.OneOf, ref removed);

        if (schema.AnyOf is { Count: > 1 })
            schema.AnyOf = DedupByRef(schema.AnyOf, ref removed);

        if (schema.Properties != null)
        {
            foreach (IOpenApiSchema? prop in schema.Properties.Values)
                if (prop is OpenApiSchema concreteProp)
                    DeduplicateCompositionBranches(concreteProp, visited, ref removed);
        }

        if (schema.Items is OpenApiSchema concreteItems)
            DeduplicateCompositionBranches(concreteItems, visited, ref removed);

        if (schema.AdditionalProperties is OpenApiSchema concreteAdditional)
            DeduplicateCompositionBranches(concreteAdditional, visited, ref removed);

        if (schema.AllOf != null)
        {
            foreach (IOpenApiSchema? child in schema.AllOf)
                if (child is OpenApiSchema concreteChild)
                    DeduplicateCompositionBranches(concreteChild, visited, ref removed);
        }

        if (schema.OneOf != null)
        {
            foreach (IOpenApiSchema? child in schema.OneOf)
                if (child is OpenApiSchema concreteChild)
                    DeduplicateCompositionBranches(concreteChild, visited, ref removed);
        }

        if (schema.AnyOf != null)
        {
            foreach (IOpenApiSchema? child in schema.AnyOf)
                if (child is OpenApiSchema concreteChild)
                    DeduplicateCompositionBranches(concreteChild, visited, ref removed);
        }
    }
}

