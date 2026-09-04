using Microsoft.Extensions.Logging;
using Microsoft.OpenApi;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Text.Json.Nodes;
using Soenneker.OpenApi.Fixer.Fixers.Abstract;

namespace Soenneker.OpenApi.Fixer.Fixers;

/// <summary>
/// Provides functionality to clean, transform, and fix OpenAPI schemas, including removing empty schemas,
/// fixing defaults, cleaning for serialization, and injecting types for nullable schemas.
/// </summary>
/// <inheritdoc cref="IOpenApiSchemaFixer" />
public sealed class OpenApiSchemaFixer : IOpenApiSchemaFixer
{
    private readonly ILogger<OpenApiSchemaFixer> _logger;

    public OpenApiSchemaFixer(ILogger<OpenApiSchemaFixer> logger)
    {
        _logger = logger;
    }

    public void RemoveEmptyInlineSchemas(OpenApiDocument document)
    {
        if (document.Components?.Schemas == null)
            return;

        var visited = new HashSet<OpenApiSchema>();

        foreach (IOpenApiSchema? schema in document.Components.Schemas.Values)
            if (schema is OpenApiSchema concreteSchema)
                Clean(concreteSchema, visited);
    }

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

    public bool IsSchemaEmpty(IOpenApiSchema schema)
    {
        if (schema == null)
            return true;

        bool hasContent = schema is OpenApiSchemaReference || schema.Type != null || (schema.Properties?.Any() ?? false) || (schema.AllOf?.Any() ?? false) ||
                          (schema.OneOf?.Any() ?? false) || (schema.AnyOf?.Any() ?? false) || (schema.Enum?.Any() ?? false) || schema.Items != null ||
                          schema.AdditionalProperties != null || schema.AdditionalPropertiesAllowed;

        return !hasContent;
    }

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

    public void FixInvalidDefaults(OpenApiDocument document)
    {
        if (document.Components?.Schemas == null)
            return;

        RecoverMisplacedArrayDefaults(document);

        var visited = new HashSet<IOpenApiSchema>();
        foreach (IOpenApiSchema? schema in document.Components.Schemas.Values)
        {
            FixSchemaDefaults(schema, visited);
        }
    }

    private void RecoverMisplacedArrayDefaults(OpenApiDocument document)
    {
        IDictionary<string, IOpenApiSchema> schemas = document.Components!.Schemas!;
        var visited = new HashSet<IOpenApiSchema>(ReferenceEqualityComparer.Instance);
        var promotedItemDefaults = new HashSet<OpenApiSchema>(ReferenceEqualityComparer.Instance);

        OpenApiSchema? Resolve(IOpenApiSchema? schema)
        {
            if (schema is OpenApiSchema concrete)
                return concrete;

            return schema is OpenApiSchemaReference reference && reference.Reference.Id is { } id && schemas.TryGetValue(id, out IOpenApiSchema? target)
                ? target as OpenApiSchema
                : null;
        }

        void Visit(IOpenApiSchema? schema)
        {
            if (schema is not OpenApiSchema concreteSchema || !visited.Add(schema))
                return;

            if (concreteSchema.Items != null && Resolve(concreteSchema.Items) is { } itemSchema)
            {
                if (concreteSchema.Default == null && itemSchema.Default is JsonArray misplacedDefault &&
                    !HasSchemaType(itemSchema, JsonSchemaType.Array) && misplacedDefault.All(value => IsDefaultValueValidForSchema(value, itemSchema)))
                {
                    concreteSchema.Default = misplacedDefault.DeepClone();
                    promotedItemDefaults.Add(itemSchema);
                    _logger.LogWarning("Moved array-shaped item default to its parent array schema '{SchemaTitle}'", concreteSchema.Title ?? "(no title)");
                }

                if (concreteSchema.Default is not null and not JsonArray && IsDefaultValueValidForSchema(concreteSchema.Default, itemSchema))
                {
                    concreteSchema.Default = new JsonArray(concreteSchema.Default.DeepClone());
                    _logger.LogWarning("Wrapped scalar default in an array on schema '{SchemaTitle}'", concreteSchema.Title ?? "(no title)");
                }
            }

            if (concreteSchema.Properties != null)
                foreach (IOpenApiSchema property in concreteSchema.Properties.Values)
                    Visit(property);
            Visit(concreteSchema.Items);
            Visit(concreteSchema.AdditionalProperties);

            if (concreteSchema.AllOf != null)
                foreach (IOpenApiSchema branch in concreteSchema.AllOf)
                    Visit(branch);
            if (concreteSchema.AnyOf != null)
                foreach (IOpenApiSchema branch in concreteSchema.AnyOf)
                    Visit(branch);
            if (concreteSchema.OneOf != null)
                foreach (IOpenApiSchema branch in concreteSchema.OneOf)
                    Visit(branch);
        }

        foreach (IOpenApiSchema schema in schemas.Values)
            Visit(schema);

        foreach (OpenApiSchema itemSchema in promotedItemDefaults)
            itemSchema.Default = null;
    }

    private static bool HasSchemaType(OpenApiSchema schema, JsonSchemaType type)
    {
        return schema.Type.HasValue && schema.Type.Value.HasFlag(type);
    }

    private static bool IsDefaultValueValidForSchema(JsonNode? value, OpenApiSchema schema)
    {
        if (schema.Enum is { Count: > 0 })
            return schema.Enum.Any(enumValue => JsonNode.DeepEquals(enumValue, value));

        if (value is null)
            return HasSchemaType(schema, JsonSchemaType.Null);

        JsonValueKind kind = value.GetValueKind();

        return kind switch
        {
            JsonValueKind.String => HasSchemaType(schema, JsonSchemaType.String),
            JsonValueKind.True or JsonValueKind.False => HasSchemaType(schema, JsonSchemaType.Boolean),
            JsonValueKind.Number => HasSchemaType(schema, JsonSchemaType.Integer) || HasSchemaType(schema, JsonSchemaType.Number),
            JsonValueKind.Array => HasSchemaType(schema, JsonSchemaType.Array),
            JsonValueKind.Object => HasSchemaType(schema, JsonSchemaType.Object),
            JsonValueKind.Null => HasSchemaType(schema, JsonSchemaType.Null),
            _ => false
        };
    }

    public void FixSchemaDefaults(IOpenApiSchema? schema, HashSet<IOpenApiSchema> visited)
    {
        if (schema == null || !visited.Add(schema))
            return;

        if (schema is not OpenApiSchema concreteSchema)
            return;

        // KIOTA SAFETY (based on observed Cloudflare schema):
        // Some schemas are "union/discriminator-shaped" but still declare type: string, e.g.
        //   type: "string"
        //   anyOf: [ { $ref: ...Wrapper } ]
        //   discriminator: { propertyName: "type" }
        //   default: "lite"
        //
        // Kiota generates these as model types (not strings), and then a scalar string default becomes invalid C#:
        //   Value = "lite";
        //
        // For discriminator/union schemas, drop scalar defaults to prevent broken generated code.
        if (concreteSchema.Default is JsonValue unionDefault &&
            unionDefault.GetValueKind() == JsonValueKind.String &&
            !(concreteSchema.Enum?.Any() ?? false) &&
            (concreteSchema.Discriminator != null ||
             (concreteSchema.AnyOf?.Any() ?? false) ||
             (concreteSchema.OneOf?.Any() ?? false) ||
             (concreteSchema.AllOf?.Any() ?? false)))
        {
            concreteSchema.Default = null;
        }

        // --- ENUM DEFAULTS: robust matching & assignment ---
        if (schema.Enum is { Count: > 0 })
        {
            var enumByText = new Dictionary<string, JsonNode?>(StringComparer.Ordinal);
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
                    else
                        enumByText[e.ToJsonString()] = e;
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
                    if (string.IsNullOrWhiteSpace(defText))
                    {
                        concreteSchema.Default = null;
                        _logger.LogWarning("Removed whitespace-only enum default on '{SchemaTitle}' because Kiota cannot emit a valid enum default reference", schema.Title ?? "(no title)");
                    }
                    else
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
                            _logger.LogWarning("Removed invalid enum default '{Default}' from '{SchemaTitle}' because it is not an enum member", schema.Default,
                                schema.Title ?? "(no title)");
                            concreteSchema.Default = null;
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

                    if (normalized.HasValue)
                        concreteSchema.Default = JsonValue.Create(normalized.Value);
                    else
                    {
                        _logger.LogWarning("Removed invalid boolean default '{Default}' from '{SchemaTitle}'", schema.Default,
                            schema.Title ?? "(no title)");
                        concreteSchema.Default = null;
                    }

                    break;
                }

                case JsonSchemaType.Array:
                    if (schema.Default is not JsonArray)
                    {
                        if (schema.Items is OpenApiSchema itemSchema && IsDefaultValueValidForSchema(schema.Default, itemSchema))
                        {
                            concreteSchema.Default = new JsonArray(schema.Default.DeepClone());
                            _logger.LogWarning("Wrapped scalar default in an array on schema '{SchemaTitle}'", schema.Title ?? "(no title)");
                        }
                        else
                        {
                            _logger.LogWarning("Removed invalid array default '{Default}' from '{SchemaTitle}'", schema.Default,
                                schema.Title ?? "(no title)");
                            concreteSchema.Default = null;
                        }
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

            if (s is OpenApiSchemaReference {Reference.Id: not null} r && document.Components?.Schemas != null &&
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
            foreach ((string pathKey, var pathItem) in document.Paths)
            {
                if (pathItem?.Operations == null)
                    continue;

                foreach ((HttpMethod method, var operation) in pathItem.Operations)
                {
                    // Request bodies
                    if (operation?.RequestBody?.Content != null)
                    {
                        foreach ((string mediaType, IOpenApiMediaType mediaInterface) in operation.RequestBody.Content)
                        {
                            if (mediaInterface is not OpenApiMediaType media)
                                continue;

                            Visit(media.Schema);
                        }
                    }

                    // Responses
                    if (operation?.Responses != null)
                    {
                        foreach ((string responseCode, var response) in operation.Responses)
                        {
                            if (response?.Content == null)
                                continue;

                            foreach ((string mediaType, IOpenApiMediaType mediaInterface) in response.Content)
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
                        foreach (IOpenApiParameter param in operation.Parameters)
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

    public void NormalizeNullablePrimitiveCompositions(OpenApiDocument document)
    {
        if (document == null)
            return;

        var visited = new HashSet<IOpenApiSchema>();
        var normalized = 0;

        void Visit(IOpenApiSchema? schema)
        {
            if (schema is not OpenApiSchema concreteSchema || !visited.Add(concreteSchema))
                return;

            NormalizeComposition(concreteSchema.AnyOf, branches => concreteSchema.AnyOf = branches, concreteSchema, ref normalized);
            NormalizeComposition(concreteSchema.OneOf, branches => concreteSchema.OneOf = branches, concreteSchema, ref normalized);

            if (concreteSchema.Properties != null)
                foreach (IOpenApiSchema property in concreteSchema.Properties.Values)
                    Visit(property);

            if (concreteSchema.Items != null)
                Visit(concreteSchema.Items);

            if (concreteSchema.AdditionalProperties != null)
                Visit(concreteSchema.AdditionalProperties);

            if (concreteSchema.AllOf != null)
                foreach (IOpenApiSchema branch in concreteSchema.AllOf)
                    Visit(branch);

            if (concreteSchema.AnyOf != null)
                foreach (IOpenApiSchema branch in concreteSchema.AnyOf)
                    Visit(branch);

            if (concreteSchema.OneOf != null)
                foreach (IOpenApiSchema branch in concreteSchema.OneOf)
                    Visit(branch);
        }

        if (document.Components?.Schemas != null)
            foreach (IOpenApiSchema schema in document.Components.Schemas.Values)
                Visit(schema);

        if (document.Paths != null)
        {
            foreach (IOpenApiPathItem pathItem in document.Paths.Values)
            {
                if (pathItem?.Parameters != null)
                    foreach (IOpenApiParameter parameter in pathItem.Parameters)
                        if (parameter is OpenApiParameter concreteParameter)
                            Visit(concreteParameter.Schema);

                if (pathItem?.Operations == null)
                    continue;

                foreach (OpenApiOperation operation in pathItem.Operations.Values)
                {
                    if (operation?.Parameters != null)
                        foreach (IOpenApiParameter parameter in operation.Parameters)
                            if (parameter is OpenApiParameter concreteParameter)
                                Visit(concreteParameter.Schema);

                    if (operation?.RequestBody?.Content != null)
                        foreach (IOpenApiMediaType mediaType in operation.RequestBody.Content.Values)
                            if (mediaType is OpenApiMediaType concreteMediaType)
                                Visit(concreteMediaType.Schema);

                    if (operation?.Responses == null)
                        continue;

                    foreach (IOpenApiResponse response in operation.Responses.Values)
                    {
                        if (response?.Content != null)
                            foreach (IOpenApiMediaType mediaType in response.Content.Values)
                                if (mediaType is OpenApiMediaType concreteMediaType)
                                    Visit(concreteMediaType.Schema);

                        if (response?.Headers != null)
                            foreach (IOpenApiHeader header in response.Headers.Values)
                                if (header is OpenApiHeader concreteHeader)
                                    Visit(concreteHeader.Schema);
                    }
                }
            }
        }

        if (normalized > 0)
            _logger.LogInformation("Normalized {Count} nullable primitive, array, or object-like anyOf/oneOf schemas", normalized);
    }

    private static void NormalizeComposition(IList<IOpenApiSchema>? branches, Action<List<IOpenApiSchema>?> assignBranches, OpenApiSchema target, ref int normalized)
    {
        if (branches is not { Count: 2 })
            return;

        OpenApiSchema? valueBranch = null;
        var hasNullBranch = false;

        foreach (IOpenApiSchema branch in branches)
        {
            if (branch is not OpenApiSchema concreteBranch)
                return;

            JsonSchemaType? branchType = concreteBranch.Type;

            if (branchType.HasValue && branchType.Value == JsonSchemaType.Null && IsSimpleBranch(concreteBranch))
            {
                hasNullBranch = true;
                continue;
            }

            if (IsCollapsibleValueSchema(concreteBranch) || IsObjectLikeComposition(concreteBranch))
            {
                valueBranch = concreteBranch;
                continue;
            }

            return;
        }

        if (!hasNullBranch || valueBranch == null)
            return;

        target.Type = (valueBranch.Type ?? JsonSchemaType.Object) | JsonSchemaType.Null;
        target.Format = valueBranch.Format;
        target.Pattern = valueBranch.Pattern;
        target.MinLength = valueBranch.MinLength;
        target.MaxLength = valueBranch.MaxLength;
        target.Minimum = valueBranch.Minimum;
        target.Maximum = valueBranch.Maximum;
        target.ExclusiveMinimum = valueBranch.ExclusiveMinimum;
        target.ExclusiveMaximum = valueBranch.ExclusiveMaximum;
        target.MultipleOf = valueBranch.MultipleOf;
        target.Enum = valueBranch.Enum;
        target.Properties = valueBranch.Properties;
        target.Required = valueBranch.Required;
        target.Items = valueBranch.Items;
        target.MinItems = valueBranch.MinItems;
        target.MaxItems = valueBranch.MaxItems;
        target.UniqueItems = valueBranch.UniqueItems;
        target.AdditionalProperties = valueBranch.AdditionalProperties;
        target.AdditionalPropertiesAllowed = valueBranch.AdditionalPropertiesAllowed;
        target.AllOf = valueBranch.AllOf;
        target.Default ??= valueBranch.Default;
        target.Example ??= valueBranch.Example;
        target.Description ??= valueBranch.Description;

        IList<IOpenApiSchema>? nestedAnyOf = valueBranch.AnyOf;
        IList<IOpenApiSchema>? nestedOneOf = valueBranch.OneOf;
        assignBranches(null);
        target.AnyOf = nestedAnyOf;
        target.OneOf = nestedOneOf;
        normalized++;
    }

    private static bool IsObjectLikeComposition(OpenApiSchema schema)
    {
        if (schema.Type.HasValue && schema.Type.Value.HasFlag(JsonSchemaType.Object))
            return true;

        if (schema.Properties is { Count: > 0 })
            return true;

        return ContainsObjectLikeBranch(schema.AllOf) || ContainsObjectLikeBranch(schema.AnyOf) || ContainsObjectLikeBranch(schema.OneOf);
    }

    private static bool ContainsObjectLikeBranch(IList<IOpenApiSchema>? branches)
    {
        if (branches == null)
            return false;

        foreach (IOpenApiSchema branch in branches)
        {
            if (branch is OpenApiSchema concreteBranch && IsObjectLikeComposition(concreteBranch))
                return true;
        }

        return false;
    }

    private static bool IsCollapsibleValueSchema(OpenApiSchema schema)
    {
        if (!schema.Type.HasValue)
            return false;

        JsonSchemaType type = schema.Type.Value;

        if (type == JsonSchemaType.Array)
        {
            return schema.Items != null &&
                   (schema.Properties?.Count ?? 0) == 0 &&
                   schema.AdditionalProperties == null &&
                   (schema.AllOf?.Count ?? 0) == 0 &&
                   (schema.AnyOf?.Count ?? 0) == 0 &&
                   (schema.OneOf?.Count ?? 0) == 0 &&
                   schema.Discriminator == null;
        }

        return (type == JsonSchemaType.String || type == JsonSchemaType.Integer || type == JsonSchemaType.Number || type == JsonSchemaType.Boolean) &&
               IsSimpleBranch(schema);
    }

    private static bool IsSimpleBranch(OpenApiSchema schema)
    {
        return (schema.Properties?.Count ?? 0) == 0 &&
               schema.Items == null &&
               schema.AdditionalProperties == null &&
               (schema.AllOf?.Count ?? 0) == 0 &&
               (schema.AnyOf?.Count ?? 0) == 0 &&
               (schema.OneOf?.Count ?? 0) == 0 &&
               schema.Discriminator == null;
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
