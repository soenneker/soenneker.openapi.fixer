using Microsoft.OpenApi;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
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

        var counter = 0;

        foreach ((string? schemaName, IOpenApiSchema? schema) in document.Components.Schemas.ToList())
        {
            if (schema.Type != JsonSchemaType.Array || schema.Items == null || schema.Items is OpenApiSchemaReference)
                continue;

            IOpenApiSchema? itemsSchema = schema.Items;

            if (itemsSchema.Type != JsonSchemaType.Object || (itemsSchema.Properties == null || !itemsSchema.Properties.Any()))
                continue;

            string itemName = ReserveUniqueSchemaName(document.Components.Schemas, $"{schemaName} Item", $"Item{++counter}");

            if (schema is OpenApiSchema concreteSchema)
            {
                string finalItemName = itemName;

                if (itemsSchema is OpenApiSchema concreteItemsSchema)
                    finalItemName = AddComponentSchema(document, itemName, concreteItemsSchema);

                concreteSchema.Items = new OpenApiSchemaReference(finalItemName);
            }

            _logger.LogVerbose("Promoted inline array item schema from '{Parent}' to components schema '{ItemName}'", schemaName,
                itemName);
        }
    }

    private static bool TryGetSchemaRefId(IOpenApiSchema schema, out string? id)
    {
        id = null;

        switch (schema)
        {
            case OpenApiSchemaReference sr when string.IsNullOrEmpty(sr.Reference.ExternalResource) && sr.Reference.Id is { Length: > 0 }:
                id = sr.Reference.Id;
                return true;


        }

        return false;
    }

    private static void RemoveStringDefaultsFromEnumOrConstSchemas(OpenApiDocument document)
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

            bool hasStringEnumOrConst = concrete.Enum is { Count: > 0 } || concrete.Const is not null;

            if (hasStringEnumOrConst && concrete.Default is JsonValue defaultValue &&
                defaultValue.GetValueKind() == JsonValueKind.String)
            {
                concrete.Default = null;
            }

            if (concrete.Properties != null)
            {
                foreach (IOpenApiSchema propSchema in concrete.Properties.Values)
                {
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

    private static void RemoveStringDefaultsFromUuidSchemas(OpenApiDocument document)
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

            if (concrete.Type == JsonSchemaType.String &&
                (string.Equals(concrete.Format, "uuid", StringComparison.OrdinalIgnoreCase) ||
                 string.Equals(concrete.Format, "uuid4", StringComparison.OrdinalIgnoreCase)) && concrete.Default is JsonValue defaultValue &&
                defaultValue.GetValueKind() == JsonValueKind.String)
            {
                concrete.Default = null;
            }

            if (concrete.Properties != null)
            {
                foreach (IOpenApiSchema propSchema in concrete.Properties.Values)
                {
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

            if (concrete.Not != null)
                Visit(concrete.Not);
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
        if (HasExplicitNonObjectType(os))
            return true;

        // Enum-only or effectively-enum (no object properties)
        bool hasEnum = os.Enum is { Count: > 0 };
        bool isNotObject = !HasSchemaType(os, JsonSchemaType.Object);
        bool hasNoProps = os.Properties is null || os.Properties.Count == 0;
        if (hasEnum && (isNotObject || hasNoProps))
            return true;

        // Objects without properties but without enum can remain as objects
        return false;
    }

    private static bool HasSchemaType(OpenApiSchema schema, JsonSchemaType type)
    {
        return schema.Type?.HasFlag(type) == true;
    }

    private static bool IsObjectLikeSchema(OpenApiSchema schema)
    {
        return HasSchemaType(schema, JsonSchemaType.Object) || schema.Properties is { Count: > 0 } || schema.AdditionalProperties != null;
    }

    private static bool HasExplicitNonObjectType(OpenApiSchema schema)
    {
        if (schema.Type is null)
            return false;

        if (HasSchemaType(schema, JsonSchemaType.Object))
            return false;

        return HasSchemaType(schema, JsonSchemaType.Array) || HasSchemaType(schema, JsonSchemaType.String) || HasSchemaType(schema, JsonSchemaType.Integer) ||
               HasSchemaType(schema, JsonSchemaType.Number) || HasSchemaType(schema, JsonSchemaType.Boolean);
    }
    private static void PromoteNestedDiscriminatorUnions(OpenApiDocument doc)
    {
        if (doc.Components?.Schemas is not { Count: > 0 } components)
            return;

        foreach (IOpenApiSchema schema in components.Values)
        {
            if (schema is not OpenApiSchema parent || parent.Discriminator is null || parent.OneOf is { Count: > 0 } || parent.AnyOf is { Count: > 0 } ||
                parent.AllOf is not { Count: > 0 })
                continue;

            OpenApiSchema? unionWrapper = parent.AllOf.OfType<OpenApiSchema>()
                                                .FirstOrDefault(branch => branch.Properties is not { Count: > 0 } && branch.Items is null &&
                                                                          branch.AdditionalProperties is null &&
                                                                          (branch.OneOf is { Count: > 0 } || branch.AnyOf is { Count: > 0 }));

            if (unionWrapper is null)
                continue;

            parent.OneOf = unionWrapper.OneOf;
            parent.AnyOf = unionWrapper.AnyOf;
            parent.AllOf = parent.AllOf.Where(branch => !ReferenceEquals(branch, unionWrapper)).ToList();

            if (parent.AllOf.Count == 0)
                parent.AllOf = null;
        }
    }

    private static void RemoveDiscriminatorsFromNonObjectSchemas(OpenApiDocument doc)
    {
        IDictionary<string, IOpenApiSchema>? comps = doc.Components?.Schemas;
        if (comps is null || comps.Count == 0)
            return;

        var visited = new HashSet<IOpenApiSchema>(ReferenceEqualityComparer<IOpenApiSchema>.Instance);

        bool ShouldRemove(OpenApiSchema schema)
        {
            if (schema.Discriminator is null)
                return false;

            if (HasExplicitNonObjectType(schema))
                return true;

            IList<IOpenApiSchema>? branches = schema.OneOf ?? schema.AnyOf;
            return branches is { Count: > 0 } && (!HasObjectLikeBranch(branches, comps) || branches.Any(branch =>
            {
                if (GetSchemaRefId(branch) is string id && comps.TryGetValue(id, out IOpenApiSchema? target)) branch = target;
                return branch is OpenApiSchema value && HasExplicitNonObjectType(value);
            }));
        }

        static bool IsSyntheticDiscriminatorProperty(IOpenApiSchema property, string discriminatorPropertyName)
        {
            if (property is not OpenApiSchema propertySchema)
                return false;

            if (string.Equals(propertySchema.Description, "Union discriminator", StringComparison.Ordinal) ||
                string.Equals(propertySchema.Title, discriminatorPropertyName, StringComparison.OrdinalIgnoreCase))
                return true;

            bool stringOnly = HasSchemaType(propertySchema, JsonSchemaType.String) && propertySchema.Items is null && propertySchema.Properties is not { Count: > 0 } &&
                              propertySchema.AllOf is not { Count: > 0 } && propertySchema.AnyOf is not { Count: > 0 } &&
                              propertySchema.OneOf is not { Count: > 0 } && propertySchema.Enum is not { Count: > 0 };

            return stringOnly;
        }

        static void RemoveDiscriminator(OpenApiSchema schema)
        {
            string discriminatorPropertyName = schema.Discriminator?.PropertyName ?? "type";
            schema.Discriminator = null;

            if (schema.Required?.Remove(discriminatorPropertyName) == true && schema.Required.Count == 0)
                schema.Required = null;

            if (schema.Properties?.TryGetValue(discriminatorPropertyName, out IOpenApiSchema? property) == true &&
                IsSyntheticDiscriminatorProperty(property, discriminatorPropertyName))
            {
                schema.Properties.Remove(discriminatorPropertyName);
                if (schema.Properties.Count == 0)
                    schema.Properties = null;
            }
        }

        void Visit(IOpenApiSchema? schema)
        {
            if (schema is null || !visited.Add(schema))
                return;

            if (schema is OpenApiSchema concrete)
            {
                if (ShouldRemove(concrete))
                    RemoveDiscriminator(concrete);

                if (concrete.Properties != null)
                    foreach (IOpenApiSchema child in concrete.Properties.Values)
                        Visit(child);

                if (concrete.Items != null)
                    Visit(concrete.Items);

                if (concrete.AdditionalProperties != null)
                    Visit(concrete.AdditionalProperties);

                if (concrete.AllOf != null)
                    foreach (IOpenApiSchema child in concrete.AllOf)
                        Visit(child);

                if (concrete.AnyOf != null)
                    foreach (IOpenApiSchema child in concrete.AnyOf)
                        Visit(child);

                if (concrete.OneOf != null)
                    foreach (IOpenApiSchema child in concrete.OneOf)
                        Visit(child);
            }
        }

        foreach (IOpenApiSchema schema in comps.Values)
            Visit(schema);
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

}
