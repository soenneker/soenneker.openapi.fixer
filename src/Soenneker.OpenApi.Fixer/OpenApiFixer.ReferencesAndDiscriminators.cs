using Microsoft.Extensions.Logging;
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

            _logger.LogInformation("Promoted inline array item schema from '{Parent}' to components schema '{ItemName}'", schemaName,
                itemName);
        }
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

    private static void CollapseNonDiscriminatedInlineObjectUnions(OpenApiDocument doc)
    {
        IDictionary<string, IOpenApiSchema>? comps = doc.Components?.Schemas;
        if (comps is null || comps.Count == 0)
            return;

        var visited = new HashSet<IOpenApiSchema>(ReferenceEqualityComparer<IOpenApiSchema>.Instance);

        static IList<IOpenApiSchema>? GetUnionBranches(OpenApiSchema schema)
        {
            if (schema.OneOf is { Count: > 0 })
                return schema.OneOf;
            if (schema.AnyOf is { Count: > 0 })
                return schema.AnyOf;
            return null;
        }

        static bool IsInlinePlainObjectBranch(IOpenApiSchema branch)
        {
            return branch is OpenApiSchema schema && schema.Discriminator is null && schema.Properties is { Count: > 0 } &&
                   (schema.Type is null || schema.Type.Value.HasFlag(JsonSchemaType.Object));
        }

        static HashSet<string> RequiredSet(OpenApiSchema schema)
        {
            return schema.Required is { Count: > 0 } ? new HashSet<string>(schema.Required, StringComparer.Ordinal) : new HashSet<string>(StringComparer.Ordinal);
        }

        static void MergeEnumProperty(OpenApiSchema existing, OpenApiSchema incoming)
        {
            if (incoming.Enum is not { Count: > 0 })
                return;

            existing.Enum ??= [];

            foreach (JsonNode? value in incoming.Enum)
            {
                if (value is null)
                    continue;

                string valueText = value.ToJsonString();
                bool exists = existing.Enum.Any(existingValue => existingValue?.ToJsonString() == valueText);
                if (!exists)
                    existing.Enum.Add(value.DeepClone());
            }

            if (existing.Type is null && incoming.Type is not null)
                existing.Type = incoming.Type;
        }

        static void MergeProperty(IDictionary<string, IOpenApiSchema> mergedProperties, string propertyName, IOpenApiSchema property)
        {
            if (!mergedProperties.TryGetValue(propertyName, out IOpenApiSchema? existingProperty))
            {
                mergedProperties[propertyName] = property;
                return;
            }

            if (existingProperty is OpenApiSchema existingSchema && property is OpenApiSchema incomingSchema &&
                existingSchema.Enum is { Count: > 0 } && incomingSchema.Enum is { Count: > 0 })
            {
                MergeEnumProperty(existingSchema, incomingSchema);
            }
        }

        static void Collapse(OpenApiSchema schema, IList<IOpenApiSchema> branches)
        {
            var mergedProperties = new Dictionary<string, IOpenApiSchema>(StringComparer.Ordinal);
            HashSet<string>? required = null;

            foreach (IOpenApiSchema branch in branches)
            {
                var branchSchema = (OpenApiSchema)branch;

                foreach ((string propertyName, IOpenApiSchema property) in branchSchema.Properties!)
                    MergeProperty(mergedProperties, propertyName, property);

                HashSet<string> branchRequired = RequiredSet(branchSchema);
                if (required is null)
                    required = branchRequired;
                else
                    required.IntersectWith(branchRequired);
            }

            schema.Type = JsonSchemaType.Object;
            schema.Properties = mergedProperties;
            schema.Required = required is { Count: > 0 } ? required : null;

            if (ReferenceEquals(schema.OneOf, branches))
                schema.OneOf = null;
            if (ReferenceEquals(schema.AnyOf, branches))
                schema.AnyOf = null;
        }

        void Visit(IOpenApiSchema? schema)
        {
            if (schema is null || !visited.Add(schema))
                return;

            if (schema is not OpenApiSchema concrete)
                return;

            if (concrete.Discriminator is null && concrete.Properties is not { Count: > 0 })
            {
                IList<IOpenApiSchema>? branches = GetUnionBranches(concrete);
                if (branches is { Count: > 1 } && branches.All(IsInlinePlainObjectBranch))
                    Collapse(concrete, branches);
            }

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

        foreach (IOpenApiSchema schema in comps.Values)
            Visit(schema);
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

    private static void ExposeComposedObjectPropertiesForGenerators(OpenApiDocument doc)
    {
        IDictionary<string, IOpenApiSchema>? components = doc.Components?.Schemas;
        if (components is null || components.Count == 0)
            return;

        static bool AreCompatible(IOpenApiSchema left, IOpenApiSchema right)
        {
            string? leftRef = GetSchemaRefId(left);
            string? rightRef = GetSchemaRefId(right);

            if (leftRef is not null || rightRef is not null)
                return string.Equals(leftRef, rightRef, StringComparison.Ordinal);

            if (left is not OpenApiSchema leftSchema || right is not OpenApiSchema rightSchema)
                return false;

            if (leftSchema.Enum is { Count: > 0 } || rightSchema.Enum is { Count: > 0 } || leftSchema.Const is not null || rightSchema.Const is not null)
                return false;

            return leftSchema.Type == rightSchema.Type && string.Equals(leftSchema.Format, rightSchema.Format, StringComparison.Ordinal);
        }

        OpenApiSchema? Resolve(IOpenApiSchema schema)
        {
            string? id = GetSchemaRefId(schema);
            if (id is not null && components.TryGetValue(id, out IOpenApiSchema? resolved))
                return resolved as OpenApiSchema;

            return schema as OpenApiSchema;
        }

        Dictionary<string, List<IOpenApiSchema>> CollectProperties(IOpenApiSchema branch)
        {
            var result = new Dictionary<string, List<IOpenApiSchema>>(StringComparer.Ordinal);
            var visited = new HashSet<IOpenApiSchema>(ReferenceEqualityComparer<IOpenApiSchema>.Instance);

            void Visit(IOpenApiSchema? current)
            {
                if (current is null || !visited.Add(current))
                    return;

                OpenApiSchema? concrete = Resolve(current);
                if (concrete is null)
                    return;

                if (concrete.Properties is { Count: > 0 })
                {
                    foreach ((string name, IOpenApiSchema property) in concrete.Properties)
                    {
                        if (!result.TryGetValue(name, out List<IOpenApiSchema>? candidates))
                        {
                            candidates = [];
                            result[name] = candidates;
                        }

                        candidates.Add(property);
                    }
                }

                if (concrete.AllOf is { Count: > 0 })
                    foreach (IOpenApiSchema child in concrete.AllOf)
                        Visit(child);
            }

            Visit(branch);
            return result;
        }

        foreach (IOpenApiSchema component in components.Values.ToList())
        {
            if (component is not OpenApiSchema parent)
                continue;

            IList<IOpenApiSchema>? branches = parent.OneOf is { Count: > 0 } ? parent.OneOf : parent.AnyOf;
            if (branches is not { Count: > 0 })
                continue;

            var candidatesByName = new Dictionary<string, List<IOpenApiSchema>>(StringComparer.Ordinal);

            foreach (IOpenApiSchema branch in branches)
            {
                foreach ((string name, List<IOpenApiSchema> candidates) in CollectProperties(branch))
                {
                    if (!candidatesByName.TryGetValue(name, out List<IOpenApiSchema>? allCandidates))
                    {
                        allCandidates = [];
                        candidatesByName[name] = allCandidates;
                    }

                    allCandidates.AddRange(candidates);
                }
            }

            foreach ((string name, List<IOpenApiSchema> candidates) in candidatesByName)
            {
                if (candidates.Count == 0 || candidates.Skip(1).Any(candidate => !AreCompatible(candidates[0], candidate)))
                    continue;

                parent.Properties ??= new Dictionary<string, IOpenApiSchema>(StringComparer.Ordinal);
                parent.Properties.TryAdd(name, candidates[0]);
            }
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
            return branches is { Count: > 0 } && !HasObjectLikeBranch(branches, comps);
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

}
