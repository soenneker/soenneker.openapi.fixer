using Microsoft.Extensions.Logging;
using Microsoft.OpenApi;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.CompilerServices;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private void StripEmptyPropertyNames(OpenApiDocument doc)
    {
        if (doc.Components?.Schemas == null)
            return;

        var visited = new HashSet<IOpenApiSchema>(ReferenceEqualityComparer<IOpenApiSchema>.Instance);

        foreach ((string schemaName, IOpenApiSchema schema) in doc.Components.Schemas)
        {
            StripEmptyPropertyNames(schema, $"#/components/schemas/{schemaName}", visited);
        }
    }

    private void StripEmptyPropertyNames(IOpenApiSchema? schema, string location, HashSet<IOpenApiSchema> visited)
    {
        if (schema == null || !visited.Add(schema) || schema is not OpenApiSchema concreteSchema)
            return;

        if (concreteSchema.Properties is { Count: > 0 })
        {
            string[] invalidKeys = concreteSchema.Properties.Keys.Where(string.IsNullOrWhiteSpace)
                                                 .ToArray();

            foreach (string invalidKey in invalidKeys)
            {
                concreteSchema.Properties.Remove(invalidKey);
                _logger.LogWarning("Removed invalid empty property name from schema at '{Location}'.", location);
            }

            if (invalidKeys.Length > 0 && concreteSchema.Required is { Count: > 0 })
            {
                concreteSchema.Required = concreteSchema.Required.Where(required => !string.IsNullOrWhiteSpace(required))
                                                        .ToHashSet(StringComparer.Ordinal);

                if (concreteSchema.Required.Count == 0)
                    concreteSchema.Required = null;
            }

            foreach ((string propertyName, IOpenApiSchema propertySchema) in concreteSchema.Properties.ToList())
            {
                StripEmptyPropertyNames(propertySchema, $"{location}/properties/{propertyName}", visited);
            }
        }

        if (concreteSchema.Items != null)
            StripEmptyPropertyNames(concreteSchema.Items, $"{location}/items", visited);

        if (concreteSchema.AdditionalProperties != null)
            StripEmptyPropertyNames(concreteSchema.AdditionalProperties, $"{location}/additionalProperties", visited);

        if (concreteSchema.AllOf != null)
        {
            for (var i = 0; i < concreteSchema.AllOf.Count; i++)
            {
                StripEmptyPropertyNames(concreteSchema.AllOf[i], $"{location}/allOf/{i}", visited);
            }
        }

        if (concreteSchema.AnyOf != null)
        {
            for (var i = 0; i < concreteSchema.AnyOf.Count; i++)
            {
                StripEmptyPropertyNames(concreteSchema.AnyOf[i], $"{location}/anyOf/{i}", visited);
            }
        }

        if (concreteSchema.OneOf != null)
        {
            for (var i = 0; i < concreteSchema.OneOf.Count; i++)
            {
                StripEmptyPropertyNames(concreteSchema.OneOf[i], $"{location}/oneOf/{i}", visited);
            }
        }
    }

    private sealed class ReferenceEqualityComparer<T> : IEqualityComparer<T> where T : class
    {
        public static ReferenceEqualityComparer<T> Instance { get; } = new();

        public bool Equals(T? x, T? y) => ReferenceEquals(x, y);

        public int GetHashCode(T obj) => RuntimeHelpers.GetHashCode(obj);
    }
}
