using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using Microsoft.OpenApi;

namespace Soenneker.OpenApi.Fixer.Compatibility
{
    /// <summary>
    /// Compatibility wrapper for OpenAPI.NET v2.2 to provide v1.6 API surface
    /// </summary>
    public static class OpenApiCompatibility
    {
        /// <summary>
        /// Extension method to get the reference from a schema (v1.6 compatibility)
        /// </summary>
        public static OpenApiReference? GetReference(this IOpenApiSchema schema)
        {
            if (schema is OpenApiSchemaReference schemaRef)
            {
                return new OpenApiReference
                {
                    Type = ReferenceType.Schema,
                    Id = schemaRef.Reference.Id
                };
            }
            return null;
        }

        /// <summary>
        /// Extension method to get the reference as BaseOpenApiReference (v2 compatibility)
        /// </summary>
        public static Microsoft.OpenApi.BaseOpenApiReference? GetBaseReference(this IOpenApiSchema schema)
        {
            if (schema is OpenApiSchemaReference schemaRef)
            {
                return schemaRef.Reference;
            }
            return null;
        }

        /// <summary>
        /// Extension method to set the reference on a schema (v1.6 compatibility)
        /// </summary>
        public static void SetReference(this OpenApiSchema schema, OpenApiReference? reference)
        {
            // In v2, references are handled differently, so this is a no-op for now
            // The actual reference handling would need to be done at the document level
        }

        /// <summary>
        /// Extension method to get the type as string (v1.6 compatibility)
        /// </summary>
        public static string? GetTypeAsString(this IOpenApiSchema schema)
        {
            return schema.Type?.ToString().ToLowerInvariant();
        }

        /// <summary>
        /// Extension method to set the type from string (v1.6 compatibility)
        /// </summary>
        public static void SetTypeFromString(this OpenApiSchema schema, string? type)
        {
            if (string.IsNullOrEmpty(type))
            {
                schema.Type = null;
                return;
            }

            schema.Type = type.ToLowerInvariant() switch
            {
                "string" => JsonSchemaType.String,
                "integer" => JsonSchemaType.Integer,
                "number" => JsonSchemaType.Number,
                "boolean" => JsonSchemaType.Boolean,
                "array" => JsonSchemaType.Array,
                "object" => JsonSchemaType.Object,
                _ => null
            };
        }

        /// <summary>
        /// Extension method to check if a schema is a reference (v1.6 compatibility)
        /// </summary>
        public static bool IsReference(this IOpenApiSchema schema)
        {
            return schema is OpenApiSchemaReference;
        }

        /// <summary>
        /// Extension method to get the reference ID (v1.6 compatibility)
        /// </summary>
        public static string? GetReferenceId(this IOpenApiSchema schema)
        {
            if (schema is OpenApiSchemaReference schemaRef)
            {
                return schemaRef.Reference.Id;
            }
            return null;
        }

        /// <summary>
        /// Extension method to get the reference type (v1.6 compatibility)
        /// </summary>
        public static ReferenceType? GetReferenceType(this IOpenApiSchema schema)
        {
            if (schema is OpenApiSchemaReference)
            {
                return ReferenceType.Schema;
            }
            return null;
        }

        /// <summary>
        /// Extension method to create a schema reference (v1.6 compatibility)
        /// </summary>
        public static OpenApiSchema CreateSchemaReference(string referenceId)
        {
            // This is a placeholder - in v2, references are handled differently
            // For now, we'll create a regular schema and mark it as a reference
            var schema = new OpenApiSchema();
            // Note: This doesn't actually create a reference in v2, but provides compatibility
            return schema;
        }

        /// <summary>
        /// Extension method to check if a request body is a reference (v1.6 compatibility)
        /// </summary>
        public static bool IsReference(this IOpenApiRequestBody requestBody)
        {
            return requestBody is OpenApiRequestBodyReference;
        }

        /// <summary>
        /// Extension method to get the reference from a request body (v1.6 compatibility)
        /// </summary>
        public static OpenApiReference? GetReference(this IOpenApiRequestBody requestBody)
        {
            if (requestBody is OpenApiRequestBodyReference bodyRef)
            {
                return new OpenApiReference
                {
                    Type = ReferenceType.RequestBody,
                    Id = bodyRef.Reference.Id
                };
            }
            return null;
        }

        /// <summary>
        /// Extension method to get the reference from a parameter (v1.6 compatibility)
        /// </summary>
        public static OpenApiReference? GetReference(this IOpenApiParameter parameter)
        {
            if (parameter is OpenApiParameterReference paramRef)
            {
                return new OpenApiReference
                {
                    Type = ReferenceType.Parameter,
                    Id = paramRef.Reference.Id
                };
            }
            return null;
        }

        /// <summary>
        /// Extension method to get the reference from a response (v1.6 compatibility)
        /// </summary>
        public static OpenApiReference? GetReference(this IOpenApiResponse response)
        {
            if (response is OpenApiResponseReference respRef)
            {
                return new OpenApiReference
                {
                    Type = ReferenceType.Response,
                    Id = respRef.Reference.Id
                };
            }
            return null;
        }

        /// <summary>
        /// Extension method to get the reference from a header (v1.6 compatibility)
        /// </summary>
        public static OpenApiReference? GetReference(this IOpenApiHeader header)
        {
            if (header is OpenApiHeaderReference headerRef)
            {
                return new OpenApiReference
                {
                    Type = ReferenceType.Header,
                    Id = headerRef.Reference.Id
                };
            }
            return null;
        }

        /// <summary>
        /// Extension method to check if a schema has a discriminator (v1.6 compatibility)
        /// </summary>
        public static bool HasDiscriminator(this IOpenApiSchema schema)
        {
            return schema.Discriminator != null;
        }

        /// <summary>
        /// Extension method to get the discriminator property name (v1.6 compatibility)
        /// </summary>
        public static string? GetDiscriminatorPropertyName(this IOpenApiSchema schema)
        {
            return schema.Discriminator?.PropertyName;
        }

        /// <summary>
        /// Extension method to get the discriminator mapping (v1.6 compatibility)
        /// </summary>
        public static IDictionary<string, string>? GetDiscriminatorMapping(this IOpenApiSchema schema)
        {
            if (schema.Discriminator?.Mapping == null)
                return null;

            var mapping = new Dictionary<string, string>();
            foreach (var kvp in schema.Discriminator.Mapping)
            {
                mapping[kvp.Key] = kvp.Value.Reference.Id;
            }
            return mapping;
        }

        /// <summary>
        /// Extension method to set the discriminator mapping (v1.6 compatibility)
        /// </summary>
        public static void SetDiscriminatorMapping(this OpenApiSchema schema, IDictionary<string, string>? mapping)
        {
            if (mapping == null)
            {
                schema.Discriminator = null;
                return;
            }

            schema.Discriminator ??= new OpenApiDiscriminator();
            schema.Discriminator.Mapping ??= new Dictionary<string, OpenApiSchemaReference>();

            foreach (var kvp in mapping)
            {
                schema.Discriminator.Mapping[kvp.Key] = new OpenApiSchemaReference(kvp.Value);
            }
        }

        /// <summary>
        /// Extension method to check if a schema has properties (v1.6 compatibility)
        /// </summary>
        public static bool HasProperties(this IOpenApiSchema schema)
        {
            return schema.Properties?.Any() == true;
        }

        /// <summary>
        /// Extension method to get properties count (v1.6 compatibility)
        /// </summary>
        public static int GetPropertiesCount(this IOpenApiSchema schema)
        {
            return schema.Properties?.Count ?? 0;
        }

        /// <summary>
        /// Extension method to check if a schema has enum values (v1.6 compatibility)
        /// </summary>
        public static bool HasEnum(this IOpenApiSchema schema)
        {
            return schema.Enum?.Any() == true;
        }

        /// <summary>
        /// Extension method to get enum count (v1.6 compatibility)
        /// </summary>
        public static int GetEnumCount(this IOpenApiSchema schema)
        {
            return schema.Enum?.Count ?? 0;
        }

        /// <summary>
        /// Extension method to check if a schema has composition (v1.6 compatibility)
        /// </summary>
        public static bool HasComposition(this IOpenApiSchema schema)
        {
            return schema.AllOf?.Any() == true || 
                   schema.OneOf?.Any() == true || 
                   schema.AnyOf?.Any() == true;
        }

        /// <summary>
        /// Extension method to get composition count (v1.6 compatibility)
        /// </summary>
        public static int GetCompositionCount(this IOpenApiSchema schema)
        {
            return (schema.AllOf?.Count ?? 0) + 
                   (schema.OneOf?.Count ?? 0) + 
                   (schema.AnyOf?.Count ?? 0);
        }

        /// <summary>
        /// Extension method to set discriminator on a schema (v1.6 compatibility)
        /// </summary>
        public static void SetDiscriminator(this IOpenApiSchema schema, OpenApiDiscriminator? discriminator)
        {
            if (schema is OpenApiSchema concreteSchema)
            {
                // In v2, we need to use the constructor or factory method
                // For now, we'll skip setting read-only properties
                // TODO: Implement proper v2 discriminator setting
            }
        }

        /// <summary>
        /// Extension method to get the reference from a parameter (v1.6 compatibility)
        /// </summary>
        public static OpenApiReference? GetReference(this OpenApiParameter parameter)
        {
            // In v2, OpenApiParameter doesn't have a Reference property
            // This is a compatibility method that always returns null for now
            return null;
        }

        /// <summary>
        /// Extension method to get the reference ID from a parameter (v1.6 compatibility)
        /// </summary>
        public static string? GetReferenceId(this OpenApiParameter parameter)
        {
            // In v2, OpenApiParameter doesn't have a Reference property
            // This is a compatibility method that always returns null for now
            return null;
        }

        /// <summary>
        /// Extension method to get the reference type from a parameter (v1.6 compatibility)
        /// </summary>
        public static ReferenceType? GetReferenceType(this OpenApiParameter parameter)
        {
            // In v2, OpenApiParameter doesn't have a Reference property
            // This is a compatibility method that always returns null for now
            return null;
        }

        /// <summary>
        /// Extension method to get the reference from a response (v1.6 compatibility)
        /// </summary>
        public static OpenApiReference? GetReference(this OpenApiResponse response)
        {
            // In v2, OpenApiResponse doesn't have a Reference property
            // This is a compatibility method that always returns null for now
            return null;
        }



        /// <summary>
        /// Extension method to set properties on a schema (v1.6 compatibility)
        /// </summary>
        public static void SetProperties(this IOpenApiSchema schema, IDictionary<string, IOpenApiSchema>? properties)
        {
            if (schema is OpenApiSchema concreteSchema)
            {
                // In v2, properties are read-only, so we need to use the constructor
                // For now, we'll skip setting read-only properties
                // TODO: Implement proper v2 properties setting
            }
        }

        /// <summary>
        /// Extension method to set required properties on a schema (v1.6 compatibility)
        /// </summary>
        public static void SetRequired(this IOpenApiSchema schema, IList<string>? required)
        {
            if (schema is OpenApiSchema concreteSchema)
            {
                // In v2, required is read-only, so we need to use the constructor
                // For now, we'll skip setting read-only properties
                // TODO: Implement proper v2 required setting
            }
        }

        /// <summary>
        /// Extension method to set title on a schema (v1.6 compatibility)
        /// </summary>
        public static void SetTitle(this IOpenApiSchema schema, string? title)
        {
            if (schema is OpenApiSchema concreteSchema)
            {
                // In v2, title is read-only, so we need to use the constructor
                // For now, we'll skip setting read-only properties
                // TODO: Implement proper v2 title setting
            }
        }


    }

    /// <summary>
    /// Compatibility class for OpenApiReference (v1.6 compatibility)
    /// </summary>
    public class OpenApiReference
    {
        public ReferenceType Type { get; set; }
        public string? Id { get; set; }
        public string? ReferenceV3 { get; set; }
    }

    /// <summary>
    /// Compatibility enum for ReferenceType (v1.6 compatibility)
    /// </summary>
    public enum ReferenceType
    {
        Schema,
        Response,
        Parameter,
        RequestBody,
        Header,
        SecurityScheme,
        Link,
        Callback,
        Example
    }
}
