using Microsoft.Extensions.Logging;
using Microsoft.OpenApi;
using Soenneker.OpenApi.Fixer.Fixers.Abstract;
using System;
using System.Collections.Generic;

namespace Soenneker.OpenApi.Fixer.Fixers;

///<inheritdoc cref="IOpenApiInt32IdFixer"/>
public sealed class OpenApiInt32IdFixer : IOpenApiInt32IdFixer
{
    public OpenApiInt32IdFixer(ILogger<OpenApiInt32IdFixer> logger)
    {
    }

    public void Transform(OpenApiDocument document)
    {
        IDictionary<string, IOpenApiSchema> componentSchemas = document.Components?.Schemas ?? new Dictionary<string, IOpenApiSchema>();
        var visited = new HashSet<OpenApiSchema>();

        OpenApiSchema? ResolveSchema(IOpenApiSchema? schema)
        {
            if (schema == null)
                return null;

            if (schema is OpenApiSchema concreteSchema)
                return concreteSchema;

            if (TryGetSchemaRefId(schema, out string? refId) && refId != null && componentSchemas.TryGetValue(refId, out IOpenApiSchema? resolvedSchema) &&
                resolvedSchema is OpenApiSchema resolvedConcreteSchema)
                return resolvedConcreteSchema;

            return null;
        }

        static bool ShouldTransform(string? name, OpenApiSchema schema)
        {
            if (string.IsNullOrWhiteSpace(name))
                return false;

            bool isInt32 = schema.Type == JsonSchemaType.Integer &&
                           (string.IsNullOrWhiteSpace(schema.Format) || schema.Format.Equals("int32", StringComparison.OrdinalIgnoreCase));

            return isInt32 && name.EndsWith("id", StringComparison.OrdinalIgnoreCase);
        }

        void VisitSchema(IOpenApiSchema? schema, string? name)
        {
            OpenApiSchema? concreteSchema = ResolveSchema(schema);

            if (concreteSchema == null || !visited.Add(concreteSchema))
                return;

            if (ShouldTransform(name, concreteSchema))
                concreteSchema.Format = "int64";

            if (concreteSchema.Properties != null)
            {
                foreach ((string propertyName, IOpenApiSchema propertySchema) in concreteSchema.Properties)
                {
                    OpenApiSchema? resolvedPropertySchema = ResolveSchema(propertySchema);

                    if (resolvedPropertySchema != null && ShouldTransform(propertyName, resolvedPropertySchema))
                        resolvedPropertySchema.Format = "int64";

                    VisitSchema(propertySchema, propertyName);
                }
            }

            if (concreteSchema.Items != null)
                VisitSchema(concreteSchema.Items, null);

            if (concreteSchema.AdditionalProperties != null)
                VisitSchema(concreteSchema.AdditionalProperties, null);

            if (concreteSchema.AllOf != null)
                foreach (IOpenApiSchema child in concreteSchema.AllOf)
                    VisitSchema(child, null);

            if (concreteSchema.OneOf != null)
                foreach (IOpenApiSchema child in concreteSchema.OneOf)
                    VisitSchema(child, null);

            if (concreteSchema.AnyOf != null)
                foreach (IOpenApiSchema child in concreteSchema.AnyOf)
                    VisitSchema(child, null);
        }

        void VisitParameter(IOpenApiParameter? parameter)
        {
            if (parameter is not OpenApiParameter concreteParameter || concreteParameter.Schema == null)
                return;

            OpenApiSchema? schema = ResolveSchema(concreteParameter.Schema);

            if (schema != null && ShouldTransform(concreteParameter.Name, schema))
                schema.Format = "int64";

            VisitSchema(concreteParameter.Schema, concreteParameter.Name);
        }

        if (document.Components?.Schemas != null)
        {
            foreach (IOpenApiSchema schema in document.Components.Schemas.Values)
                VisitSchema(schema, null);
        }

        if (document.Components?.Parameters != null)
        {
            foreach (IOpenApiParameter parameter in document.Components.Parameters.Values)
                VisitParameter(parameter);
        }

        if (document.Components?.RequestBodies != null)
        {
            foreach (IOpenApiRequestBody requestBody in document.Components.RequestBodies.Values)
            {
                if (requestBody?.Content == null)
                    continue;

                foreach (IOpenApiMediaType mediaType in requestBody.Content.Values)
                    VisitSchema(mediaType?.Schema, null);
            }
        }

        if (document.Components?.Responses != null)
        {
            foreach (IOpenApiResponse response in document.Components.Responses.Values)
            {
                if (response?.Content == null)
                    continue;

                foreach (IOpenApiMediaType mediaType in response.Content.Values)
                    VisitSchema(mediaType?.Schema, null);
            }
        }

        if (document.Paths == null)
            return;

        foreach (IOpenApiPathItem pathItem in document.Paths.Values)
        {
            if (pathItem?.Parameters != null)
            {
                foreach (IOpenApiParameter parameter in pathItem.Parameters)
                    VisitParameter(parameter);
            }

            if (pathItem?.Operations == null)
                continue;

            foreach (OpenApiOperation operation in pathItem.Operations.Values)
            {
                if (operation?.Parameters != null)
                {
                    foreach (IOpenApiParameter parameter in operation.Parameters)
                        VisitParameter(parameter);
                }

                if (operation?.RequestBody?.Content != null)
                {
                    foreach (IOpenApiMediaType mediaType in operation.RequestBody.Content.Values)
                        VisitSchema(mediaType?.Schema, null);
                }

                if (operation?.Responses == null)
                    continue;

                foreach (IOpenApiResponse response in operation.Responses.Values)
                {
                    if (response?.Content == null)
                        continue;

                    foreach (IOpenApiMediaType mediaType in response.Content.Values)
                        VisitSchema(mediaType?.Schema, null);
                }
            }
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

            default:
                return false;
        }
    }
}