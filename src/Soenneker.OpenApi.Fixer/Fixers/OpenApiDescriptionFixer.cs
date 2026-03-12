using Microsoft.Extensions.Logging;
using Microsoft.OpenApi;
using Soenneker.OpenApi.Fixer.Fixers.Abstract;
using System;
using System.Collections.Generic;

namespace Soenneker.OpenApi.Fixer.Fixers;

/// <summary>
/// Provides functionality to fix and sanitize descriptions in OpenAPI documents, particularly handling YAML-unsafe strings.
/// </summary>
public sealed class OpenApiDescriptionFixer : IOpenApiDescriptionFixer
{
    private readonly ILogger<OpenApiDescriptionFixer> _logger;

    public OpenApiDescriptionFixer(ILogger<OpenApiDescriptionFixer> logger)
    {
        _logger = logger;
    }

    /// <inheritdoc />
    public void FixYamlUnsafeDescriptions(OpenApiDocument document)
    {
        if (document == null)
            return;

        // Fix top-level descriptions
        if (document.Info != null)
        {
            document.Info.Description = FixYamlUnsafeString(document.Info.Description);
        }

        // Create visited set to prevent circular reference issues
        var visited = new HashSet<IOpenApiSchema>();

        // Fix all components (schemas, parameters, responses, etc.)
        if (document.Components != null)
        {
            if (document.Components.Schemas != null)
            {
                foreach (var schema in document.Components.Schemas.Values)
                    FixSchemaDescriptions(schema, visited);
            }

            if (document.Components.Parameters != null)
            {
                foreach (var parameter in document.Components.Parameters.Values)
                    FixParameterDescriptions(parameter, visited);
            }

            if (document.Components.Responses != null)
            {
                foreach (var response in document.Components.Responses.Values)
                    FixResponseDescriptions(response, visited);
            }

            if (document.Components.RequestBodies != null)
            {
                foreach (var requestBody in document.Components.RequestBodies.Values)
                    FixRequestBodyDescriptions(requestBody, visited);
            }
        }

        // Fix all paths & operations
        if (document.Paths != null)
        {
            foreach (var path in document.Paths.Values)
                FixPathItemDescriptions(path, visited);
        }
    }

    private static string? FixYamlUnsafeString(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return value;

        // YAML forbids unquoted "key: value" patterns in many scalar contexts
        // We detect colon+space OR leading/trailing colon situations.
        bool containsYamlMappingPattern = value.Contains(": ", StringComparison.Ordinal) || value.EndsWith(":", StringComparison.Ordinal) ||
                                          value.StartsWith(":", StringComparison.Ordinal);

        if (!containsYamlMappingPattern)
            return value;

        // Already quoted? Skip.
        if ((value.StartsWith("\"") && value.EndsWith("\"")) || (value.StartsWith("'") && value.EndsWith("'")))
            return value;

        // Escape internal quotes minimally but safely
        string escaped = value.Replace("\"", "\\\"");

        return $"\"{escaped}\"";
    }

    private void FixSchemaDescriptions(IOpenApiSchema schema, HashSet<IOpenApiSchema> visited)
    {
        if (schema == null || !visited.Add(schema))
            return;

        // Skip references - they don't have their own descriptions/properties to fix.
        // The referenced schema will be processed separately when iterating through Components.Schemas.
        if (schema is OpenApiSchemaReference)
            return;

        schema.Description = FixYamlUnsafeString(schema.Description);

        if (schema is OpenApiSchema concreteSchema && concreteSchema.Properties != null)
        {
            foreach (var prop in concreteSchema.Properties.Values)
            {
                prop.Description = FixYamlUnsafeString(prop.Description);
                FixSchemaDescriptions(prop, visited); // recursive
            }
        }

        // Handle `items`, `additionalProperties`
        if (schema.Items != null)
            FixSchemaDescriptions(schema.Items, visited);
        if (schema.AdditionalProperties is OpenApiSchema addl)
            FixSchemaDescriptions(addl, visited);

        // Handle oneOf/anyOf/allOf
        if (schema.OneOf != null)
        {
            foreach (var s in schema.OneOf)
                FixSchemaDescriptions(s, visited);
        }
        if (schema.AnyOf != null)
        {
            foreach (var s in schema.AnyOf)
                FixSchemaDescriptions(s, visited);
        }
        if (schema.AllOf != null)
        {
            foreach (var s in schema.AllOf)
                FixSchemaDescriptions(s, visited);
        }
    }

    private void FixPathItemDescriptions(IOpenApiPathItem item, HashSet<IOpenApiSchema> visited)
    {
        if (item == null)
            return;

        item.Description = FixYamlUnsafeString(item.Description);
        item.Summary = FixYamlUnsafeString(item.Summary);

        if (item.Operations != null)
        {
            foreach (var op in item.Operations.Values)
                FixOperationDescriptions(op, visited);
        }
    }

    private void FixOperationDescriptions(OpenApiOperation op, HashSet<IOpenApiSchema> visited)
    {
        if (op == null)
            return;

        op.Description = FixYamlUnsafeString(op.Description);
        op.Summary = FixYamlUnsafeString(op.Summary);

        if (op.Parameters != null)
        {
            foreach (var param in op.Parameters)
                FixParameterDescriptions(param, visited);
        }

        if (op.RequestBody != null)
            FixRequestBodyDescriptions(op.RequestBody, visited);

        if (op.Responses != null)
        {
            foreach (var resp in op.Responses.Values)
                FixResponseDescriptions(resp, visited);
        }
    }

    private void FixParameterDescriptions(IOpenApiParameter parameter, HashSet<IOpenApiSchema> visited)
    {
        if (parameter == null)
            return;

        parameter.Description = FixYamlUnsafeString(parameter.Description);
        if (parameter.Schema != null)
            FixSchemaDescriptions(parameter.Schema, visited);
    }

    private void FixRequestBodyDescriptions(IOpenApiRequestBody body, HashSet<IOpenApiSchema> visited)
    {
        if (body == null)
            return;

        body.Description = FixYamlUnsafeString(body.Description);

        if (body.Content != null)
        {
            foreach (var content in body.Content.Values)
            {
                if (content.Schema != null)
                    FixSchemaDescriptions(content.Schema, visited);
            }
        }
    }

    private void FixResponseDescriptions(IOpenApiResponse response, HashSet<IOpenApiSchema> visited)
    {
        if (response == null)
            return;

        response.Description = FixYamlUnsafeString(response.Description);

        if (response.Content != null)
        {
            foreach (var content in response.Content.Values)
            {
                if (content.Schema != null)
                    FixSchemaDescriptions(content.Schema, visited);
            }
        }
    }
}

