using Microsoft.Extensions.Logging;
using Microsoft.OpenApi;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text.Json.Nodes;
using System.Threading;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private string AddComponentSchema(OpenApiDocument doc, string compName, OpenApiSchema schema)
    {
        if (string.IsNullOrWhiteSpace(compName))
        {
            _logger.LogWarning("Skipped adding a component schema because its generated name was empty.");
            return string.Empty;
        }

        doc.Components ??= new OpenApiComponents();
        doc.Components.Schemas ??= new Dictionary<string, IOpenApiSchema>();

        string validatedName = OpenApiNameNormalizer.ReserveComponentName(doc.Components.Schemas.Keys, compName, "Schema");

        if (SchemaReferencesComponent(schema, validatedName))
        {
            string guardedBaseName = $"{validatedName} Wrapper";
            IEnumerable<string> reservedNames = doc.Components.Schemas.Keys.Concat([validatedName]);
            validatedName = OpenApiNameNormalizer.ReserveComponentName(reservedNames, guardedBaseName, "Schema");
        }

        doc.Components.Schemas[validatedName] = schema;

        return validatedName;
    }

    private static bool SchemaReferencesComponent(IOpenApiSchema? schema, string componentName)
    {
        var visited = new HashSet<IOpenApiSchema>(ReferenceEqualityComparer<IOpenApiSchema>.Instance);

        bool Visit(IOpenApiSchema? current)
        {
            if (current == null)
                return false;

            if (TryGetSchemaRefId(current, out string? refId))
                return string.Equals(refId, componentName, StringComparison.Ordinal);

            if (current is not OpenApiSchema concreteSchema || !visited.Add(concreteSchema))
                return false;

            if (concreteSchema.Properties != null && concreteSchema.Properties.Values.Any(Visit))
                return true;

            if (Visit(concreteSchema.Items))
                return true;

            if (Visit(concreteSchema.AdditionalProperties))
                return true;

            if (concreteSchema.AllOf != null && concreteSchema.AllOf.Any(Visit))
                return true;

            if (concreteSchema.AnyOf != null && concreteSchema.AnyOf.Any(Visit))
                return true;

            if (concreteSchema.OneOf != null && concreteSchema.OneOf.Any(Visit))
                return true;

            return Visit(concreteSchema.Not);
        }

        return Visit(schema);
    }

    private static string DetermineInlineResponseComponentBaseName(OpenApiSchema schema, ISet<string> repeatedTitles, string safeOpId, string statusCode,
        string mediaName)
    {
        if (!string.IsNullOrWhiteSpace(schema.Title))
        {
            string titleBasedName = OpenApiNameNormalizer.NormalizeComponentName(schema.Title, $"{safeOpId} {statusCode} Response");

            // Preserve useful semantic titles, but not titles reused by unrelated operations. Reused titles make
            // component identity depend on traversal order and produce unstable numeric suffixes when schemas diverge.
            if (!string.IsNullOrWhiteSpace(titleBasedName) && !repeatedTitles.Contains(titleBasedName))
                return titleBasedName;
        }

        string mediaContext = mediaName.Equals("Json", StringComparison.Ordinal) ? string.Empty : $" {mediaName}";
        return OpenApiNameNormalizer.NormalizeComponentName($"{safeOpId} {statusCode}{mediaContext} Response");
    }

    private static bool IsPrimitiveEnvelopeMetadata(IOpenApiSchema schema)
    {
        if (schema is OpenApiSchemaReference)
            return false;

        if (schema is not OpenApiSchema concreteSchema)
            return false;

        if (concreteSchema.Type == JsonSchemaType.Array)
            return concreteSchema.Items is not OpenApiSchemaReference;

        return concreteSchema.Type != JsonSchemaType.Object && concreteSchema.Properties?.Any() != true && concreteSchema.AllOf?.Any() != true &&
               concreteSchema.AnyOf?.Any() != true && concreteSchema.OneOf?.Any() != true;
    }

    private static bool IsSimpleCollectionEnvelope(OpenApiSchema schema)
    {
        if (schema.Type != JsonSchemaType.Object || schema.Properties?.Any() != true)
            return false;

        int anchoredCollectionCount = 0;

        foreach ((string _, IOpenApiSchema propertySchema) in schema.Properties)
        {
            if (propertySchema is OpenApiSchemaReference)
            {
                anchoredCollectionCount++;
                continue;
            }

            if (propertySchema is OpenApiSchema concretePropertySchema && concretePropertySchema.Type == JsonSchemaType.Array &&
                concretePropertySchema.Items is OpenApiSchemaReference)
            {
                anchoredCollectionCount++;
                continue;
            }

            if (!IsPrimitiveEnvelopeMetadata(propertySchema))
                return false;
        }

        return anchoredCollectionCount == 1;
    }

    private void ExtractInlineSchemas(OpenApiDocument document, CancellationToken cancellationToken)
    {
        ExtractInlineSchemasCore(document, cancellationToken, true);
    }

    private void ExtractInlineSchemasCore(OpenApiDocument document, CancellationToken cancellationToken, bool preserveSimpleEnvelopes)
    {
        static bool IsSimpleEnvelope(OpenApiSchema s) =>
            s.Properties?.Count == 1 && s.Properties.TryGetValue("data", out IOpenApiSchema? p) && p is OpenApiSchemaReference &&
            (s.Required == null || s.Required.Count <= 1);

        IDictionary<string, IOpenApiSchema>? comps = document.Components?.Schemas;
        if (comps == null)
            return;

        var titleCounts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);

        foreach (IOpenApiPathItem pathItem in document.Paths.Values)
        {
            if (pathItem.Operations == null)
                continue;

            foreach (OpenApiOperation operation in pathItem.Operations.Values)
            {
                if (operation.Responses == null)
                    continue;

                foreach (IOpenApiResponse response in operation.Responses.Values)
                {
                    if (response is OpenApiResponseReference || response.Content == null)
                        continue;

                    foreach (IOpenApiMediaType mediaType in response.Content.Values)
                    {
                        if (mediaType.Schema is not OpenApiSchema { Title: not null } titledSchema || string.IsNullOrWhiteSpace(titledSchema.Title))
                            continue;

                        string normalizedTitle = OpenApiNameNormalizer.NormalizeComponentName(titledSchema.Title);
                        titleCounts[normalizedTitle] = titleCounts.GetValueOrDefault(normalizedTitle) + 1;
                    }
                }
            }
        }

        var repeatedTitles = new HashSet<string>(titleCounts.Where(pair => pair.Value > 1).Select(pair => pair.Key), StringComparer.OrdinalIgnoreCase);

        foreach (IOpenApiPathItem pathItem in document.Paths.Values)
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (pathItem.Operations != null)
            {
                foreach ((HttpMethod opType, OpenApiOperation operation) in pathItem.Operations)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    if (operation == null)
                        continue;

                    string safeOpId = OpenApiNameNormalizer.NormalizeOperationId(operation.OperationId, opType, null);

                    if (operation.Parameters != null)
                    {
                        foreach (IOpenApiParameter? param in operation.Parameters.ToList())
                        {
                            // Skip parameter references as they don't need inline schema extraction
                            if (param is OpenApiParameterReference)
                                continue;

                            if (param is OpenApiParameter concreteParam && concreteParam.Content?.Any() == true)
                            {
                                IOpenApiMediaType? first = concreteParam.Content.Values.FirstOrDefault();
                                if (first?.Schema != null)
                                    concreteParam.Schema = first.Schema;

                                concreteParam.Content = null;
                            }
                        }
                    }

                    if (operation.RequestBody != null && operation.RequestBody is not OpenApiRequestBodyReference && operation.RequestBody.Content != null)
                    {
                        foreach ((string mediaType, IOpenApiMediaType mediaInterface) in operation.RequestBody.Content!.ToList())
                        {
                            if (mediaInterface is not OpenApiMediaType media)
                                continue;

                            IOpenApiSchema? schemaReq = media.Schema;
                            if (schemaReq == null || schemaReq is OpenApiSchemaReference)
                                continue;
                            if (schemaReq is OpenApiSchema concreteSchemaReq1 && IsSimpleEnvelope(concreteSchemaReq1))
                                continue;

                            string safeMedia;
                            string subtype = mediaType.Split(';')[0]
                                                      .Split('/')
                                                      .Last();
                            if (subtype.Equals("json", StringComparison.OrdinalIgnoreCase))
                                safeMedia = "";
                            else
                                safeMedia = OpenApiNameNormalizer.NormalizeMediaTypeName(mediaType);

                            string baseName = string.IsNullOrWhiteSpace(safeMedia) ? $"{safeOpId} Request" : $"{safeOpId} {safeMedia} Request";
                            string compName = ReserveUniqueSchemaName(comps, baseName, "RequestBody");

                            string finalComponentName = compName;

                            if (schemaReq is OpenApiSchema concreteSchemaReq2)
                                finalComponentName = AddComponentSchema(document, compName, concreteSchemaReq2);

                            media.Schema = new OpenApiSchemaReference(finalComponentName);
                        }
                    }

                    if (operation.Responses != null)
                    {
                        foreach ((string statusCode, IOpenApiResponse response) in operation.Responses)
                        {
                            if (response == null || response is OpenApiResponseReference)
                            {
                                continue;
                            }

                            if (response.Content == null)
                                continue;

                            foreach ((string mediaType, IOpenApiMediaType mediaInterface) in response.Content!.ToList())
                            {
                                if (mediaInterface is not OpenApiMediaType media)
                                    continue;

                                IOpenApiSchema? schemaResp = media.Schema;
                                if (schemaResp == null || schemaResp is OpenApiSchemaReference)
                                    continue;
                                if (preserveSimpleEnvelopes && schemaResp is OpenApiSchema concreteSchemaResp1 && IsSimpleEnvelope(concreteSchemaResp1))
                                    continue;
                                if (preserveSimpleEnvelopes && schemaResp is OpenApiSchema concreteSchemaRespEnvelope &&
                                    IsSimpleCollectionEnvelope(concreteSchemaRespEnvelope))
                                    continue;
                                if (schemaResp is not OpenApiSchema concreteSchemaResp2)
                                    continue;

                                string safeMedia = OpenApiNameNormalizer.NormalizeMediaTypeName(mediaType);
                                string baseName = DetermineInlineResponseComponentBaseName(concreteSchemaResp2, repeatedTitles, safeOpId, statusCode, safeMedia);
                                string compName = ReserveUniqueSchemaName(comps, baseName, "Schema");

                                string finalComponentName = compName;

                                finalComponentName = AddComponentSchema(document, compName, concreteSchemaResp2);

                                media.Schema = new OpenApiSchemaReference(finalComponentName);
                            }
                        }
                    }
                }
            }
        }
    }

    private void ExtractInlineComponentContentSchemas(OpenApiDocument document)
    {
        IDictionary<string, IOpenApiSchema>? comps = document.Components?.Schemas;
        if (comps == null)
            return;

        void ExtractContentSchemas(IDictionary<string, IOpenApiMediaType>? content, string baseName)
        {
            if (content == null)
                return;

            foreach ((string mediaType, IOpenApiMediaType mediaInterface) in content.ToList())
            {
                if (mediaInterface is not OpenApiMediaType media || media.Schema is null or OpenApiSchemaReference)
                    continue;

                if (media.Schema is not OpenApiSchema concreteSchema)
                    continue;

                string mediaName = OpenApiNameNormalizer.NormalizeMediaTypeName(mediaType);
                string schemaBaseName = mediaName.Equals("Json", StringComparison.Ordinal) ? baseName : $"{baseName} {mediaName}";
                string reservedName = ReserveUniqueSchemaName(comps, schemaBaseName, "Content");
                string finalComponentName = AddComponentSchema(document, reservedName, concreteSchema);

                if (string.IsNullOrWhiteSpace(finalComponentName))
                    continue;

                media.Schema = new OpenApiSchemaReference(finalComponentName);
                _logger.LogInformation("Promoted component content schema '{Context}' to components schema '{ComponentName}'", schemaBaseName,
                    finalComponentName);
            }
        }

        if (document.Components?.RequestBodies != null)
        {
            foreach ((string requestBodyName, IOpenApiRequestBody requestBody) in document.Components.RequestBodies)
            {
                string baseName = OpenApiNameNormalizer.NormalizeComponentName($"{requestBodyName} Request");
                ExtractContentSchemas(requestBody?.Content, baseName);
            }
        }

        if (document.Components?.Responses != null)
        {
            foreach ((string responseName, IOpenApiResponse response) in document.Components.Responses)
            {
                string baseName = OpenApiNameNormalizer.NormalizeComponentName($"{responseName} Response");
                ExtractContentSchemas(response?.Content, baseName);
            }
        }
    }

    private void ExtractInlineObjectPropertySchemas(OpenApiDocument document)
    {
        if (document.Components?.Schemas == null)
            return;

        static bool IsPromotableInlineObjectShape(IOpenApiSchema schema)
        {
            if (schema is OpenApiSchemaReference || schema is not OpenApiSchema concreteSchema)
                return false;

            bool hasOwnObjectMembers = HasOwnObjectMembers(concreteSchema);
            bool hasArrayType = HasSchemaType(concreteSchema, JsonSchemaType.Array);
            bool hasObjectType = HasSchemaType(concreteSchema, JsonSchemaType.Object);
            bool hasComposition = concreteSchema.AllOf?.Any() == true || concreteSchema.AnyOf?.Any() == true || concreteSchema.OneOf?.Any() == true;

            if (!hasArrayType && (hasOwnObjectMembers || hasObjectType && !hasComposition))
                return true;

            static bool BranchesContainPromotableInlineObject(IList<IOpenApiSchema>? branches)
            {
                if (branches == null)
                    return false;

                foreach (IOpenApiSchema branch in branches)
                {
                    if (IsPromotableInlineObjectShape(branch))
                        return true;
                }

                return false;
            }

            return BranchesContainPromotableInlineObject(concreteSchema.AllOf) || BranchesContainPromotableInlineObject(concreteSchema.AnyOf) ||
                   BranchesContainPromotableInlineObject(concreteSchema.OneOf);
        }

        static bool HasOwnObjectMembers(OpenApiSchema schema)
        {
            return schema.Properties?.Any() == true || schema.AdditionalProperties != null || schema.PatternProperties?.Any() == true;
        }

        bool TryPromoteInlineSchema(IOpenApiSchema schema, string baseName, out IOpenApiSchema reference)
        {
            reference = schema;

            if (schema is OpenApiSchemaReference || schema is not OpenApiSchema concreteSchema || !IsPromotableInlineObjectShape(concreteSchema))
                return false;

            if (!HasSchemaType(concreteSchema, JsonSchemaType.Object) && HasOwnObjectMembers(concreteSchema))
                concreteSchema.Type = JsonSchemaType.Object;

            string reservedName = ReserveUniqueSchemaName(document.Components.Schemas, baseName, "Property");
            string finalComponentName = AddComponentSchema(document, reservedName, concreteSchema);

            if (string.IsNullOrWhiteSpace(finalComponentName))
                return false;

            reference = new OpenApiSchemaReference(finalComponentName);
            _logger.LogInformation("Promoted inline property schema '{Context}' to components schema '{ComponentName}'", baseName, finalComponentName);
            return true;
        }

        bool changed;

        do
        {
            changed = false;

            foreach ((string schemaName, IOpenApiSchema schemaInterface) in document.Components.Schemas.ToList())
            {
                if (schemaInterface is not OpenApiSchema schema)
                    continue;

                if (schema.Items != null && TryPromoteInlineSchema(schema.Items, $"{schemaName} Item", out IOpenApiSchema componentItemReference))
                {
                    schema.Items = componentItemReference;
                    changed = true;
                }

                if (schema.AdditionalProperties != null &&
                    TryPromoteInlineSchema(schema.AdditionalProperties, $"{schemaName} AdditionalProperties", out IOpenApiSchema componentAdditionalReference))
                {
                    schema.AdditionalProperties = componentAdditionalReference;
                    changed = true;
                }

                if (schema.Properties == null)
                    continue;

                foreach ((string propertyName, IOpenApiSchema propertySchemaInterface) in schema.Properties.ToList())
                {
                    if (propertySchemaInterface is not OpenApiSchema propertySchema || propertySchemaInterface is OpenApiSchemaReference)
                        continue;

                    string propertyContext = $"{schemaName} {propertyName}";

                    if (TryPromoteInlineSchema(propertySchema, propertyContext, out IOpenApiSchema propertyReference))
                    {
                        schema.Properties[propertyName] = propertyReference;
                        changed = true;
                        continue;
                    }

                    if (propertySchema.Items != null &&
                        TryPromoteInlineSchema(propertySchema.Items, $"{propertyContext} Item", out IOpenApiSchema itemReference))
                    {
                        propertySchema.Items = itemReference;
                        changed = true;
                    }

                    if (propertySchema.AdditionalProperties != null &&
                        TryPromoteInlineSchema(propertySchema.AdditionalProperties, $"{propertyContext} AdditionalProperties", out IOpenApiSchema additionalReference))
                    {
                        propertySchema.AdditionalProperties = additionalReference;
                        changed = true;
                    }
                }
            }
        }
        while (changed);
    }

    private void ExtractInlineComposedSchemas(OpenApiDocument document)
    {
        IDictionary<string, IOpenApiSchema>? comps = document.Components?.Schemas;
        if (comps == null)
            return;

        static bool HasComposition(OpenApiSchema schema)
        {
            return schema.AllOf?.Any() == true || schema.AnyOf?.Any() == true || schema.OneOf?.Any() == true;
        }

        static bool IsPromotableCompositionBranch(OpenApiSchema schema)
        {
            static bool HasOwnObjectMembers(OpenApiSchema candidate)
            {
                return candidate.Properties?.Any() == true || candidate.PatternProperties?.Any() == true;
            }

            if (HasOwnObjectMembers(schema))
                return true;

            return HasSchemaType(schema, JsonSchemaType.Array) && schema.Items is OpenApiSchema itemSchema && HasOwnObjectMembers(itemSchema);
        }

        var promotedSchemas = new Dictionary<IOpenApiSchema, string>(ReferenceEqualityComparer<IOpenApiSchema>.Instance);
        bool changed;

        bool TryPromoteCore(OpenApiSchema concreteSchema, string baseName, out IOpenApiSchema replacement)
        {
            replacement = concreteSchema;

            if (!promotedSchemas.TryGetValue(concreteSchema, out string? componentName))
            {
                string reservedName = ReserveUniqueSchemaName(comps, baseName, "Composed");
                componentName = AddComponentSchema(document, reservedName, concreteSchema);

                if (string.IsNullOrWhiteSpace(componentName))
                    return false;

                promotedSchemas[concreteSchema] = componentName;
                _logger.LogInformation("Promoted inline composed schema '{Context}' to components schema '{ComponentName}'", baseName, componentName);
            }

            replacement = new OpenApiSchemaReference(componentName);
            return true;
        }

        bool TryPromote(IOpenApiSchema? schema, string baseName, out IOpenApiSchema replacement)
        {
            replacement = schema ?? new OpenApiSchema();

            if (schema is OpenApiSchemaReference || schema is not OpenApiSchema concreteSchema || !HasComposition(concreteSchema))
                return false;

            return TryPromoteCore(concreteSchema, baseName, out replacement);
        }

        bool TryPromoteBranch(IOpenApiSchema? schema, string baseName, out IOpenApiSchema replacement)
        {
            replacement = schema ?? new OpenApiSchema();

            if (schema is OpenApiSchemaReference || schema is not OpenApiSchema concreteSchema || !IsPromotableCompositionBranch(concreteSchema))
                return false;

            return TryPromoteCore(concreteSchema, baseName, out replacement);
        }

        void VisitSchema(IOpenApiSchema? schema, string contextName, HashSet<IOpenApiSchema> visited)
        {
            if (schema is OpenApiSchemaReference || schema is not OpenApiSchema concreteSchema || !visited.Add(concreteSchema))
                return;

            if (concreteSchema.Properties != null)
            {
                foreach ((string propertyName, IOpenApiSchema propertySchema) in concreteSchema.Properties.ToList())
                {
                    string propertyContext = $"{contextName} {propertyName}";

                    if (TryPromote(propertySchema, propertyContext, out IOpenApiSchema replacement))
                    {
                        concreteSchema.Properties[propertyName] = replacement;
                        changed = true;
                    }
                    else
                    {
                        VisitSchema(propertySchema, propertyContext, visited);
                    }
                }
            }

            if (concreteSchema.Items != null)
            {
                string itemContext = $"{contextName} Item";

                if (TryPromote(concreteSchema.Items, itemContext, out IOpenApiSchema replacement))
                {
                    concreteSchema.Items = replacement;
                    changed = true;
                }
                else
                {
                    VisitSchema(concreteSchema.Items, itemContext, visited);
                }
            }

            if (concreteSchema.AdditionalProperties != null)
            {
                string additionalPropertiesContext = $"{contextName} AdditionalProperties";

                if (TryPromote(concreteSchema.AdditionalProperties, additionalPropertiesContext, out IOpenApiSchema replacement))
                {
                    concreteSchema.AdditionalProperties = replacement;
                    changed = true;
                }
                else
                {
                    VisitSchema(concreteSchema.AdditionalProperties, additionalPropertiesContext, visited);
                }
            }

            PromoteCompositionBranches(concreteSchema.AllOf, contextName, "AllOf", visited);
            PromoteCompositionBranches(concreteSchema.AnyOf, contextName, "AnyOf", visited);
            PromoteCompositionBranches(concreteSchema.OneOf, contextName, "OneOf", visited);
        }

        void PromoteCompositionBranches(IList<IOpenApiSchema>? branches, string contextName, string compositionKind, HashSet<IOpenApiSchema> visited)
        {
            if (branches == null)
                return;

            for (var i = 0; i < branches.Count; i++)
            {
                string branchContext = BuildCompositionBranchContext(contextName, branches[i], compositionKind, i + 1);

                if (TryPromoteBranch(branches[i], branchContext, out IOpenApiSchema replacement))
                {
                    branches[i] = replacement;
                    changed = true;
                }
                else
                {
                    VisitSchema(branches[i], branchContext, visited);
                }
            }
        }

        static string BuildCompositionBranchContext(string contextName, IOpenApiSchema schema, string compositionKind, int index)
        {
            if (schema is OpenApiSchema concreteSchema && !string.IsNullOrWhiteSpace(concreteSchema.Title))
                return $"{contextName} {concreteSchema.Title}";

            return $"{contextName} {compositionKind} {index}";
        }

        do
        {
            changed = false;
            var visited = new HashSet<IOpenApiSchema>(ReferenceEqualityComparer<IOpenApiSchema>.Instance);

            foreach ((string schemaName, IOpenApiSchema schema) in comps.ToList())
                VisitSchema(schema, schemaName, visited);
        }
        while (changed);
    }

    private void NormalizeSingletonStringConstsAsEnums(OpenApiDocument document)
    {
        var visited = new HashSet<IOpenApiSchema>(ReferenceEqualityComparer<IOpenApiSchema>.Instance);
        var normalized = 0;

        void VisitSchema(IOpenApiSchema? schema)
        {
            if (schema is not OpenApiSchema concreteSchema || !visited.Add(schema))
                return;

            if (concreteSchema.Const is { } value && concreteSchema.Enum is not { Count: > 0 })
            {
                concreteSchema.Enum = [JsonValue.Create(value)];
                concreteSchema.Const = null;
                normalized++;
            }

            if (concreteSchema.Properties != null)
                foreach (IOpenApiSchema property in concreteSchema.Properties.Values)
                    VisitSchema(property);

            VisitSchema(concreteSchema.Items);
            VisitSchema(concreteSchema.AdditionalProperties);

            if (concreteSchema.AllOf != null)
                foreach (IOpenApiSchema child in concreteSchema.AllOf)
                    VisitSchema(child);

            if (concreteSchema.AnyOf != null)
                foreach (IOpenApiSchema child in concreteSchema.AnyOf)
                    VisitSchema(child);

            if (concreteSchema.OneOf != null)
                foreach (IOpenApiSchema child in concreteSchema.OneOf)
                    VisitSchema(child);

            VisitSchema(concreteSchema.Not);
        }

        static void VisitContent(IDictionary<string, IOpenApiMediaType>? content, Action<IOpenApiSchema?> visitSchema)
        {
            if (content == null)
                return;

            foreach (IOpenApiMediaType mediaType in content.Values)
                visitSchema(mediaType?.Schema);
        }

        void VisitResponse(IOpenApiResponse? response)
        {
            if (response is not OpenApiResponse concreteResponse)
                return;

            VisitContent(concreteResponse.Content, VisitSchema);

            if (concreteResponse.Headers != null)
                foreach (IOpenApiHeader header in concreteResponse.Headers.Values)
                    VisitSchema(header?.Schema);
        }

        if (document.Components?.Schemas != null)
            foreach (IOpenApiSchema schema in document.Components.Schemas.Values)
                VisitSchema(schema);

        if (document.Components?.Parameters != null)
            foreach (IOpenApiParameter parameter in document.Components.Parameters.Values)
                VisitSchema(parameter?.Schema);

        if (document.Components?.Headers != null)
            foreach (IOpenApiHeader header in document.Components.Headers.Values)
                VisitSchema(header?.Schema);

        if (document.Components?.RequestBodies != null)
            foreach (IOpenApiRequestBody requestBody in document.Components.RequestBodies.Values)
                if (requestBody is OpenApiRequestBody concreteRequestBody)
                    VisitContent(concreteRequestBody.Content, VisitSchema);

        if (document.Components?.Responses != null)
            foreach (IOpenApiResponse response in document.Components.Responses.Values)
                VisitResponse(response);

        if (document.Paths != null)
        {
            foreach (IOpenApiPathItem pathItem in document.Paths.Values)
            {
                if (pathItem?.Parameters != null)
                    foreach (IOpenApiParameter parameter in pathItem.Parameters)
                        VisitSchema(parameter?.Schema);

                if (pathItem?.Operations == null)
                    continue;

                foreach (OpenApiOperation operation in pathItem.Operations.Values)
                {
                    if (operation?.Parameters != null)
                        foreach (IOpenApiParameter parameter in operation.Parameters)
                            VisitSchema(parameter?.Schema);

                    if (operation?.RequestBody is OpenApiRequestBody requestBody)
                        VisitContent(requestBody.Content, VisitSchema);

                    if (operation?.Responses != null)
                        foreach (IOpenApiResponse response in operation.Responses.Values)
                            VisitResponse(response);
                }
            }
        }

        if (normalized > 0)
            _logger.LogInformation("Normalized {Count} singleton string const schemas into Kiota-compatible enums", normalized);
    }

    private void ExtractInlineEnumSchemas(OpenApiDocument document)
    {
        IDictionary<string, IOpenApiSchema>? comps = document.Components?.Schemas;
        if (comps == null)
            return;

        var promotedSchemas = new Dictionary<IOpenApiSchema, string>(ReferenceEqualityComparer<IOpenApiSchema>.Instance);
        var visited = new HashSet<IOpenApiSchema>(ReferenceEqualityComparer<IOpenApiSchema>.Instance);

        bool TryPromote(IOpenApiSchema? schema, string baseName, string? roleName, out IOpenApiSchema replacement)
        {
            replacement = schema ?? new OpenApiSchema();

            if (schema is OpenApiSchemaReference || schema is not OpenApiSchema concreteSchema || concreteSchema.Enum is not { Count: > 0 })
                return false;

            if (!promotedSchemas.TryGetValue(concreteSchema, out string? componentName))
            {
                string semanticBaseName = BuildInlineEnumComponentName(concreteSchema, baseName, roleName);

                if (!string.Equals(semanticBaseName, baseName, StringComparison.Ordinal))
                {
                    string semanticComponentName = OpenApiNameNormalizer.NormalizeComponentName(semanticBaseName);

                    if (comps.TryGetValue(semanticComponentName, out IOpenApiSchema? existingSchema))
                    {
                        if (HaveEquivalentStringEnums(existingSchema, concreteSchema))
                        {
                            componentName = semanticComponentName;
                            promotedSchemas[concreteSchema] = componentName;
                            replacement = new OpenApiSchemaReference(componentName);
                            return true;
                        }

                        semanticBaseName = baseName;
                    }
                }

                string reservedName = ReserveUniqueSchemaName(comps, semanticBaseName, "Enum");
                componentName = AddComponentSchema(document, reservedName, concreteSchema);

                if (string.IsNullOrWhiteSpace(componentName))
                    return false;

                promotedSchemas[concreteSchema] = componentName;
                _logger.LogInformation("Promoted inline enum schema '{Context}' to components schema '{ComponentName}'", baseName, componentName);
            }

            replacement = new OpenApiSchemaReference(componentName);
            return true;
        }

        void VisitSchema(IOpenApiSchema? schema, string contextName)
        {
            if (schema is OpenApiSchemaReference || schema is not OpenApiSchema concreteSchema || !visited.Add(concreteSchema))
                return;

            if (concreteSchema.Properties != null)
            {
                foreach ((string propertyName, IOpenApiSchema propertySchema) in concreteSchema.Properties.ToList())
                {
                    string propertyContext = $"{contextName} {propertyName}";

                    if (TryPromote(propertySchema, propertyContext, propertyName, out IOpenApiSchema replacement))
                        concreteSchema.Properties[propertyName] = replacement;
                    else
                        VisitSchema(propertySchema, propertyContext);
                }
            }

            if (concreteSchema.Items != null)
            {
                string itemContext = $"{contextName} Item";

                if (TryPromote(concreteSchema.Items, itemContext, "Item", out IOpenApiSchema replacement))
                    concreteSchema.Items = replacement;
                else
                    VisitSchema(concreteSchema.Items, itemContext);
            }

            if (concreteSchema.AdditionalProperties != null)
            {
                string additionalPropertiesContext = $"{contextName} AdditionalProperties";

                if (TryPromote(concreteSchema.AdditionalProperties, additionalPropertiesContext, "AdditionalProperties", out IOpenApiSchema replacement))
                    concreteSchema.AdditionalProperties = replacement;
                else
                    VisitSchema(concreteSchema.AdditionalProperties, additionalPropertiesContext);
            }

            PromoteCompositionBranches(concreteSchema.AllOf, $"{contextName} AllOf");
            PromoteCompositionBranches(concreteSchema.AnyOf, $"{contextName} AnyOf");
            PromoteCompositionBranches(concreteSchema.OneOf, $"{contextName} OneOf");
        }

        void PromoteCompositionBranches(IList<IOpenApiSchema>? branches, string contextName)
        {
            if (branches == null)
                return;

            for (var i = 0; i < branches.Count; i++)
            {
                string branchContext = $"{contextName} {i + 1}";

                if (TryPromote(branches[i], branchContext, null, out IOpenApiSchema replacement))
                    branches[i] = replacement;
                else
                    VisitSchema(branches[i], branchContext);
            }
        }

        void VisitParameter(IOpenApiParameter? parameter, string contextName)
        {
            if (parameter is not OpenApiParameter concreteParameter || concreteParameter.Schema == null)
                return;

            if (TryPromote(concreteParameter.Schema, contextName, concreteParameter.Name, out IOpenApiSchema replacement))
                concreteParameter.Schema = replacement;
            else
                VisitSchema(concreteParameter.Schema, contextName);
        }

        foreach ((string schemaName, IOpenApiSchema schema) in comps.ToList())
            VisitSchema(schema, schemaName);

        if (document.Components?.Parameters != null)
        {
            foreach ((string parameterName, IOpenApiParameter parameter) in document.Components.Parameters)
                VisitParameter(parameter, parameterName);
        }

        if (document.Components?.Headers != null)
        {
            foreach ((string headerName, IOpenApiHeader header) in document.Components.Headers)
                VisitSchema(header?.Schema, headerName);
        }

        if (document.Paths == null)
            return;

        foreach ((string path, IOpenApiPathItem pathItem) in document.Paths)
        {
            if (pathItem?.Parameters != null)
            {
                foreach (IOpenApiParameter parameter in pathItem.Parameters)
                    VisitParameter(parameter, $"{path} {parameter.Name ?? "Parameter"}");
            }

            if (pathItem?.Operations == null)
                continue;

            foreach ((HttpMethod method, OpenApiOperation operation) in pathItem.Operations)
            {
                string operationContext = OpenApiNameNormalizer.NormalizeOperationId(operation.OperationId, method, path);

                if (operation?.Parameters != null)
                {
                    foreach (IOpenApiParameter parameter in operation.Parameters)
                        VisitParameter(parameter, $"{operationContext} {parameter.Name ?? "Parameter"} Parameter");
                }
            }
        }
    }

    private static string BuildInlineEnumComponentName(OpenApiSchema schema, string contextName, string? roleName)
    {
        if (string.IsNullOrWhiteSpace(roleName) || schema.Enum is not { Count: 1 } || schema.Enum[0] is not JsonValue enumValue ||
            !enumValue.TryGetValue(out string? wireValue) || string.IsNullOrWhiteSpace(wireValue))
            return contextName;

        string valueName = BuildSafeEnumMemberName(wireValue);
        return OpenApiNameNormalizer.NormalizeComponentName($"{valueName} {roleName}");
    }

    private static bool HaveEquivalentStringEnums(IOpenApiSchema left, OpenApiSchema right)
    {
        if (left is not OpenApiSchema leftSchema || leftSchema.Enum is not { Count: > 0 } || right.Enum is not { Count: > 0 } ||
            leftSchema.Enum.Count != right.Enum.Count)
            return false;

        for (var i = 0; i < leftSchema.Enum.Count; i++)
        {
            if (leftSchema.Enum[i] is not JsonValue leftValue || right.Enum[i] is not JsonValue rightValue ||
                !leftValue.TryGetValue(out string? leftText) || !rightValue.TryGetValue(out string? rightText) ||
                !string.Equals(leftText, rightText, StringComparison.Ordinal))
                return false;
        }

        return true;
    }

    private static string ReserveUniqueSchemaName(IDictionary<string, IOpenApiSchema> comps, string baseName, string fallbackSuffix)
    {
        return OpenApiNameNormalizer.ReserveComponentName(comps.Keys, baseName, fallbackSuffix);
    }

}
