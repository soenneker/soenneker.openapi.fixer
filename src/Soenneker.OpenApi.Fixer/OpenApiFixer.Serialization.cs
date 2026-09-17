using Microsoft.Extensions.Logging;
using Microsoft.OpenApi;
using Microsoft.OpenApi.Reader;
using Soenneker.Extensions.Task;
using Soenneker.Extensions.ValueTask;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static string NormalizeMediaType(string mediaType)
    {
        if (string.IsNullOrWhiteSpace(mediaType))
            return "application/json";
        string trimmed = mediaType.Trim();
        int separator = trimmed.IndexOf(';');
        string baseType = separator >= 0 ? trimmed[..separator].Trim() : trimmed;
        if (!baseType.Contains('/'))
            return "application/json";
        // Media ranges and parameters are part of the contract. Distinct profiles/versions can
        // describe different payloads; dropping the suffix silently discards an entire variant.
        return trimmed;
    }

    private Dictionary<string, IOpenApiMediaType> NormalizeMediaTypes(IDictionary<string, IOpenApiMediaType> content)
    {
        var normalized = new Dictionary<string, IOpenApiMediaType>(StringComparer.Ordinal);

        foreach (KeyValuePair<string, IOpenApiMediaType> entry in content)
        {
            if (entry.Key == null || entry.Value == null)
                continue;

            string normalizedKey = NormalizeMediaType(entry.Key);

            if (normalized.TryGetValue(normalizedKey, out IOpenApiMediaType? existing))
            {
                normalized[normalizedKey] = SelectPreferredMediaType(existing, entry.Value, normalizedKey, entry.Key);
                continue;
            }

            normalized[normalizedKey] = entry.Value;
        }

        return normalized;
    }

    private IOpenApiMediaType SelectPreferredMediaType(IOpenApiMediaType existing, IOpenApiMediaType candidate, string normalizedKey, string originalKey)
    {
        bool existingEmpty = IsMediaEmpty(existing);
        bool candidateEmpty = IsMediaEmpty(candidate);

        if (existingEmpty && !candidateEmpty)
        {
            _logger.LogDebug("Replacing empty media type '{MediaType}' while normalizing duplicate '{OriginalMediaType}'.", normalizedKey, originalKey);
            return candidate;
        }

        if (!existingEmpty && candidateEmpty)
        {
            _logger.LogDebug("Keeping non-empty media type '{MediaType}' while skipping duplicate '{OriginalMediaType}'.", normalizedKey, originalKey);
            return existing;
        }

        _logger.LogDebug("Keeping first normalized media type '{MediaType}' and skipping duplicate '{OriginalMediaType}'.", normalizedKey, originalKey);
        return existing;
    }

    private static bool IsMediaEmpty(IOpenApiMediaType media)
    {
        IOpenApiSchema? s = media.Schema;
        bool schemaEmpty = s == null || (s.Type == null && (s.Properties == null || !s.Properties.Any()) && s.Items == null &&
                                         (s.AllOf == null || !s.AllOf.Any()) && (s.AnyOf == null || !s.AnyOf.Any()) && (s.OneOf == null || !s.OneOf.Any()));
        bool hasExample = s?.Example != null || (media.Examples?.Any() == true);
        return schemaEmpty && !hasExample;
    }

    private void EnsureResponseDescriptions(OpenApiResponses responses)
    {
        foreach (KeyValuePair<string, IOpenApiResponse> kv in responses)
        {
            string code = kv.Key;
            IOpenApiResponse resp = kv.Value;
            if (resp is OpenApiResponse concreteResp && string.IsNullOrWhiteSpace(concreteResp.Description))
            {
                concreteResp.Description = code == "default" ? "Default response" : $"{code} response";
            }
        }
    }

    private void EnsureValidResponses(OpenApiDocument document)
    {
        if (document.Components?.Responses != null)
        {
            foreach (IOpenApiResponse response in document.Components.Responses.Values)
            {
                if (response is OpenApiResponse concreteResponse && string.IsNullOrWhiteSpace(concreteResponse.Description))
                    concreteResponse.Description = "Response";
            }
        }

        if (document.Paths == null)
            return;

        foreach (IOpenApiPathItem pathItem in document.Paths.Values)
        {
            if (pathItem.Operations == null)
                continue;

            foreach ((HttpMethod method, OpenApiOperation operation) in pathItem.Operations)
            {
                if (operation.Responses == null || operation.Responses.Count == 0)
                    operation.Responses = CreateFallbackResponses(method);

                EnsureResponseDescriptions(operation.Responses);
            }
        }
    }

    private async ValueTask ReadAndValidateOpenApi(string filePath, CancellationToken cancellationToken)
    {
        // Validate exactly the bytes that will replace the target, without repairing a different copy.
        string json = await _fileUtil.Read(filePath, cancellationToken: cancellationToken);
        await using MemoryStream stream = await _memoryStreamUtil.Get(json, cancellationToken).NoSync();

        var reader = new OpenApiJsonReader(); // force JSON
        ReadResult read = await reader.ReadAsync(stream, new Uri(Path.GetFullPath(filePath)), // base URI for relative $refs
                                          new OpenApiReaderSettings(), cancellationToken)
                                      .NoSync();

        OpenApiDiagnostic? diagnostics = read.Diagnostic;
        if (read.Document == null)
            throw new InvalidOperationException($"Unable to parse OpenAPI document '{Path.GetFileName(filePath)}'.");

        if (diagnostics?.Errors?.Any() == true)
        {
            string messages = string.Join("; ", diagnostics.Errors.Select(error => error.Message));
            _logger.LogWarning("OpenAPI parsing errors in {File}: {Msgs}", Path.GetFileName(filePath),
                messages);

            throw new InvalidOperationException($"The fixed OpenAPI document is invalid: {messages}");
        }
    }


    /// <summary>
    /// Converts schemas that declare boolean type with enum constraints into plain booleans,
    /// and assigns a default when the enum contains a single boolean value.
    /// Example to normalize:
    ///   { "type": "boolean", "enum": [ true ] } -> { "type": "boolean", "default": true }
    ///   { "type": "boolean", "enum": [ true, false ] } -> { "type": "boolean" }
    /// This also handles cases where type is null but all enum values are booleans.
    /// </summary>
    private void NormalizeBooleanEnums(OpenApiDocument doc)
    {
        if (doc.Components?.Schemas == null)
            return;

        var visited = new HashSet<IOpenApiSchema>();

        void Visit(IOpenApiSchema? s)
        {
            if (s is not OpenApiSchema os)
                return;
            if (!visited.Add(os))
                return;

            if (os.Enum is { Count: > 0 })
            {
                bool allBoolean = os.Enum.All(e => e is JsonValue jv && (jv.GetValueKind() == JsonValueKind.True || jv.GetValueKind() == JsonValueKind.False));

                if (allBoolean)
                {
                    // Ensure type is boolean
                    os.Type = JsonSchemaType.Boolean;

                    // If only a single enum entry, set it as default
                    if (os.Enum.Count == 1)
                    {
                        os.Default = os.Enum[0];
                    }

                    // Drop enum to avoid CodeEnum generation on booleans
                    os.Enum = null;

                    // Make sure no object facets linger
                    os.Properties = null;
                    os.AdditionalProperties = null;
                    os.AdditionalPropertiesAllowed = false;
                }
            }

            if (os.Properties != null)
            {
                foreach (IOpenApiSchema child in os.Properties.Values)
                    Visit(child);
            }

            if (os.Items != null)
                Visit(os.Items);
            if (os.AllOf != null)
                foreach (IOpenApiSchema c in os.AllOf)
                    Visit(c);
            if (os.AnyOf != null)
                foreach (IOpenApiSchema c in os.AnyOf)
                    Visit(c);
            if (os.OneOf != null)
                foreach (IOpenApiSchema c in os.OneOf)
                    Visit(c);
            if (os.AdditionalProperties != null)
                Visit(os.AdditionalProperties);
        }

        foreach (IOpenApiSchema s in doc.Components.Schemas.Values)
            Visit(s);
    }


    private static string FixJsonBooleanValues(string json)
    {
        StringBuilder? builder = null;
        bool inString = false;
        bool escaped = false;

        for (var i = 0; i < json.Length; i++)
        {
            char current = json[i];

            if (inString)
            {
                if (escaped)
                    escaped = false;
                else if (current == '\\')
                    escaped = true;
                else if (current == '"')
                    inString = false;

                continue;
            }

            if (current == '"')
            {
                inString = true;
                continue;
            }

            int tokenLength = current == 'T' && json.AsSpan(i).StartsWith("True", StringComparison.Ordinal) ? 4
                : current == 'F' && json.AsSpan(i).StartsWith("False", StringComparison.Ordinal) ? 5 : 0;

            if (tokenLength == 0 || !IsJsonValueToken(json, i, tokenLength))
                continue;

            builder ??= new StringBuilder(json);
            builder[i] = char.ToLowerInvariant(current);
            i += tokenLength - 1;
        }

        return builder?.ToString() ?? json;
    }

    private static bool IsJsonValueToken(string json, int start, int length)
    {
        int previous = start - 1;
        while (previous >= 0 && char.IsWhiteSpace(json[previous]))
            previous--;

        if (previous < 0 || json[previous] is not (':' or ',' or '['))
            return false;

        int next = start + length;
        while (next < json.Length && char.IsWhiteSpace(json[next]))
            next++;

        return next == json.Length || json[next] is ',' or ']' or '}';
    }

    private string NormalizeKiotaIncompatibleMultiTypes(string json)
    {
        if (string.IsNullOrWhiteSpace(json))
            return json;

        JsonNode? root;

        try
        {
            root = JsonNode.Parse(json);
        }
        catch (JsonException ex)
        {
            _logger.LogDebug(ex, "Unable to parse serialized OpenAPI JSON when normalizing multi-type schemas");
            return json;
        }

        if (root is null)
            return json;

        var normalized = 0;
        OpenApiJsonSchemaWalker.Visit(root, (schema, _) =>
        {
            if (!TryNormalizeKiotaIncompatibleMultiType(schema))
                return false;
            normalized++;
            return true;
        });
        int narrowed = NarrowKiotaIncompatibleMixedUnionAllOfReferences(root);

        if (normalized == 0 && narrowed == 0)
            return json;

        if (normalized > 0)
            _logger.LogInformation("Normalized {Count} multi-type schemas into Kiota-compatible anyOf constraints", normalized);

        if (narrowed > 0)
            _logger.LogInformation("Narrowed {Count} mixed-union allOf references to their object-compatible branches", narrowed);

        return root.ToJsonString(new JsonSerializerOptions { WriteIndented = true });
    }

    private static int NarrowKiotaIncompatibleMixedUnionAllOfReferences(JsonNode root)
    {
        if (root["components"]?["schemas"] is not JsonObject components)
            return 0;

        var narrowed = 0;
        OpenApiJsonSchemaWalker.Visit(root, (schema, _) =>
        {
            NarrowSchema(schema);
            return false;
        });
        return narrowed;

        void NarrowSchema(JsonObject schema)
        {
            if (schema["allOf"] is not JsonArray allOf || allOf.Count == 0)
                return;

            for (var i = 0; i < allOf.Count; i++)
            {
                if (allOf[i] is not JsonObject referenceBranch ||
                    !TryResolveComponentReference(referenceBranch, components, out JsonObject? referencedSchema) || referencedSchema is null)
                    continue;

                JsonArray? unionBranches = referencedSchema["oneOf"] as JsonArray ?? referencedSchema["anyOf"] as JsonArray;
                if (unionBranches is not { Count: > 1 })
                    continue;

                List<JsonObject> objectBranches = unionBranches.OfType<JsonObject>()
                                                                    .Where(branch => IsObjectCompatible(branch, components, []))
                                                                    .ToList();
                if (objectBranches.Count != 1 || objectBranches.Count == unionBranches.Count)
                    continue;

                bool surroundingAllOfRequiresObject = IsObjectCompatible(schema, components, []) ||
                                                      allOf.Where((_, index) => index != i)
                                                           .OfType<JsonObject>()
                                                           .Any(branch => IsObjectCompatible(branch, components, []));
                if (!surroundingAllOfRequiresObject)
                    continue;

                allOf[i] = objectBranches[0].DeepClone();
                narrowed++;
            }
        }
    }

    private static bool IsObjectCompatible(JsonObject schema, JsonObject components, HashSet<string> activeReferences)
    {
        if (schema["type"] is JsonValue typeValue && typeValue.TryGetValue(out string? type) && type == "object")
            return true;

        if (schema["type"] is JsonArray types && types.OfType<JsonValue>().Any(value => value.TryGetValue(out string? type) && type == "object"))
            return true;

        if (schema["properties"] is JsonObject || schema["additionalProperties"] is JsonObject)
            return true;

        if (!TryResolveComponentReference(schema, components, out JsonObject? referencedSchema, out string? referenceId) ||
            referencedSchema is null || referenceId is null || !activeReferences.Add(referenceId))
            return false;

        try
        {
            return IsObjectCompatible(referencedSchema, components, activeReferences);
        }
        finally
        {
            activeReferences.Remove(referenceId);
        }
    }

    private static bool TryResolveComponentReference(JsonObject schema, JsonObject components, out JsonObject? referencedSchema) =>
        TryResolveComponentReference(schema, components, out referencedSchema, out _);

    private static bool TryResolveComponentReference(JsonObject schema, JsonObject components, out JsonObject? referencedSchema, out string? referenceId)
    {
        referencedSchema = null;
        referenceId = null;

        if (schema["$ref"] is not JsonValue referenceValue || !referenceValue.TryGetValue(out string? reference) ||
            reference is null || !reference.StartsWith("#/components/schemas/", StringComparison.Ordinal))
            return false;

        referenceId = reference["#/components/schemas/".Length..].Replace("~1", "/", StringComparison.Ordinal).Replace("~0", "~", StringComparison.Ordinal);
        referencedSchema = components[referenceId] as JsonObject;
        return referencedSchema is not null;
    }

    private static bool TryNormalizeKiotaIncompatibleMultiType(JsonObject schema)
    {
        if (schema["type"] is not JsonArray types)
            return false;

        var distinctTypes = new List<string>(types.Count);

        foreach (JsonNode? node in types)
        {
            if (node is not JsonValue value || !value.TryGetValue(out string? type) || !IsJsonSchemaType(type))
                return false;

            if (!distinctTypes.Contains(type, StringComparer.Ordinal))
                distinctTypes.Add(type);
        }

        if (distinctTypes.Count(type => type != "null") <= 1)
            return false;

        var branches = new JsonArray(distinctTypes.Select(type => (JsonNode)new JsonObject { ["type"] = type }).ToArray());
        schema.Remove("type");

        if (schema["anyOf"] is null)
        {
            schema["anyOf"] = branches;
        }
        else
        {
            var typeConstraint = new JsonObject { ["anyOf"] = branches };

            if (schema["allOf"] is JsonArray allOf)
                allOf.Add(typeConstraint);
            else
                schema["allOf"] = new JsonArray(typeConstraint);
        }

        return true;
    }

    private static bool IsJsonSchemaType(string? type) => type is "null" or "boolean" or "object" or "array" or "number" or "string" or "integer";

}
