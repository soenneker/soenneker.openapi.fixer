using Microsoft.Extensions.Logging;
using Microsoft.OpenApi;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text.RegularExpressions;
using Soenneker.OpenApi.Fixer;
using Soenneker.OpenApi.Fixer.Fixers.Abstract;

namespace Soenneker.OpenApi.Fixer.Fixers;

/// <summary>
/// Provides functionality to validate, sanitize, and normalize names and identifiers in OpenAPI documents,
/// including schema names, operation IDs, and path names.
/// </summary>
public sealed class OpenApiNamingFixer : IOpenApiNamingFixer
{
    private static readonly Regex _pathDateSuffixRegex = new(@"_(?:19|20)\d{2}-\d{2}(?=/|$)", RegexOptions.Compiled | RegexOptions.CultureInvariant);
    private static readonly Regex _operationIdDateSuffixRegex = new(@"-(?:19|20)\d{2}-\d{2}(?=-|$)", RegexOptions.Compiled | RegexOptions.CultureInvariant);
    private static readonly Regex _schemaDateSuffixRegex = new(@"(?<=[A-Za-z])(?:19|20)\d{4}(?=[A-Z_]|$)", RegexOptions.Compiled | RegexOptions.CultureInvariant);

    private readonly ILogger<OpenApiNamingFixer> _logger;
    private readonly IOpenApiReferenceFixer _referenceFixer;

    public OpenApiNamingFixer(ILogger<OpenApiNamingFixer> logger, IOpenApiReferenceFixer referenceFixer)
    {
        _logger = logger;
        _referenceFixer = referenceFixer;
    }

    private void NormalizeComponentSchemaKeys(OpenApiDocument document, string reason)
    {
        IDictionary<string, IOpenApiSchema>? schemas = document.Components?.Schemas;
        if (schemas == null || schemas.Count == 0)
            return;

        var mapping = new Dictionary<string, string>(StringComparer.Ordinal);
        var normalizedSchemas = new Dictionary<string, IOpenApiSchema>(StringComparer.Ordinal);
        var usedNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (string key in schemas.Keys.ToList())
        {
            string normalized = OpenApiNameNormalizer.NormalizeComponentName(key);
            string uniqueName = OpenApiNameNormalizer.MakeUniqueIdentifier(normalized, usedNames);
            usedNames.Add(uniqueName);
            normalizedSchemas[uniqueName] = schemas[key];

            if (!string.Equals(key, uniqueName, StringComparison.Ordinal))
            {
                mapping[key] = uniqueName;
                _logger.LogDebug("Renamed schema '{OldName}' to '{NewName}' during {Reason}.", key, uniqueName, reason);
            }
        }

        if (mapping.Count == 0)
            return;

        schemas.Clear();

        foreach ((string key, IOpenApiSchema schema) in normalizedSchemas)
        {
            schemas[key] = schema;
        }

        _referenceFixer.UpdateAllReferences(document, mapping);
        _logger.LogInformation("Normalized {SchemaRenameCount} component schema names during {Reason}.", mapping.Count, reason);
    }

    public void RenameInvalidComponentSchemas(OpenApiDocument document)
    {
        NormalizeComponentSchemaKeys(document, "initial component schema normalization");
    }

    /// <inheritdoc />
    public void ValidateAndFixSchemaNames(OpenApiDocument doc)
    {
        NormalizeComponentSchemaKeys(doc, "final component schema validation");
    }

    public void EnsureUniqueOperationIds(OpenApiDocument doc)
    {
        if (doc.Paths == null)
            return;

        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach ((string pathKey, var pathItem) in doc.Paths)
        {
            if (pathItem?.Operations == null)
                continue;
            foreach ((HttpMethod method, OpenApiOperation operation) in pathItem.Operations)
            {
                string baseId = OpenApiNameNormalizer.NormalizeOperationId(operation.OperationId, method, pathKey);
                string unique = baseId;
                var i = 2;

                while (!seen.Add(unique))
                {
                    unique = $"{baseId}{i++}";
                }

                if (!string.Equals(operation.OperationId, unique, StringComparison.Ordinal))
                {
                    _logger.LogDebug("Normalized operationId from '{OldOperationId}' to '{NewOperationId}'", operation.OperationId ?? "(missing)",
                        unique);
                    operation.OperationId = unique;
                }
            }
        }
    }

    /// <inheritdoc />
    public void NormalizeOperationIds(OpenApiDocument doc)
    {
        if (doc.Paths == null)
            return;

        foreach ((string pathKey, IOpenApiPathItem path) in doc.Paths)
        {
            if (path?.Operations == null)
                continue;

            foreach ((HttpMethod method, OpenApiOperation op) in path.Operations)
            {
                if (op == null)
                    continue;

                string normalized = OpenApiNameNormalizer.NormalizeOperationId(op.OperationId, method, pathKey);

                if (!string.Equals(normalized, op.OperationId, StringComparison.Ordinal))
                    op.OperationId = normalized;
            }
        }
    }

    public void ResolveSchemaOperationNameCollisions(OpenApiDocument doc)
    {
        if (doc.Components?.Schemas == null || doc.Paths == null)
            return;

        var operationIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach (IOpenApiPathItem pathItem in doc.Paths.Values)
        {
            if (pathItem?.Operations == null)
                continue;

            foreach (OpenApiOperation operation in pathItem.Operations.Values)
            {
                if (!string.IsNullOrWhiteSpace(operation.OperationId))
                    operationIds.Add(operation.OperationId!);
            }
        }

        if (!operationIds.Any())
            return;

        var mapping = new Dictionary<string, string>(StringComparer.Ordinal);
        var reservedNames = new HashSet<string>(doc.Components.Schemas.Keys, StringComparer.OrdinalIgnoreCase);

        // Use ToList() to create a copy of the keys, allowing modification of the collection during iteration.
        foreach (string key in doc.Components.Schemas.Keys.ToList())
        {
            // Check if the schema name (case-insensitive) collides with any operationId
            if (!operationIds.Contains(key))
                continue;

            // A collision exists. We must rename the schema.
            reservedNames.Remove(key);
            string newKey = OpenApiNameNormalizer.ReserveComponentName(reservedNames, $"{key}Body", "Schema");
            reservedNames.Add(newKey);

            mapping[key] = newKey;
            _logger.LogWarning("Schema name '{OldKey}' conflicts with an operationId. Renaming schema to '{NewKey}'.", key, newKey);
        }

        if (mapping.Any())
        {
            foreach ((string oldKey, string newKey) in mapping)
            {
                if (doc.Components.Schemas.Remove(oldKey, out IOpenApiSchema? schema))
                {
                    // In v2.3, Title is read-only, so we can't modify it directly
                    doc.Components.Schemas[newKey] = schema;
                }
            }

            // After all renames are done, update all references throughout the entire document.
            _logger.LogInformation("Applying global reference updates for operationId/schema name collisions...");
            _referenceFixer.UpdateAllReferences(doc, mapping);
        }
    }

    /// <inheritdoc />
    public void RenameConflictingPaths(OpenApiDocument doc)
    {
        if (doc.Paths == null || !doc.Paths.Any())
        {
            _logger.LogInformation("Document contains no paths to process in RenameConflictingPaths. Skipping.");
            return;
        }

        var newPaths = new OpenApiPaths();
        var signatureToCanonicalPath = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (KeyValuePair<string, IOpenApiPathItem> kvp in doc.Paths)
        {
            string originalPath = kvp.Key;
            string newPath = originalPath;

            if (originalPath.Contains("/accounts/{account_id}/addressing/address_maps/{address_map_id}/accounts/{account_id}"))
            {
                newPath = originalPath.Replace("/accounts/{account_id}/addressing/address_maps/{address_map_id}/accounts/{account_id}",
                    "/accounts/{account_id}/addressing/address_maps/{address_map_id}/accounts/{member_account_id}");

                if (kvp.Value.Operations != null)
                {
                    foreach (OpenApiOperation? operation in kvp.Value.Operations.Values)
                    {
                        if (operation.Parameters == null)
                        {
                            operation.Parameters = new List<IOpenApiParameter>();
                        }

                        bool hasAccountId = operation.Parameters.Any(p => p?.Name == "account_id" && p?.In == ParameterLocation.Path);
                        bool hasMemberAccountId = operation.Parameters.Any(p => p?.Name == "member_account_id" && p?.In == ParameterLocation.Path);

                        if (!hasAccountId)
                        {
                            operation.Parameters.Add(new OpenApiParameter
                            {
                                Name = "account_id",
                                In = ParameterLocation.Path,
                                Required = true,
                                Schema = new OpenApiSchema
                                {
                                    Type = JsonSchemaType.String,
                                    MaxLength = 32,
                                    Description = "Identifier of a Cloudflare account."
                                }
                            });
                        }

                        if (!hasMemberAccountId)
                        {
                            operation.Parameters.Add(new OpenApiParameter
                            {
                                Name = "member_account_id",
                                In = ParameterLocation.Path,
                                Required = true,
                                Schema = new OpenApiSchema
                                {
                                    Type = JsonSchemaType.String,
                                    MaxLength = 32,
                                    Description = "Identifier of the member account to add/remove from the Address Map."
                                }
                            });
                        }

                        foreach (IOpenApiParameter? param in operation.Parameters)
                        {
                            if (param?.Name == "member_account_id" && param?.In == ParameterLocation.Path)
                            {
                                // In v2.3, Schema is read-only, so we can't modify it directly
                                // We'll handle this in a different way if needed
                                _logger.LogDebug("Would update schema description for parameter 'member_account_id'");
                            }
                        }
                    }
                }
            }
            else if (originalPath.EndsWith("/item", StringComparison.OrdinalIgnoreCase))
            {
                newPath = originalPath.Replace("/item", "/item_static");
            }
            else if (originalPath.Contains("/item/{", StringComparison.OrdinalIgnoreCase))
            {
                newPath = originalPath.Replace("/item", "/item_by_id");
            }

            newPath = NormalizePathSegments(newPath);
            string parameterNormalizedPath = NormalizePathParameterNames(newPath, out Dictionary<string, string> parameterRenameMap);

            if (parameterRenameMap.Count > 0)
            {
                RenamePathParameters(kvp.Value, parameterRenameMap);
                newPath = parameterNormalizedPath;
            }

            if (newPaths.TryGetValue(newPath, out IOpenApiPathItem? existingPathItem))
            {
                MergePathItems(existingPathItem, kvp.Value, originalPath, newPath);
                continue;
            }

            string pathSignature = NormalizePathSignature(newPath);

            if (signatureToCanonicalPath.TryGetValue(pathSignature, out string? canonicalPath) && newPaths.TryGetValue(canonicalPath, out IOpenApiPathItem? canonicalPathItem))
            {
                RenamePathParametersToCanonicalNames(kvp.Value, newPath, canonicalPath);
                MergePathItems(canonicalPathItem, kvp.Value, originalPath, canonicalPath);
                continue;
            }

            newPaths.Add(newPath, kvp.Value);
            signatureToCanonicalPath[pathSignature] = newPath;
        }

        doc.Paths = newPaths;
    }

    /// <inheritdoc />
    public void StripDateSuffixesFromGeneratedNames(OpenApiDocument doc)
    {
        StripDateSuffixesFromPathPrefixes(doc);
        StripDateSuffixesFromOperationIds(doc);
        StripDateSuffixesFromSchemaNames(doc);
    }

    private void StripDateSuffixesFromPathPrefixes(OpenApiDocument doc)
    {
        if (doc.Paths == null || doc.Paths.Count == 0)
            return;

        var newPaths = new OpenApiPaths();

        foreach ((string path, IOpenApiPathItem pathItem) in doc.Paths)
        {
            string normalizedPath = _pathDateSuffixRegex.Replace(path, "");

            if (newPaths.ContainsKey(normalizedPath))
                throw new InvalidOperationException($"Cannot strip date suffix from path '{path}' because '{normalizedPath}' already exists.");

            newPaths[normalizedPath] = pathItem;

            if (!string.Equals(path, normalizedPath, StringComparison.Ordinal))
                _logger.LogInformation("Stripped date suffix from path '{Path}' to '{NormalizedPath}'", path, normalizedPath);
        }

        doc.Paths = newPaths;
    }

    private void StripDateSuffixesFromOperationIds(OpenApiDocument doc)
    {
        if (doc.Paths == null)
            return;

        foreach (IOpenApiPathItem pathItem in doc.Paths.Values)
        {
            if (pathItem?.Operations == null)
                continue;

            foreach (OpenApiOperation operation in pathItem.Operations.Values)
            {
                if (string.IsNullOrWhiteSpace(operation.OperationId))
                    continue;

                string normalizedOperationId = _operationIdDateSuffixRegex.Replace(operation.OperationId!, "");

                if (!string.Equals(operation.OperationId, normalizedOperationId, StringComparison.Ordinal))
                {
                    _logger.LogInformation("Stripped date suffix from operationId '{OperationId}' to '{NormalizedOperationId}'", operation.OperationId,
                        normalizedOperationId);
                    operation.OperationId = normalizedOperationId;
                }
            }
        }
    }

    private void StripDateSuffixesFromSchemaNames(OpenApiDocument doc)
    {
        IDictionary<string, IOpenApiSchema>? schemas = doc.Components?.Schemas;
        if (schemas == null || schemas.Count == 0)
            return;

        var mapping = new Dictionary<string, string>(StringComparer.Ordinal);
        var reservedNames = new HashSet<string>(schemas.Keys, StringComparer.OrdinalIgnoreCase);

        foreach (string key in schemas.Keys.ToList())
        {
            string normalizedName = _schemaDateSuffixRegex.Replace(key, "");

            if (string.Equals(key, normalizedName, StringComparison.Ordinal))
                continue;

            reservedNames.Remove(key);

            if (reservedNames.Contains(normalizedName))
                throw new InvalidOperationException($"Cannot strip date suffix from schema '{key}' because '{normalizedName}' already exists.");

            mapping[key] = normalizedName;
            reservedNames.Add(normalizedName);
        }

        if (mapping.Count == 0)
            return;

        foreach ((string oldKey, string newKey) in mapping)
        {
            IOpenApiSchema schema = schemas[oldKey];
            schemas.Remove(oldKey);
            schemas[newKey] = schema;
            _logger.LogInformation("Stripped date suffix from schema '{OldName}' to '{NewName}'", oldKey, newKey);
        }

        _referenceFixer.UpdateAllReferences(doc, mapping);
    }

    private string NormalizePathSegments(string path)
    {
        if (string.IsNullOrWhiteSpace(path))
            return path;

        string normalized = path.Replace("/(-/)", "/", StringComparison.Ordinal)
                                .Replace("/-/", "/", StringComparison.Ordinal);

        while (normalized.Contains("//", StringComparison.Ordinal))
        {
            normalized = normalized.Replace("//", "/", StringComparison.Ordinal);
        }

        if (normalized.Length > 1)
            normalized = normalized.TrimEnd('/');

        if (!string.Equals(path, normalized, StringComparison.Ordinal))
        {
            _logger.LogInformation("Canonicalized path '{OriginalPath}' to '{NormalizedPath}'", path, normalized);
        }

        return normalized;
    }

    private static string NormalizePathSignature(string path) => Regex.Replace(path, @"\{[^/]+\}", "{}", RegexOptions.CultureInvariant);

    private void RenamePathParametersToCanonicalNames(IOpenApiPathItem pathItem, string incomingPath, string canonicalPath)
    {
        string[] incomingNames = GetPathParameterNames(incomingPath);
        string[] canonicalNames = GetPathParameterNames(canonicalPath);

        if (incomingNames.Length != canonicalNames.Length)
            return;

        Dictionary<string, string> renameMap = incomingNames.Zip(canonicalNames, static (incoming, canonical) => new { incoming, canonical })
                                                            .Where(pair => !string.Equals(pair.incoming, pair.canonical, StringComparison.Ordinal))
                                                            .ToDictionary(pair => pair.incoming, pair => pair.canonical, StringComparer.OrdinalIgnoreCase);

        if (renameMap.Count == 0)
            return;

        _logger.LogInformation("Merging same-signature path '{IncomingPath}' into canonical path '{CanonicalPath}'", incomingPath, canonicalPath);

        if (pathItem.Parameters != null)
        {
            foreach (IOpenApiParameter parameter in pathItem.Parameters)
            {
                RenamePathParameter(parameter, renameMap);
            }
        }

        if (pathItem.Operations == null)
            return;

        foreach (OpenApiOperation operation in pathItem.Operations.Values)
        {
            if (operation?.Parameters == null)
                continue;

            foreach (IOpenApiParameter parameter in operation.Parameters)
            {
                RenamePathParameter(parameter, renameMap);
            }
        }
    }

    private static string[] GetPathParameterNames(string path) => Regex.Matches(path, @"\{([^/]+)\}", RegexOptions.CultureInvariant)
                                                                     .Select(match => match.Groups[1].Value)
                                                                     .ToArray();

    private string NormalizePathParameterNames(string path, out Dictionary<string, string> renameMap)
    {
        var localRenameMap = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        if (string.IsNullOrWhiteSpace(path))
        {
            renameMap = localRenameMap;
            return path;
        }

        var usedNames = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var normalizedNames = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);

        string NormalizeParameterName(string originalName)
        {
            if (normalizedNames.TryGetValue(originalName, out string? existingName))
                return existingName;

            string pascalName = OpenApiNameNormalizer.NormalizeNamePart(originalName, "Parameter");
            string baseName = char.ToLowerInvariant(pascalName[0]) + pascalName[1..];
            string candidate = baseName;
            var suffix = 2;

            while (usedNames.Contains(candidate))
                candidate = $"{baseName}{suffix++}";

            usedNames.Add(candidate);
            normalizedNames[originalName] = candidate;

            if (!string.Equals(originalName, candidate, StringComparison.Ordinal))
                localRenameMap[originalName] = candidate;

            return candidate;
        }

        string normalizedPath = Regex.Replace(path, @"\{([^/{}]+)\}", match => $"{{{NormalizeParameterName(match.Groups[1].Value)}}}",
            RegexOptions.CultureInvariant);

        renameMap = localRenameMap;

        if (renameMap.Count > 0)
            _logger.LogInformation("Normalized path parameter names in '{Path}' to '{NormalizedPath}'", path, normalizedPath);

        return normalizedPath;
    }

    private static void RenamePathParameters(IOpenApiPathItem pathItem, IReadOnlyDictionary<string, string> renameMap)
    {
        if (renameMap.Count == 0)
            return;

        if (pathItem.Parameters != null)
        {
            foreach (IOpenApiParameter parameter in pathItem.Parameters)
                RenamePathParameter(parameter, renameMap);
        }

        if (pathItem.Operations == null)
            return;

        foreach (OpenApiOperation operation in pathItem.Operations.Values)
        {
            if (operation?.Parameters == null)
                continue;

            foreach (IOpenApiParameter parameter in operation.Parameters)
                RenamePathParameter(parameter, renameMap);
        }
    }

    private static void RenamePathParameter(IOpenApiParameter parameter, IReadOnlyDictionary<string, string> renameMap)
    {
        if (parameter is not OpenApiParameter concreteParameter || concreteParameter.In != ParameterLocation.Path || string.IsNullOrWhiteSpace(concreteParameter.Name))
            return;

        if (renameMap.TryGetValue(concreteParameter.Name, out string? canonicalName))
        {
            concreteParameter.Name = canonicalName;
        }
    }

    private void MergePathItems(IOpenApiPathItem existingPathItem, IOpenApiPathItem incomingPathItem, string originalPath, string normalizedPath)
    {
        if (existingPathItem is not OpenApiPathItem existingConcrete || incomingPathItem is not OpenApiPathItem incomingConcrete)
        {
            _logger.LogWarning("Unable to merge normalized path '{NormalizedPath}' from '{OriginalPath}' because one of the path items is not mutable.", normalizedPath,
                originalPath);
            return;
        }

        if (incomingConcrete.Operations != null)
        {
            existingConcrete.Operations ??= new Dictionary<HttpMethod, OpenApiOperation>();

            foreach ((HttpMethod method, OpenApiOperation operation) in incomingConcrete.Operations)
            {
                if (existingConcrete.Operations.ContainsKey(method))
                {
                    _logger.LogInformation("Dropping duplicate {Method} operation from aliased path '{OriginalPath}' after normalization to '{NormalizedPath}'", method,
                        originalPath, normalizedPath);
                    continue;
                }

                existingConcrete.Operations[method] = operation;
            }
        }

        if (incomingConcrete.Parameters != null)
        {
            existingConcrete.Parameters ??= new List<IOpenApiParameter>();

            foreach (IOpenApiParameter parameter in incomingConcrete.Parameters)
            {
                bool alreadyExists = existingConcrete.Parameters.Any(existing =>
                    existing?.In == parameter?.In &&
                    string.Equals(existing?.Name, parameter?.Name, StringComparison.OrdinalIgnoreCase));

                if (!alreadyExists)
                {
                    existingConcrete.Parameters.Add(parameter);
                }
            }
        }
    }

    /// <inheritdoc />
    public string SanitizeName(string input)
    {
        return OpenApiNameNormalizer.NormalizeComponentName(input, "Schema");
    }

    /// <summary>
    /// Normalizes operationIds by removing parentheses and collapsing punctuation to single dashes/underscores.
    /// Ensures result is non-empty and starts with a letter.
    /// </summary>
    /// <inheritdoc />
    public string NormalizeOperationId(string input)
    {
        return OpenApiNameNormalizer.NormalizeOperationId(input, null, null);
    }

    /// <inheritdoc />
    public bool IsValidIdentifier(string id) => OpenApiNameNormalizer.IsValidCSharpIdentifier(id);

    /// <inheritdoc />
    public string GenerateSafePart(string? input, string fallback = "unnamed")
    {
        return OpenApiNameNormalizer.NormalizeNamePart(input, fallback);
    }

    /// <inheritdoc />
    public string ValidateComponentName(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            _logger.LogWarning("Component name was empty, using fallback name");
        }

        return OpenApiNameNormalizer.NormalizeComponentName(name);
    }
}
