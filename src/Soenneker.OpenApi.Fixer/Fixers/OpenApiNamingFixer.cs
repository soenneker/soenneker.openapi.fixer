using Microsoft.Extensions.Logging;
using Microsoft.OpenApi;
using System;
using System.Buffers;
using System.Collections.Generic;
using System.Linq;
using System.Net.Http;
using System.Text.RegularExpressions;
using Soenneker.Utils.PooledStringBuilders;
using Soenneker.OpenApi.Fixer.Fixers.Abstract;

namespace Soenneker.OpenApi.Fixer.Fixers;

/// <summary>
/// Provides functionality to validate, sanitize, and normalize names and identifiers in OpenAPI documents,
/// including schema names, operation IDs, and path names.
/// </summary>
public sealed class OpenApiNamingFixer : IOpenApiNamingFixer
{
    private static readonly SearchValues<char> AllowedIdPunctuation = SearchValues.Create("_-.");

    private readonly ILogger<OpenApiNamingFixer> _logger;
    private readonly IOpenApiReferenceFixer _referenceFixer;

    public OpenApiNamingFixer(ILogger<OpenApiNamingFixer> logger, IOpenApiReferenceFixer referenceFixer)
    {
        _logger = logger;
        _referenceFixer = referenceFixer;
    }

    /// <inheritdoc />
    public void RenameInvalidComponentSchemas(OpenApiDocument document)
    {
        IDictionary<string, IOpenApiSchema>? schemas = document.Components?.Schemas;
        if (schemas == null)
            return;

        var mapping = new Dictionary<string, string>();
        var existingKeys = new HashSet<string>(schemas.Keys, StringComparer.OrdinalIgnoreCase);

        foreach (string key in schemas.Keys.ToList())
        {
            // Use strict validation suitable for Kiota: only [A-Za-z0-9_] and starts with a letter.
            // Also rename if our canonical validation would change the name.
            string validated = ValidateComponentName(key);
            bool needsRename = !IsValidIdentifier(key) || !string.Equals(validated, key, StringComparison.Ordinal);

            if (needsRename)
            {
                _logger.LogInformation("Found invalid schema name '{InvalidName}', sanitizing...", key);
                string baseName = validated;

                // Fallback to "Schema" if sanitization fails
                if (string.IsNullOrWhiteSpace(baseName))
                    baseName = "Schema";

                string newKey = baseName;
                var i = 1;

                // Ensure uniqueness deterministically
                while (existingKeys.Contains(newKey))
                {
                    newKey = $"{baseName}_{i++}";
                }

                mapping[key] = newKey;
                existingKeys.Add(newKey);
                _logger.LogInformation("Renamed schema '{OldName}' to '{NewName}'", key, newKey);
            }
        }

        foreach ((string oldKey, string newKey) in mapping)
        {
            IOpenApiSchema schema = schemas[oldKey];
            schemas.Remove(oldKey);

            // In v2.3, Title is read-only, so we can't modify it directly
            // The schema will keep its original title

            schemas[newKey] = schema;
        }

        if (mapping.Count > 0)
            _referenceFixer.UpdateAllReferences(document, mapping);
    }

    /// <inheritdoc />
    public void ValidateAndFixSchemaNames(OpenApiDocument doc)
    {
        IDictionary<string, IOpenApiSchema>? schemas = doc.Components?.Schemas;
        if (schemas == null)
            return;

        var mapping = new Dictionary<string, string>();
        var existingKeys = new HashSet<string>(schemas.Keys, StringComparer.OrdinalIgnoreCase);

        foreach (string key in schemas.Keys.ToList())
        {
            if (!IsValidIdentifier(key))
            {
                string baseName = SanitizeName(key);
                if (string.IsNullOrWhiteSpace(baseName))
                    baseName = "Schema";

                string newKey = baseName;
                var i = 1;
                while (existingKeys.Contains(newKey))
                {
                    newKey = $"{baseName}_{i++}";
                }

                mapping[key] = newKey;
                existingKeys.Add(newKey);
            }
        }

        if (mapping.Count > 0)
        {
            foreach ((string oldKey, string newKey) in mapping)
            {
                IOpenApiSchema schema = schemas[oldKey];
                schemas.Remove(oldKey);
                schemas[newKey] = schema;
            }

            _referenceFixer.UpdateAllReferences(doc, mapping);
        }
    }

    public void EnsureUniqueOperationIds(OpenApiDocument doc)
    {
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);

        foreach ((string pathKey, var pathItem) in doc.Paths)
        {
            if (pathItem?.Operations == null)
                continue;
            foreach ((HttpMethod method, OpenApiOperation operation) in pathItem.Operations)
            {
                if (string.IsNullOrWhiteSpace(operation.OperationId))
                {
                    // Deterministic base name: e.g., "get_/users/{id}"
                    string baseId = $"{method.ToString().ToLowerInvariant()}_{pathKey.Trim('/')}".Replace("/", "_")
                                                                                                 .Replace("{", "")
                                                                                                 .Replace("}", "")
                                                                                                 .Replace("-", "_");

                    string uniqueId = baseId;
                    var i = 1;

                    while (!seen.Add(uniqueId))
                    {
                        uniqueId = $"{baseId}_{i++}";
                    }

                    operation.OperationId = uniqueId;
                    _logger.LogDebug($"Assigned deterministic OperationId: {operation.OperationId}");
                }
                else
                {
                    string baseId = operation.OperationId;
                    string unique = baseId;
                    var i = 1;

                    while (!seen.Add(unique))
                    {
                        unique = $"{baseId}_{i++}";
                    }

                    if (operation.OperationId != unique)
                    {
                        _logger.LogDebug($"Renaming duplicate OperationId from '{operation.OperationId}' to '{unique}'");
                        operation.OperationId = unique;
                    }
                }
            }
        }
    }

    /// <inheritdoc />
    public void NormalizeOperationIds(OpenApiDocument doc)
    {
        if (doc.Paths == null)
            return;
        foreach (var path in doc.Paths.Values)
        {
            if (path?.Operations == null)
                continue;
            foreach (var op in path.Operations.Values)
            {
                if (op == null)
                    continue;
                if (!string.IsNullOrWhiteSpace(op.OperationId))
                {
                    string normalized = NormalizeOperationId(op.OperationId!);
                    if (!string.Equals(normalized, op.OperationId, StringComparison.Ordinal))
                        op.OperationId = normalized;
                }
            }
        }
    }

    /// <inheritdoc />
    public void ResolveSchemaOperationNameCollisions(OpenApiDocument doc)
    {
        if (doc.Components?.Schemas == null || doc.Paths == null)
            return;

        var operationIds = new HashSet<string>(doc.Paths.Values.Where(p => p?.Operations != null)
                                                  .SelectMany(p => p.Operations.Values)
                                                  .Where(op => op != null && !string.IsNullOrWhiteSpace(op.OperationId))
                                                  .Select(op => op.OperationId!), StringComparer.OrdinalIgnoreCase);

        if (!operationIds.Any())
            return;

        var mapping = new Dictionary<string, string>();

        // Use ToList() to create a copy of the keys, allowing modification of the collection during iteration.
        foreach (string key in doc.Components.Schemas.Keys.ToList())
        {
            // Check if the schema name (case-insensitive) collides with any operationId
            if (!operationIds.Contains(key))
                continue;

            // A collision exists. We must rename the schema.
            var newKey = $"{key}Body"; // A common and effective convention
            var i = 2;
            while (doc.Components.Schemas.ContainsKey(newKey) || mapping.ContainsKey(newKey))
            {
                newKey = $"{key}Body{i++}";
            }

            mapping[key] = newKey;
            _logger.LogWarning("Schema name '{OldKey}' conflicts with an operationId. Renaming schema to '{NewKey}'.", key, newKey);
        }

        if (mapping.Any())
        {
            foreach ((string oldKey, string newKey) in mapping)
            {
                if (doc.Components.Schemas.TryGetValue(oldKey, out IOpenApiSchema? schema))
                {
                    doc.Components.Schemas.Remove(oldKey);
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

            newPath = NormalizeAliasedPathSegments(newPath);

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

    private string NormalizeAliasedPathSegments(string path)
    {
        if (string.IsNullOrWhiteSpace(path))
            return path;

        string normalized = path.Replace("/(-/)", "/", StringComparison.Ordinal)
                                .Replace("/-/", "/", StringComparison.Ordinal);

        while (normalized.Contains("//", StringComparison.Ordinal))
        {
            normalized = normalized.Replace("//", "/", StringComparison.Ordinal);
        }

        if (!string.Equals(path, normalized, StringComparison.Ordinal))
        {
            _logger.LogInformation("Canonicalized aliased path '{OriginalPath}' to '{NormalizedPath}'", path, normalized);
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

        var renameMap = incomingNames.Zip(canonicalNames, static (incoming, canonical) => new { incoming, canonical })
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
        if (string.IsNullOrWhiteSpace(input))
            return string.Empty;
        using var psb = new PooledStringBuilder(input.Length);
        foreach (char c in input)
        {
            if (char.IsLetterOrDigit(c) || c == '_')
                psb.Append(c);
            else
                psb.Append('_');
        }

        return psb.ToString();
    }

    /// <summary>
    /// Normalizes operationIds by removing parentheses and collapsing punctuation to single dashes/underscores.
    /// Ensures result is non-empty and starts with a letter.
    /// </summary>
    /// <inheritdoc />
    public string NormalizeOperationId(string input)
    {
        if (string.IsNullOrWhiteSpace(input))
            return string.Empty;
        // Remove parentheses segments like (-deprecated)
        string noParens = Regex.Replace(input, "[()]+", string.Empty);
        // Replace any non-alphanumeric with '-'
        string collapsed = Regex.Replace(noParens, "[^a-zA-Z0-9]+", "-");
        // Collapse consecutive '-'
        collapsed = Regex.Replace(collapsed, "-+", "-")
                         .Trim('-');
        if (string.IsNullOrEmpty(collapsed))
            return "unnamed";
        // Ensure starts with a letter
        if (!char.IsLetter(collapsed[0]))
            collapsed = "op-" + collapsed;
        return collapsed;
    }

    /// <inheritdoc />
    public bool IsValidIdentifier(string id) =>
        !string.IsNullOrWhiteSpace(id) && id.All(c => char.IsLetterOrDigit(c) || AllowedIdPunctuation.Contains(c));

    /// <inheritdoc />
    public string GenerateSafePart(string? input, string fallback = "unnamed")
    {
        if (string.IsNullOrWhiteSpace(input))
        {
            return fallback;
        }

        string sanitized = NormalizeOperationId(input);
        return string.IsNullOrWhiteSpace(sanitized) ? fallback : sanitized;
    }

    /// <inheritdoc />
    public string ValidateComponentName(string name)
    {
        if (string.IsNullOrWhiteSpace(name))
        {
            _logger.LogWarning("Component name was empty, using fallback name");
            return "UnnamedComponent";
        }

        string sanitized = Regex.Replace(name, @"[^a-zA-Z0-9_]", "_");

        if (!char.IsLetter(sanitized[0]))
        {
            sanitized = "C" + sanitized;
        }

        return sanitized;
    }
}

