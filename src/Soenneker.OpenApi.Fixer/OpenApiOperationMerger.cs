using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Nodes;
using Microsoft.OpenApi;

namespace Soenneker.OpenApi.Fixer;

internal static class OpenApiOperationMerger
{
    internal static void Merge(OpenApiOperation target, OpenApiOperation source, IList<OpenApiSecurityRequirement>? inheritedSecurity)
    {
        static int TrailingSlashes(OpenApiOperation operation) => operation.Extensions?.TryGetValue("x-original-trailing-slashes", out IOpenApiExtension? value) == true &&
            value is JsonNodeExtension { Node: JsonValue count } && count.TryGetValue(out int result) ? result : 0;
        if (TrailingSlashes(target) != TrailingSlashes(source))
            throw new InvalidOperationException("Distinct trailing-slash routes with the same HTTP method cannot be merged without losing a wire URL.");
        target.Extensions ??= new Dictionary<string, IOpenApiExtension>();
        foreach ((string key, IOpenApiExtension value) in source.Extensions ?? new Dictionary<string, IOpenApiExtension>())
            target.Extensions.TryAdd(key, value);
        const string aliasesKey = "x-original-operation-ids";
        JsonArray aliases = target.Extensions.TryGetValue(aliasesKey, out IOpenApiExtension? extension) && extension is JsonNodeExtension { Node: JsonArray prior }
            ? prior.DeepClone().AsArray() : new JsonArray();
        foreach (string? id in new[] { target.OperationId, source.OperationId })
            if (!string.IsNullOrWhiteSpace(id) && !aliases.Any(n => n?.GetValue<string>() == id)) aliases.Add(JsonValue.Create(id));
        target.Extensions[aliasesKey] = new JsonNodeExtension(aliases);
        target.Servers = (target.Servers ?? new List<OpenApiServer>()).Concat(source.Servers ?? new List<OpenApiServer>()).ToList();
        target.Description = Join(target.Description, source.Description);
        target.Summary = Join(target.Summary, source.Summary);
        target.Deprecated &= source.Deprecated;
        target.Tags ??= new HashSet<OpenApiTagReference>();
        foreach (OpenApiTagReference tag in source.Tags ?? new HashSet<OpenApiTagReference>()) target.Tags.Add(tag);
        target.Parameters ??= new List<IOpenApiParameter>();
        target.Parameters = target.Parameters.Select(parameter =>
        {
            IOpenApiParameter? resolved = parameter is OpenApiParameterReference reference ? reference.Target : parameter;
            return resolved is OpenApiParameter concrete ? (IOpenApiParameter)concrete.CreateShallowCopy() : parameter;
        }).ToList();
        IList<IOpenApiParameter> incoming = source.Parameters ?? new List<IOpenApiParameter>();
        foreach (IOpenApiParameter parameter in target.Parameters)
            if (parameter is OpenApiParameter concrete && concrete.In != ParameterLocation.Path &&
                !incoming.Any(p => p.Name == concrete.Name && p.In == concrete.In)) concrete.Required = false;
        foreach (IOpenApiParameter parameter in incoming)
        {
            int index = target.Parameters.ToList().FindIndex(p => p.Name == parameter.Name && p.In == parameter.In);
            OpenApiParameter? resolved = parameter is OpenApiParameterReference reference ? reference.Target as OpenApiParameter : parameter as OpenApiParameter;
            if (resolved == null) throw new InvalidOperationException("Cannot merge an unresolved parameter reference.");
            var copy = (OpenApiParameter)resolved.CreateShallowCopy();
            if (index < 0)
            {
                if (copy.In != ParameterLocation.Path) copy.Required = false;
                target.Parameters.Add(copy);
            }
            else
            {
                IOpenApiParameter current = target.Parameters[index];
                if (current.Style != copy.Style || current.Explode != copy.Explode || current.AllowReserved != copy.AllowReserved)
                    throw new InvalidOperationException("Cannot merge parameter alternatives with different wire encodings.");
                if (current.Content != null || copy.Content != null)
                {
                    if (current.Schema != null || copy.Schema != null)
                        throw new InvalidOperationException("Cannot merge schema-based and content-based declarations of the same parameter.");
                    copy.Content = MergeContent(current.Content, copy.Content);
                }
                else copy.Schema = Union(current.Schema, copy.Schema);
                copy.Required &= current.Required;
                copy.Description = Join(current.Description, copy.Description);
                target.Parameters[index] = copy;
            }
        }

        OpenApiRequestBody? existingBody = target.RequestBody is OpenApiRequestBodyReference existingRef ? existingRef.Target as OpenApiRequestBody : target.RequestBody as OpenApiRequestBody;
        OpenApiRequestBody? incomingBody = source.RequestBody is OpenApiRequestBodyReference incomingRef ? incomingRef.Target as OpenApiRequestBody : source.RequestBody as OpenApiRequestBody;
        if (target.RequestBody != null && existingBody == null || source.RequestBody != null && incomingBody == null)
            throw new InvalidOperationException("Cannot merge an unresolved request body reference.");
        if (existingBody != null || incomingBody != null)
        {
            var body = (OpenApiRequestBody)(existingBody ?? incomingBody!).CreateShallowCopy();
            body.Required = existingBody?.Required == true && incomingBody?.Required == true;
            body.Content = MergeContent(existingBody?.Content, incomingBody?.Content);
            body.Description = Join(existingBody?.Description, incomingBody?.Description);
            target.RequestBody = body;
        }

        target.Responses ??= new OpenApiResponses();
        foreach ((string status, IOpenApiResponse response) in source.Responses ?? new OpenApiResponses())
        {
            if (!target.Responses.TryGetValue(status, out IOpenApiResponse? existing)) { target.Responses[status] = response; continue; }
            OpenApiResponse? left = existing is OpenApiResponseReference leftRef ? leftRef.Target as OpenApiResponse : existing as OpenApiResponse;
            OpenApiResponse? right = response is OpenApiResponseReference rightRef ? rightRef.Target as OpenApiResponse : response as OpenApiResponse;
            if (left == null || right == null) throw new InvalidOperationException("Cannot merge an unresolved response reference.");
            var merged = (OpenApiResponse)left.CreateShallowCopy();
            merged.Description = Join(left.Description, right.Description);
            merged.Content = MergeContent(left.Content, right.Content);
            merged.Headers = left.Headers != null ? new Dictionary<string, IOpenApiHeader>(left.Headers) : new Dictionary<string, IOpenApiHeader>();
            foreach ((string key, IOpenApiHeader header) in right.Headers ?? new Dictionary<string, IOpenApiHeader>())
            {
                if (!merged.Headers.TryGetValue(key, out IOpenApiHeader? priorHeader)) { merged.Headers[key] = header; continue; }
                OpenApiHeader? first = priorHeader is OpenApiHeaderReference firstRef ? firstRef.Target as OpenApiHeader : priorHeader as OpenApiHeader;
                OpenApiHeader? second = header is OpenApiHeaderReference secondRef ? secondRef.Target as OpenApiHeader : header as OpenApiHeader;
                if (first == null || second == null) throw new InvalidOperationException("Cannot merge unresolved response headers.");
                if (first.Style != second.Style || first.Explode != second.Explode)
                    throw new InvalidOperationException("Cannot merge response headers with different wire encodings.");
                var combined = (OpenApiHeader)first.CreateShallowCopy();
                combined.Description = Join(first.Description, second.Description);
                combined.Required &= second.Required;
                if (first.Content != null || second.Content != null)
                {
                    if (first.Schema != null || second.Schema != null) throw new InvalidOperationException("Cannot merge schema-based and content-based response headers.");
                    combined.Content = MergeContent(first.Content, second.Content);
                }
                else combined.Schema = Union(first.Schema, second.Schema);
                merged.Headers[key] = combined;
            }
            merged.Links = left.Links != null ? new Dictionary<string, IOpenApiLink>(left.Links) : new Dictionary<string, IOpenApiLink>();
            foreach ((string key, IOpenApiLink link) in right.Links ?? new Dictionary<string, IOpenApiLink>())
            {
                string name = key;
                int suffix = 2;
                while (merged.Links.ContainsKey(name)) name = key + suffix++;
                merged.Links[name] = link;
            }
            target.Responses[status] = merged;
        }
        if (target.Security != null || source.Security != null)
        {
            IList<OpenApiSecurityRequirement>? left = target.Security ?? inheritedSecurity;
            IList<OpenApiSecurityRequirement>? right = source.Security ?? inheritedSecurity;
            target.Security = (left is { Count: > 0 } ? left : new[] { new OpenApiSecurityRequirement() })
                .Concat(right is { Count: > 0 } ? right : new[] { new OpenApiSecurityRequirement() }).ToList();
        }
    }

    private static IDictionary<string, IOpenApiMediaType> MergeContent(IDictionary<string, IOpenApiMediaType>? left, IDictionary<string, IOpenApiMediaType>? right)
    {
        var result = left != null ? new Dictionary<string, IOpenApiMediaType>(left) : new Dictionary<string, IOpenApiMediaType>();
        foreach ((string media, IOpenApiMediaType incoming) in right ?? new Dictionary<string, IOpenApiMediaType>())
        {
            if (!result.TryGetValue(media, out IOpenApiMediaType? existing)) { result[media] = incoming; continue; }
            var merged = new OpenApiMediaType { Schema = Union(existing.Schema, incoming.Schema), Encoding = existing.Encoding ?? incoming.Encoding,
                Example = existing.Example ?? incoming.Example, Examples = existing.Examples ?? incoming.Examples };
            merged.Encoding = existing.Encoding?.ToDictionary() ?? incoming.Encoding?.ToDictionary();
            if (incoming.Encoding != null && merged.Encoding != null)
                foreach (var (key, encoding) in incoming.Encoding)
                {
                    if (merged.Encoding.TryGetValue(key, out var prior) &&
                        (prior.Style != encoding.Style || prior.Explode != encoding.Explode ||
                         prior.AllowReserved != encoding.AllowReserved || prior.ContentType != encoding.ContentType))
                        throw new InvalidOperationException("Cannot merge request bodies with different property encodings.");
                    merged.Encoding.TryAdd(key, encoding);
                }
            result[media] = merged;
        }
        return result;
    }

    private static IOpenApiSchema Union(IOpenApiSchema? left, IOpenApiSchema? right) => ReferenceEquals(left, right) && left != null ? left :
        new OpenApiSchema { AnyOf = new List<IOpenApiSchema> { left ?? new OpenApiSchema(), right ?? new OpenApiSchema() } };

    private static string? Join(string? left, string? right) => string.IsNullOrWhiteSpace(left) ? right :
        string.IsNullOrWhiteSpace(right) || left == right ? left : left + "\n\n" + right;
}

