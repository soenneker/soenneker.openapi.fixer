using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using Microsoft.OpenApi;

namespace Soenneker.OpenApi.Fixer;

internal static class OpenApiRepeatedPathParameterNormalizer
{
    public static string Normalize(string path, IOpenApiPathItem pathItem)
    {
        MatchCollection matches = Regex.Matches(path, @"\{([^/{}]+)\}", RegexOptions.CultureInvariant);
        var usedNames = new HashSet<string>(matches.Select(match => match.Groups[1].Value), StringComparer.OrdinalIgnoreCase);
        var names = new Dictionary<string, List<string>>(StringComparer.Ordinal);
        string normalized = Regex.Replace(path, @"\{([^/{}]+)\}", match =>
        {
            string name = match.Groups[1].Value;
            if (!names.TryGetValue(name, out List<string>? occurrences))
            {
                names[name] = [name];
                return match.Value;
            }

            int suffix = 2;
            string candidate;
            do { candidate = name + suffix++; } while (!usedNames.Add(candidate));
            occurrences.Add(candidate);
            return "{" + candidate + "}";
        }, RegexOptions.CultureInvariant);

        if (normalized == path)
            return path;

        NormalizeParameters(pathItem.Parameters, names);
        if (pathItem.Operations != null)
        {
            foreach (OpenApiOperation operation in pathItem.Operations.Values)
                NormalizeParameters(operation.Parameters, names);
        }

        return normalized;
    }

    private static void NormalizeParameters(IList<IOpenApiParameter>? parameters, Dictionary<string, List<string>> names)
    {
        if (parameters == null)
            return;

        foreach ((string original, List<string> replacements) in names)
        {
            if (replacements.Count < 2)
                continue;

            IOpenApiParameter[] declarations = parameters.Where(p => p.In == ParameterLocation.Path && p.Name == original).ToArray();
            if (declarations.Length == 0)
                continue;

            int insertionIndex = parameters.IndexOf(declarations[0]);
            foreach (IOpenApiParameter declaration in declarations)
                parameters.Remove(declaration);

            for (int i = 0; i < replacements.Count; i++)
            {
                // Copy references and shared declarations so other routes retain their original parameter names.
                IOpenApiParameter source = declarations[Math.Min(i, declarations.Length - 1)];
                var parameter = new OpenApiParameter
                {
                    Name = replacements[i],
                    In = ParameterLocation.Path,
                    Required = true,
                    Schema = source.Schema,
                    Description = source.Description,
                    Deprecated = source.Deprecated,
                    Style = source.Style,
                    Explode = source.Explode,
                    AllowEmptyValue = source.AllowEmptyValue,
                    AllowReserved = source.AllowReserved,
                    Examples = source.Examples,
                    Example = source.Example,
                    Content = source.Content,
                    Extensions = source.Extensions
                };
                parameters.Insert(insertionIndex + i, parameter);
            }
        }
    }
}
