using System;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text.Json.Nodes;
using System.Text.Json;
using Soenneker.OpenApi.Fixer.Fixers;
using Microsoft.OpenApi;

namespace Soenneker.OpenApi.Fixer;

internal static class OpenApiSchemaConstraints
{
    // A transformation may discard only annotations and the keywords it explicitly handles.
    // Serializing also covers newer JSON Schema keywords without maintaining a lossy field list.
    internal static bool HasOnly(OpenApiSchema schema, params string[] supported)
    {
        using var text = new StringWriter(CultureInfo.InvariantCulture);
        schema.SerializeAsV31(new OpenApiJsonWriter(text));
        JsonObject json;
        try { json = JsonNode.Parse(text.ToString())!.AsObject(); }
        catch (JsonException)
        {
            json = JsonNode.Parse(OpenApiPreprocessingFixer.NormalizeLooseJsonSyntax(text.ToString()))!.AsObject();
        }
        return json.All(entry => entry.Key is "title" or "description" or "default" or "example" or "examples" or
            "deprecated" or "readOnly" or "writeOnly" or "xml" or "externalDocs" ||
            entry.Key.StartsWith("x-", StringComparison.Ordinal) || supported.Contains(entry.Key, StringComparer.Ordinal) ||
            (entry.Key == "additionalProperties" && entry.Value is JsonValue value && value.TryGetValue(out bool allowed) && allowed));
    }
}
