using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static async ValueTask<HashSet<string>> ReadGeneratedFactorySchemas(string root, CancellationToken ct)
    {
        var names = new HashSet<string>(StringComparer.Ordinal);
        string lockPath = Path.Combine(root, "kiota-lock.json");
        if (!File.Exists(lockPath)) return names;
        JsonNode? configuration = JsonNode.Parse(await File.ReadAllTextAsync(lockPath, ct));
        string? location = configuration?["descriptionLocation"]?.GetValue<string>();
        if (string.IsNullOrWhiteSpace(location)) return names;
        string path;
        if (Uri.TryCreate(location, UriKind.Absolute, out Uri? uri))
        {
            if (!uri.IsFile) return names;
            path = uri.LocalPath;
        }
        else path = Path.GetFullPath(Path.Combine(root, location));
        if (!File.Exists(path)) return names;
        // These metadata repairs consume the fixer's JSON output; raw YAML has no fixer annotations.
        if (Path.GetExtension(path).Equals(".yaml", StringComparison.OrdinalIgnoreCase) ||
            Path.GetExtension(path).Equals(".yml", StringComparison.OrdinalIgnoreCase)) return names;
        JsonNode? document = JsonNode.Parse(await File.ReadAllTextAsync(path, ct));
        if (document?["components"]?["schemas"] is not JsonObject schemas) return names;
        foreach ((string name, JsonNode? schema) in schemas)
            if (schema is JsonObject obj && obj["discriminator"] == null && obj["anyOf"] == null && obj["oneOf"] == null && obj["$ref"] == null)
                names.Add(name);
        return names;
    }

    private static string RepairGeneratedMissingFactories(string source, HashSet<string> schemas)
    {
        if (Regex.IsMatch(source, @"public\s+(?:new\s+)?static\s+[\w:.<>?]+\s+CreateFromDiscriminatorValue\s*\(")) return source;
        Match declaration = Regex.Match(source, @"public partial class (?<name>\w+)\s*:\s*(?:IAdditionalDataHolder,\s*)?IParsable\s*(?:#pragma warning restore CS1591\s*)?\{");
        if (!declaration.Success || !schemas.Contains(declaration.Groups["name"].Value)) return source;
        string name = declaration.Groups["name"].Value;
        string factory = "\n        public static " + name + " CreateFromDiscriminatorValue(global::Microsoft.Kiota.Abstractions.Serialization.IParseNode parseNode)\n" +
            "        {\n            if (global::System.Object.ReferenceEquals(parseNode, null)) throw new global::System.ArgumentNullException(nameof(parseNode));\n" +
            "            return new " + name + "();\n        }\n";
        return source.Insert(declaration.Index + declaration.Length, factory);
    }
}
