using System.Text.RegularExpressions;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static string SanitizeGeneratedErrorMessageForContent(string content)
    {
        // Kiota 1.36 checks the element type but not the collection shape when selecting
        // Exception.Message. Keep the deserialized collection and adapt only the override.
        if (!Regex.IsMatch(content, @"public\s+(?:System\.Collections\.Generic\.)?List<string>\??\s+MessageEscaped\s*\{"))
            return content;

        return Regex.Replace(content,
            @"public\s+override\s+string\s+Message\s*\{\s*get\s*=>\s*MessageEscaped\s*\?\?\s*string\.Empty;\s*\}",
            "public override string Message { get => MessageEscaped is null ? string.Empty : string.Join(\"; \", MessageEscaped); }");
    }
}
