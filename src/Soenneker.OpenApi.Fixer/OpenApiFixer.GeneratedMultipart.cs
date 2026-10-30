using System.Text.RegularExpressions;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static string RepairGeneratedMultipartBodies(string source)
    {
        var methods = Regex.Matches(source,
            @"public RequestInformation To(?<verb>\w+)RequestInformation\([^{}]+\)\s*\{(?<body>.*?)return requestInfo;",
            RegexOptions.Singleline);
        foreach (Match method in methods)
        {
            if (!method.Groups["body"].Value.Contains("SetContentFromParsable(RequestAdapter, \"multipart/form-data\", body)"))
                continue;
            string verb = Regex.Escape(method.Groups["verb"].Value);
            source = Regex.Replace(source,
                @"(?<prefix>\b(?:To" + verb + @"RequestInformation|" + verb + @"Async)\()(?<type>[\w:.]+\??)(?= body\b)",
                "${prefix}global::Microsoft.Kiota.Abstractions.MultipartBody");
        }
        return source;
    }
}
