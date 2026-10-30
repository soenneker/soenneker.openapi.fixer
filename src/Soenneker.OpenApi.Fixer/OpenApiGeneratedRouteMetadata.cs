using System;
using System.Collections.Generic;

namespace Soenneker.OpenApi.Fixer;

internal sealed class OpenApiGeneratedRouteMetadata
{
    internal Dictionary<string, Dictionary<string, (string Path, string Server)>> Routes { get; } = new(StringComparer.Ordinal);
    internal HashSet<string> Servers { get; } = new(StringComparer.OrdinalIgnoreCase);
    internal string? DefaultServer { get; set; }
}
