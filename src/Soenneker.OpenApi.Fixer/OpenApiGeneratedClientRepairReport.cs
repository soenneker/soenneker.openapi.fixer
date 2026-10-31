using System.Collections.Generic;

namespace Soenneker.OpenApi.Fixer;

/// <summary>Describes generated-client repairs without implying compilation or runtime validation.</summary>
public sealed record OpenApiGeneratedClientRepairReport
{
    /// <summary>Gets the number of C# files present after repair.</summary>
    public required int FileCount { get; init; }

    /// <summary>Gets relative paths of added or modified C# files.</summary>
    public required IReadOnlyList<string> ChangedFiles { get; init; }

    /// <summary>Gets prerequisites that prevented schema-dependent repairs.</summary>
    public required IReadOnlyList<string> Warnings { get; init; }
}
