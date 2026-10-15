using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.OpenApi;
using Soenneker.OpenApi.Fixer.Fixers;

namespace Soenneker.OpenApi.Fixer.Tests;

public sealed class RepeatedPathParameterTests
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Repeated_names_are_unique_without_mutating_shared_parameters(bool singleDeclaration)
    {
        var shared = new OpenApiParameter { Name = "id", In = ParameterLocation.Path, Required = true, Description = "Identifier" };
        var operation = new OpenApiOperation
        {
            Parameters = new List<IOpenApiParameter> { shared }
        };
        if (!singleDeclaration)
            operation.Parameters.Add(new OpenApiParameter { Name = "id", In = ParameterLocation.Path, Description = "Identifier" });
        operation.Parameters.Add(new OpenApiParameter { Name = "id", In = ParameterLocation.Query });
        var document = new OpenApiDocument
        {
            Paths = new OpenApiPaths
            {
                ["/parents/{id}/children/{id}/{id2}"] = new OpenApiPathItem
                {
                    Operations = new Dictionary<HttpMethod, OpenApiOperation> { [HttpMethod.Get] = operation }
                }
            }
        };
        var fixer = new OpenApiNamingFixer(NullLogger<OpenApiNamingFixer>.Instance, null!);
        fixer.RenameConflictingPaths(document);
        fixer.RenameConflictingPaths(document);

        await Assert.That(string.Join(",", document.Paths.Keys)).IsEqualTo("/parents/{id}/children/{id3}/{id2}");
        await Assert.That(operation.Parameters[0].Name).IsEqualTo("id");
        await Assert.That(operation.Parameters[1].Name).IsEqualTo("id3");
        await Assert.That(operation.Parameters[1].Description).IsEqualTo("Identifier");
        await Assert.That(operation.Parameters[1].In).IsEqualTo(ParameterLocation.Path);
        await Assert.That(operation.Parameters[2].Name).IsEqualTo("id");
        await Assert.That(shared.Name).IsEqualTo("id");
    }

    [Test]
    public async Task Repeated_parameters_merge_into_canonical_names_by_position()
    {
        var operation = new OpenApiOperation
        {
            Parameters = new List<IOpenApiParameter>
            {
                new OpenApiParameter { Name = "id", In = ParameterLocation.Path },
                new OpenApiParameter { Name = "id", In = ParameterLocation.Path }
            }
        };
        var document = new OpenApiDocument
        {
            Paths = new OpenApiPaths
            {
                ["/parents/{parentId}/children/{childId}"] = new OpenApiPathItem(),
                ["/parents/{id}/children/{id}"] = new OpenApiPathItem
                {
                    Operations = new Dictionary<HttpMethod, OpenApiOperation> { [HttpMethod.Get] = operation }
                }
            }
        };
        new OpenApiNamingFixer(NullLogger<OpenApiNamingFixer>.Instance, null!).RenameConflictingPaths(document);
        await Assert.That(document.Paths.Count).IsEqualTo(1);
        await Assert.That(operation.Parameters[0].Name).IsEqualTo("parentId");
        await Assert.That(operation.Parameters[1].Name).IsEqualTo("childId");
    }

    [Test]
    public async Task Compound_segments_are_not_merged_with_single_parameters()
    {
        var document = new OpenApiDocument
        {
            Paths = new OpenApiPaths
            {
                ["/items/{slug}-{id}"] = new OpenApiPathItem(),
                ["/items/{slug}"] = new OpenApiPathItem()
            }
        };
        new OpenApiNamingFixer(NullLogger<OpenApiNamingFixer>.Instance, null!).RenameConflictingPaths(document);
        await Assert.That(document.Paths.Count).IsEqualTo(2);
    }
}
