[![](https://img.shields.io/nuget/v/soenneker.openapi.fixer.svg?style=for-the-badge)](https://www.nuget.org/packages/soenneker.openapi.fixer/)
[![](https://img.shields.io/github/actions/workflow/status/soenneker/soenneker.openapi.fixer/publish-package.yml?style=for-the-badge)](https://github.com/soenneker/soenneker.openapi.fixer/actions/workflows/publish-package.yml)
[![](https://img.shields.io/nuget/dt/soenneker.openapi.fixer.svg?style=for-the-badge)](https://www.nuget.org/packages/soenneker.openapi.fixer/)
[![](https://img.shields.io/github/actions/workflow/status/soenneker/soenneker.openapi.fixer/codeql.yml?label=CodeQL&style=for-the-badge)](https://github.com/soenneker/soenneker.openapi.fixer/actions/workflows/codeql.yml)

# ![](https://user-images.githubusercontent.com/4441470/224455560-91ed3ee7-f510-4041-a8d2-3fc093025112.png) Soenneker.OpenApi.Fixer

Normalize OpenAPI JSON documents that contain naming, schema, reference, enum, or composition patterns that commonly break generated clients.

## Installation

```bash
dotnet add package Soenneker.OpenApi.Fixer
```

## Registration

```csharp
using Soenneker.OpenApi.Fixer.Registrars;

services.AddOpenApiFixerAsScoped();
```

`AddOpenApiFixerAsSingleton()` is also available for applications that share the fixer.

## Fix a document

Inject `IOpenApiFixer`, then provide separate source and target paths:

```csharp
using Soenneker.OpenApi.Fixer.Abstract;

await fixer.Fix(
    "openapi.json",
    "openapi.fixed.json",
    cancellationToken);
```

The fixer reads JSON, normalizes the document, and writes formatted JSON. It uses a temporary file in the target directory and replaces an existing target only after the result parses successfully. Cancellation and processing failures propagate to the caller.

The default normalization focuses on generated-client compatibility, including component and operation names, references, inline schemas, compositions, discriminators, enum representations, media types, defaults, and empty structures. These are material contract transformations: keep the source file, review the output diff, and generate and test the client before publishing it.

## Optional transformations

Pass `OpenApiFixerOptions` when you need behavior beyond the defaults:

```csharp
using Microsoft.OpenApi;
using Soenneker.OpenApi.Fixer;

var options = new OpenApiFixerOptions
{
    OutputSpecVersion = OpenApiSpecVersion.OpenApi3_1,
    Int32IdTransform = true,
    StripDateSuffixesFromGeneratedNames = true,
    RedactCredentialLikeValues = true
};

await fixer.Fix(
    "openapi.json",
    "openapi.fixed.json",
    options,
    cancellationToken);
```

- `OutputSpecVersion` overrides the serialized OpenAPI version. When omitted, the source version is preserved.
- `Int32IdTransform` changes integer properties and parameters ending in `Id` from `int32` to `int64`.
- `StripDateSuffixesFromGeneratedNames` removes trailing date tokens from generated path prefixes, operation IDs, and schema names.
- `RedactCredentialLikeValues` removes credential-like values from examples and descriptions. It is disabled by default because it intentionally changes documentation content.

## Repair generated enum members

Kiota can emit invalid C# identifiers for symbolic enum values. After generation, repair those identifiers without changing their serialized wire values:

```csharp
await fixer.SanitizeGeneratedEnumMembers(
    "GeneratedClient",
    cancellationToken);
```

Only generated `.cs` files carrying Kiota or auto-generated markers are considered. A missing directory is ignored.
