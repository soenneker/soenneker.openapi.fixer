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

The fixer logs source and target file paths, major processing stages, temporary output and validation paths, completion timing, warnings, and errors by default. Set `OpenApiFixerOptions.VerboseLogging` to `true` to also see detailed progress, repair summaries, and individual repair details.

Input recovery accepts comments, trailing commas, unescaped control characters in strings, and bare `True`, `False`, and `None` values. Quoted text is preserved. Duplicate JSON properties use the last occurrence and produce a warning; conflicting duplicate values are inherently ambiguous. Version detection runs after recovery, so malformed JSON does not prevent an otherwise readable version from being recognized.

The preprocessing rules also recover numeric version metadata, quoted numeric constraints, singleton `required`/`enum`/composition collections, and singleton parameter objects. Missing route placeholders receive required string parameters only where no path-level or operation-level definition exists, including local parameter references. Existing parameter types and constraints are retained.

JSON schema traversal follows OpenAPI structure through callbacks, webhooks, reusable components, Swagger definitions, and nested JSON Schema keywords. Properties named `example`, `type`, or `x-*` receive the same repairs as other properties. These traversal passes treat examples, defaults, constants, enum values, and extensions as payload data instead of nested schema definitions. Separate compatibility passes can still normalize enum and default values. Numeric exclusive bounds in OpenAPI 3.1 and media-type parameters and ranges are preserved.

Recovery cannot infer missing contracts from arbitrary text or resolve conflicting definitions without a policy. Unrecoverable JSON and unsupported versions still fail explicitly. Final validation reads the exact output without preprocessing it again, and a failed or canceled run leaves an existing target intact.

The default normalization focuses on generated-client compatibility, including component and operation names, references, inline schemas, compositions, discriminators, enum representations, media types, defaults, and empty structures. These are material contract transformations: keep the source file, review the output diff, and generate and test the client before publishing it.

## Optional transformations

By default, missing JSON media schemas are inferred from inline examples or locally referenced Example Objects. Inferred schemas combine observed properties and array element types without declaring fields required or inventing enums. Explicit schemas remain authoritative; nested integer formats are widened to `int64` when payload examples demonstrate values outside Int32. This includes reusable schemas. Example payloads and vendor extensions, including response inference warnings, are preserved. No external examples are downloaded and no HTTP success status is guessed by the fixer.

Set `InferSchemasFromExamples = false` to disable this recovery. Media schemas created from examples carry `x-schema-inferred-from-examples: true`.

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

## Logging

Turn on diagnostics for a call with one flag:

```csharp
var options = new OpenApiFixerOptions { VerboseLogging = true };
await fixer.Fix("openapi.json", "openapi.fixed.json", options, cancellationToken);
```

`VerboseLogging` defaults to `false`. Progress and verbose messages use `Information` severity; warnings and errors are always available. The host's logging filters still apply. The flag is captured per call, applies to built-in helpers, and is isolated between concurrent calls, including singleton use. Standalone preprocessing accepts the same flag.

## Repair generated enum members

Kiota can emit invalid C# identifiers for symbolic enum values. After generation, repair those identifiers without changing their serialized wire values:

```csharp
await fixer.SanitizeGeneratedEnumMembers(
    "GeneratedClient",
    cancellationToken);
```

Only generated `.cs` files carrying Kiota or auto-generated markers are considered. A missing directory is ignored.
