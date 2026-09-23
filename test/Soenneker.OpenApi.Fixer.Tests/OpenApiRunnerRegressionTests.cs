using System;
using System.Collections.Generic;
using System.Reflection;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.OpenApi;
using Soenneker.OpenApi.Fixer.Fixers;

namespace Soenneker.OpenApi.Fixer.Tests;

public sealed class OpenApiRunnerRegressionTests
{
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task Recursive_array_components_keep_a_reference_at_the_cycle(bool mutual)
    {
        var array = new OpenApiSchema { Type = JsonSchemaType.Array, Items = new OpenApiSchemaReference(mutual ? "Other" : "Recursive") };
        var document = new OpenApiDocument
        {
            Components = new OpenApiComponents { Schemas = new Dictionary<string, IOpenApiSchema>
            {
                ["Recursive"] = array,
                ["Other"] = new OpenApiSchema { Type = JsonSchemaType.Array, Items = new OpenApiSchemaReference("Recursive") },
                ["Text"] = new OpenApiSchema { Type = JsonSchemaType.String, MinLength = 3 },
                ["Model"] = new OpenApiSchema { Type = JsonSchemaType.Object, Properties = new Dictionary<string, IOpenApiSchema>
                {
                    ["first"] = new OpenApiSchemaReference("Recursive"),
                    ["second"] = new OpenApiSchemaReference("Recursive"),
                    ["text"] = new OpenApiSchemaReference("Text")
                } }
            } }
        };
        RunInlinePass(document);
        foreach (string name in new[] { "first", "second" })
        {
            IOpenApiSchema schema = document.Components.Schemas["Model"].Properties![name];
            int depth = 0;
            while (schema is OpenApiSchema { Items: not null } concrete && depth++ < 10)
                schema = concrete.Items;
            await Assert.That(schema is OpenApiSchemaReference).IsTrue();
            await Assert.That(depth < 10).IsTrue();
        }
        await Assert.That(document.Components.Schemas["Model"].Properties!["text"].MinLength).IsEqualTo(3);
    }

    [Test]
    public async Task Cyclic_component_aliases_do_not_loop_during_primitive_resolution()
    {
        var document = new OpenApiDocument { Components = new OpenApiComponents { Schemas = new Dictionary<string, IOpenApiSchema>
        {
            ["First"] = new OpenApiSchemaReference("Second"),
            ["Second"] = new OpenApiSchemaReference("First")
        } } };
        RunInlinePass(document);
        await Assert.That(document.Components.Schemas["First"] is OpenApiSchemaReference).IsTrue();
    }

    private static void RunInlinePass(OpenApiDocument document) => typeof(OpenApiFixer)
        .GetMethod("InlinePrimitivePropertyRefs", BindingFlags.Static | BindingFlags.NonPublic)!.Invoke(null, [document]);

    [Test]
    public async Task Preprocessing_repairs_publisher_schema_shapes_without_modifying_example_payloads()
    {
        const string source = """
            {"openapi":"3.0.3","info":{"title":"API","version":"1"},"paths":{},"components":{"schemas":{
              "Ids":{"type":"array","items":[{"type":"string"}]},
              "Value":{"type":["string","number","null"],"examples":{"type":["literal"],"items":[1]}},
              "Constrained":{"type":["string","number"],"allOf":{"maxLength":3},"anyOf":[{"enum":["a",1]}]}
            }}}
            """;
        var fixer = new OpenApiPreprocessingFixer(NullLogger<OpenApiPreprocessingFixer>.Instance);
        JsonNode root = JsonNode.Parse(fixer.Fix(source))!;
        JsonNode schemas = root["components"]!["schemas"]!;
        await Assert.That(schemas["Ids"]!["items"]!["type"]!.GetValue<string>()).IsEqualTo("string");
        await Assert.That(schemas["Value"]!["anyOf"]!.AsArray().Count).IsEqualTo(3);
        await Assert.That(schemas["Value"]!["examples"]![0]!["type"]![0]!.GetValue<string>()).IsEqualTo("literal");
        await Assert.That(schemas["Constrained"]!["allOf"]![0]!["maxLength"]!.GetValue<int>()).IsEqualTo(3);
        await Assert.That(schemas["Constrained"]!["allOf"]![1]!["anyOf"]!.AsArray().Count).IsEqualTo(2);
        await Assert.That(schemas["Constrained"]!["anyOf"]![0]!["enum"]!.AsArray().Count).IsEqualTo(2);
    }
}
