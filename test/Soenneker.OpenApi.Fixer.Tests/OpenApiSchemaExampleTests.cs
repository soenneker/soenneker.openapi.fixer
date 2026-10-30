using System.Reflection;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Fixer.Tests;

public sealed class OpenApiSchemaExampleTests
{
    [Test]
    public async ValueTask Object_examples_are_reconciled_without_discarding_original_values()
    {
        const string json = """
            {"components":{"schemas":{
              "Bucket":{"type":"object","required":["name"],"properties":{"name":{"type":"string"}},"example":"{\"name\":\"bucket\"}"},
              "Source":{"type":"object","required":["pointer"],"properties":{"pointer":{"type":"string"}},"example":"/rules/0"},
              "Setting":{"type":"object","properties":{"enabled":{"type":"boolean"}},"default":{"enabled":false},"example":"on"},
              "Apo":{"type":"object","required":["enabled","hostnames"],"properties":{"enabled":{"type":"boolean","example":true},"hostnames":{"type":"array","items":{"type":"string"},"example":["example.com"]}},"example":"on"}
            }}}
            """;
        string repaired = Repair(json);
        JsonNode schemas = JsonNode.Parse(repaired)!["components"]!["schemas"]!;
        await Assert.That(schemas["Bucket"]!["example"]!["name"]!.GetValue<string>()).IsEqualTo("bucket");
        await Assert.That(schemas["Source"]!["example"]!["pointer"]!.GetValue<string>()).IsEqualTo("/rules/0");
        await Assert.That(schemas["Setting"]!["example"]!["enabled"]!.GetValue<bool>()).IsFalse();
        await Assert.That(schemas["Apo"]!["example"]!["hostnames"]![0]!.GetValue<string>()).IsEqualTo("example.com");
        await Assert.That(schemas["Setting"]!["x-client-original-example"]!.GetValue<string>()).IsEqualTo("on");
        await Assert.That(Repair(repaired)).IsEqualTo(repaired);
    }

    [Test]
    public async ValueTask Unstructured_values_keep_scalar_and_object_payloads_without_narrowing_constrained_objects()
    {
        const string json = """
            {"components":{"schemas":{
              "Scope":{"type":"object","example":"accounts"},
              "Json":{"type":"object","x-stainless-any":true},
              "Constrained":{"type":"object","required":["id"],"properties":{"id":{"type":"string"}},"example":"accounts"},
              "Closed":{"type":"object","additionalProperties":false,"example":"accounts"}
            }}}
            """;
        JsonNode schemas = JsonNode.Parse(Repair(json))!["components"]!["schemas"]!;
        await Assert.That(schemas["Scope"]!["anyOf"]!.AsArray().Count).IsEqualTo(2);
        await Assert.That(schemas["Scope"]!["example"]!.GetValue<string>()).IsEqualTo("accounts");
        await Assert.That(schemas["Json"]!["type"]).IsNull();
        await Assert.That(schemas["Json"]!["x-client-untyped-value"]!.GetValue<bool>()).IsTrue();
        await Assert.That(schemas["Constrained"]!["example"]!.GetValue<string>()).IsEqualTo("accounts");
        await Assert.That(schemas["Closed"]!["anyOf"]).IsNull();
    }

    private static string Repair(string json) => (string)typeof(OpenApiFixer).GetMethod("ReconcileSchemaExamples", BindingFlags.Static | BindingFlags.NonPublic)!.Invoke(null, [json])!;
}
