using System.Reflection;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Fixer.Tests;

public sealed class OpenApiIntegerRangeTests
{
    [Test]
    public async ValueTask Wide_integer_bounds_use_exact_types_and_preserve_integer_multiples()
    {
        JsonNode input = JsonNode.Parse("""
            {"components":{"schemas":{
              "Wide":{"type":"integer","format":"int32","maximum":4294967295},
              "Huge":{"type":["integer","null"],"maximum":18446744073709551615,"multipleOf":2.5},
              "Small":{"type":"integer","maximum":127},
              "Payload":{"type":"object","example":{"type":"integer","maximum":4294967295}}
            }}}
            """)!;
        JsonNode result = Run(input);
        JsonNode schemas = result["components"]!["schemas"]!;
        await Assert.That(schemas["Wide"]!["format"]!.GetValue<string>()).IsEqualTo("int64");
        await Assert.That(schemas["Wide"]!["x-client-original-format"]!.GetValue<string>()).IsEqualTo("int32");
        await Assert.That(schemas["Huge"]!["format"]!.GetValue<string>()).IsEqualTo("decimal");
        await Assert.That(schemas["Huge"]!["type"]!.ToJsonString()).IsEqualTo("[\"number\",\"null\"]");
        await Assert.That(schemas["Huge"]!["maximum"]!.GetValue<decimal>()).IsEqualTo(18446744073709551615m);
        await Assert.That(schemas["Huge"]!["multipleOf"]!.GetValue<decimal>()).IsEqualTo(5m);
        await Assert.That(schemas["Small"]!["format"]).IsNull();
        await Assert.That(schemas["Payload"]!["example"]!["format"]).IsNull();
        await Assert.That(JsonNode.DeepEquals(result, Run(result))).IsTrue();
    }

    private static JsonNode Run(JsonNode input) => JsonNode.Parse((string)typeof(OpenApiFixer)
        .GetMethod("NormalizeIntegerRanges", BindingFlags.NonPublic | BindingFlags.Static)!.Invoke(null, [input.ToJsonString()])!)!;
}
