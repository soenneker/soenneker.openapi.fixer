using System.Reflection;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Fixer.Tests;

public sealed class OpenApiKiotaWarningTests
{
    [Test]
    [Arguments("uri")]
    [Arguments("email")]
    [Arguments("hostname")]
    [Arguments("ipv4")]
    [Arguments("ipv6")]
    [Arguments("uri-template")]
    [Arguments("url-encoded-date-time")]
    [Arguments("A.B.C.D/N")]
    [Arguments("var-str")]
    [Arguments("cidr")]
    [Arguments("password")]
    [Arguments("uri-reference")]
    public async ValueTask Reported_string_formats_keep_wire_strings_and_original_metadata(string format)
    {
        JsonNode input = JsonNode.Parse("""{"components":{"schemas":{"Value":{"type":["string","null"],"example":{"format":"email"}}}}}""")!;
        input["components"]!["schemas"]!["Value"]!["format"] = format;
        JsonNode result = Run("NormalizeKiotaResponseCompatibility", input, new OpenApiFixerOptions());
        JsonNode value = result["components"]!["schemas"]!["Value"]!;
        await Assert.That(value["format"]).IsNull();
        await Assert.That(value["x-client-original-format"]!.GetValue<string>()).IsEqualTo(format);
        await Assert.That(value["type"]!.ToJsonString()).IsEqualTo("[\"string\",\"null\"]");
        await Assert.That(value["example"]!["format"]!.GetValue<string>()).IsEqualTo("email");
        await Assert.That(JsonNode.DeepEquals(result, Run("NormalizeKiotaResponseCompatibility", result, new OpenApiFixerOptions()))).IsTrue();
    }

    [Test]
    [Arguments("uint32", "int64", "integer")]
    [Arguments("uint64", "decimal", "number")]
    public async ValueTask Unsigned_formats_retain_large_values_and_constraints(string format, string expectedFormat, string expectedType)
    {
        JsonNode input = JsonNode.Parse("""{"components":{"schemas":{"Value":{"type":"integer","maximum":18446744073709551615,"minimum":0,"example":18446744073709551615}}}}""")!;
        input["components"]!["schemas"]!["Value"]!["format"] = format;
        JsonNode result = Run("NormalizeKiotaResponseCompatibility", input, new OpenApiFixerOptions());
        JsonNode value = result["components"]!["schemas"]!["Value"]!;
        await Assert.That(value["format"]!.GetValue<string>()).IsEqualTo(expectedFormat);
        await Assert.That(value["type"]!.GetValue<string>()).IsEqualTo(expectedType);
        await Assert.That(value["maximum"]!.GetValue<decimal>()).IsEqualTo(18446744073709551615m);
        await Assert.That(value["example"]!.GetValue<decimal>()).IsEqualTo(18446744073709551615m);
        await Assert.That(JsonNode.DeepEquals(input, Run("NormalizeKiotaResponseCompatibility", input,
            new OpenApiFixerOptions { NormalizeUnsignedIntegerFormats = false }))).IsTrue();
        await Assert.That(JsonNode.DeepEquals(result, Run("NormalizeKiotaResponseCompatibility", result, new OpenApiFixerOptions()))).IsTrue();
    }

    [Test]
    [Arguments("api.cloudflare.com", true)]
    [Arguments("example.com", false)]
    public async ValueTask Only_the_known_Cloudflare_GET_body_is_removed(string host, bool corrected)
    {
        JsonNode input = JsonNode.Parse("""{"servers":[{}],"paths":{"/accounts/{accountId}/teamnet/virtual_networks/{virtualNetworkId}":{"get":{"requestBody":{"required":true,"content":{}}},"patch":{"requestBody":{"required":true,"content":{}}}},"/other":{"get":{"requestBody":{"required":true,"content":{}}}}}}""")!;
        input["servers"]![0]!["url"] = "https://" + host + "/client/v4";
        JsonNode result = Run("CorrectKnownProviderOperations", input);
        JsonNode operation = result["paths"]!["/accounts/{accountId}/teamnet/virtual_networks/{virtualNetworkId}"]!["get"]!;
        await Assert.That(operation["requestBody"] == null).IsEqualTo(corrected);
        if (corrected) await Assert.That(operation["x-client-original-request-body"]!["required"]!.GetValue<bool>()).IsTrue();
        await Assert.That(result["paths"]!["/other"]!["get"]!["requestBody"]).IsNotNull();
        await Assert.That(result["paths"]!["/accounts/{accountId}/teamnet/virtual_networks/{virtualNetworkId}"]!["patch"]!["requestBody"]).IsNotNull();
        await Assert.That(JsonNode.DeepEquals(result, Run("CorrectKnownProviderOperations", result))).IsTrue();
    }

    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async ValueTask Real_discriminator_labels_are_rebuilt_after_intersection_normalization(bool overlap)
    {
        JsonNode input = JsonNode.Parse("""
            {"components":{"schemas":{
              "Choice":{"anyOf":[{"$ref":"#/components/schemas/A"},{"$ref":"#/components/schemas/B"}],
                "x-client-original-discriminator":{"propertyName":"kind","mapping":{"wrong":"#/components/schemas/A"}}},
              "A":{"type":"object","required":["kind"],"properties":{"kind":{"type":"string","enum":["a","alias"]}}},
              "B":{"type":"object","required":["kind"],"properties":{"kind":{"type":"string","const":"b"}}}
            }}}
            """)!;
        if (overlap) input["components"]!["schemas"]!["B"]!["properties"]!["kind"]!["const"] = "a";
        JsonNode result = Run("PreserveGeneratorUnionBranches", input);
        JsonNode choice = result["components"]!["schemas"]!["Choice"]!;
        await Assert.That(choice["discriminator"] != null).IsEqualTo(!overlap);
        if (!overlap)
        {
            await Assert.That(choice["discriminator"]!["mapping"]!["alias"]!.GetValue<string>()).IsEqualTo("#/components/schemas/A");
            await Assert.That(choice["discriminator"]!["mapping"]!["b"]!.GetValue<string>()).IsEqualTo("#/components/schemas/B");
            await Assert.That(choice["anyOf"]).IsNull();
            await Assert.That(choice["oneOf"]).IsNotNull();
        }
        await Assert.That(JsonNode.DeepEquals(result, Run("PreserveGeneratorUnionBranches", result))).IsTrue();
    }

    [Test]
    [Arguments("0.5", "1")]
    [Arguments("1.5", "3")]
    [Arguments("2", "2")]
    public async ValueTask Decimal_projection_preserves_integer_multiples_and_nullability(string step, string expected)
    {
        JsonNode input = JsonNode.Parse("""{"components":{"schemas":{"Value":{"type":["integer","null"],"format":"uint64"}}}}""")!;
        input["components"]!["schemas"]!["Value"]!["multipleOf"] = JsonNode.Parse(step);
        JsonNode value = Run("NormalizeKiotaResponseCompatibility", input, new OpenApiFixerOptions())["components"]!["schemas"]!["Value"]!;
        await Assert.That(value["multipleOf"]!.GetValue<decimal>()).IsEqualTo(decimal.Parse(expected, System.Globalization.CultureInfo.InvariantCulture));
        await Assert.That(value["type"]!.ToJsonString()).IsEqualTo("[\"number\",\"null\"]");
        await Assert.That(value["x-client-original-multipleOf"]!.ToJsonString()).IsEqualTo(step);
    }

    private static JsonNode Run(string method, JsonNode input, OpenApiFixerOptions? options = null) =>
        JsonNode.Parse((string)typeof(OpenApiFixer).GetMethod(method, BindingFlags.NonPublic | BindingFlags.Static)!
            .Invoke(null, options == null ? [input.ToJsonString()] : [input.ToJsonString(), options])!)!;
}
