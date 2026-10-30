using System.Reflection;
using System.Text.Json.Nodes;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Fixer.Tests;

public sealed class OpenApiSuccessResponseTests
{
    [Test]
    public async ValueTask Alternate_success_statuses_share_a_union_without_modifying_error_responses()
    {
        const string source = """
            {"components":{"schemas":{},"responses":{"Pending":{"description":"Pending","content":{"application/json":{"schema":{"type":"object"}}}}}},
             "paths":{"/jobs":{"get":{"operationId":"getJobs","responses":{
               "200":{"description":"Complete","content":{"application/json":{"schema":{"type":"array","items":{"type":"string"}}}}},
               "202":{"$ref":"#/components/responses/Pending"},"400":{"$ref":"#/components/responses/Pending"},"204":{"description":"Empty"}
             }}}}}
            """;
        string repaired = Repair(source);
        JsonNode root = JsonNode.Parse(repaired)!;
        JsonNode responses = root["paths"]!["/jobs"]!["get"]!["responses"]!;
        await Assert.That(responses["200"]!["content"]!["application/json"]!["schema"]!.ToJsonString()).IsEqualTo(responses["202"]!["content"]!["application/json"]!["schema"]!.ToJsonString());
        await Assert.That(responses["200"]!["content"]!["application/json"]!["x-client-original-success-schema"]!["type"]!.GetValue<string>()).IsEqualTo("array");
        await Assert.That(responses["400"]!["$ref"]!.GetValue<string>()).IsEqualTo("#/components/responses/Pending");
        await Assert.That(responses["204"]!["content"]).IsNull();
        await Assert.That(root["components"]!["responses"]!["Pending"]!["content"]!["application/json"]!["schema"]!["type"]!.GetValue<string>()).IsEqualTo("object");
        await Assert.That(Repair(repaired)).IsEqualTo(repaired);
    }

    [Test]
    public async ValueTask Mixed_json_and_binary_representations_preserve_both_as_response_streams()
    {
        const string source = """
            {"components":{"schemas":{}},"paths":{"/image":{"get":{"responses":{"200":{"description":"Image or metadata","content":{
              "application/json":{"schema":{"type":"object"}},"image/png":{"schema":{"type":"string","format":"binary"}}
            }}}}}}}
            """;
        JsonNode media = JsonNode.Parse(Repair(source))!["paths"]!["/image"]!["get"]!["responses"]!["200"]!["content"]!;
        await Assert.That(media["application/json"]!["schema"]!["format"]!.GetValue<string>()).IsEqualTo("binary");
        await Assert.That(media["image/png"]!["schema"]!["format"]!.GetValue<string>()).IsEqualTo("binary");
    }

    private static string Repair(string json) => (string)typeof(OpenApiFixer).GetMethod("ProjectDivergentSuccessResponses", BindingFlags.NonPublic | BindingFlags.Static)!.Invoke(null, [json])!;
}
