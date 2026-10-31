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
    public async ValueTask Mixed_json_and_binary_representations_prefer_json()
    {
        const string source = """
            {"components":{"schemas":{}},"paths":{"/image":{"get":{"responses":{"200":{"description":"Image or metadata","content":{
              "application/json":{"schema":{"type":"object"}},"image/png":{"schema":{"type":"string","format":"binary"}}
            }}}}}}}
            """;
        string repaired = Repair(source);
        JsonNode response = JsonNode.Parse(repaired)!["paths"]!["/image"]!["get"]!["responses"]!["200"]!;
        await Assert.That(response["content"]!["application/json"]!["schema"]!["type"]!.GetValue<string>()).IsEqualTo("object");
        await Assert.That(response["content"]!["image/png"]).IsNull();
        await Assert.That(response["x-client-original-non-json-content"]!["image/png"]!["schema"]!["format"]!.GetValue<string>()).IsEqualTo("binary");
        await Assert.That(Repair(repaired)).IsEqualTo(repaired);
    }

    [Test]
    public async ValueTask Vendor_json_preserves_references_and_shared_responses()
    {
        const string source = """
            {"components":{"schemas":{"Comparison":{"type":"object","properties":{"behind_by":{"type":"integer"}}}},
             "responses":{"Comparison":{"description":"Comparison","content":{
               "application/vnd.github+json":{"schema":{"$ref":"#/components/schemas/Comparison"}},
               "application/vnd.github.diff":{"schema":{"type":"string"}},
               "application/vnd.github.patch":{"schema":{"type":"string"}}
             }}}},"paths":{"/compare":{"get":{"responses":{"200":{"$ref":"#/components/responses/Comparison"},"204":{"description":"Empty"}}}}}}
            """;
        string repaired = Repair(source);
        JsonNode root = JsonNode.Parse(repaired)!;
        JsonNode content = root["paths"]!["/compare"]!["get"]!["responses"]!["200"]!["content"]!;
        await Assert.That(content.AsObject().Count).IsEqualTo(1);
        await Assert.That(content["application/vnd.github+json"]!["schema"]!["$ref"]!.GetValue<string>()).IsEqualTo("#/components/schemas/Comparison");
        await Assert.That(root["components"]!["responses"]!["Comparison"]!["content"]!.AsObject().Count).IsEqualTo(3);
        await Assert.That(Repair(repaired)).IsEqualTo(repaired);
    }

    [Test]
    public async ValueTask Binary_only_success_status_keeps_stream_fallback()
    {
        const string source = """
            {"paths":{"/download":{"get":{"responses":{
              "200":{"description":"Data","content":{"application/octet-stream":{"schema":{"type":"string","format":"binary"}}}},
              "202":{"description":"Pending","content":{"application/json":{"schema":{"type":"object"}}}}
            }}}}}
            """;
        JsonNode responses = JsonNode.Parse(Repair(source))!["paths"]!["/download"]!["get"]!["responses"]!;
        await Assert.That(responses["200"]!["content"]!["application/octet-stream"]!["schema"]!["format"]!.GetValue<string>()).IsEqualTo("binary");
        await Assert.That(responses["202"]!["content"]!["application/json"]!["schema"]!["format"]!.GetValue<string>()).IsEqualTo("binary");
    }

    [Test]
    public async ValueTask Json_preference_applies_without_components_or_distinct_schemas()
    {
        const string source = """
            {"paths":{"/text":{"get":{"responses":{"200":{"description":"Text","content":{
              "application/json":{"schema":{"type":"string"}},"text/plain":{"schema":{"type":"string"}}
            }}}}}}}
            """;
        JsonNode content = JsonNode.Parse(Repair(source))!["paths"]!["/text"]!["get"]!["responses"]!["200"]!["content"]!;
        await Assert.That(content.AsObject().Count).IsEqualTo(1);
        await Assert.That(content["application/json"]!["schema"]!["type"]!.GetValue<string>()).IsEqualTo("string");
    }

    private static string Repair(string json) => (string)typeof(OpenApiFixer).GetMethod("ProjectDivergentSuccessResponses", BindingFlags.NonPublic | BindingFlags.Static)!.Invoke(null, [json])!;

    [Test]
    public async ValueTask Multi_status_bodies_get_a_generation_alias_without_losing_the_original_status()
    {
        const string source = """
            {"paths":{"/batch":{"post":{"responses":{"207":{"description":"Multi-status","content":{
              "application/json":{"schema":{"type":"object","properties":{"items":{"type":"array","items":{"type":"string"}}}}}
            }}}}}}}
            """;
        string repaired = Repair(source);
        JsonNode responses = JsonNode.Parse(repaired)!["paths"]!["/batch"]!["post"]!["responses"]!;
        await Assert.That(responses["200"]!["x-client-success-response-status"]!.GetValue<string>()).IsEqualTo("207");
        await Assert.That(responses["200"]!["content"]!.ToJsonString()).IsEqualTo(responses["207"]!["content"]!.ToJsonString());
        await Assert.That(Repair(repaired)).IsEqualTo(repaired);
    }

    [Test]
    public async ValueTask Preferred_json_statuses_still_share_a_union()
    {
        const string source = """
            {"paths":{"/jobs":{"get":{"operationId":"getJobs","responses":{
              "200":{"description":"Complete","content":{"application/json":{"schema":{"type":"array","items":{"type":"string"}}},"text/plain":{"schema":{"type":"string"}}}},
              "202":{"description":"Pending","content":{"application/json":{"schema":{"type":"object"}}}}
            }}}}}
            """;
        string repaired = Repair(source);
        JsonNode root = JsonNode.Parse(repaired)!;
        JsonNode responses = root["paths"]!["/jobs"]!["get"]!["responses"]!;
        await Assert.That(responses["200"]!["content"]!["text/plain"]).IsNull();
        await Assert.That(responses["200"]!["content"]!["application/json"]!["schema"]!.ToJsonString()).IsEqualTo(responses["202"]!["content"]!["application/json"]!["schema"]!.ToJsonString());
        await Assert.That(root["components"]!["schemas"]!.AsObject().Count).IsEqualTo(1);
        await Assert.That(Repair(repaired)).IsEqualTo(repaired);
    }
}
