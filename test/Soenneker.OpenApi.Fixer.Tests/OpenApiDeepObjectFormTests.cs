using System.IO;
using System.Linq;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Soenneker.OpenApi.Fixer.Abstract;
using Soenneker.Tests.HostedUnit;

namespace Soenneker.OpenApi.Fixer.Tests;

[ClassDataSource<Host>(Shared = SharedType.PerTestSession)]
public sealed class OpenApiDeepObjectFormTests : HostedUnitTest
{
    private readonly IOpenApiFixer _fixer;
    public OpenApiDeepObjectFormTests(Host host) : base(host) => _fixer = Resolve<IOpenApiFixer>(true);

    [Test]
    public async ValueTask Fixed_scalar_fields_are_flattened_only_for_form_requests(CancellationToken ct)
    {
        JsonObject document = Document();
        JsonObject result = await Fix(document, ct);
        JsonNode form = result["paths"]!["/contacts"]!["post"]!["requestBody"]!["content"]!["application/x-www-form-urlencoded"]!;
        JsonNode properties = form["schema"]!["properties"]!;
        await Assert.That(properties["address[city]"]!["maxLength"]!.GetValue<int>()).IsEqualTo(50);
        await Assert.That(properties["address[postal_code]"]).IsNotNull();
        await Assert.That(properties["address"]).IsNull();
        await Assert.That(form["encoding"]!["address"]).IsNull();
        await Assert.That(result["components"]!["schemas"]!["Address"]!["properties"]!["city"]).IsNotNull();
        await Assert.That(result["components"]!["schemas"]!["Contact"]!["properties"]!["address"]).IsNotNull();
        JsonNode json = result["paths"]!["/contacts"]!["post"]!["requestBody"]!["content"]!["application/json"]!;
        await Assert.That(json["schema"]!["$ref"]!.GetValue<string>()).IsEqualTo("#/components/schemas/Contact");
        await Assert.That(result["paths"]!["/contacts"]!["get"]!["parameters"]![0]!["style"]!.GetValue<string>()).IsEqualTo("deepObject");
    }

    [Test]
    public async ValueTask Required_parent_and_children_remain_required(CancellationToken ct)
    {
        JsonObject doc = Document();
        doc["components"]!["schemas"]!["Contact"]!["required"] = new JsonArray("address");
        doc["components"]!["schemas"]!["Address"]!["required"] = new JsonArray("city");
        JsonObject result = await Fix(doc, ct);
        JsonNode schema = FormSchema(result);
        await Assert.That(schema["required"]!.AsArray().Any(n => n!.GetValue<string>() == "address[city]")).IsTrue();
        await Assert.That(schema["required"]!.AsArray().Any(n => n!.GetValue<string>() == "address")).IsFalse();
    }

    [Test]
    public async ValueTask Unsupported_shapes_and_constraints_are_not_flattened(CancellationToken ct)
    {
        foreach (string variant in new[] { "open", "implicitOpen", "dictionary", "array", "nested", "optionalRequired", "requiredEmpty", "constraint", "nullable", "collision", "explodeFalse", "default", "responseOnly", "mediaExample", "childNullable", "parentConstraint" })
        {
            JsonObject doc = Document();
            JsonNode address = doc["components"]!["schemas"]!["Address"]!;
            JsonNode contact = doc["components"]!["schemas"]!["Contact"]!;
            JsonNode media = doc["paths"]!["/contacts"]!["post"]!["requestBody"]!["content"]!["application/x-www-form-urlencoded"]!;
            switch (variant)
            {
                case "open": address["additionalProperties"] = true; break;
                case "implicitOpen": address.AsObject().Remove("additionalProperties"); break;
                case "dictionary": address["additionalProperties"] = new JsonObject { ["type"] = "string" }; break;
                case "array": address["properties"]!["city"] = JsonNode.Parse("""{"type":"array","items":{"type":"string"}}"""); break;
                case "nested": address["properties"]!["city"] = JsonNode.Parse("""{"type":"object","properties":{"name":{"type":"string"}}}"""); break;
                case "optionalRequired": address["required"] = new JsonArray("city"); break;
                case "requiredEmpty": contact["required"] = new JsonArray("address"); break;
                case "constraint": address["minProperties"] = 1; break;
                case "nullable": address["nullable"] = true; break;
                case "collision": contact["properties"]!["address[city]"] = new JsonObject { ["type"] = "integer" }; break;
                case "explodeFalse": media["encoding"]!["address"]!["explode"] = false; break;
                case "default": address["default"] = new JsonObject { ["city"] = "Example" }; break;
                case "mediaExample": media["example"] = new JsonObject { ["address"] = new JsonObject { ["city"] = "Example" } }; break;
                case "childNullable": address["properties"]!["city"]!["nullable"] = true; break;
                case "parentConstraint": contact["maxProperties"] = 2; break;
                case "responseOnly": media.AsObject().Remove("encoding"); break;
            }
            JsonObject result = await Fix(doc, ct);
            JsonNode schema = Resolve(result, FormSchema(result));
            await Assert.That(schema["properties"]!["address"]).IsNotNull();
        }
    }

    private static JsonNode Resolve(JsonNode root, JsonNode node)
    {
        while (node["$ref"] is JsonValue reference) node = root["components"]!["schemas"]![reference.GetValue<string>().Split('/')[^1]]!;
        return node;
    }

    private static JsonNode FormSchema(JsonNode root) => root["paths"]!["/contacts"]!["post"]!["requestBody"]!["content"]!["application/x-www-form-urlencoded"]!["schema"]!;

    private static JsonObject Document() => JsonNode.Parse("""
    {"openapi":"3.0.3","info":{"title":"Form test","version":"1"},"paths":{"/contacts":{
      "post":{"operationId":"createContact","requestBody":{"content":{
        "application/x-www-form-urlencoded":{"schema":{"$ref":"#/components/schemas/Contact"},"encoding":{"address":{"style":"deepObject","explode":true}}},
        "application/json":{"schema":{"$ref":"#/components/schemas/Contact"}}}},"responses":{"204":{"description":"Created"}}},
      "get":{"operationId":"getContacts","parameters":[{"name":"address","in":"query","style":"deepObject","explode":true,"schema":{"$ref":"#/components/schemas/Address"}}],"responses":{"200":{"description":"Contact","content":{"application/json":{"schema":{"$ref":"#/components/schemas/Contact"}}}}}}
    }},"components":{"schemas":{
      "Contact":{"type":"object","properties":{"name":{"type":"string"},"address":{"$ref":"#/components/schemas/Address"}}},
      "Address":{"type":"object","additionalProperties":false,"properties":{"city":{"type":"string","maxLength":50},"postal_code":{"type":"string"}}}
    }}}
    """)!.AsObject();

    private async ValueTask<JsonObject> Fix(JsonObject document, CancellationToken ct)
    {
        string source = Path.GetTempFileName(), target = Path.GetTempFileName();
        try
        {
            await File.WriteAllTextAsync(source, document.ToJsonString(), ct);
            await _fixer.Fix(source, target, ct);
            return JsonNode.Parse(await File.ReadAllTextAsync(target, ct))!.AsObject();
        }
        finally { File.Delete(source); File.Delete(target); }
    }
}
