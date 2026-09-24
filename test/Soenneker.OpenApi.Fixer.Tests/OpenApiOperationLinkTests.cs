using Soenneker.Utils.File.Abstract;
using System;
using System.IO;
using System.Text.Json.Nodes;
using System.Threading;
using System.Threading.Tasks;
using Soenneker.OpenApi.Fixer.Abstract;
using Soenneker.Tests.HostedUnit;

namespace Soenneker.OpenApi.Fixer.Tests;

[ClassDataSource<Host>(Shared = SharedType.PerTestSession)]
public sealed class OpenApiOperationLinkTests(Host host) : HostedUnitTest(host)
{
    [Test]
    [Arguments("missing")]
    [Arguments("unique")]
    [Arguments("ambiguous")]
    [Arguments("local")]
    public async Task FixOperationLinks_resolves_only_unambiguous_targets_and_preserves_payloads(string scenario, CancellationToken token)
    {
        string directory = Path.Combine(Path.GetTempPath(), "operation-links-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            JsonObject source = JsonNode.Parse("""
                {"openapi":"3.1.0","info":{"title":"Control","version":"1"},"paths":{"/indexes/{index_name}":{"get":{
                "operationId":"describe","responses":{"200":{"description":"OK","links":{
                "target":{"operationId":"upsert","server":{"url":"$response.body#/host"}},
                "explicit":{"operationRef":"https://example.com/spec.json#/paths/~1upsert/post"}},
                "content":{"application/json":{"example":{"links":{"fake":{"operationId":"missing"}}}}}}}}}}}
                """)!.AsObject();
            if (scenario == "local")
                source["paths"]!["/indexes/{index_name}"]!["get"]!["operationId"] = "upsert";
            string sourcePath = Path.Combine(directory, "control.json");
            await Resolve<IFileUtil>(true).Write(sourcePath, source.ToJsonString(), cancellationToken: token);
            const string target = """{"openapi":"3.1.0","paths":{"/vectors/upsert":{"post":{"operationId":"upsert","responses":{"200":{"description":"OK"}}}}}}""";
            if (scenario != "missing")
            {
                Directory.CreateDirectory(Path.Combine(directory, "data files"));
                await Resolve<IFileUtil>(true).Write(Path.Combine(directory, "data files", "data.json"), target, cancellationToken: token);
            }
            if (scenario is "ambiguous" or "local")
                await Resolve<IFileUtil>(true).Write(Path.Combine(directory, "other.json"), target, cancellationToken: token);

            IOpenApiFixer fixer = Resolve<IOpenApiFixer>(true);
            await fixer.FixOperationLinks(directory, token);
            string result = await Resolve<IFileUtil>(true).Read(sourcePath, cancellationToken: token);
            JsonNode response = JsonNode.Parse(result)!["paths"]!["/indexes/{index_name}"]!["get"]!["responses"]!["200"]!;
            JsonNode? link = response["links"]!["target"];
            if (scenario is "missing" or "ambiguous")
                await Assert.That(link).IsNull();
            else
            {
                await Assert.That(link!["server"]!["url"]!.GetValue<string>()).IsEqualTo("$response.body#/host");
                if (scenario == "unique")
                {
                    await Assert.That(link["operationRef"]!.GetValue<string>()).IsEqualTo("data%20files/data.json#/paths/~1vectors~1upsert/post");
                    await Assert.That(link["operationId"]).IsNull();
                }
                else
                    await Assert.That(link["operationId"]!.GetValue<string>()).IsEqualTo("upsert");
            }
            await Assert.That(response["links"]!["explicit"]!["operationRef"]!.GetValue<string>()).IsEqualTo("https://example.com/spec.json#/paths/~1upsert/post");
            await Assert.That(response["content"]!["application/json"]!["example"]!["links"]!["fake"]!["operationId"]!.GetValue<string>()).IsEqualTo("missing");
            await fixer.FixOperationLinks(directory, token);
            await Assert.That(await Resolve<IFileUtil>(true).Read(sourcePath, cancellationToken: token)).IsEqualTo(result);
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }

    [Test]
    public async Task FixOperationLinks_removes_all_six_stale_Pinecone_links_without_guessing_renamed_operations(CancellationToken token)
    {
        string directory = Path.Combine(Path.GetTempPath(), "pinecone-links-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(directory);
        try
        {
            const string control = """
                {"openapi":"3.0.3","paths":{"/indexes/{index_name}":{"get":{"operationId":"describeIndex","responses":{"200":{"description":"OK","links":{
                "UpsertVector":{"operationId":"upsert"},"UpdateVector":{"operationId":"update"},"QueryVector":{"operationId":"query"},
                "FetchVector":{"operationId":"fetch"},"DeleteOneVector":{"operationId":"delete1"},"DeleteVector":{"operationId":"delete"}
                }}}}}}}
                """;
            string path = Path.Combine(directory, "db_control_2026-07.json");
            await Resolve<IFileUtil>(true).Write(path, control, cancellationToken: token);
            await Resolve<IFileUtil>(true).Write(Path.Combine(directory, "db_data_2026-07.json"), """{"openapi":"3.0.3","paths":{"/vectors/upsert":{"post":{"operationId":"upsertVectors","responses":{"200":{"description":"OK"}}}}}}""", cancellationToken: token);
            await Resolve<IOpenApiFixer>(true).FixOperationLinks(directory, token);
            JsonNode result = JsonNode.Parse(await Resolve<IFileUtil>(true).Read(path, cancellationToken: token))!;
            await Assert.That(result["paths"]!["/indexes/{index_name}"]!["get"]!["responses"]!["200"]!["links"]!.AsObject().Count).IsEqualTo(0);
            await Assert.That(result["paths"]!["/indexes/{index_name}"]!["get"]!["operationId"]!.GetValue<string>()).IsEqualTo("describeIndex");
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
    }
}
