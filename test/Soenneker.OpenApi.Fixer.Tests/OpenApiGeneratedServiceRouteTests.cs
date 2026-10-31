using System;
using System.Collections.Generic;
using System.Reflection;
using System.IO;
using System.Text.Json.Nodes;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Soenneker.OpenApi.Fixer.Tests;

public sealed class OpenApiGeneratedServiceRouteTests
{
    [Test]
    public async ValueTask Parameter_suffix_routes_do_not_match_their_container_builder()
    {
        Type type = typeof(OpenApiFixer).Assembly.GetType("Soenneker.OpenApi.Fixer.OpenApiGeneratedRouteMetadata")!;
        object metadata = Activator.CreateInstance(type, true)!;
        var routes = (Dictionary<string, Dictionary<string, (string Path, string Server)>>)type.GetProperty("Routes", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(metadata)!;
        routes["/streams/{}.{}"] = new() { ["GET"] = ("/streams/{streamId}.{ext}", "https://example.com") };
        const string source = "public Builder(IRequestAdapter requestAdapter) : base(requestAdapter, \"{+baseurl}/streams/{streamId}\") {}";
        string result = (string)typeof(OpenApiFixer).GetMethod("RepairGeneratedServiceRoutes", BindingFlags.Static | BindingFlags.NonPublic)!.Invoke(null, [source, metadata])!;
        await Assert.That(result).IsEqualTo(source);
    }

    [Test]
    [Arguments("?start_date={start_date}{&limit*}", "", false)]
    [Arguments("?start_date={start_date}{&limit*}", "", true)]
    [Arguments("{?limit*}", "", false)]
    [Arguments("{?limit*}", "", true)]
    [Arguments("", "", false)]
    [Arguments("", "", true)]
    [Arguments("", ".json", false)]
    [Arguments("", ".json", true)]
    public async ValueTask Namespaced_routes_with_required_queries_restore_wire_paths(string query, string suffix, bool sameServer)
    {
        Type metadataType = typeof(OpenApiFixer).Assembly.GetType("Soenneker.OpenApi.Fixer.OpenApiGeneratedRouteMetadata")!;
        object metadata = Activator.CreateInstance(metadataType, true)!;
        if (sameServer) metadataType.GetProperty("DefaultServer", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(metadata, "https://api.example.com/v1");
        var routes = (Dictionary<string, Dictionary<string, (string Path, string Server)>>)metadataType.GetProperty("Routes", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(metadata)!;
        routes["/source/items/{}" + suffix] = new() { ["GET"] = ("/items/{id}" + suffix, "https://api.example.com/v1") };
        string source = "public Builder(IRequestAdapter requestAdapter) : base(requestAdapter, \"{+baseurl}/source/items/{itemId}" + query + "\") {}\nvoid Get() { var info = new RequestInformation(Method.GET, UrlTemplate, PathParameters); }";
        MethodInfo repair = typeof(OpenApiFixer).GetMethod("RepairGeneratedServiceRoutes", BindingFlags.Static | BindingFlags.NonPublic)!;
        string result = (string)repair.Invoke(null, [source, metadata])!;
        if (sameServer)
        {
            await Assert.That(result).DoesNotContain(".Resolve(");
            await Assert.That(result).Contains(JsonSerializer.Serialize("{+baseurl}/items/{itemId}" + suffix + query));
        }
        else
            await Assert.That(result).Contains(".Resolve(UrlTemplate, RequestAdapter.BaseUrl, \"https://api.example.com/v1\", \"/items/{itemId}" + suffix + "\", PathParameters.ContainsKey");
        await Assert.That((string)repair.Invoke(null, [result, metadata])!).IsEqualTo(result);
    }
    [Test]
    [Arguments("{baseUrl}")]
    [Arguments("https://api.example.com/{tenant}")]
    public async ValueTask Unresolved_service_variables_remain_explicit_in_route_metadata(string server)
    {
        string root = Path.Combine(Path.GetTempPath(), "route-metadata-" + Guid.NewGuid().ToString("N"));
        Directory.CreateDirectory(root);
        try
        {
            JsonObject document = JsonNode.Parse("""{"paths":{"/namespace/items":{"get":{"x-client-original-path":"/items"}}}}""")!.AsObject();
            document["servers"] = new JsonArray(new JsonObject { ["url"] = server });
            await File.WriteAllTextAsync(Path.Combine(root, "spec.json"), document.ToJsonString());
            await File.WriteAllTextAsync(Path.Combine(root, "kiota-lock.json"), """{"descriptionLocation":"spec.json"}""");
            object pending = typeof(OpenApiFixer).GetMethod("ReadGeneratedServiceRoutes", BindingFlags.Static | BindingFlags.NonPublic)!.Invoke(null, [root, CancellationToken.None])!;
            Task task = (Task)pending.GetType().GetMethod("AsTask")!.Invoke(pending, null)!;
            await task;
            object metadata = task.GetType().GetProperty("Result")!.GetValue(task)!;
            Type type = metadata.GetType();
            var routes = (Dictionary<string, Dictionary<string, (string Path, string Server)>>)type.GetProperty("Routes", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(metadata)!;
            await Assert.That(routes["/namespace/items"]["GET"].Server).IsEqualTo(server);
            await Assert.That(type.GetProperty("DefaultServer", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(metadata)).IsNull();
        }
        finally { Directory.Delete(root, true); }
    }
    [Test]
    public async ValueTask Ordinary_routes_keep_the_original_template_without_runtime_resolution()
    {
        Type type = typeof(OpenApiFixer).Assembly.GetType("Soenneker.OpenApi.Fixer.OpenApiGeneratedRouteMetadata")!;
        object metadata = Activator.CreateInstance(type, true)!;
        type.GetProperty("DefaultServer", BindingFlags.Instance | BindingFlags.NonPublic)!.SetValue(metadata, "https://api.example.com/v2");
        var routes = (Dictionary<string, Dictionary<string, (string Path, string Server)>>)type.GetProperty("Routes", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(metadata)!;
        routes["/items"] = new() { ["GET"] = ("/items", "https://api.example.com/v2") };
        const string source = """
            public Builder(IRequestAdapter requestAdapter) : base(requestAdapter, "{+baseurl}/items{?limit*}") {}
            void Get() { var request = new RequestInformation(Method.GET, UrlTemplate, PathParameters); }
            """;
        string result = (string)typeof(OpenApiFixer).GetMethod("RepairGeneratedServiceRoutes", BindingFlags.Static | BindingFlags.NonPublic)!.Invoke(null, [source, metadata])!;
        await Assert.That(result).IsEqualTo(source);
    }
}
