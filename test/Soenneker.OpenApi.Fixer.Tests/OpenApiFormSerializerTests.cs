using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Microsoft.Kiota.Abstractions;
using Soenneker.OpenApi.Generated;

namespace Soenneker.OpenApi.Fixer.Tests;

public sealed class OpenApiFormSerializerTests
{
    [Test]
    public async Task Deep_fields_and_indexed_arrays_preserve_empty_values()
    {
        var body = new FormSerializationTestBody();
        body.Fields["name"] = "A & B";
        body.Fields["metadata"] = new Dictionary<string, object> { ["accountId"] = "acct/1" };
        body.Fields["items"] = new[] { new Dictionary<string, object> { ["quantity"] = 2 } };
        body.Fields["empty"] = new Dictionary<string, object>();
        body.Fields["clear"] = "";
        body.Fields["enabled"] = true;
        body.Fields["amount"] = 1.25m;
        string form = await Serialize(body, """{"metadata":{"style":"deepObject","explode":true},"items":{"style":"deepObject","explode":true},"empty":{"style":"deepObject","explode":true}}""");
        await Assert.That(form).IsEqualTo("name=A%20%26%20B&metadata%5BaccountId%5D=acct%2F1&items%5B0%5D%5Bquantity%5D=2&empty=&clear=&enabled=true&amount=1.25");
    }

    [Test]
    public async Task Ordinary_form_and_explode_rules_are_not_changed_to_brackets()
    {
        var body = new FormSerializationTestBody();
        body.Fields["repeat"] = new[] { "a", "b" };
        body.Fields["csv"] = new[] { "a", "b" };
        body.Fields["map"] = new Dictionary<string, object> { ["city"] = "London", ["code"] = "AB" };
        body.Fields["compact"] = new Dictionary<string, object> { ["a"] = 1, ["b"] = 2 };
        body.Fields["spaces"] = new[] { "x", "y" };
        body.Fields["pipes"] = new[] { "x", "y" };
        string form = await Serialize(body, """{"csv":{"style":"form","explode":false},"compact":{"explode":false},"spaces":{"style":"spaceDelimited"},"pipes":{"style":"pipeDelimited"}}""");
        await Assert.That(form).IsEqualTo("repeat=a&repeat=b&csv=a%2Cb&city=London&code=AB&compact=a%2C1%2Cb%2C2&spaces=x%20y&pipes=x%7Cy");
    }

    private static async Task<string> Serialize(FormSerializationTestBody body, string rules)
    {
        var request = new RequestInformation();
        OpenApiFormSerializer.SetContent(request, body, rules);
        using var reader = new StreamReader(request.Content!);
        return await reader.ReadToEndAsync();
    }
}
