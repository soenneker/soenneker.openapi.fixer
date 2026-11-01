using System;
using System.IO;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Soenneker.OpenApi.Fixer.Abstract;
using Soenneker.Tests.HostedUnit;

namespace Soenneker.OpenApi.Fixer.Tests;

[ClassDataSource<Host>(Shared = SharedType.PerTestSession)]
public sealed class OpenApiStripeCustomerUnionTests(Host host) : HostedUnitTest(host)
{
    [Test]
    public async ValueTask Actual_Stripe_schema_distinguishes_active_deleted_and_invalid_customers()
    {
        // Actual Stripe source schemas (documentation removed), through the full normalization pipeline.
        string input = Path.Combine(AppContext.BaseDirectory, "Fixtures", "stripe-customer-union.json");
        string output = Path.Combine(Path.GetTempPath(), $"stripe-union-{Guid.NewGuid()}.json");
        JsonObject schemas;
        try
        {
            await Resolve<IOpenApiFixer>(true).Fix(input, output);
            schemas = JsonNode.Parse(await File.ReadAllTextAsync(output))!["components"]!["schemas"]!.AsObject();
        }
        finally { if (File.Exists(output)) File.Delete(output); }
        var active = GeneratedUnionRuntimeCompiler.Matcher(schemas["Customer"]!.AsObject(), schemas);
        var deleted = GeneratedUnionRuntimeCompiler.Matcher(schemas["DeletedCustomer"]!.AsObject(), schemas);
        const string customer = """
            {"id":"cus_example","object":"customer","created":1700000000,"livemode":false,
             "email":"test@example.com","name":"Example","phone":"+12025550123","balance":0,"delinquent":false,
             "currency":null,"description":null,"discount":null,"default_source":null,"shipping":null,
             "metadata":{"organization_id":"example"},"invoice_settings":{"custom_fields":null,"default_payment_method":null,"footer":null,"rendering_options":null},
             "address":{"city":"Austin","country":"US","line1":"123 Example St","line2":null,"postal_code":"78701","state":"TX"}}
            """;
        await Assert.That(active(customer)).IsTrue();
        await Assert.That(deleted(customer)).IsFalse();
        const string removed = """{"id":"cus_example","object":"customer","deleted":true}""";
        await Assert.That(active(removed)).IsFalse();
        await Assert.That(deleted(removed)).IsTrue();
        await Assert.That(active(customer.Replace("1700000000", "\"invalid\"", StringComparison.Ordinal))).IsFalse();
        await Assert.That(active("""{"id":"cus_example","object":"customer"}""")).IsFalse();
        const string overlapping = """{"id":"cus_example","object":"customer","created":1,"livemode":false,"deleted":true}""";
        await Assert.That(active(overlapping)).IsTrue();
        await Assert.That(deleted(overlapping)).IsTrue();
    }
}
