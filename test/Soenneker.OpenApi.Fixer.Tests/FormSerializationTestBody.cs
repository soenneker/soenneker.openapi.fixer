using System;
using System.Collections.Generic;
using Microsoft.Kiota.Abstractions.Serialization;

namespace Soenneker.OpenApi.Fixer.Tests;

internal sealed class FormSerializationTestBody : IParsable
{
    public Dictionary<string, object> Fields { get; } = new();
    public IDictionary<string, Action<IParseNode>> GetFieldDeserializers() => new Dictionary<string, Action<IParseNode>>();
    public void Serialize(ISerializationWriter writer) => writer.WriteAdditionalData(Fields);
}
