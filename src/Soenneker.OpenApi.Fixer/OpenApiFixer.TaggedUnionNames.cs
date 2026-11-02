using System;
using System.Collections.Generic;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;

namespace Soenneker.OpenApi.Fixer;

public sealed partial class OpenApiFixer
{
    private static string GetTaggedUnionModelName(string parent, string branch, string label, HashSet<string> names)
    {
        const int maximumLength = 96;
        string candidate = branch.StartsWith(parent, StringComparison.OrdinalIgnoreCase) ? branch + "Variant" : parent + branch;
        if (candidate.Length > maximumLength)
        {
            var readable = new StringBuilder("Tagged");
            bool capitalize = true;
            foreach (char character in label)
            {
                if (!char.IsAsciiLetterOrDigit(character)) { capitalize = true; continue; }
                readable.Append(capitalize ? char.ToUpperInvariant(character) : character);
                capitalize = false;
                if (readable.Length >= 40) break;
            }
            // Hash the complete identities, not their truncated prefixes or the potentially shared wire tag.
            string hash = Convert.ToHexString(SHA256.HashData(Encoding.UTF8.GetBytes(parent + "\0" + branch)))[..16];
            candidate = readable + "Variant" + hash;
        }
        string result = candidate;
        int suffix = 2;
        while (!names.Add(result))
        {
            string number = (suffix++).ToString(CultureInfo.InvariantCulture);
            result = candidate[..Math.Min(candidate.Length, maximumLength - number.Length)] + number;
        }
        return result;
    }
}
