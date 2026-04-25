using System.Net;
using System.Text.RegularExpressions;

namespace Shared.Services;

public static partial class HtmlText
{
    public static string DecodeAndStripTags(string? html)
    {
        if (string.IsNullOrWhiteSpace(html))
            return string.Empty;

        var decoded = WebUtility.HtmlDecode(html);
        return HtmlTagRegex().Replace(decoded, " ").Trim();
    }

    [GeneratedRegex("<[^>]+>", RegexOptions.Singleline)]
    private static partial Regex HtmlTagRegex();
}
