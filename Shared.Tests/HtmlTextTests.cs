using Shared.Services;

namespace Shared.Tests;

public class HtmlTextTests
{
    [Theory]
    [InlineData(null, "")]
    [InlineData("", "")]
    [InlineData("  ", "")]
    [InlineData("&lt;strong&gt;Ultra&lt;/strong&gt;", "Ultra")]
    [InlineData("<p>Eco&nbsp;Trail</p><script>ignore()</script>", "Eco\u00a0Trail  ignore()")]
    public void DecodeAndStripTags_ReturnsPlainDecodedText(string? html, string expected)
    {
        Assert.Equal(expected, HtmlText.DecodeAndStripTags(html));
    }
}
