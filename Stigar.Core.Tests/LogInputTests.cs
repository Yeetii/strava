using Stigar.Core.Logging;

namespace Stigar.Core.Tests;

public class LogInputTests
{
    [Theory]
    [InlineData(null, "")]
    [InlineData("", "")]
    [InlineData("user-123", "user-123")]
    [InlineData("evil\r\nINFO forged line", "evilINFO forged line")]
    [InlineData("line\u2028break\u2029here", "line break here")]
    public void Sanitize_RemovesLineBreaks(string? input, string expected)
    {
        Assert.Equal(expected, LogInput.Sanitize(input));
    }
}
