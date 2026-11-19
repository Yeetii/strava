namespace Stigar.Core.Logging;

/// <summary>
/// Makes untrusted values safe to place in a structured log entry. Stripping
/// line breaks prevents a caller-controlled value from forging additional log
/// lines (CWE-117 / CodeQL "Log entries created from user input") while keeping
/// the original value readable for operators.
/// </summary>
public static class LogInput
{
    public static string Sanitize(string? value)
    {
        if (string.IsNullOrEmpty(value))
            return string.Empty;

        return value
            .Replace("\r", string.Empty, StringComparison.Ordinal)
            .Replace("\n", string.Empty, StringComparison.Ordinal)
            .Replace('\u2028', ' ')
            .Replace('\u2029', ' ');
    }
}
