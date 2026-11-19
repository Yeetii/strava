namespace API.Utils;

/// <summary>Pure helper for recognizing cancelled operations in endpoint filters.</summary>
public static class RequestCancellation
{
    public static bool IsCancellation(Exception exception, CancellationToken cancellationToken)
    {
        if (exception is OperationCanceledException)
            return true;

        var current = exception.InnerException;
        while (current is not null)
        {
            if (current is OperationCanceledException)
                return true;
            current = current.InnerException;
        }

        return cancellationToken.IsCancellationRequested;
    }
}
