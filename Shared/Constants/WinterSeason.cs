namespace Shared.Constants;

public static class WinterSeason
{
    public const int StartMonth = 11;
    public const int StartDay = 1;
    public const int EndMonth = 5;
    public const int EndDay = 9;

    public static bool IsInSeason(DateTime date)
    {
        var month = date.Month;
        var day = date.Day;

        if (month > StartMonth || month < EndMonth)
            return true;

        if (month == StartMonth)
            return day >= StartDay;

        if (month == EndMonth)
            return day <= EndDay;

        return false;
    }
}
