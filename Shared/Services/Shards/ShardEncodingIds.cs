namespace Shared.Services.Shards;

public static class ShardEncodingIds
{
    public static ulong FeatureIdFromString(string value)
        => Fnv1a64(value);

    public static uint TagIdFromString(string value)
        => unchecked((uint)Fnv1a64(value));

    private static ulong Fnv1a64(string value)
    {
        const ulong offset = 14695981039346656037;
        const ulong prime = 1099511628211;
        ulong hash = offset;
        foreach (var c in value)
        {
            hash ^= c;
            hash *= prime;
        }
        return hash;
    }
}
