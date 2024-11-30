public static class StreamExtension
{
    public static bool IsLessThanOrEqualTo(this StreamEntryId entryId, StreamEntryId otherEntryId)
    {
        return entryId.CompareTo(otherEntryId) <= 0;
    }

    public static bool IsGreaterThanOrEqualTo(this StreamEntryId entryId, StreamEntryId otherEntryId)
    {
        return entryId.CompareTo(otherEntryId) >= 0;
    }
}