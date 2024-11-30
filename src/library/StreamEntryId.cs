public class StreamEntryId : IComparable<StreamEntryId>
{
    public long Timestamp { get; set; }
    public long Sequence { get; set; }

    public int CompareTo(StreamEntryId? other)
    {
        if (this is null && other is null)
        {
            return 0;
        }

        if (other is null)
        {
            return 1;
        }

        if (Timestamp == other.Timestamp)
        {
            if (Sequence == other.Sequence)
            {
                return 0;
            }

            return Sequence > other.Sequence ? 1 : -1;
        }

        return Timestamp > other.Timestamp ? 1 : -1;
    }

    override public string ToString()
    {
        return $"{Timestamp}-{Sequence}";
    }
}