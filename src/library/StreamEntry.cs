public class StreamEntry
{
    public StreamEntryId EntryId { get; set; }
    public List<KeyValuePair<string, string>> Data { get; set; } = new();

    public StreamEntry(string entryId, List<KeyValuePair<string, string>> data)
    {
        EntryId = ParseEntryId(entryId);
        Data = data;
    }

    private StreamEntryId ParseEntryId(string entryId)
    {
        var parts = entryId.Split('-');

        return new StreamEntryId
        {
            Timestamp = long.Parse(parts[0]),
            Sequence = long.Parse(parts[1])
        };
    }
}