public class RedisStream
{
    public List<StreamEntry> Entries { get; } = new();

    public void AddToStream(string entryId, List<KeyValuePair<string, string>> data)
    {
        Entries.Add(new StreamEntry(entryId, data));
    }
}