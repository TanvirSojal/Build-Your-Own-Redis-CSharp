public class RedisStream
{
    public string Id { get; set; }
    public List<KeyValuePair<string, string>> Data { get; }

    public RedisStream(string streamId)
    {
        Id = streamId;
        Data = new List<KeyValuePair<string, string>>();
    }

    public void AddToStream(KeyValuePair<string, string> kv)
    {
        Data.Add(kv);
    }
}