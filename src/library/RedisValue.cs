public class RedisValue
{
    private readonly DateTime _expiresAt;
    public RedisValueType Type { get; }
    public string Value { get; set; }
    public RedisStream Stream { get; }
    public double? ExpiryInMilliseconds { get; set; }

    private string Expiry => ExpiryInMilliseconds.HasValue ? _expiresAt.ToString() : "never";


    public RedisValue(string value, double? expiryInMilliseconds)
    {
        Value = value;
        ExpiryInMilliseconds = expiryInMilliseconds;
        Type = RedisValueType.STRING;

        if (expiryInMilliseconds.HasValue)
        {
            _expiresAt = DateTime.UnixEpoch.AddMilliseconds(expiryInMilliseconds.Value);
            Console.WriteLine($"Added key. Expires at: {DateTime.UnixEpoch.AddMilliseconds(expiryInMilliseconds.Value)}");
        }
    }

    public RedisValue(List<KeyValuePair<string, string>> streamEntryData, string streamEntryId)
    {
        if (streamEntryData == null)
        {
            throw new ArgumentNullException(nameof(streamEntryData));
        }

        if (streamEntryId == null)
        {
            throw new ArgumentNullException(nameof(streamEntryId));
        }

        Stream = new RedisStream();
        Type = RedisValueType.STREAM;
        AddToStream(streamEntryId, streamEntryData);
    }

    public void AddToStream(string streamEntryId, List<KeyValuePair<string, string>> streamEntryData)
    {
        if (Stream is null)
        {
            throw new ArgumentException("Value is not of type 'stream'");
        }

        Stream.AddToStream(streamEntryId, streamEntryData);
    }

    public bool IsExpired()
    {
        if (!ExpiryInMilliseconds.HasValue)
        {
            return false;
        }

        return DateTime.UtcNow > _expiresAt;
    }

    public override string ToString()
    {
        return $"{Value} | __expiry: {Expiry}";
    }
}