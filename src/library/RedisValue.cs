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

    public RedisValue(KeyValuePair<string, string> keyValuePair, string streamId)
    {
        Stream = new RedisStream(streamId);
        Stream.Data.Add(keyValuePair);

        Type = RedisValueType.STREAM;
    }

    public void AddToStream(KeyValuePair<string, string> keyValuePair)
    {
        if (Stream is null)
        {
            throw new ArgumentException("Value is not of type 'stream'");
        }

        Stream.AddToStream(keyValuePair);
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