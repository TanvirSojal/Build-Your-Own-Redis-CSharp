public class RedisValue
{
    private readonly DateTime _expiresAt;
    public string Value { get; set; }
    public double? ExpiryInMilliseconds { get; set; }

    private string Expiry => ExpiryInMilliseconds.HasValue ? _expiresAt.ToString() : "never";


    public RedisValue(string value, double? expiryInMilliseconds)
    {
        Value = value;
        ExpiryInMilliseconds = expiryInMilliseconds;

        if (expiryInMilliseconds.HasValue){
            _expiresAt = DateTime.UnixEpoch.AddMilliseconds(expiryInMilliseconds.Value);
            Console.WriteLine($"Added key. Expires at: {DateTime.UnixEpoch.AddMilliseconds(expiryInMilliseconds.Value)}");
        }
    }

    public bool IsExpired()
    {
        if (!ExpiryInMilliseconds.HasValue){
            return false;
        }

        return DateTime.UtcNow > _expiresAt;
    }

    public override string ToString()
    {
        return $"{Value} | __expiry: {Expiry}";
    }
}