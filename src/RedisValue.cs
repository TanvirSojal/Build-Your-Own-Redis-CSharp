public class RedisValue
{
    private readonly DateTime _createdAt;
    public string Value { get; set; }
    public ulong? ExpiryInMilliseconds { get; set; }

    private string Expiry => ExpiryInMilliseconds.HasValue ? ExpiryInMilliseconds.Value.ToString() : "never";


    public RedisValue(string value, ulong? expiryInMilliseconds)
    {
        Value = value;
        ExpiryInMilliseconds = expiryInMilliseconds;
        _createdAt = DateTime.UtcNow;
    }

    public bool IsExpired()
    {
        if (!ExpiryInMilliseconds.HasValue){
            return false;
        }

        return (DateTime.UtcNow - _createdAt).TotalMilliseconds > ExpiryInMilliseconds.Value;
    }

    public override string ToString()
    {
        return $"{Value} | __expiry: {Expiry}";
    }
}