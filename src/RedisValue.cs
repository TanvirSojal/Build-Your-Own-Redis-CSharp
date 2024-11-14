public class RedisValue
{
    private readonly DateTime _createdAt;


    public string Value { get; set; }
    public long? ExpiryInMilliseconds { get; set; }


    public RedisValue(string value, long? expiryInMilliseconds)
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
}