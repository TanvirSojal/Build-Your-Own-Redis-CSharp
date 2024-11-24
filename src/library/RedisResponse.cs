public class RedisResponse
{
    public ResponseType ResponseType { get; set; }
    public string? StringValue { get; set; } = null;
    public long? IntegerValue { get; set; } = null;
}