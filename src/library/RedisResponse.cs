public class RedisResponse
{
    public DataType DataType { get; set; }
    public string? StringValue { get; set; } = null;
    public long? IntegerValue { get; set; } = null;
}