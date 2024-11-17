public class RedisDatabase
{
    public int Index { get; set; }
    public IDictionary<string, RedisValue> Store { get; set; } = new Dictionary<string, RedisValue>();

    public override string ToString()
    {
        var result = $"__db_index: {Index}\n";
        result += $"__keys_count: {Store.Count}\n";
        foreach (var kv in Store)
        {
            result += $"__key: {kv.Key} | __value: {kv.Value}\n";
        }
        return result;
    }
}