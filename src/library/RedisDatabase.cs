using System.Collections.Concurrent;

public class RedisDatabase
{
    public int Index { get; set; } = 0;
    public ConcurrentDictionary<string, RedisValue> Store { get; set; } = new ConcurrentDictionary<string, RedisValue>();

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