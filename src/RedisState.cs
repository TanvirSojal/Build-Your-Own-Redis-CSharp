public class RedisState
{
    public string Name { get; set; } = "";
    public string Version { get; set; } = "";
    public Dictionary<string, string> AuxFields { get; set; } = new Dictionary<string, string>();
    public Dictionary<string, RedisValue> Store { get; set; } = new Dictionary<string, RedisValue>();

    private string GetAuxFields()
    {
        var fields = $"Auxilary Fields - {AuxFields.Count}\n";

        foreach (var field in AuxFields)
        {
            fields += $"__key: {field.Key} | __value: {field.Value}\n";
        }

        fields += "\n";

        return fields;
    }

    private string GetStoreFields()
    {
        var fields = $"Redis Fields - {Store.Count}\n";

        foreach (var field in Store)
        {
            fields += $"__key: {field.Key} | __value: {field.Value}\n";
        }

        fields += "\n";

        return fields;
    }

    public override string ToString()
    {
        return
$@"Name: {Name}, Version: {Version}
{GetAuxFields()}
{GetStoreFields()}";
    }
}