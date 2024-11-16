public class RedisState
{
    public string Name { get; set; } = "";
    public string Version { get; set; } = "";
    public Dictionary<string, string> AuxFields { get; set; } = new Dictionary<string, string>();
    public Dictionary<string, RedisDatabase> Databases { get; set; } = new Dictionary<string, RedisDatabase>();

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

    private string GetDatabases()
    {
        var str = $"# Databases - {Databases.Count}\n";

        foreach (var db in Databases)
        {
            str += db.Value;
        }

        str += "\n";

        return str;
    }

    public override string ToString()
    {
        return
$@"Name: {Name}, Version: {Version}
{GetAuxFields()}
{GetDatabases()}";
    }
}