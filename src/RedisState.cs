public class RedisState
{
    public string Name { get; set; } = "";
    public string Version { get; set; } = "";
    public Dictionary<string, string> AuxFields { get; set; } = new Dictionary<string, string>();

    private string GetAuxFields()
    {
        var fields = $"Auxilary Fields - {AuxFields.Count}\n";

        foreach (var field in AuxFields)
        {
            fields += $"{field.Key} - {field.Value}\n";
        }

        fields += "\n";

        return fields;
    }

    public override string ToString()
    {
        return
$@"Name: {Name}, Version: {Version}

{GetAuxFields()}
        ";
    }
}