public class RdbHandler
{
    private readonly RdbConfiguration _rdbConfiguration;
    public string Directiory => _rdbConfiguration.Directiory;
    public string DbFileName => _rdbConfiguration.DbFileName;

    public RdbHandler(RdbConfiguration config)
    {
        _rdbConfiguration = config;
    }

    public void RestoreSnapshot()
    {
        var path = Path.Join(_rdbConfiguration.Directiory, _rdbConfiguration.DbFileName);

        using var fileStream = new FileStream(path, FileMode.Open);

        using var reader = new BinaryReader(fileStream);

        var snapshot = reader.ReadChars(5);
        Console.WriteLine(snapshot);
    }
}