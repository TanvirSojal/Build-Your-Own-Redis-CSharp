using System.Text;

public class RdbHandler
{
    public RedisState RedisState { get; private set; } = new();
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

        if (!File.Exists(path))
        {
            return;
        }

        using var fileStream = new FileStream(path, FileMode.Open);

        using var reader = new BinaryReader(fileStream);

        RedisState.Name = reader.ReadString(5);
        RedisState.Version = reader.ReadString(4);
    }
}