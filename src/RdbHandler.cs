public class RdbHandler
{
    private readonly RdbConfiguration _rdbConfiguration;
    public string Directiory => _rdbConfiguration.Directiory;
    public string DbFileName => _rdbConfiguration.DbFileName;


    public RedisState RedisState { get; private set; } = new();


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

        while (reader.BaseStream.Position < reader.BaseStream.Length)
        {
            var opCode = reader.ReadByte();

            if (opCode == RdbOpCodes.EOF)
            {
                break;
            }

            if (opCode == RdbOpCodes.AUX)
            {
                var key = reader.ReadLengthEncodedString();
                var value = reader.ReadLengthEncodedString();
                RedisState.AuxFields.TryAdd(key, value);
                continue;
            }

            if (opCode == RdbOpCodes.SELECTDB)
            {
                var database = new RedisDatabase();

                var dbIndex = reader.ReadSizeEncodedInteger();

                if (int.TryParse(dbIndex, out var index))
                {
                    database.Index = index;

                    RedisState.Databases.TryAdd(dbIndex, database);
                }
            }
        }
    }
}