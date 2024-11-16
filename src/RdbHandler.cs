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

            Console.WriteLine($"Current op: {opCode:X2}");
            if (opCode == RdbOpCodes.AUX)
            {
                try
                {
                    var key = reader.ReadLengthEncodedString();
                    var value = reader.ReadLengthEncodedString();
                    Console.WriteLine(value, key, value);
                    RedisState.AuxFields.TryAdd(key, value);
                }
                catch
                {

                }
            }

            if (opCode == RdbOpCodes.SELECTDB)
            {

            }
        }
    }
}