public class RdbHandler
{
    private readonly RdbConfiguration _rdbConfiguration;
    public string Directiory => _rdbConfiguration.Directiory;
    public string DbFileName => _rdbConfiguration.DbFileName;
    public RedisState RedisState { get; private set; }

    public RdbHandler(RdbConfiguration config)
    {
        _rdbConfiguration = config;
        RedisState = new();
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
                var database = ReadDatabase(reader);

                RedisState.Databases.TryAdd(database.Index, database);
            }
        }
    }

    private RedisDatabase ReadDatabase(BinaryReader reader)
    {
        var database = new RedisDatabase();

        var dbIndex = reader.ReadEncodedLength();

        database.Index = dbIndex;

        var resizedb = reader.ReadByte();

        var hashSize = reader.ReadEncodedLength(); // hashSize includes the expiry keys as well
        var expiryHashSize = reader.ReadEncodedLength();

        Console.WriteLine("hashSize " + hashSize + " expiry hashSize " + expiryHashSize);

        for (var it = 0; it < hashSize; it++)
        {
            var (key, value) = ReadKeyValue(reader);
            database.Store.TryAdd(key, value);
        }

        return database;
    }

    private (string, RedisValue) ReadKeyValue(BinaryReader reader)
    {
        var code = reader.ReadByte();

        double? expiryInMs = null;
        byte valueType;
        string key;
        string value;

        if (code == RdbOpCodes.EXPIRETIME)
        {
            var expiryInSeconds = reader.ReadUInt32() * 1000;
            expiryInMs = double.Parse(expiryInSeconds.ToString());

            valueType = reader.ReadByte();
        }
        else if (code == RdbOpCodes.EXPIRETIMEMS)
        {
            var expiryInMilliseconds = reader.ReadUInt64();
            expiryInMs = double.Parse(expiryInMilliseconds.ToString());

            valueType = reader.ReadByte();

            Console.WriteLine($"Expiry in MS: {expiryInMs}");

        }
        else
        {
            valueType = code;

        }

        //if (valueType == RedisEncoding.STRING)
        //{
        key = reader.ReadLengthEncodedString();
        value = reader.ReadLengthEncodedString();
        //}
        var redisValue = new RedisValue(value, expiryInMs);

        return (key, redisValue);
    }
}