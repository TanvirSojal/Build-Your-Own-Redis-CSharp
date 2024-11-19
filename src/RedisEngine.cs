using System.Net;
using System.Net.Sockets;
using System.Text;

public class RedisEngine
{
    private readonly int _defaultDbIndex = 0;
    private readonly RdbHandler _rdbHandler;
    private readonly RedisInfo _redisInfo;

    public RedisEngine(RdbHandler rdbHandler, RedisInfo redisInfo)
    {
        _rdbHandler = rdbHandler;
        _redisInfo = redisInfo;
    }

    public async Task ProcessPingAsync(Socket socket, string[] commands)
    {
        await SendSocketResponseAsync(socket, "PONG");
    }

    public async Task ProcessEchoAsync(Socket socket, string[] commands)
    {
        await SendSocketResponseAsync(socket, commands[4]);
    }

    public async Task ProcessSetAsync(Socket socket, string[] commands)
    {
        var key = commands[4];
        var value = commands[6];

        var expiry = (double?)null;

        if (commands.Length >= 10)
        {
            var argument = commands[8];

            if (argument.Equals("px", StringComparison.OrdinalIgnoreCase))
            {
                expiry = ulong.Parse(commands[10]) + (ulong)DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            }
        }

        var valueToStore = new RedisValue(value, expiry);

        var db = GetDatabase();

        if (db.Store.TryAdd(key, valueToStore))
        {
            await SendOkSocketResponseAsync(socket);
        }
        else
        {
            await SendNullSocketResponseAsync(socket);
        }
    }

    public async Task ProcessGetAsync(Socket socket, string[] commands)
    {
        var key = commands[4];

        var db = GetDatabase();

        Console.WriteLine($"Key to get: {key}");

        Console.WriteLine(db);

        if (db.Store.TryGetValue(key, out var value))
        {
            if (value.IsExpired())
            {
                db.Store.Remove(key);

                await SendNullSocketResponseAsync(socket);
            }

            else
            {
                await SendSocketResponseAsync(socket, value.Value);
            }
        }
        else
        {
            await SendNullSocketResponseAsync(socket);
        }
    }

    public async Task ProcessConfigAsync(Socket socket, string[] commands)
    {
        var subcommand = commands[4];

        if (subcommand.Equals("get", StringComparison.OrdinalIgnoreCase))
        {
            await ProcessConfigGetAsync(socket, commands);
        }
    }

    public async Task ProcessKeysAsync(Socket socket, string[] commands)
    {
        var argument = commands[4];

        if (argument.Equals("*", StringComparison.OrdinalIgnoreCase))
        {
            var db = GetDatabase();

            var keys = db.Store.Keys;

            await SendSocketResponseArrayAsync(socket, keys.ToArray());
        }
    }

    public async Task ProcessInfoAsync(Socket socket, string[] commands)
    {
        var argument = commands[4];

        Console.WriteLine($"Argument {argument}");

        if (argument.Equals("replication", StringComparison.OrdinalIgnoreCase))
        {
            await SendSocketResponseAsync(socket, _redisInfo.ToString());
        }
    }

    public async Task ProcessReplConfAsync(Socket socket, string[] commands)
    {
        await SendOkSocketResponseAsync(socket);
    }

    public async Task ProcessPsyncAsync(Socket socket, string[] commands)
    {
        var response = $"FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0"; // to be changed later

        await SendSocketResponseAsync(socket, response);

        var currentRdb = _rdbHandler.GetCurrentStateAsRdb();

        await SendRdbSocketResponseAsync(socket, currentRdb);
    }

    public async Task ConnectToMasterAsync()
    {
        if (_redisInfo.MasterEndpoint == null)
        {
            Console.WriteLine("No master specified.");
            return;
        }

        using var socket = new Socket(_redisInfo.MasterEndpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);

        await socket.ConnectAsync(_redisInfo.MasterEndpoint);

        await SendCommandsToMasterAsync(socket, ["PING"]);

        await SendCommandsToMasterAsync(socket, ["REPLCONF", "listening-port", _redisInfo.Port.ToString()]);

        await SendCommandsToMasterAsync(socket, ["REPLCONF", "capa", "psync2"]);

        await SendCommandsToMasterAsync(socket, ["PSYNC", "?", "-1"]);
    }

    private async Task SendCommandsToMasterAsync(Socket socket, string[] commands)
    {
        await SendSocketResponseArrayAsync(socket, commands);

        var buffer = new byte[1024];

        await socket.ReceiveAsync(buffer);

        Console.WriteLine($"Received: {Encoding.UTF8.GetString(buffer)}");

        //socket.Close();
    }

    private async Task ProcessConfigGetAsync(Socket socket, string[] commands)
    {
        var argument = commands[6];

        if (argument.Equals("dir", StringComparison.OrdinalIgnoreCase))
        {
            await SendSocketResponseArrayAsync(socket, [argument, _rdbHandler.Directiory]);
        }
        else if (argument.Equals("dbfilename", StringComparison.OrdinalIgnoreCase))
        {
            await SendSocketResponseArrayAsync(socket, [argument, _rdbHandler.DbFileName]);
        }
    }

    private RedisDatabase GetDatabase()
    {
        if (!_rdbHandler.RedisState.Databases.TryGetValue(_defaultDbIndex, out var db))
        {
            db = new RedisDatabase();
            _rdbHandler.RedisState.Databases.TryAdd(_defaultDbIndex, db);
        }

        return db;
    }

    private string GetRedisBulkString(string payload) => $"${payload.Length}\r\n{payload}\r\n";
    private string GetNullBulkString() => "$-1\r\n";
    private string GetOkResponseString() => "+OK\r\n";
    private string GetRedisBulkArray(string[] payload)
    {
        var response = $"*{payload.Length}\r\n";

        foreach (var item in payload)
        {
            response += GetRedisBulkString(item);
        }

        return response;
    }

    private async Task SendSocketResponseAsync(Socket socket, string message)
    {
        var bulkString = GetRedisBulkString(message);
        var response = Encoding.UTF8.GetBytes(bulkString);
        await socket.SendAsync(response, SocketFlags.None);
    }

    private async Task SendSocketResponseArrayAsync(Socket socket, string[] message)
    {
        var bulkArray = GetRedisBulkArray(message);
        var response = Encoding.UTF8.GetBytes(bulkArray);
        await socket.SendAsync(response, SocketFlags.None);
    }

    private async Task SendNullSocketResponseAsync(Socket socket)
    {
        var response = Encoding.UTF8.GetBytes(GetNullBulkString());
        await socket.SendAsync(response, SocketFlags.None);
    }

    private async Task SendOkSocketResponseAsync(Socket socket)
    {
        var response = Encoding.UTF8.GetBytes(GetOkResponseString());
        await socket.SendAsync(response, SocketFlags.None);
    }

    private async Task SendRdbSocketResponseAsync(Socket socket, byte[] data)
    {
        var response = Encoding.UTF8.GetBytes($"${data.Length}\r\n");

        await socket.SendAsync(response, SocketFlags.None);

        await socket.SendAsync(data, SocketFlags.None);
    }
}