using System.Net.Sockets;
using System.Text;
using System.Text.RegularExpressions;

public class RedisEngine
{
    private readonly int _defaultDbIndex = 0;
    private readonly RdbHandler _rdbHandler;
    private readonly RedisInfo _redisInfo;
    private readonly List<Socket> replicas = new List<Socket>();

    public RedisEngine(RdbHandler rdbHandler, RedisInfo redisInfo)
    {
        _rdbHandler = rdbHandler;
        _redisInfo = redisInfo;
    }

    public async Task ProcessRequestAsync(Socket socket, byte[] readBuffer)
    {
        var request = Encoding.UTF8.GetString(readBuffer).TrimEnd('\0');

        Console.WriteLine($"received: [{request}] Length: {request.Length}");

        // return the protocol of the command executed
        // it will be used to determine whether the command should be propagated
        string protocol = await ExecuteCommandAsync(socket, request);

        // propagate the commands
        if (_redisInfo.Role == ServerRole.Master && protocol is RedisProtocol.SET)
        {
            var propCommand = Encoding.UTF8.GetBytes(request);
            foreach (var replica in replicas)
            {
                await replica.SendAsync(propCommand, SocketFlags.None);
            }
        }
    }

    public async Task ProcessPingAsync(Socket socket, string[] commands)
    {
        await SendSocketResponseAsync(socket, "PONG");
    }

    public async Task ProcessEchoAsync(Socket socket, string[] commands)
    {
        await SendSocketResponseAsync(socket, commands[4]);
    }

    public async Task ProcessSetAsync(Socket socket, string[] commands, bool fromMaster)
    {
        var key = commands[4];
        var value = commands[6];

        Console.WriteLine($"[{Thread.CurrentThread.Name}] Processing set -> __key: {key} __value: {value}");

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

        var isSetSuccessful = db.Store.TryAdd(key, valueToStore);

        if (fromMaster)
        {
            return;
        }

        if (isSetSuccessful)
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

        Console.WriteLine($"[{Thread.CurrentThread.Name}] Key to get: {key}");

        Console.WriteLine($"[{Thread.CurrentThread.Name}]\n{db}");

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
        if (commands.Length >= 6)
        {
            var subCommand = commands[4];

            if (subCommand.Equals("GETACK", StringComparison.OrdinalIgnoreCase))
            {
                var argument = commands[6];

                if (argument.Equals("*", StringComparison.OrdinalIgnoreCase))
                {
                    await SendSocketResponseArrayAsync(socket, ["REPLCONF", "ACK", "0"]);
                }
            }
        }
        else
        {
            await SendOkSocketResponseAsync(socket);
        }

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

        await SendCommandsToMasterAsync(socket, ["PSYNC", "?", "-1"], receiveImmediateResponse: false);

        await Task.Run(async () => await ReceiveCommandsFromMasterAsync(socket));
    }

    private async Task SendCommandsToMasterAsync(Socket socket, string[] commands, bool receiveImmediateResponse = true)
    {
        await SendSocketResponseArrayAsync(socket, commands);

        // for PSYNC command, we will receive response in a different thread that will run continuously
        if (receiveImmediateResponse)
        {
            var buffer = new byte[1024];

            await socket.ReceiveAsync(buffer);

            Console.WriteLine($"Received: {Encoding.UTF8.GetString(buffer)}");
        }
    }

    private async Task ReceiveCommandsFromMasterAsync(Socket socket)
    {
        while (true)
        {
            var readBuffer = new byte[1024];

            var bytesRead = await socket.ReceiveAsync(readBuffer);

            if (bytesRead == 0)
            {
                break;
            }

            Console.WriteLine($"Received from Master: {Encoding.UTF8.GetString(readBuffer)}");

            var commands = GetCommandsFromBufferAsync(readBuffer);

            foreach (var command in commands)
            {
                await ExecuteCommandAsync(socket, command, fromMaster: true);
            }

        }
    }

    // handle buffer that contains multiple commands
    private List<string> GetCommandsFromBufferAsync(byte[] readBuffer)
    {
        var commands = new List<string>();

        var request = Encoding.UTF8.GetString(readBuffer).TrimEnd('\0');

        Console.WriteLine(request);

        var commandStarted = false;
        var command = "";

        for (var index = 0; index < request.Length; index++)
        {
            var ch = request[index];

            if (ch == '*')
            {
                // check that it is a beginning of a RESP array, not a subcommand
                if (index + 1 < request.Length && request[index + 1] != '\r')
                {
                    if (command.Length > 0)
                    {
                        commands.Add(command);
                        command = "";
                    }
                }

                command += ch;
                commandStarted = true;
            }
            else if (commandStarted)
            {
                command += ch;
            }
        }

        if (command.Length > 0)
        {
            commands.Add(command);
        }


        Console.WriteLine("Extracted the following commands:");

        foreach (var c in commands)
        {
            Console.WriteLine($"Command > {c}");
        }

        return commands;
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

    private async Task<string> ExecuteCommandAsync(Socket socket, string request, bool fromMaster = false)
    {
        var commands = Regex.Split(request, @"\s+");

        var index = 0;

        foreach (var command in commands)
        {
            Console.WriteLine($"{index++} {command}");
        }

        var protocol = GetRedisProtocol(commands);

        Console.WriteLine($"Protocol: {protocol}");

        switch (protocol)
        {
            case RedisProtocol.PING:
                await ProcessPingAsync(socket, commands);
                break;

            case RedisProtocol.ECHO:
                await ProcessEchoAsync(socket, commands);
                break;

            case RedisProtocol.SET:
                await ProcessSetAsync(socket, commands, fromMaster);
                break;

            case RedisProtocol.GET:
                await ProcessGetAsync(socket, commands);
                break;

            case RedisProtocol.CONFIG:
                await ProcessConfigAsync(socket, commands);
                break;

            case RedisProtocol.KEYS:
                await ProcessKeysAsync(socket, commands);
                break;

            case RedisProtocol.INFO:
                await ProcessInfoAsync(socket, commands);
                break;

            case RedisProtocol.REPLCONF:
                await ProcessReplConfAsync(socket, commands);
                break;

            case RedisProtocol.PSYNC:
                await ProcessPsyncAsync(socket, commands);
                replicas.Add(socket);
                break;

            case RedisProtocol.NONE:
                break;
        }

        return protocol;
    }

    string GetRedisProtocol(string[] commands) => commands[2].ToLower();
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