using System.Diagnostics;
using System.Net.Sockets;
using System.Text;
using System.Text.RegularExpressions;

public class RedisEngine
{
    private readonly int _defaultDbIndex = 0;
    private readonly RdbHandler _rdbHandler;
    private readonly RedisInstance _redisInstance;

    private bool _rdbReceivedFromMaster = false;
    private int _bytesSentByMasterSinceLastQuery = 0;

    public RedisEngine(RdbHandler rdbHandler, RedisInstance redisInstance)
    {
        _rdbHandler = rdbHandler;
        _redisInstance = redisInstance;
    }

    public async Task ProcessRequestAsync(Socket socket, byte[] readBuffer, ClientConnectionStats stats)
    {
        var request = Encoding.UTF8.GetString(readBuffer).TrimEnd('\0');

        Logger.Log($"Received (from client): [{request}] Length: {request.Length}");

        // return the protocol of the command executed
        // it will be used to determine whether the command should be propagated
        string protocol = await ExecuteCommandAsync(socket, new RedisRequest { CommandString = request }, stats);

        // propagate the commands
        await PropagateToReplicaAsync(request, protocol, stats);
    }

    private async Task<string> ExecuteCommandAsync(Socket socket, RedisRequest request, ClientConnectionStats stats, bool fromMaster = false)
    {
        var commands = Regex.Split(request.CommandString, @"\s+");

        var index = 0;

        foreach (var command in commands)
        {
            Logger.Log($"{index++} {command}");
        }

        var protocol = GetRedisProtocol(commands);

        Logger.Log($"Protocol: {protocol}");

        switch (protocol)
        {
            case RedisProtocol.PING:
                await ProcessPingAsync(socket, commands, fromMaster);
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
                await ProcessReplConfAsync(socket, commands, fromMaster);
                break;

            case RedisProtocol.PSYNC:
                await ProcessPsyncAsync(socket, commands);
                break;

            case RedisProtocol.WAIT:
                await ProcessWaitAsync(socket, commands, stats);
                break;

            case RedisProtocol.TYPE:
                await ProcessTypeAsync(socket, commands);
                break;

            case RedisProtocol.INCR:
                await ProcessIncrementAsync(socket, commands);
                break;

            case RedisProtocol.NONE:
                break;
        }

        if (fromMaster)
        {
            // add to cumulative sum of bytes received from master
            Logger.Log($"Byte sum before {_bytesSentByMasterSinceLastQuery} + request length {request.ByteLength}");

            _bytesSentByMasterSinceLastQuery += request.ByteLength;

            Logger.Log($"Byte sum after {_bytesSentByMasterSinceLastQuery}");
        }

        return protocol;
    }



    private async Task ProcessPingAsync(Socket socket, string[] commands, bool fromMaster)
    {
        if (fromMaster)
        {
            return;
        }

        await SendBulkStringSocketResponseAsync(socket, "PONG");
    }

    private async Task ProcessEchoAsync(Socket socket, string[] commands)
    {
        await SendBulkStringSocketResponseAsync(socket, commands[4]);
    }

    private async Task ProcessSetAsync(Socket socket, string[] commands, bool fromMaster)
    {
        var key = commands[4];
        var value = commands[6];

        Logger.Log($"[{Thread.CurrentThread.ManagedThreadId}] Processing set -> __key: {key} __value: {value}");

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

    private async Task ProcessGetAsync(Socket socket, string[] commands)
    {
        var key = commands[4];

        var db = GetDatabase();

        Logger.Log($"Key to get: {key}");

        Logger.Log(db);

        if (db.Store.TryGetValue(key, out var value))
        {
            if (value.IsExpired())
            {
                db.Store.TryRemove(key, out var _);

                await SendNullSocketResponseAsync(socket);
            }

            else
            {
                await SendBulkStringSocketResponseAsync(socket, value.Value);
            }
        }
        else
        {
            await SendNullSocketResponseAsync(socket);
        }
    }

    private async Task ProcessConfigAsync(Socket socket, string[] commands)
    {
        var subcommand = commands[4];

        if (subcommand.Equals("get", StringComparison.OrdinalIgnoreCase))
        {
            await ProcessConfigGetAsync(socket, commands);
        }
    }

    private async Task ProcessKeysAsync(Socket socket, string[] commands)
    {
        var argument = commands[4];

        if (argument.Equals("*", StringComparison.OrdinalIgnoreCase))
        {
            var db = GetDatabase();

            var keys = db.Store.Keys;

            await SendArraySocketResponseAsync(socket, keys.ToArray());
        }
    }

    private async Task ProcessInfoAsync(Socket socket, string[] commands)
    {
        var argument = commands[4];

        Logger.Log($"Argument {argument}");

        if (argument.Equals("replication", StringComparison.OrdinalIgnoreCase))
        {
            await SendBulkStringSocketResponseAsync(socket, _redisInstance.ToString());
        }
    }

    private async Task ProcessReplConfAsync(Socket socket, string[] commands, bool fromMaster)
    {
        if (fromMaster)
        {
            var subCommand = commands[4];

            if (subCommand.Equals("GETACK", StringComparison.OrdinalIgnoreCase))
            {
                var argument = commands[6];

                if (argument.Equals("*", StringComparison.OrdinalIgnoreCase))
                {
                    Logger.Log($"Bytes received so far {_bytesSentByMasterSinceLastQuery}");
                    await SendArraySocketResponseAsync(socket, ["REPLCONF", "ACK", $"{_bytesSentByMasterSinceLastQuery}"]);
                }
            }
        }
        else
        {
            await SendOkSocketResponseAsync(socket);
        }

    }

    private async Task ProcessPsyncAsync(Socket socket, string[] commands)
    {
        var response = $"FULLRESYNC 8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb 0"; // to be changed later

        await SendBulkStringSocketResponseAsync(socket, response);

        var currentRdb = _rdbHandler.GetCurrentStateAsRdb();

        await SendRdbSocketResponseAsync(socket, currentRdb);

        // add the socket in the replica list
        _redisInstance.ConnectedReplicas.Add(socket);
    }

    private async Task ProcessWaitAsync(Socket socket, string[] commands, ClientConnectionStats stats)
    {
        if (commands.Length < 6)
        {
            return;
        }

        var numOfReplicas = int.Parse(commands[4]);

        var timeout = long.Parse(commands[6]);

        var stopwatch = new Stopwatch();

        stopwatch.Start();

        // wait until timeout ms or specified replicas have acknowledged command
        while (stopwatch.Elapsed.TotalMilliseconds < timeout && stats.NumberOfReplicasAcknowledged < numOfReplicas)
        {
            //Logger.Log($"waiting... {stats.NumberOfReplicasAcknowledged}");
        }

        Logger.Log($"Send WAIT response: {stats.NumberOfReplicasAcknowledged}");

        await SendIntegerSocketResponseAsync(socket, Math.Min(stats.NumberOfReplicasAcknowledged, numOfReplicas));
    }

    private async Task ProcessTypeAsync(Socket socket, string[] commands)
    {
        var key = commands[4];

        var db = GetDatabase();

        if (db.Store.ContainsKey(key))
        {
            await SendSimpleStringSocketResponseAsync(socket, "string");
        }
        else
        {
            await SendSimpleStringSocketResponseAsync(socket, "none");
        }
    }

    private async Task ProcessIncrementAsync(Socket socket, string[] commands)
    {
        var key = commands[4];

        var db = GetDatabase();

        if (db.Store.TryGetValue(key, out var redisValue))
        {
            Logger.Log($"Retrieved value: {redisValue.Value}");

            if (int.TryParse(redisValue.Value, out var integerValue))
            {
                
                integerValue++;

                redisValue.Value = integerValue.ToString();

                db.Store.TryAdd(key, redisValue);

                await SendIntegerSocketResponseAsync(socket, integerValue);
            }
            else
            {
                await SendErrorStringSocketResponseAsync(socket, "value is not an integer or out of range");
            }
        }
        else
        {
            var newRedisValue = new RedisValue("1", null);

            db.Store.TryAdd(key, newRedisValue);

            await SendIntegerSocketResponseAsync(socket, 1);
        }
    }

    public async Task ConnectToMasterAsync()
    {
        if (_redisInstance.MasterEndpoint == null)
        {
            Logger.Log("No master specified.");
            return;
        }

        using var socket = new Socket(_redisInstance.MasterEndpoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);

        await socket.ConnectAsync(_redisInstance.MasterEndpoint);

        await SendCommandsAsync(socket, ["PING"]);

        await SendCommandsAsync(socket, ["REPLCONF", "listening-port", _redisInstance.Port.ToString()]);

        await SendCommandsAsync(socket, ["REPLCONF", "capa", "psync2"]);

        await SendCommandsAsync(socket, ["PSYNC", "?", "-1"], receiveImmediateResponse: false);

        await Task.Run(async () => await ReceiveCommandsFromMasterAsync(socket));
    }

    private async Task SendCommandsAsync(Socket socket, string[] commands, bool receiveImmediateResponse = true)
    {
        await SendArraySocketResponseAsync(socket, commands);

        // for PSYNC command, we will receive response in a different thread that will run continuously
        if (receiveImmediateResponse)
        {
            var buffer = new byte[1024];

            await socket.ReceiveAsync(buffer);

            Logger.Log($"Received (response): {Encoding.UTF8.GetString(buffer)}");
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

            Logger.Log($"Received (from master): {Encoding.UTF8.GetString(readBuffer)}");

            var commandProcessingStartIndex = 0;

            if (!_rdbReceivedFromMaster)
            {
                commandProcessingStartIndex = GetLengthOfFullResyncAndRdbFile(readBuffer);

                if (commandProcessingStartIndex > 0)
                {
                    _rdbReceivedFromMaster = true;
                }
            }

            var commands = GetCommandsFromBufferAsync(readBuffer, commandProcessingStartIndex);

            foreach (var command in commands)
            {
                await ExecuteCommandAsync(socket, command, new ClientConnectionStats(), fromMaster: true);
            }
        }
    }

    private int GetLengthOfFullResyncAndRdbFile(byte[] readBuffer)
    {

        for (var index = 0; index < readBuffer.Length - 1; index++)
        {
            if (readBuffer[index] == 0x2A) // the start of command '*' (not a robust solution)
            {
                return index;
            }
        }

        return 0;
    }

    // handle buffer that contains multiple commands
    private List<RedisRequest> GetCommandsFromBufferAsync(byte[] readBuffer, int startIndex)
    {
        var commands = new List<RedisRequest>();

        var commandBytes = new List<byte>();

        for (var index = startIndex; index < readBuffer.Length; index++)
        {
            if (readBuffer[index] == 0x00) // handle null character '/0' 
            {
                break;
            }

            if (readBuffer[index] == 0x2A) // check for "*" (start of a new RESP array)
            {
                // validate that it is a beginning of a RESP array, not a subcommand
                if (index + 1 < readBuffer.Length && readBuffer[index + 1] != 0x0D)
                {
                    if (commandBytes.Count > 0)
                    {
                        var command = Encoding.UTF8.GetString([.. commandBytes]);
                        commands.Add(new RedisRequest { CommandString = command, ByteLength = commandBytes.Count });
                        commandBytes.Clear();
                    }
                }
            }

            commandBytes.Add(readBuffer[index]);
        }

        if (commandBytes.Count > 0)
        {
            // foreach (var b in commandBytes)
            // {
            //     Console.Write($"{b:x2} ");
            // }
            // Console.WriteLine("");

            var command = Encoding.UTF8.GetString([.. commandBytes]);
            commands.Add(new RedisRequest { CommandString = command, ByteLength = commandBytes.Count });
        }

        Logger.Log($"Extracted the following commands: {commands.Count}");

        foreach (var c in commands)
        {
            Logger.Log($"Command > {c.CommandString}");
        }

        return commands;
    }

    private async Task ProcessConfigGetAsync(Socket socket, string[] commands)
    {
        var argument = commands[6];

        if (argument.Equals("dir", StringComparison.OrdinalIgnoreCase))
        {
            await SendArraySocketResponseAsync(socket, [argument, _rdbHandler.Directiory]);
        }
        else if (argument.Equals("dbfilename", StringComparison.OrdinalIgnoreCase))
        {
            await SendArraySocketResponseAsync(socket, [argument, _rdbHandler.DbFileName]);
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

    private async Task PropagateToReplicaAsync(string request, string protocol, ClientConnectionStats stats)
    {
        if (_redisInstance.Role == ServerRole.Master && protocol is RedisProtocol.SET)
        {
            Logger.Log($"Propagating: [{request}]");

            var propCommand = Encoding.UTF8.GetBytes(request);

            await SendCommandToReplicasAsync(propCommand, stats);
        }
    }

    private async Task SendCommandToReplicasAsync(byte[] propCommand, ClientConnectionStats stats)
    {
        Logger.Log($"Sending to {_redisInstance.ConnectedReplicas.Count} replica(s).");

        stats.NumberOfReplicasAcknowledged = 0;

        foreach (var replica in _redisInstance.ConnectedReplicas)
        {
            Logger.Log($"Sending command to replica {replica.RemoteEndPoint}");

            await replica.SendAsync(propCommand, SocketFlags.None);

            Logger.Log($"Sent command to replica {replica.RemoteEndPoint}");

            Logger.Log($"Sending GETACK * to replica {replica.RemoteEndPoint}");

            await SendCommandsAsync(replica, ["REPLCONF", "GETACK", "*"], receiveImmediateResponse: false);

            Logger.Log($"Sent GETACT * to replica {replica.RemoteEndPoint}");

            stats.NumberOfReplicasAcknowledged++;
        }
    }

    string GetRedisProtocol(string[] commands) => commands[2].ToLower();
    private string GetRespBulkString(string payload) => $"${payload.Length}\r\n{payload}\r\n";
    private string GetRespSimpleString(string payload) => $"+{payload}\r\n";
    private string GetRespErrorString(string payload) => $"-ERR {payload}\r\n";
    private string GetNullBulkString() => "$-1\r\n";
    private string GetOkResponseString() => GetRespSimpleString("OK");
    private string GetRespInteger(long number) => $":{number}\r\n";
    private string GetRespBulkArray(string[] payload)
    {
        var response = $"*{payload.Length}\r\n";

        foreach (var item in payload)
        {
            response += GetRespBulkString(item);
        }

        return response;
    }

    private async Task SendBulkStringSocketResponseAsync(Socket socket, string message)
    {
        var bulkString = GetRespBulkString(message);
        await SendSocketResponseAsync(socket, bulkString);
    }

    private async Task SendArraySocketResponseAsync(Socket socket, string[] message)
    {
        var bulkArray = GetRespBulkArray(message);
        await SendSocketResponseAsync(socket, bulkArray);
    }

    private async Task SendIntegerSocketResponseAsync(Socket socket, long number)
    {
        var integer = GetRespInteger(number);
        await SendSocketResponseAsync(socket, integer);
    }

    private async Task SendSimpleStringSocketResponseAsync(Socket socket, string message)
    {
        var simpleString = GetRespSimpleString(message);
        await SendSocketResponseAsync(socket, simpleString);
    }

    private async Task SendErrorStringSocketResponseAsync(Socket socket, string message)
    {
        var errorString = GetRespErrorString(message);
        await SendSocketResponseAsync(socket, errorString);
    }

    private async Task SendNullSocketResponseAsync(Socket socket)
    {
        await SendSocketResponseAsync(socket, GetNullBulkString());
    }

    private async Task SendOkSocketResponseAsync(Socket socket)
    {
        await SendSocketResponseAsync(socket, GetOkResponseString());
    }

    private async Task SendRdbSocketResponseAsync(Socket socket, byte[] data)
    {
        var response = Encoding.UTF8.GetBytes($"${data.Length}\r\n");

        await socket.SendAsync(response, SocketFlags.None);

        await socket.SendAsync(data, SocketFlags.None);
    }

    private static async Task SendSocketResponseAsync(Socket socket, string payload)
    {
        var response = Encoding.UTF8.GetBytes(payload);
        await socket.SendAsync(response, SocketFlags.None);
    }
}