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
    private List<RedisRequest> _redisRequestQueue = new List<RedisRequest>();
    private List<RedisResponse> _redisResponseQueue = new List<RedisResponse>();
    private bool _shouldQueueResponses = false;

    public RedisEngine(RdbHandler rdbHandler, RedisInstance redisInstance)
    {
        _rdbHandler = rdbHandler;
        _redisInstance = redisInstance;
    }

    public async Task ProcessRequestAsync(Socket socket, byte[] readBuffer, ClientConnectionState state)
    {
        var request = Encoding.UTF8.GetString(readBuffer).TrimEnd('\0');

        Logger.Log($"Received (from client): [{request}] Length: {request.Length}");

        // return the protocol of the command executed
        // it will be used to determine whether the command should be propagated
        string protocol = await ExecuteCommandAsync(socket, new RedisRequest { CommandString = request }, state);

        // propagate the commands
        await PropagateToReplicaAsync(request, protocol, state);
    }

    private async Task<string> ExecuteCommandAsync(Socket socket, RedisRequest request, ClientConnectionState state, bool fromMaster = false)
    {
        var commands = Regex.Split(request.CommandString, @"\s+");

        var index = 0;

        foreach (var command in commands)
        {
            Logger.Log($"{index++} {command}");
        }

        var protocol = GetRedisProtocol(commands);

        Logger.Log($"Protocol: {protocol}");

        Logger.Log($"Should queue request? {state.ShouldQueueRequests}");

        // handle Transactions (MULTI, EXEC, DISCARD)
        if (state.ShouldQueueRequests)
        {
            if (protocol is RedisProtocol.DISCARD)
            {
                await ProcessDiscardAsync(socket, commands, state);
                return protocol;
            }
            else if (protocol is RedisProtocol.EXEC)
            {
                await ProcessExecAsync(socket, commands, state);
                return protocol;
            }
            else
            {
                _redisRequestQueue.Add(request);
                await SendSimpleStringSocketResponseAsync(socket, "QUEUED");
                return RedisProtocol.NONE;
            }
        }

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
                await ProcessWaitAsync(socket, commands, state);
                break;

            case RedisProtocol.TYPE:
                await ProcessTypeAsync(socket, commands);
                break;

            case RedisProtocol.INCR:
                await ProcessIncrementAsync(socket, commands);
                break;

            case RedisProtocol.MULTI:
                await ProcessMultiAsync(socket, commands, state);
                break;

            case RedisProtocol.EXEC:
                await ProcessExecAsync(socket, commands, state);
                break;

            case RedisProtocol.DISCARD:
                await ProcessDiscardAsync(socket, commands, state);
                break;

            case RedisProtocol.XADD:
                await ProcessXaddAsync(socket, commands);
                break;

            case RedisProtocol.XRANGE:
                await ProcessXrangeAsync(socket, commands);
                break;

            case RedisProtocol.XREAD:
                await ProcessXreadAsync(socket, commands);
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

        var isSetSuccessful = false;

        if (db.Store.ContainsKey(key))
        {
            db.Store[key] = valueToStore;
            isSetSuccessful = true;
        }
        else
        {
            isSetSuccessful = db.Store.TryAdd(key, valueToStore);
        }

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

    private async Task ProcessWaitAsync(Socket socket, string[] commands, ClientConnectionState state)
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
        while (stopwatch.Elapsed.TotalMilliseconds < timeout && state.NumberOfReplicasAcknowledged < numOfReplicas)
        {
            //Logger.Log($"waiting... {state.NumberOfReplicasAcknowledged}");
        }

        Logger.Log($"Send WAIT response: {state.NumberOfReplicasAcknowledged}");

        await SendIntegerSocketResponseAsync(socket, Math.Min(state.NumberOfReplicasAcknowledged, numOfReplicas));
    }

    private async Task ProcessTypeAsync(Socket socket, string[] commands)
    {
        var key = commands[4];

        var db = GetDatabase();

        if (db.Store.TryGetValue(key, out var value))
        {
            await SendSimpleStringSocketResponseAsync(socket, value.Type.ToString().ToLower());
        }
        else
        {
            await SendSimpleStringSocketResponseAsync(socket, "none");
        }
    }

    private async Task ProcessXaddAsync(Socket socket, string[] commands)
    {
        var streamKey = commands[4];

        var streamEntryId = commands[6];

        Logger.Log("Here");

        var validatedStreamEntryId = await validateAndGetStreamIdAsync(socket, streamKey, streamEntryId);

        if (validatedStreamEntryId == null)
        {
            return;
        }

        Logger.Log($"Validated stream entry id: {validatedStreamEntryId}");

        var db = GetDatabase();

        var commandIndex = 8;

        var streamEntries = new List<KeyValuePair<string, string>>();

        while (commandIndex + 2 < commands.Length)
        {
            var keyIndex = commandIndex;

            var valueIndex = commandIndex + 2;

            var key = commands[keyIndex];

            var value = commands[valueIndex];

            var keyValuePair = new KeyValuePair<string, string>(key, value);

            streamEntries.Add(keyValuePair);

            commandIndex += 4;
        }

        if (db.Store.TryGetValue(streamKey, out var streamValue))
        {
            streamValue.AddToStream(validatedStreamEntryId, streamEntries);
        }
        else
        {
            var redisValue = new RedisValue(streamEntries, validatedStreamEntryId);
            db.Store.TryAdd(streamKey, redisValue);
        }

        await SendBulkStringSocketResponseAsync(socket, validatedStreamEntryId);
    }

    private async Task ProcessXrangeAsync(Socket socket, string[] commands)
    {
        var streamKey = commands[4];

        var streamIdStart = commands[6];

        var streamIdEnd = commands[8];

        Logger.Log($"Stream Id Start: {streamIdStart}");
        Logger.Log($"Stream Id end: {streamIdEnd}");

        var rangeStart = streamIdStart.ToStreamRangeStartId();

        var rangeEnd = streamIdEnd.ToStreamRangeEndId();

        Logger.Log($"Query range {rangeStart} - {rangeEnd}");

        var db = GetDatabase();

        if (!(db.Store.TryGetValue(streamKey, out var value) && value.Stream is not null))
        {
            throw new InvalidOperationException($"No stream found for key: {streamKey}");
        }

        var result = value.Stream.Entries
            .Where(e => e.EntryId.IsGreaterThanOrEqualTo(rangeStart) && e.EntryId.IsLessThanOrEqualTo(rangeEnd))
            .ToList();

        Logger.Log($"Total found: {result.Count}");

        foreach (var v in result)
        {
            Logger.Log($">{v.EntryId}");
        }

        var response = result.ToRespBulkArray();

        Logger.Log($"Sending [{response}]");

        await SendSocketResponseAsync(socket, response);
    }

    private async Task ProcessXreadAsync(Socket socket, string[] commands)
    {
        var commandIndex = 4;

        var isBlockCommand = false;

        var blockTimeout = 0;

        var blockUntilAnyEntryAvailable = false;

        if (commands[commandIndex].Equals("block", StringComparison.OrdinalIgnoreCase))
        {
            isBlockCommand = true;

            commandIndex += 2;

            blockTimeout = int.Parse(commands[commandIndex]);

            commandIndex += 2;
        }

        var subCommand = commands[commandIndex]; // The string "streams" does not add value right now

        commandIndex += 2;

        // the size of "streamKeys" and "streamIds" should be equal
        var streamKeys = new List<string>();

        var streamIds = new List<string>();

        while (commandIndex < commands.Length && !commands[commandIndex].IsStreamId() && commands[commandIndex] != "$")
        {
            var streamKey = commands[commandIndex];

            streamKeys.Add(streamKey);

            commandIndex += 2;
        }

        while (commandIndex < commands.Length)
        {
            var streamId = commands[commandIndex];

            if (streamId == "$")
            {
                blockUntilAnyEntryAvailable = true;
            }

            streamIds.Add(streamId);

            commandIndex += 2;
        }

        // foreach (var s in streamKeys) Logger.Log($"key: {s}");
        // foreach (var i in streamIds) Logger.Log($"id: {i}");

        var infoSnapshot = new Dictionary<string, StreamEntryId>();

        if (blockUntilAnyEntryAvailable)
        {
            // we keep a snapshot so that our query parameter (lastTimestamp and lastSequence) does not change as new values are added
            // otherwise we will be stuck in an infinite loop chasing the carrot
            var infoDictionary = GetStreamInformationDictionary();

            foreach (var streamKey in streamKeys)
            {
                if (infoDictionary.TryGetValue(streamKey, out var info))
                {
                    var entryId = new StreamEntryId
                    {
                        Timestamp = info.LastStreamIdTimestamp,
                        Sequence = info.LastStreamIdSequenceNumber
                    };

                    infoSnapshot.TryAdd(streamKey, entryId);
                }
            }
        }

        var db = GetDatabase();

        List<string> resultList = GetStreamDataAsResp(streamKeys, streamIds, db, infoSnapshot);

        if (resultList.Count == 0 && isBlockCommand)
        {
            Logger.Log($"Item not found, blocking for {blockTimeout} ms.");

            if (blockTimeout > 0)
            {
                // instead of returning when any one stream is available, we are simply blocking the thread for the entire timeout for simplicity
                await Task.Delay(blockTimeout);

                Logger.Log($"Checking again for item after block.");

                resultList = GetStreamDataAsResp(streamKeys, streamIds, db, infoSnapshot);

                Logger.Log($"Check done.");

                if (resultList.Count == 0)
                {
                    await SendNullSocketResponseAsync(socket);

                    return;
                }
            }
            else if (blockTimeout == 0)
            {
                // for blocking without timeout, instead of event-driven architecture, we are polling (periodically checking) for simplicity
                while (resultList.Count == 0)
                {
                    await Task.Delay(200);

                    resultList = GetStreamDataAsResp(streamKeys, streamIds, db, infoSnapshot);
                }
            }
        }

        var response = RespUtility.GetRespBulkArrayWithoutConversion(resultList.ToArray());

        Logger.Log($"XREAD response: [{response}]");

        await SendSocketResponseAsync(socket, response);
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

    private async Task ProcessMultiAsync(Socket socket, string[] commands, ClientConnectionState state)
    {
        await SendOkSocketResponseAsync(socket);
        state.ShouldQueueRequests = true;
    }

    private async Task ProcessExecAsync(Socket socket, string[] commands, ClientConnectionState state)
    {
        if (!state.ShouldQueueRequests)
        {
            await SendErrorStringSocketResponseAsync(socket, "EXEC without MULTI");
        }
        else
        {
            state.ShouldQueueRequests = false; // remove "MULTI" flag so the requeusts can update redis state now
            Logger.Log($"Should queue requests? {state.ShouldQueueRequests}");
            if (_redisRequestQueue.Count == 0)
            {
                await SendArraySocketResponseAsync(socket, []);
            }
            else
            {
                _shouldQueueResponses = true; // queue all response

                foreach (var request in _redisRequestQueue)
                {
                    Logger.Log($"EXEC > [${request.CommandString}]");
                    await ExecuteCommandAsync(socket, request, state);
                }

                _shouldQueueResponses = false; // unset queue-response flag, because now we will send the EXEC response array

                string array = GetExecResponseArray();

                // array of mixed elements (string, int. etc.)
                Logger.Log($"EXEC response: {array}");

                await SendSocketResponseAsync(socket, array);
            }

            // reset
            _redisResponseQueue.Clear();
        }
    }

    private async Task ProcessDiscardAsync(Socket socket, string[] commands, ClientConnectionState state)
    {
        if (!state.ShouldQueueRequests)
        {
            await SendErrorStringSocketResponseAsync(socket, "DISCARD without MULTI");
        }
        else
        {
            state.ShouldQueueRequests = false;
            _redisRequestQueue.Clear();
            await SendOkSocketResponseAsync(socket);
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
                await ExecuteCommandAsync(socket, command, new ClientConnectionState(), fromMaster: true);
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

    private Dictionary<string, StreamInformation> GetStreamInformationDictionary()
    {
        return _rdbHandler.RedisState.StreamInformationDictionary;
    }

    private async Task PropagateToReplicaAsync(string request, string protocol, ClientConnectionState state)
    {
        if (_redisInstance.Role == ServerRole.Master && protocol is RedisProtocol.SET)
        {
            Logger.Log($"Propagating: [{request}]");

            var propCommand = Encoding.UTF8.GetBytes(request);

            await SendCommandToReplicasAsync(propCommand, state);
        }
    }

    private async Task SendCommandToReplicasAsync(byte[] propCommand, ClientConnectionState state)
    {
        Logger.Log($"Sending to {_redisInstance.ConnectedReplicas.Count} replica(s).");

        state.NumberOfReplicasAcknowledged = 0;

        foreach (var replica in _redisInstance.ConnectedReplicas)
        {
            Logger.Log($"Sending command to replica {replica.RemoteEndPoint}");

            await replica.SendAsync(propCommand, SocketFlags.None);

            Logger.Log($"Sent command to replica {replica.RemoteEndPoint}");

            Logger.Log($"Sending GETACK * to replica {replica.RemoteEndPoint}");

            await SendCommandsAsync(replica, ["REPLCONF", "GETACK", "*"], receiveImmediateResponse: false);

            Logger.Log($"Sent GETACT * to replica {replica.RemoteEndPoint}");

            state.NumberOfReplicasAcknowledged++;
        }
    }

    private string GetExecResponseArray()
    {
        var array = $"*{_redisResponseQueue.Count}\r\n";

        foreach (var item in _redisResponseQueue)
        {
            if (item.ResponseType == ResponseType.INTEGER && item.IntegerValue.HasValue)
            {
                array += RespUtility.GetRespInteger(item.IntegerValue.Value);
            }
            else if (item.ResponseType == ResponseType.BULK_STRING && item.StringValue != null)
            {
                array += RespUtility.GetRespBulkString(item.StringValue);
            }
            else if (item.ResponseType == ResponseType.SIMPLE_STRING && item.StringValue != null)
            {
                array += RespUtility.GetRespSimpleString(item.StringValue);
            }
            else if (item.ResponseType == ResponseType.ERROR && item.StringValue != null)
            {
                array += RespUtility.GetRespErrorString(item.StringValue);
            }
        }

        return array;
    }

    private async Task<string?> validateAndGetStreamIdAsync(Socket socket, string streamKey, string streamId)
    {
        // handle 0-0, -1-0 (first part negative), 0--1 (second part negative), -1--1 (both parts negative)
        // pragmatically assuming if '-' appears more than once, it is used for a negative sign
        if (streamId == "0-0" || streamId.StartsWith("-") || streamId.EndsWith("-") || (streamId != "*" && streamId.Split("-").Length != 2))
        {
            await SendErrorStringSocketResponseAsync(socket, "The ID specified in XADD must be greater than 0-0");

            return null;
        }

        var streamInfoDictionary = GetStreamInformationDictionary();

        var streamKeyExists = streamInfoDictionary.ContainsKey(streamKey);

        var streamInfo = streamKeyExists ? streamInfoDictionary[streamKey] : new StreamInformation(streamKey);

        Logger.Log($"Retrieved stream info: {streamInfo}");

        if (streamId == "*") // handle full generation
        {
            var currentUnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

            if (streamInfo.LastStreamIdTimestamp == currentUnixMs)
            {
                streamInfo.LastStreamIdSequenceNumber++;
            }

            streamInfo.LastStreamIdTimestamp = currentUnixMs;

            streamInfoDictionary[streamKey] = streamInfo;

            return streamInfo.StreamId;
        }

        var newIdParts = streamId.Split("-");

        if (newIdParts[1] == "*" && long.TryParse(newIdParts[0], out var msValue)) // ends with '*'
        {
            if (streamKeyExists && msValue == streamInfo.LastStreamIdTimestamp)
            {
                newIdParts[1] = (streamInfo.LastStreamIdSequenceNumber + 1).ToString();
            }
            else
            {
                newIdParts[1] = newIdParts[0] == "0" ? "1" : "0";
            }
        }

        Logger.Log($"New stream id {newIdParts[0]}-{newIdParts[1]}");

        if (long.TryParse(newIdParts[0], out var newMsValue) && long.TryParse(newIdParts[1], out var newSequenceNumber))
        {
            if (newMsValue > streamInfo.LastStreamIdTimestamp || newMsValue == streamInfo.LastStreamIdTimestamp && newSequenceNumber > streamInfo.LastStreamIdSequenceNumber)
            {
                streamInfo.LastStreamIdTimestamp = newMsValue;
                streamInfo.LastStreamIdSequenceNumber = newSequenceNumber;
                streamInfoDictionary[streamKey] = streamInfo;

                return streamInfo.StreamId;
            }
        }

        await SendErrorStringSocketResponseAsync(socket, "The ID specified in XADD is equal or smaller than the target stream top item");

        return null;
    }

    private List<string> GetStreamDataAsResp(List<string> streamKeys, List<string> streamIds, RedisDatabase db, Dictionary<string, StreamEntryId> infoSnapshot)
    {
        var resultList = new List<string>();

        for (var index = 0; index < streamKeys.Count; index++)
        {
            var streamKey = streamKeys[index];

            StreamEntryId streamId = new();

            if (streamIds[index] == "$") // we only want new entry (equivalent to querying with last stream id)
            {
                if (infoSnapshot.TryGetValue(streamKey, out var idSnapshot))
                {
                    streamId = idSnapshot;
                }
            }
            else
            {
                streamId = streamIds[index].ToStreamRangeStartId();
            }

            Logger.Log($"Querying: {streamKey} | {streamId}");

            if (db.Store.TryGetValue(streamKey, out var value) && value.Stream != null)
            {
                var result = value.Stream.Entries.Where(e => e.EntryId.IsGreaterThan(streamId)).ToList();

                Logger.Log($"result count: {result.Count}");

                if (result.Count > 0)
                {
                    var array = new string[]
                    {
                        RespUtility.GetRespBulkString(streamKey),
                        result.ToRespBulkArray()
                    };

                    resultList.Add(RespUtility.GetRespBulkArrayWithoutConversion(array));
                }
            }
        }

        Logger.Log($"DB query done for XREAD. resultList size: {resultList.Count}");

        return resultList;
    }

    string GetRedisProtocol(string[] commands) => commands[2].ToLower();


    private async Task SendBulkStringSocketResponseAsync(Socket socket, string message)
    {
        if (_shouldQueueResponses)
        {
            AddToResponseQueue(message, ResponseType.BULK_STRING);
            return;
        }

        var bulkString = RespUtility.GetRespBulkString(message);
        await SendSocketResponseAsync(socket, bulkString);
    }

    private async Task SendArraySocketResponseAsync(Socket socket, string[] message)
    {
        // if (_shouldQueueResponses)
        // {
        //     AddToResponseQueue(message);
        //     return;
        // }

        var bulkArray = RespUtility.GetRespBulkArray(message);
        await SendSocketResponseAsync(socket, bulkArray);
    }

    private async Task SendIntegerSocketResponseAsync(Socket socket, long number)
    {
        if (_shouldQueueResponses)
        {
            AddToResponseQueue(number);
            return;
        }

        var integer = RespUtility.GetRespInteger(number);
        await SendSocketResponseAsync(socket, integer);
    }

    private async Task SendSimpleStringSocketResponseAsync(Socket socket, string message)
    {
        if (_shouldQueueResponses)
        {
            AddToResponseQueue(message, ResponseType.SIMPLE_STRING);
            return;
        }

        var simpleString = RespUtility.GetRespSimpleString(message);
        await SendSocketResponseAsync(socket, simpleString);
    }

    private async Task SendErrorStringSocketResponseAsync(Socket socket, string message)
    {
        if (_shouldQueueResponses)
        {
            AddToResponseQueue(message, ResponseType.ERROR);
            return;
        }

        var errorString = RespUtility.GetRespErrorString(message);
        await SendSocketResponseAsync(socket, errorString);
    }

    private async Task SendNullSocketResponseAsync(Socket socket)
    {
        if (_shouldQueueResponses)
        {
            AddToResponseQueue("-1", ResponseType.NULL);
            return;
        }

        await SendSocketResponseAsync(socket, RespUtility.GetNullBulkString());
    }

    private async Task SendOkSocketResponseAsync(Socket socket)
    {
        if (_shouldQueueResponses)
        {
            AddToResponseQueue("OK", ResponseType.SIMPLE_STRING);
            return;
        }

        await SendSocketResponseAsync(socket, RespUtility.GetOkResponseString());
    }

    private async Task SendRdbSocketResponseAsync(Socket socket, byte[] data)
    {
        var response = Encoding.UTF8.GetBytes($"${data.Length}\r\n");

        await socket.SendAsync(response, SocketFlags.None);

        await socket.SendAsync(data, SocketFlags.None);
    }

    private async Task SendSocketResponseAsync(Socket socket, string payload)
    {
        var response = Encoding.UTF8.GetBytes(payload);
        await socket.SendAsync(response, SocketFlags.None);
    }

    private void AddToResponseQueue(string payload, ResponseType responseType = ResponseType.BULK_STRING)
    {
        _redisResponseQueue.Add(new RedisResponse
        {
            ResponseType = responseType,
            StringValue = payload
        });
    }

    private void AddToResponseQueue(long payload)
    {
        _redisResponseQueue.Add(new RedisResponse
        {
            ResponseType = ResponseType.INTEGER,
            IntegerValue = payload
        });
    }


}