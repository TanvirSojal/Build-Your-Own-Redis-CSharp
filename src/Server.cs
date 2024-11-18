using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.RegularExpressions;

var redisInfo = new RedisInfo();
redisInfo.Port = 6379;

var rdbConfig = new RdbConfiguration();
// Read CLI arguments
for (var index = 0; index < args.Length; index++)
{
    if (args[index].Equals("--dir", StringComparison.OrdinalIgnoreCase))
    {
        rdbConfig.Directiory = args[index+1];
    }
    if (args[index].Equals("--dbfilename", StringComparison.OrdinalIgnoreCase))
    {
        rdbConfig.DbFileName = args[index+1];
    }
    if (args[index].Equals("--port", StringComparison.OrdinalIgnoreCase)){
        redisInfo.Port = int.Parse(args[index+1]);
    }
    if (args[index].Equals("--replicaof", StringComparison.OrdinalIgnoreCase)){
        redisInfo.Role = ServerRole.Slave;
        redisInfo.SetMasterEndpoint(args[index+1]);
    }
}

var rdbHandler = new RdbHandler(rdbConfig);
rdbHandler.RestoreSnapshot();

//Console.WriteLine(rdbHandler.RedisState);

//Console.WriteLine(redisInfo);

var engine = new RedisEngine(rdbHandler, redisInfo);

TcpListener server = new TcpListener(IPAddress.Any, redisInfo.Port);
server.Start();

Console.WriteLine($"Server started on port {redisInfo.Port}");

if (redisInfo.Role == ServerRole.Slave)
{
    await engine.ConnectToMasterAsync();
}

while (true)
{
    var acceptedSocket = await server.AcceptSocketAsync(); // wait for client
    Console.WriteLine("Accepted a new connection");

    _ = Task.Run(async () => await HandleIncomingRequestAsync(acceptedSocket));
}

// *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
async Task HandleIncomingRequestAsync(Socket socket)
{
    while (true)
    {
        var readBuffer = new byte[1024];

        await socket.ReceiveAsync(readBuffer);

        var request = Encoding.UTF8.GetString(readBuffer).TrimEnd('\0');

        Console.WriteLine($"received: [{request}] Length: {request.Length}");

        var commands = Regex.Split(request, @"\s+");

        var index = 0;

        foreach (var command in commands)
        {
            Console.WriteLine($"{index++} {command}");
        }

        var protocol = GetRedisProtocol(commands[2]);

        Console.WriteLine($"Protocol: {protocol}");

        switch (protocol)
        {
            case RedisProtocol.PING:
                await engine.ProcessPingAsync(socket, commands);
                break;
            
            case RedisProtocol.ECHO:
                await engine.ProcessEchoAsync(socket, commands);
                break;

            case RedisProtocol.SET:
                await engine.ProcessSetAsync(socket, commands);
                break;
            
            case RedisProtocol.GET:
                await engine.ProcessGetAsync(socket, commands);
                break;

            case RedisProtocol.CONFIG:
                await engine.ProcessConfigAsync(socket, commands);
                break;

            case RedisProtocol.KEYS:
                await engine.ProcessKeysAsync(socket, commands);
                break;

            case RedisProtocol.INFO:
                await engine.ProcessInfoAsync(socket, commands);
                break;
            
            case RedisProtocol.REPLCONF:
                await engine.ProcessReplConfAsync(socket, commands);
                break;

            case RedisProtocol.NONE:
                break;
        }
        
    }
}

RedisProtocol GetRedisProtocol(string protocol)
{
    return protocol.ToLower() switch
    {
        RedisKeyword.PING => RedisProtocol.PING,
        RedisKeyword.ECHO => RedisProtocol.ECHO,
        RedisKeyword.SET => RedisProtocol.SET,
        RedisKeyword.GET => RedisProtocol.GET,
        RedisKeyword.CONFIG => RedisProtocol.CONFIG,
        RedisKeyword.KEYS => RedisProtocol.KEYS,
        RedisKeyword.INFO => RedisProtocol.INFO,
        RedisKeyword.REPLCONF => RedisProtocol.REPLCONF,
        _ => RedisProtocol.NONE,
    };
}