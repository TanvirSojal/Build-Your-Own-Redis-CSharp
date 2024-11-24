using System.Net;
using System.Net.Sockets;

var redisInfo = new RedisInfo();
redisInfo.Port = 6379;

var rdbConfig = new RdbConfiguration();
// Read CLI arguments
for (var index = 0; index < args.Length; index++)
{
    if (args[index].Equals("--dir", StringComparison.OrdinalIgnoreCase))
    {
        rdbConfig.Directiory = args[index + 1];
    }
    if (args[index].Equals("--dbfilename", StringComparison.OrdinalIgnoreCase))
    {
        rdbConfig.DbFileName = args[index + 1];
    }
    if (args[index].Equals("--port", StringComparison.OrdinalIgnoreCase))
    {
        redisInfo.Port = int.Parse(args[index + 1]);
    }
    if (args[index].Equals("--replicaof", StringComparison.OrdinalIgnoreCase))
    {
        redisInfo.Role = ServerRole.Slave;
        redisInfo.SetMasterEndpoint(args[index + 1]);
    }
}

var rdbHandler = new RdbHandler(rdbConfig);
rdbHandler.RestoreSnapshot();

var engine = new RedisEngine(rdbHandler, redisInfo);

TcpListener server = new TcpListener(IPAddress.Any, redisInfo.Port);
server.Start();

Console.WriteLine($"Server started on port {redisInfo.Port}");

if (redisInfo.Role == ServerRole.Slave)
{
    // handshake to master and receive propagated commands in a separate thread
    _ = Task.Run(engine.ConnectToMasterAsync);
}

var replicas = new List<Socket>();

while (true)
{
    var acceptedSocket = await server.AcceptSocketAsync(); // wait for client
    Console.WriteLine("Accepted a new connection");

    _ = Task.Run(async () => await HandleIncomingRequestAsync(acceptedSocket));
}

// *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
async Task HandleIncomingRequestAsync(Socket socket)
{
    // a common store of data for the current client connection
    // this is accessible from different methods
    var stats = new ClientConnectionStats();

    while (true)
    {
        var readBuffer = new byte[1024];

        await socket.ReceiveAsync(readBuffer);

        await engine.ProcessRequestAsync(socket, readBuffer, stats);
    }
}