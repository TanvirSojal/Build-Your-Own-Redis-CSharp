using System.Net;
using System.Net.Sockets;

var redisInstance = new RedisInstance();
redisInstance.Port = 6379;

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
        redisInstance.Port = int.Parse(args[index + 1]);
    }
    if (args[index].Equals("--replicaof", StringComparison.OrdinalIgnoreCase))
    {
        redisInstance.Role = ServerRole.Slave;
        redisInstance.SetMasterEndpoint(args[index + 1]);
    }
}

var rdbHandler = new RdbHandler(rdbConfig);
rdbHandler.RestoreSnapshot();

var engine = new RedisEngine(rdbHandler, redisInstance);

TcpListener server = new TcpListener(IPAddress.Any, redisInstance.Port);
server.Start();

Console.WriteLine($"Server started on port {redisInstance.Port}");

if (redisInstance.Role == ServerRole.Slave)
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

    // new Redis Engine instantiated for every client connection
    var engine = new RedisEngine(rdbHandler, redisInstance);

    while (true)
    {
        var readBuffer = new byte[1024];

        var bytesRead = await socket.ReceiveAsync(readBuffer);

        if (bytesRead == 0)
        {
            break;
        }

        await engine.ProcessRequestAsync(socket, readBuffer, stats);
    }
}