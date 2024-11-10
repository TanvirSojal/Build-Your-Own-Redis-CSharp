using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.RegularExpressions;

Console.WriteLine("Logs from your program will appear here!");

TcpListener server = new TcpListener(IPAddress.Any, 6379);
server.Start();

while (true)
{
    var acceptedSocket = await server.AcceptSocketAsync(); // wait for client
    Console.WriteLine("Accepted a new connection");
    _ = Task.Run(async () => await HandleIncomingRequestAsync(acceptedSocket));
}

// *2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n
async Task HandleIncomingRequestAsync(Socket socket)
{
    var buffer = new byte[1024];
    await socket.ReceiveAsync(buffer);

    var request = Encoding.UTF8.GetString(buffer).TrimEnd('\0');

    //Console.WriteLine($"received: [{request}]");

    var command = request.Split("\\r\\n"); //Regex.Split(request, @"\s+");

    var index = 0;
    foreach (var r in command)
    {
        Console.WriteLine($"{index++} {r}");
    }

    // if (command.Length < 2){
    //     await socket.SendAsync(Encoding.UTF8.GetBytes("INVALID\r\n"), SocketFlags.None);
    //     return;
    // }

    var protocol = GetRedisProtocol(command[2]);

    Console.WriteLine($"Protocol: {protocol}");

    if (protocol == RedisProtocol.PING)
    {
        var response = Encoding.UTF8.GetBytes("+PONG\r\n");
        await socket.SendAsync(response, SocketFlags.None);
    }
    else if (protocol == RedisProtocol.ECHO)
    {
        // if (command.Length < 3){
        //     await socket.SendAsync(Encoding.UTF8.GetBytes("INVALID\r\n"), SocketFlags.None);
        //     return;
        // }
        Console.WriteLine($"Payload: {command[4]}");

        var bulkString = GetRedisBulkString(command[4]);
        var response = Encoding.UTF8.GetBytes(bulkString);
        await socket.SendAsync(response, SocketFlags.None);
    }
    else
    {
        await socket.SendAsync(Encoding.UTF8.GetBytes("INVALID\r\n"), SocketFlags.None);
    }
}

string GetRedisBulkString(string payload)
{
    return $"${payload.Length}\\r\\n{payload}\\r\\n";
}

RedisProtocol GetRedisProtocol(string protocol)
{
    return protocol.ToLower() switch
    {
        "ping" => RedisProtocol.PING,
        "echo" => RedisProtocol.ECHO,
        _ => RedisProtocol.NONE,
    };
}

enum RedisProtocol
{
    NONE,
    PING,
    ECHO
}