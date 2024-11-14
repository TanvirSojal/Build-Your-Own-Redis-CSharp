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
    var store = new Dictionary<string, RedisValue>();

    var buffer = new byte[1024];

    while (true)
    {
        await socket.ReceiveAsync(buffer);

        var request = Encoding.UTF8.GetString(buffer).TrimEnd('\0');

        Console.WriteLine($"received: [{request}] Length: {request.Length}");

        var command = Regex.Split(request, @"\s+");

        var index = 0;
        foreach (var r in command)
        {
            Console.WriteLine($"{index++} {r}");
        }

        Console.WriteLine($"Protocol String: {command[2]}");

        var protocol = GetRedisProtocol(command[2]);

        Console.WriteLine($"Protocol: {protocol}");

        if (protocol == RedisProtocol.PING)
        {
            await SendSocketResponseAsync(socket, "PONG");
        }
        else if (protocol == RedisProtocol.ECHO)
        {
            Console.WriteLine($"Payload: {command[4]}");

            await SendSocketResponseAsync(socket, command[4]);
        }
        else if (protocol == RedisProtocol.SET)
        {
            var key = command[4];
            var value = command[6];

            var expiry = (long?)null;

            if (command.Length >= 10)
            {
                var argument = command[8];

                if (argument.ToLower() == "px")
                {
                    expiry = long.Parse(command[10]);
                }
            }

            var valueToStore = new RedisValue(value, expiry);

            if (store.TryAdd(key, valueToStore))
            {
                await SendOkSocketResponseAsync(socket);
            }
        }
        else if (protocol == RedisProtocol.GET)
        {
            var key = command[4];

            if (store.TryGetValue(key, out var value))
            {
                if (value.IsExpired())
                {
                    store.Remove(key);

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
        else
        {
            await SendNullSocketResponseAsync(socket);
        }
    }
}

string GetRedisBulkString(string payload)
{
    return $"${payload.Length}\r\n{payload}\r\n";
}

string GetNullBulkString() => "$-1\r\n";
string GetOkResponseString() => "+OK\r\n";

async Task SendSocketResponseAsync(Socket socket, string message)
{
    var bulkString = GetRedisBulkString(message);
    var response = Encoding.UTF8.GetBytes(bulkString);
    await socket.SendAsync(response, SocketFlags.None);
}

async Task SendNullSocketResponseAsync(Socket socket)
{
    var response = Encoding.UTF8.GetBytes(GetNullBulkString());
    await socket.SendAsync(response, SocketFlags.None);
}

async Task SendOkSocketResponseAsync(Socket socket)
{
    var response = Encoding.UTF8.GetBytes(GetOkResponseString());
    await socket.SendAsync(response, SocketFlags.None);
}

RedisProtocol GetRedisProtocol(string protocol)
{
    return protocol.ToLower() switch
    {
        "ping" => RedisProtocol.PING,
        "echo" => RedisProtocol.ECHO,
        "set" => RedisProtocol.SET,
        "get" => RedisProtocol.GET,
        _ => RedisProtocol.NONE,
    };
}

enum RedisProtocol
{
    NONE,
    PING,
    ECHO,
    GET,
    SET
}