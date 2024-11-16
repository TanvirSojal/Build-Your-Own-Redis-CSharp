using System.Net.Sockets;
using System.Text;

public class RedisEngine
{
    private readonly RdbHandler _rdbHandler;
    private Dictionary<string, RedisValue> store = new Dictionary<string, RedisValue>();

    public RedisEngine(RdbHandler rdbHandler)
    {
        _rdbHandler = rdbHandler;
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

        var expiry = (long?)null;

        if (commands.Length >= 10)
        {
            var argument = commands[8];

            if (argument.Equals("px", StringComparison.OrdinalIgnoreCase))
            {
                expiry = long.Parse(commands[10]);
            }
        }

        var valueToStore = new RedisValue(value, expiry);

        if (store.TryAdd(key, valueToStore))
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

    public async Task ProcessConfigAsync(Socket socket, string[] commands)
    {
        var subcommand = commands[4];

        if (subcommand.Equals("get", StringComparison.OrdinalIgnoreCase))
        {
            await ProcessConfigGetAsync(socket, commands);
        }
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
        var bulkString = GetRedisBulkArray(message);
        var response = Encoding.UTF8.GetBytes(bulkString);
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
}