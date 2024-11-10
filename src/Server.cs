using System.Net;
using System.Net.Sockets;
using System.Text;
using Microsoft.VisualBasic;

Console.WriteLine("Logs from your program will appear here!");

TcpListener server = new TcpListener(IPAddress.Any, 6379);
server.Start();

while(true){
    var acceptedSocket = await server.AcceptSocketAsync(); // wait for client
    Console.WriteLine("Accepted a new connection");
    _ = Task.Run(async () => await HandleIncomingRequestAsync(acceptedSocket));
}

async Task HandleIncomingRequestAsync(Socket socket){
    var buffer = new byte[socket.ReceiveBufferSize];
    await socket.ReceiveAsync(buffer);

    var request = Encoding.UTF8.GetString(buffer);

    Console.WriteLine(request);
    
    var command = request.Split(" ");

    if (command.Length == 0){
        await socket.SendAsync(Encoding.UTF8.GetBytes("+PONG\r\n"), SocketFlags.None);
        return;
    }

    var protocol = GetRedisProtocol(command[0]);

    Console.WriteLine($"Protocol: {protocol}");

    if (protocol == RedisProtocol.PING)
    {
        var response = Encoding.UTF8.GetBytes("+PONG\r\n");
        await socket.SendAsync(response, SocketFlags.None);
    }   
    else if (protocol == RedisProtocol.ECHO)
    {
        var response = Encoding.UTF8.GetBytes(command[1]);
        await socket.SendAsync(response, SocketFlags.None);
    }
    Console.WriteLine("Finished processing request");
}

RedisProtocol GetRedisProtocol(string protocol){
    return protocol switch
    {
        "PING" => RedisProtocol.PING,
        "ECHO" => RedisProtocol.ECHO,
        _ => RedisProtocol.NONE,
    };
}

enum RedisProtocol {
    NONE,
    PING,
    ECHO
}