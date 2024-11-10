using System.Net;
using System.Net.Sockets;
using System.Text;

// You can use print statements as follows for debugging, they'll be visible when running tests.
Console.WriteLine("Logs from your program will appear here!");

// Uncomment this block to pass the first stage
TcpListener server = new TcpListener(IPAddress.Any, 6379);
server.Start();

while(true){
    var acceptedSocket = await server.AcceptSocketAsync(); // wait for client
    Console.WriteLine("Accepted a new connection");
    _ = Task.Run(async () => await HandleIncomingRequestAsync(acceptedSocket));
}

async Task HandleIncomingRequestAsync(Socket socket){
    Console.WriteLine("Handling request");
    var buffer = new byte[256];
    var bytesReceived = 0;
    var totalBytesReceived = 0;
    var data = "";

    var response = Encoding.UTF8.GetBytes("+PONG\r\n");

    while((bytesReceived = await socket.ReceiveAsync(buffer)) > 0){
        totalBytesReceived += bytesReceived;
        data += Encoding.UTF8.GetString(buffer);
        //Console.WriteLine(data, bytesReceived);
        await socket.SendAsync(response);
    }
}

// Console.WriteLine(data);
// Console.WriteLine($"Total bytes received: {totalBytesReceived}");