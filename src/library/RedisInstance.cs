using System.Net;
using System.Net.Sockets;

public class RedisInstance
{
    public ServerRole Role { get; set; } = ServerRole.Master;
    public IPEndPoint? MasterEndpoint { get; set; }
    public readonly List<Socket> ConnectedReplicas = new List<Socket>();
    public int Port { get; set; }

    public void SetMasterEndpoint(string address){
        var uriParts = address.Split(" ");
        if (uriParts.Length > 0 && uriParts[0] == "localhost"){
            uriParts[0] = "127.0.0.1";
        }

        var ip = IPAddress.Parse(uriParts[0]);
        var port = int.Parse(uriParts[1]);

        MasterEndpoint = new IPEndPoint(ip, port);
    }

    public override string ToString()
    {
        return @$"# Replication
role:{Role.ToString().ToLower()}
connected_slaves:0
master_replid:8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb
master_repl_offset:0
second_repl_offset:-1
repl_backlog_active:0
repl_backlog_size:1048576
repl_backlog_first_byte_offset:0
repl_backlog_histlen:";
    }
}