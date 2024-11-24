public class ClientConnectionState
{
    public int NumberOfReplicasAcknowledged { get; set; } = 0;
    public bool ShouldQueueRequests { get; set; } = false;
}