public class ClientConnectionState
{
    private readonly object _lock = new object();
    private int _numberOfReplicasAcknowledged = 0;
    private bool _shouldQueueRequests = false;

    public int NumberOfReplicasAcknowledged 
    { 
        get { lock (_lock) return _numberOfReplicasAcknowledged; }
        set { lock (_lock) _numberOfReplicasAcknowledged = value; }
    }
    
    public bool ShouldQueueRequests 
    { 
        get { lock (_lock) return _shouldQueueRequests; }
        set { lock (_lock) _shouldQueueRequests = value; }
    }
}