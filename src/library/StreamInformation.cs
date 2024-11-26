public class StreamInformation
{
    public string StreamKey { get; set; }
    public long LastStreamIdMsValue { get; set; } = 0;
    public long LastStreamIdSequenceNumber { get; set; } = 0;
    public string StreamId => $"{LastStreamIdMsValue}-{LastStreamIdSequenceNumber}";

    public StreamInformation(string streamKey)
    {
        StreamKey = streamKey;
    }

    public StreamInformation Initialize()
    {
        LastStreamIdMsValue = 0;
        LastStreamIdSequenceNumber = 1;
        return this;
    }

    public override string ToString()
    {
        return $"__streamKey: {StreamKey} | __streamId: {StreamId} | __lastMs: {LastStreamIdMsValue} | __lastSeq: {LastStreamIdSequenceNumber}";
    }
}