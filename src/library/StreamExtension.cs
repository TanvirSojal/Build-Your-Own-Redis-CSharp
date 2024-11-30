public static class StreamExtension
{
    public static bool IsLessThanOrEqualTo(this StreamEntryId entryId, StreamEntryId otherEntryId)
    {
        return entryId.CompareTo(otherEntryId) <= 0;
    }

    public static bool IsGreaterThanOrEqualTo(this StreamEntryId entryId, StreamEntryId otherEntryId)
    {
        return entryId.CompareTo(otherEntryId) >= 0;
    }

    public static string ToRespBulkArray(this List<StreamEntry> entries)
    {
        // key -> bulk string
        // value -> bulk string
        // kv list -> bulk array
        // id, kv list -> bulk array
        // entry -> bulk array

        var array = new List<string>();

        foreach (var entry in entries)
        {
            array.Add(entry.ToArray());
        }

        return RespUtility.GetRespBulkArrayWithoutConversion(array.ToArray());
    }

    private static string ToArray(this StreamEntry entry)
    {
        var dataArray = new List<string>();

        foreach (var data in entry.Data)
        {
            dataArray.Add(data.Key);
            dataArray.Add(data.Value);
        }

        var dataBulkArray = RespUtility.GetRespBulkArray(dataArray.ToArray());

        Logger.Log($"data bulk array: {dataBulkArray}");

        var array = new List<string>
        {
            RespUtility.GetRespBulkString(entry.EntryId.ToString()),
            dataBulkArray
        };

        Logger.Log($"entry bulk array: {array}");

        return RespUtility.GetRespBulkArrayWithoutConversion(array.ToArray());
    }

}