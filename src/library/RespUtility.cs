public static class RespUtility
{
    public static string GetRespBulkString(string payload) => $"${payload.Length}\r\n{payload}\r\n";
    public static string GetRespSimpleString(string payload) => $"+{payload}\r\n";
    public static string GetRespErrorString(string payload) => $"-ERR {payload}\r\n";
    public static string GetNullBulkString() => "$-1\r\n";
    public static string GetOkResponseString() => GetRespSimpleString("OK");
    public static string GetRespInteger(long number) => $":{number}\r\n";
    public static string GetRespBulkArray(string[] payload)
    {
        var response = $"*{payload.Length}\r\n";

        foreach (var item in payload)
        {
            response += GetRespBulkString(item);
        }

        return response;
    }
    public static string GetRespBulkArrayWithoutConversion(string[] payload)
    {
        var response = $"*{payload.Length}\r\n";

        foreach (var item in payload)
        {
            response += item;
        }

        return response;
    }
}