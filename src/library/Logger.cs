public static class Logger
{
    public static void Log(string? message)
    {
        var sanitizedMessage = message == null ? message : message.Replace("\r", "\\r").Replace("\n", "\\n");
        Console.WriteLine($"{GetCurrentDateTime()} | thread_id: [{Thread.CurrentThread.ManagedThreadId}] {sanitizedMessage}");
    }

    public static void Log(object? value)
    {
        Console.WriteLine($"{GetCurrentDateTime()} | thread_id: [{Thread.CurrentThread.ManagedThreadId}] {value}");
    }

    private static string GetCurrentDateTime()
    {
        return DateTime.UtcNow.ToString("HH:mm:ss.fff");
    }
}