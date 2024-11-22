public static class Logger
{
    public static void Log(string? message)
    {
        Console.WriteLine($"{GetCurrentDateTime()} | thread_id: [{Thread.CurrentThread.ManagedThreadId}] {message}");
    }

    public static void Log(object? value)
    {
        Console.WriteLine($"{GetCurrentDateTime()} | thread_id: [{Thread.CurrentThread.ManagedThreadId}] {value}");
    }

    private string GetCurrentDateTime()
    {
        return DateTime.UtcNow.ToString("HH:mm:ss.fff");
    }
}