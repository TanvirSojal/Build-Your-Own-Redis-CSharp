public static class Logger
{
    public static void Log(string? message)
    {
        Console.WriteLine($"{DateTime.UtcNow} | thread_id: [{Thread.CurrentThread.ManagedThreadId}] {message}");
    }

    public static void Log(object? value)
    {
        Console.WriteLine($"{DateTime.UtcNow} | thread_id: [{Thread.CurrentThread.ManagedThreadId}] {value}");
    }
}