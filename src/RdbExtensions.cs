using System.Text;

public static class RdbExtensions
{
    public static string ReadString(this BinaryReader reader, int count)
    {
        var bytes = reader.ReadBytes(count);

        if (bytes == null)
        {
            return "";
        }

        return Encoding.UTF8.GetString(bytes);
    }
}