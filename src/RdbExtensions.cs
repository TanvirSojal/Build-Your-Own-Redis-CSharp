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

    public static string ReadLengthEncodedString(this BinaryReader reader)
    {
        byte firstByte = reader.ReadByte();

        var length = 0;

        if (firstByte.IsCurrentByteEnoughToGetLength())
        {
            length = firstByte;
        }
        else if (firstByte.IsAdditionalByteNeededToGetLength())
        {
            var nextByte = reader.ReadByte();
            length = (firstByte << sizeof(byte)) | nextByte;
        }
        else if (firstByte.IsNextFourBytesNeededToGetLength())
        {
            length = reader.ReadInt32();
        }
        else
        {
            var __int = 0;
            if (firstByte.IsFollowedBy8BitInteger())
            {
                __int = reader.ReadByte();
            }
            if (firstByte.IsFollowedBy16BitInteger())
            {
                __int = reader.ReadInt16();
            }
            if (firstByte.IsFollowedBy32BitInteger())
            {
                __int = reader.ReadInt32();
            }

            // LZF is ignored

            return __int.ToString();
        }

        var bytes = reader.ReadBytes(length);
        return Encoding.UTF8.GetString(bytes);
    }

    public static bool IsCurrentByteEnoughToGetLength(this byte __byte)
    {
        return (__byte >> 6) == 0b00;
    }

    public static bool IsAdditionalByteNeededToGetLength(this byte __byte)
    {
        return (__byte >> 6) == 0b01;
    }

    public static bool IsNextFourBytesNeededToGetLength(this byte __byte)
    {
        return (__byte >> 6) == 0b10;
    }

    public static bool IsSpecialFormatString(this byte __byte)
    {
        return (__byte >> 6) == 0b11;
    }

    public static bool IsFollowedBy8BitInteger(this byte __byte)
    {
        return __byte == 0b11000000;
    }

    public static bool IsFollowedBy16BitInteger(this byte __byte)
    {
        return __byte == 0b11000001;
    }

    public static bool IsFollowedBy32BitInteger(this byte __byte)
    {
        return __byte == 0b11000010;
    }
}