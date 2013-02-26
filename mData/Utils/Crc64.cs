using System;

namespace mData.Utils
{

    public static class Crc64
    {

        private const ulong Poly64Rev = 0x95AC9329AC4BC9B5;
        public const ulong InitialCrc = 0xFFFFFFFFFFFFFFFF;

        private static ulong[] crc64Table = null;

        private static void FillCrcTable()
        {
            ulong[] table = new ulong[256];
            for (ulong i = 0; i < 256; i++)
            {
                ulong part = i;
                for (int j = 0; j < 8; j++)
                {
                    if ((part & 1) > 0)
                    {
                        part = (part >> 1) ^ Crc64.Poly64Rev;
                    }
                    else
                    {
                        part >>= 1;
                    }
                }
                table[i] = part;
            }
            Crc64.crc64Table = table;
            table = null;
        }

        public static unsafe ulong Update(string s, ulong crc)
        {
            if (s == null) return crc;
            int l = s.Length;
            if (l == 0) return crc;
            if (Crc64.crc64Table == null) Crc64.FillCrcTable();
            fixed (char* p = s)
            {
                crc = Crc64.Add((byte*)p, l << 1, crc);
            }
            return crc;
        }

        public static unsafe ulong Update(ulong l, ulong crc)
        {
            if (Crc64.crc64Table == null) Crc64.FillCrcTable();
            return Crc64.Add((byte*)(&l), 8, crc);
        }

        public static unsafe ulong Update(int i, ulong crc)
        {
            if (Crc64.crc64Table == null) Crc64.FillCrcTable();
            return Crc64.Add((byte*)(&i), 4, crc);
        }

        public static unsafe ulong Calculate(string s)
        {
            return Crc64.Update(s, Crc64.InitialCrc);
        }

        public static unsafe ulong Calculate(byte[] buffer)
        {
            return Crc64.Update(buffer, 0, buffer.Length, Crc64.InitialCrc);
        }

        public static unsafe ulong Calculate(byte[] buffer, int offset, int count)
        {
            return Crc64.Update(buffer, offset, count, Crc64.InitialCrc);
        }

        public static unsafe ulong Update(byte[] buffer, ulong crc)
        {
            return Crc64.Update(buffer, 0, buffer.Length, crc);
        }

        public static unsafe ulong Update(byte[] buffer, int offset, int count, ulong crc)
        {
            if (Crc64.crc64Table == null) Crc64.FillCrcTable();
            fixed (byte* p = buffer)
            {
                crc = Crc64.Add(p + offset, count, crc);
            }
            return crc;
        }

        private static unsafe ulong Add(byte* p, int count, ulong crc)
        {
            for (int i = 0; i < count; i++)
            {
                ulong f = (crc >> 56) ^ ((ulong)(*(p++)));
                crc = Crc64.crc64Table[(byte)(f & 0xff)] ^ (crc << 8);
            }
            return crc;
        }

    }

}