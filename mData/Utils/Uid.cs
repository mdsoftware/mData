using System;
using System.Threading;

namespace mData.Utils
{
    public static class Uid
    {
        private static string machineName = Environment.MachineName;
        private static Random random = new Random();
        private static ulong hash = 0;
        private static ulong count = Uid.MaxCount;
        private static object sync = new Object();

        private const ulong MaxCount = 0x7fffffff;

        public static ulong NextHash()
        {
            long ticks = DateTime.Now.Ticks;
            ulong crc = Crc64.InitialCrc;
            for (int i = 0; i < 16; i++)
            {
                switch (Uid.random.Next(2))
                {
                    case 0:
                        crc = Crc64.Update(Uid.machineName, crc);
                        break;

                    case 1:
                        crc = Crc64.Update((ulong)ticks, crc);
                        break;

                    default:
                        crc = Crc64.Update(Uid.random.Next(), crc);
                        break;
                }
            }
            return crc;
        }

        public static string Next()
        {
            return Char32.Convert130Bits(Uid.NextHash(), Uid.NextHash());
        }

        public static string NextSequental()
        {
            Monitor.Enter(Uid.sync);
            if (Uid.count == Uid.MaxCount)
            {
                Uid.count = 0;
                Uid.hash = Uid.NextHash();
            }
            else
            {
                Uid.count++;
            }
            ulong high = Uid.hash;
            ulong low = Uid.count;
            Monitor.Exit(Uid.sync);

            return Char32.Convert95Bits(high, low);
        }

    }

}