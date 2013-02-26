using System;

namespace mData.Utils
{

    public static class Char32
    {

        private const string Chars = "0123456789abcdefghijklmnopqrsxyz"; //"x7y6fz1g83isnkphc4lbjar902dqem5o";
        
        public static string Convert130Bits(ulong high, ulong low)
        {
            char[] buf = new char[26];
            int p = Char32.Add(high >> 4, 60, buf, 0);
            p = Char32.Add((low >> 8) | (high << 56), 60, buf, p);
            p = Char32.Add(low << 2, 10, buf, p);
            return new String(buf, 0, p);
        }

        public static string Convert95Bits(ulong high, ulong low)
        {
            char[] buf = new char[19];
            int p = Char32.Add(high >> 4, 60, buf, 0);
            p = Char32.Add((low & 0x7fffffff) | (high << 31), 35, buf, p);
            return new String(buf, 0, p);
        }

        private static int Add(ulong bits, int count, char[] buffer, int pos)
        {
            int c = 1;
            while (count > 5)
            {
                c++;
                count -= 5;
            }
            int p = pos + c - 1;
            while (true)
            {
                buffer[p] = Char32.Chars[(int)(bits & 0x1f)];
                ++pos;
                if (c == 1) break;
                --c;
                --p;
                bits = bits >> 5;
            }
            return pos;
        }


    }



}