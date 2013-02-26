using System;

namespace mData.Utils
{

    static class Comparers
    {
        public static int Compare(string x, string y)
        {
            return String.Compare(x, y, true);
        }

        public static int Compare(int x, int y)
        {
            return x.CompareTo(y);
        }

    }
}