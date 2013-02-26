using System;

namespace mData.Utils
{
    public static class Utils
    {
        public static void Sort<T>(T[] array, Func<T, T, int> comparer)
        {
            if (array == null) return;
            if (array.Length < 2) return;
            Utils.Quicksort(array, 0, array.Length - 1, comparer);
        }

        private static void Quicksort<T>(T[] array, int l, int r, Func<T, T, int> comparer)
        {
            if (r <= l) return;
            int i = l - 1, j = r, p = l - 1, q = r; T v = array[r];
            while (true)
            {
                while (comparer(array[++i], v) < 0) { }
                while (comparer(v, array[--j]) < 0) if (j == l) break;
                if (i >= j) break;
                Utils.Exchange(array, i, j);
                if (comparer(array[i], v) == 0) { p++; Utils.Exchange(array, p, i); }
                if (comparer(v, array[j]) == 0) { q--; Utils.Exchange(array, j, q); }
            }
            Utils.Exchange(array, i, r); j = i - 1; i = i + 1;
            for (int k = l; k < p; k++, j--) Utils.Exchange(array, k, j);
            for (int k = r - 1; k > q; k--, i++) Utils.Exchange(array, i, k);
            Utils.Quicksort(array, l, j, comparer);
            Utils.Quicksort(array, i, r, comparer);
        }

        private static void Exchange<T>(T[] array, int i, int j)
        {
            T x = array[i];
            array[i] = array[j];
            array[j] = x;
        }

    }

}