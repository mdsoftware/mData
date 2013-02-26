using System;
using System.Collections.Generic;
using mData.Testing;

namespace mData.DataStructures
{
    public enum PageFactor : int
    {
        Page16 = 4,
        Page32 = 5,
        Page64 = 6,
        Page128 = 7,
        Page256 = 8,
        Page512 = 9,
        Page1024 = 10,
        Page2048 = 11,
        Page4096 = 12
    }

    public class PagedList<TItem>
    {

        private ListPage[] pages;
        private int shift;
        private int mask;
        private int pageCount;
        private int count;

        public const int InitialGrowPagesCount = 32;

        public PagedList(PageFactor factor, int pages)
        {
            if (pages <= 0) pages = PagedList<TItem>.InitialGrowPagesCount;
            this.pages = new ListPage[pages];
            this.shift = (int)factor;
            this.mask = (0x1 << this.shift) - 1;
            this.count = 0;
            this.pageCount = 0;
        }

        public PagedList(PageFactor factor)
            : this(factor, PagedList<TItem>.InitialGrowPagesCount)
        {
        }

        public PagedList()
            : this(PageFactor.Page1024, PagedList<TItem>.InitialGrowPagesCount)
        {
        }

        public int PageCount
        {
            get { return this.pageCount; }
        }

        public int Count
        {
            get { return this.count; }
            set
            {
                int p = value >> this.shift;
                int i = p + 1;
                while (i < this.pageCount)
                    this.pages[i++].Data = null;
                if (p >= this.pageCount)
                {
                    if (p >= this.pages.Length)
                        this.Expand(p + 1);
                    i = this.pageCount;
                    while (i <= p)
                        this.pages[i++].Data = new TItem[0x1 << this.shift];
                }
                this.pageCount = p + 1;
                this.count = value;
            }
        }

        public void Clear()
        {
            for (int i = 0; i < this.pageCount; i++)
                this.pages[i].Data = null;
            this.pageCount = 0;
            this.count = 0;
        }

        public TItem[] ToArray()
        {
            TItem[] a = new TItem[this.count];
            for (int i = 0; i < this.count; i++)
                a[i] = this.pages[i >> this.shift].Data[i & this.mask];
            return a;
        }

        public TItem this[int i]
        {
            get
            {
                if ((i < 0) || (i >= this.count))
                    throw new IndexOutOfRangeException("Index is out of list bounds");
                return this.pages[i >> this.shift].Data[i & this.mask];
            }
            set
            {
                if ((i < 0) || (i >= this.count))
                    throw new IndexOutOfRangeException("Index is out of list bounds");
                this.pages[i >> this.shift].Data[i & this.mask] = value;
            }
        }

        public TItem[] Direct(int i, out int pos)
        {
            pos = i & this.mask;
            if ((i < 0) || (i >= this.count))
                throw new IndexOutOfRangeException("Index is out of list bounds");
            return this.pages[i >> this.shift].Data;
        }

        private void Expand(int size)
        {
            ListPage[] l = new ListPage[size];
            for (int i = 0; i < this.pageCount; i++)
                l[i] = this.pages[i];
            this.pages = null;
            this.pages = l;
            l = null;
        }

        public void Add(TItem item)
        {
            int p = this.count >> this.shift;
            if (p == this.pageCount)
            {
                if (this.pageCount >= this.pages.Length)
                    this.Expand(this.pages.Length + PagedList<TItem>.InitialGrowPagesCount);
                this.pages[this.pageCount++].Data = new TItem[0x1 << this.shift];
            }
            this.pages[p].Data[this.count & this.mask] = item;
            ++this.count;
        }

        private void Exchange(int i, int j)
        {
            TItem t = this[i];
            this[i] = this[j];
            this[j] = t;
        }

        public void Sort(Func<TItem, TItem, int> comparer)
        {
            if (this.count <= 1) return;
            this.Quicksort(0, this.count - 1, comparer);
        }

        public void SortReference(Func<TItem, TItem, int> comparer, int[] reference)
        {
            if (reference.Length <= 1) return;
            this.QuicksortRef(0, reference.Length - 1, comparer, reference);
        }

        private void Quicksort(int l, int r, Func<TItem, TItem, int> comparer)
        {
            if (r <= l) return;
            int i = l - 1, j = r, p = l - 1, q = r; TItem v = this[r];
            while (true)
            {
                while (comparer(this[++i], v) < 0) { }
                while (comparer(v, this[--j]) < 0) if (j == l) break;
                if (i >= j) break;
                this.Exchange(i, j);
                if (comparer(this[i], v) == 0) { p++; this.Exchange(p, i); }
                if (comparer(v, this[j]) == 0) { q--; this.Exchange(j, q); }
            }
            this.Exchange(i, r); j = i - 1; i = i + 1;
            for (int k = l; k < p; k++, j--) this.Exchange(k, j);
            for (int k = r - 1; k > q; k--, i++) this.Exchange(i, k);
            this.Quicksort(l, j, comparer);
            this.Quicksort(i, r, comparer);
        }

        private static void ExchangeRef(int i, int j, int[] reference)
        {
            int x = reference[i];
            reference[i] = reference[j];
            reference[j] = x;
        }

        private void QuicksortRef(int l, int r, Func<TItem, TItem, int> comparer, int[] reference)
        {
            if (r <= l) return;
            int i = l - 1, j = r, p = l - 1, q = r; 
            TItem v = this[reference[r]];
            while (true)
            {
                while (comparer(this[reference[++i]], v) < 0) { }
                while (comparer(v, this[reference[--j]]) < 0) if (j == l) break;
                if (i >= j) break;
                PagedList<TItem>.ExchangeRef(i, j, reference);
                if (comparer(this[reference[i]], v) == 0) { p++; PagedList<TItem>.ExchangeRef(p, i, reference); }
                if (comparer(v, this[reference[j]]) == 0) { q--; PagedList<TItem>.ExchangeRef(j, q, reference); }
            }
            PagedList<TItem>.ExchangeRef(i, r, reference); 
            j = i - 1; 
            i = i + 1;
            for (int k = l; k < p; k++, j--) PagedList<TItem>.ExchangeRef(k, j, reference);
            for (int k = r - 1; k > q; k--, i++) PagedList<TItem>.ExchangeRef(i, k, reference);
            this.QuicksortRef(l, j, comparer, reference);
            this.QuicksortRef(i, r, comparer, reference);
        }

        /*
        private void NonRecursiveQuickSort(int min, int max, Func<TItem, TItem, int> comparer)
        {
            int[] stack = new int[128];
            int top = -1;
            int n = max + 1;

            stack[++top] = min;  // Initialize stack
            stack[++top] = max;

            while (top > 0) // While there are unprocessed subarrays 
            {
                // Pop Stack
                int j = stack[top--];
                int i = stack[top--];

                // Findpivot
                int pivotindex = (i + j) >> 1;
                TItem pivot = this[pivotindex];

                // Stick pivot at end
                this[pivotindex] = this[j];
                this[j] = pivot;

                // Partition
                int l = i - 1;
                int r = j;
                do
                {
                    while (comparer(this[++l], pivot) < 0) { }
                    while ((r != 0) && (comparer(this[--r], pivot) > 0)) { }
                    this.Exchange(l, r);
                } while (l < r);

                // Undo final swap
                this.Exchange(l, r);

                // Put pivot value in place
                this.Exchange(l, j);

                // Put new subarrays onto Stack if they are small
                if ((l - i) > 10) // Left partition / 10 could be adjusted from 0 - ...
                {
                    stack[++top] = i;
                    stack[++top] = l - 1;
                }

                if ((j - l) > 10) // Right partition / 10 could be adjusted from 0 - ...
                {
                    stack[++top] = l + 1;
                    stack[++top] = j;
                }
            }

            for (int j = 1; j < n; ++j)
            {
                TItem temp = this[j];
                int i = j - 1;
                while (i >= 0 && (comparer(this[i], temp) > 0))
                    this[i + 1] = this[i--];
                this[i + 1] = temp;
            }
        }
        */

        struct ListPage
        {
            public TItem[] Data;
        }
    }

}