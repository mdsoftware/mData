using System;
using System.Collections.Generic;

namespace mData.DataStructures
{

    struct SortedInt2List
    {

        private ulong[] items;
        private int count;

        public SortedInt2List(int size)
        {
            this.count = 0;
            this.items = new ulong[size];
        }

        public void Remove(int key)
        {
            int p;
            if (this.Find(key, out p))
                this.RemoveAt(p);
        }

        public int Search(int key, int defaultValue)
        {
            int p;
            if (this.Find(key, out p))
                return SortedInt2List.Unpack(this.items[p]).Value;
            return defaultValue;
        }

        public int Count
        {
            get { return this.count; }
        }

        public bool Contains(int key)
        {
            int p;
            return this.Find(key, out p);
        }

        public int this[int key]
        {
            get
            {
                int p;
                if (!this.Find(key, out p))
                    throw new DataException("Key {0} 0x{1} not found", key, key.ToString("x8"));
                return SortedInt2List.Unpack(this.items[p]).Value;
            }
            set
            {
                int p;
                if (this.Find(key, out p))
                {
                    this.items[p] = SortedInt2List.Pack(key, value);
                }
                else
                {
                    if (this.count >= this.items.Length)
                        throw new DataException("Sorted Int2 list is full");
                    this.Insert(p, SortedInt2List.Pack(key, value));
                }
            }
        }

        public Int2ListEntry RemoveLast()
        {
            if (this.count == 0)
                throw new DataException("List is empty");
            --this.count;
            return SortedInt2List.Unpack(this.items[this.count]);
        }

        private unsafe void Insert(int p, ulong u)
        {
            int c = this.count - 1;
            fixed (ulong* p0 = this.items)
            {
                ulong* p1 = p0 + c;
                ulong* p2 = (p1++);
                while (c >= p)
                {
                    *(p1--) = *(p2--);
                    c--;
                }
                *p1 = u;
            }
            ++this.count;
        }

        private unsafe void RemoveAt(int p)
        {
            int c = p + 1;
            fixed (ulong* p0 = this.items)
            {
                ulong* p1 = p0 + c;
                ulong* p2 = (p1--);
                while (c < this.count)
                {
                    *(p1++) = *(p2++);
                    c++;
                }
            }
            --this.count;
        }

        private bool Find(int key, out int position)
        {
            position = 0;
            if (this.count == 0)
                return false;
            int min = 0;
            int max = this.count - 1;
            while (true)
            {
                int mid = (min + max) >> 1;
                int r = key.CompareTo((int)(this.items[mid] & 0xffffffff));
                if (r == 0)
                {
                    position = mid;
                    return true;
                }
                if (r > 0)
                {
                    min = mid + 1;
                }
                else
                {
                    max = mid - 1;
                }
                if (min > max) break;
            }
            position = min;
            return false;
        }

        public static unsafe ulong Pack(int key, int value)
        {
            ulong u;
            int* p = (int*)&u;
            *p = key;
            p++;
            *p = value;
            return u;
        }

        public static unsafe Int2ListEntry Unpack(ulong u)
        {
            int* p = (int*)&p;
            int k = *p;
            p++;
            int v = *p;
            return new Int2ListEntry(k, v);
        }

    }

    struct Int2ListEntry
    {
        public int Key;
        public int Value;

        public Int2ListEntry(int key, int value)
        {
            this.Key = key;
            this.Value = value;
        }
    }

}