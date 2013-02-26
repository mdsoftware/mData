using System;
using System.Collections.Generic;
using mData.DataStructures;
using mData.Storage;
using mData.Threading;
using mData.Testing;

namespace mData.Services
{

    static class DataIndexing
    {
        public static PageTreeIndex<long, ulong> Create(IDataPageProvider provider, int lockTimeout)
        {
            return PageTreeIndex<long, ulong>.Create(provider, new DataIndexingSupport(), lockTimeout);
        }

        public static PageTreeIndex<long, ulong> Open(int rootPage, IDataPageProvider provider, int lockTimeout)
        {
            return PageTreeIndex<long, ulong>.Open(rootPage, provider, new DataIndexingSupport(), lockTimeout);
        }

    }

    sealed class DataIndexingSupport : IPageIndexItemSupport<long, ulong>
    {

        public DataIndexingSupport()
        {
        }

        public int ItemSize { get { return 32; } }

        public unsafe long GetKey(byte[] page, int offset)
        {
            long x = 0;
            fixed (byte* p = page)
            {
                x = *(long*)(p + offset + IndexItemHeader.Size);
            }
            return x;
        }

        public unsafe ulong GetValue(byte[] page, int offset)
        {
            ulong x = 0;
            fixed (byte* p = page)
            {
                x = *(ulong*)(p + offset + IndexItemHeader.Size + 8);
            }
            return x;
        }

        public unsafe IndexItemHeader GetHeader(byte[] page, int offset)
        {
            IndexItemHeader h = new IndexItemHeader();
            fixed (byte* p = page)
            {
                byte* pp = (p + offset);
                h.Flags = *(uint*)pp;
                pp += 4;
                h.Left.Page = *(int*)pp;
                pp += 4;
                h.Left.Item = *(short*)pp;
                pp += 2;
                h.Right.Page = *(int*)pp;
                pp += 4;
                h.Right.Item = *(short*)pp;
            }
            return h;
        }

        public unsafe void Update(byte[] page, int offset, IndexItemHeader hdr)
        {
            fixed (byte* p = page)
            {
                DataIndexingSupport.PutHeader(p + offset, hdr);
            }
        }

        private static unsafe byte* PutHeader(byte* pp, IndexItemHeader hdr)
        {
            *(uint*)pp = hdr.Flags;
            pp += 4;
            *(int*)pp = hdr.Left.Page;
            pp += 4;
            *(short*)pp = hdr.Left.Item;
            pp += 2;
            *(int*)pp = hdr.Right.Page;
            pp += 4;
            *(short*)pp = hdr.Right.Item;
            pp += 2;
            return pp;
        }

        public unsafe void Update(byte[] page, int offset, IndexItemHeader hdr, long key, ulong value)
        {
            fixed (byte* p = page)
            {
                byte* pp = DataIndexingSupport.PutHeader(p + offset, hdr);
                *(long*)pp = key;
                pp += 8;
                *(ulong*)pp = value;
            }
        }

        public int CompareKeys(long x, long y)
        {
            return x.CompareTo(y);
        }

        public ulong EmptyValue { get { return 0; } }

        public long EmptyKey { get { return 0; } }

    }

}