using System;
using System.Collections.Generic;
using mData.DataStructures;
using mData.Storage;
using mData.Threading;
using mData.Serialization;
using mData.Expressions;

namespace mData.Services
{

    sealed class DataSymbols : ISymbolProvider
    {
        private IDataPageProvider provider;
        private int lastPage;
        private int lastId;
        private ArrayTree<int, string> idIndex;
        private ArrayTree<string, int> symbolIndex;
        private ReadWriteLock sync;
        private int maxSymPerPage;
        private SymbolItem[] lastPageSymbols;
        private int lastPageCount;
        private int pageSize;
        private int rootPage;

        public const int MaxSymbolSize = 128;

        public const ulong PageSignature = 0x6567615078626d53;

        DataSymbols(IDataPageProvider provider, int timeout)
        {
            this.provider = provider;
            this.sync = new ReadWriteLock("SymbolLock", timeout);
            this.lastPage = -1;
            this.lastId = 0;
            this.idIndex = new ArrayTree<int, string>(DataSymbols.Compare, null, 2048, PageFactor.Page512);
            this.symbolIndex = new ArrayTree<string, int>(DataSymbols.Compare, 0, 2048, PageFactor.Page512);
            this.pageSize = this.provider.PageSize - this.provider.PageHeaderSize - 16;
            this.maxSymPerPage = this.pageSize / 16;
            this.lastPageSymbols = new SymbolItem[this.maxSymPerPage];
            this.lastPageCount = 0;
            this.rootPage = -1;
        }

        public static DataSymbols Create(IDataPageProvider provider, int lockTimeout)
        {
            DataSymbols sym = new DataSymbols(provider, lockTimeout);
            sym.lastPage = sym.AllocatePage(-1);
            sym.rootPage = sym.lastPage;
            sym.lastId = 1000;
            return sym;
        }

        public static DataSymbols Open(int rootPage, IDataPageProvider provider, int lockTimeout)
        {
            DataSymbols sym = new DataSymbols(provider, lockTimeout);
            sym.rootPage = rootPage;
            int p = sym.rootPage;
            while (p != -1)
                p = sym.ReadPage(p);
            return sym;
        }

        public int RootPage
        {
            get { return this.rootPage; }
        }

        public string Get(int id)
        {
            string s = null;
            this.sync.ReadLock();
            try
            {
                if (!this.idIndex.TryGetValue(id, out s))
                    s = null;
            }
            finally
            {
                this.sync.ReadUnlock();
            }
            return s;
        }

        public int Get(string symbol)
        {
            int id = -1;
            this.sync.ReadLock();
            try
            {
                if (!this.symbolIndex.TryGetValue(symbol, out id))
                    id = -1;
            }
            finally
            {
                this.sync.ReadUnlock();
            }
            if (id != -1) return id;
            if (!Expression.IsSymbol(symbol))
                throw new ArgumentException(String.Format("Invalid symbol format '{0}'"));
            this.sync.WriteLock();
            try
            {
                if (!this.symbolIndex.TryGetValue(symbol, out id))
                {
                    SymbolItem s = new SymbolItem(this.lastId++, symbol);
                    this.symbolIndex.Insert(s.Symbol, s.Id);
                    this.idIndex.Insert(s.Id, s.Symbol);
                    if (!this.FitsLastPage(symbol))
                    {
                        int p = this.AllocatePage(this.lastPage);
                        this.SaveLastPage();
                        this.lastPage = p;
                        this.lastPageCount = 0;
                    }
                    this.lastPageSymbols[this.lastPageCount++] = s;
                    this.SaveLastPage();
                    id = s.Id;
                }
            }
            finally
            {
                this.sync.WriteUnlock(false);
            }
            return id;
        }

        private void SaveLastPage()
        {
            byte[] page = this.provider.WriteLock(this.lastPage);
            DataSymbols.Put(page, this.provider.PageHeaderSize, this.lastPageSymbols, this.lastPageCount);
            this.provider.WriteUnlock(this.lastPage, true);
        }

        private int ReadPage(int p)
        {
            byte[] page = provider.ReadLock(p);
            PageHeaderData h = provider.GetPageHeader(page);
            if (h.Data0 != DataSymbols.PageSignature)
            {
                provider.ReadUnlock(p);
                throw new DataException("Invalid symbol page signature");
            }
            int ofs = provider.PageHeaderSize;
            this.lastPageCount = 0;
            SymbolItem s = new SymbolItem();
            while (true)
            {
                ofs += DataSymbols.Get(page, ofs, ref s);
                if (s.Symbol == null)
                    break;
                if (s.Id > this.lastId) this.lastId = s.Id;
                this.idIndex.Insert(s.Id, s.Symbol);
                this.symbolIndex.Insert(s.Symbol, s.Id);
                this.lastPageSymbols[this.lastPageCount++] = s;
                if (this.lastPageCount >= this.maxSymPerPage)
                    throw new DataException("Last page symbol buffer overflow");
            }
            this.lastPage = p;
            provider.ReadUnlock(p);
            return h.NextPage;
        }

        private static int Compare(int x, int y)
        {
            return x.CompareTo(y);
        }

        private static int Compare(string x, string y)
        {
            return String.Compare(x, y, true);
        }

        private bool FitsLastPage(string s)
        {
            if (this.lastPageCount >= this.lastPageSymbols.Length)
                return false;
            int size = 4;
            for (int i = 0; i < this.lastPageCount; i++)
                size += (8 + (this.lastPageSymbols[i].Symbol.Length << 1));
            size += (8 + (s.Length << 1));
            return (size <= this.pageSize);
        }

        private int AllocatePage(int prevPage)
        {
            int p = this.provider.Allocate();
            byte[] page = this.provider.WriteLock(p);
            PageHeaderData h = this.provider.GetPageHeader(page);
            h.Data0 = DataSymbols.PageSignature;
            h.NextPage = -1;
            this.provider.SetPageHeader(p, page, h);
            DataSymbols.Put(page, this.provider.PageHeaderSize, null, 0);
            this.provider.WriteUnlock(p, true);
            if (prevPage != -1)
            {
                page = this.provider.WriteLock(prevPage);
                h = this.provider.GetPageHeader(page);
                h.NextPage = p;
                this.provider.SetPageHeader(prevPage, page, h);
                this.provider.WriteUnlock(prevPage, true);
            }
            return p;
        }

        private static unsafe void Put(byte[] page, int offset, SymbolItem[] items, int count)
        {
            fixed (byte* pb = page)
            {
                byte* ppb = pb + offset;
                if (count > 0)
                {
                    for (int i = 0; i < count; i++)
                    {
                        *(int*)ppb = items[i].Id;
                        ppb += 4;
                        int l = items[i].Symbol.Length;
                        *(int*)ppb = l;
                        ppb += 4;
                        fixed (char* pc = items[i].Symbol)
                        {
                            char* ppc = pc;
                            for (int j = 0; j < l; j++)
                            {
                                *((char*)ppb) = *(ppc++);
                                ppb += 2;
                            }
                        }

                    }
                }
                *(int*)ppb = -1;
            }
        }

        private static unsafe int Get(byte[] page, int offset, ref SymbolItem item)
        {
            int size = 0;
            fixed (byte* pb = page)
            {
                byte* ppb = pb + offset;
                int x = *(int*)ppb;
                ppb += 4;
                size += 4;
                if (x == -1)
                {
                    item.Id = 0;
                    item.Symbol = null;
                }
                else
                {
                    item.Id = x;
                    x = *(int*)ppb;
                    size += 4;
                    ppb += 4;
                    char[] c = new char[DataSymbols.MaxSymbolSize];
                    for (int i = 0; i < x; i++)
                    {
                        c[i] = *(char*)ppb;
                        ppb += 2;
                        size += 2;
                    }
                    item.Symbol = new String(c, 0, x);
                }
            }
            return size;
        }
    }

    struct SymbolItem
    {
        public int Id;
        public string Symbol;

        public SymbolItem(int id, string symbol)
        {
            this.Id = id;
            this.Symbol = symbol;
        }

        public override string ToString()
        {
            return String.Format("#{0} '{1}'", this.Id, this.Symbol);
        }
    }

}