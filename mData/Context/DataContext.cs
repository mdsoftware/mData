using System;
using System.Threading;
using mData.Storage;
using mData.Testing;
using mData.DataStructures;
using mData.Services;
using mData.Value;
using mData.Threading;
using mData.Serialization;
using mData.Caching;
using mData.Utils;

namespace mData.Context
{

    sealed class DataContext : IDataContext
    {
        private IDataPageProvider provider;
        private DataContextControlInfo ctrl;
        private string description;
        private PageTreeIndex<long, ulong> index;
        private DataSymbols symbols;
        private CommonStatistics stats;
        private ArrayTree<string, ReadWriteLock> locks;
        private ReadWriteLock[] idLocks;
        private long idLockMask;
        private object sync;

        public const long ControlSignature = 0x6c72744361746144;
        public const ulong FreePageSignature = 0x6567615065657246;
        public const long ObjectSignature = 0x7463656a624f6244;

        private const int RootPage = 0;

        DataContext(IDataPageProvider provider)
        {
            this.provider = provider;
            this.description = null;
            this.ctrl = new DataContextControlInfo();
            this.index = null;
            this.stats = new CommonStatistics();
            this.sync = new Object();
            this.locks = new ArrayTree<string, ReadWriteLock>(DataContext.Compare, null, 8192, PageFactor.Page256);

            this.idLockMask = (0x1L << (int)PageFactor.Page256) - 1;
            this.idLocks = new ReadWriteLock[this.idLockMask + 1];
            for (int i = 0; i < this.idLocks.Length; i++)
                this.idLocks[i] = new ReadWriteLock(String.Format("idlockhash_{0}", i), 0);

        }

        public void Dispose()
        {
            lock (this.sync)
            {
                if (this.provider != null)
                {
                    this.provider.Dispose();
                    this.provider = null;
                }
            }
        }

        public void ReadLock(long id, int timeout)
        {
            this.idLocks[(int)(id & this.idLockMask)].ReadLock(timeout);
        }

        public void ReadUnlock(long id)
        {
            this.idLocks[(int)(id & this.idLockMask)].ReadUnlock();
        }

        public void WriteLock(long id, int timeout)
        {
            this.idLocks[(int)(id & this.idLockMask)].WriteLock(timeout);
        }

        public void WriteUnlock(long id, bool readLock)
        {
            this.idLocks[(int)(id & this.idLockMask)].WriteUnlock(readLock);
        }

        public void ReadLock(string semaphore, int timeout)
        {
            this.GetLock(semaphore).ReadLock(timeout);
        }

        public void ReadUnlock(string semaphore)
        {
            this.GetLock(semaphore).ReadUnlock();
        }

        public void WriteLock(string semaphore, int timeout)
        {
            this.GetLock(semaphore).WriteLock(timeout);
        }

        public void WriteUnlock(string semaphore, bool readLock)
        {
            this.GetLock(semaphore).WriteUnlock(readLock);
        }

        private ReadWriteLock GetLock(string name)
        {
            Monitor.Enter(this.sync);
            ReadWriteLock l;
            if (!this.locks.TryGetValue(name, out l))
            {
                l = new ReadWriteLock(name, 0);
                this.locks.Insert(name, l);
            }
            Monitor.Exit(this.sync);
            return l;
        }

        public CommonStatistics Statistics
        {
            get
            {
                Monitor.Enter(this.sync);
                CommonStatistics s = this.stats;
                Monitor.Exit(this.sync);
                return s;
            }
        }

        private void UpdateStats(int read, int written)
        {
            int psize = this.provider.PageSize;
            Monitor.Enter(this.sync);
            this.stats.PagesRead += read;
            this.stats.PagesWritten += written;
            this.stats.BytesRead += (read * psize);
            this.stats.BytesWritten += (written * psize);
            Monitor.Exit(this.sync);
        }

        private static int Compare(string x, string y)
        {
            return String.Compare(x, y, true);
        }

        public IDataPageProvider PageProvider
        {
            get { return this.provider; }
        }

        public string Description
        {
            get { return this.description; }
        }

        public string Uid
        {
            get { return this.provider.Uid; }
        }

        public static DataContext Create(IDataPageProvider provider, int lockTimeout, string description)
        {
            int root = provider.Allocate();
            if (root != DataContext.RootPage)
                throw new DataException("Page storage must be empty to create data context");


            DataContextControlInfo ctrl = new DataContextControlInfo();
            ctrl.Signature = DataContext.ControlSignature;
            ctrl.LastId = 1000;
            ctrl.FreePage = 0;
            PageTreeIndex<long, ulong> index = DataIndexing.Create(provider, lockTimeout);
            ctrl.IndexPage = index.RootPage;
            DataSymbols sym = DataSymbols.Create(provider, lockTimeout);
            sym.Get("Class");
            sym.Get("Version");
            sym.Get("Id");
            ctrl.SymbolPage = sym.RootPage;
            ctrl.RootPage = 0;

            byte[] page = provider.WriteLock(root);
            ctrl.Put(page, provider.PageHeaderSize, provider.PageSize, description);
            provider.WriteUnlock(root, true);
            provider.Flush();

            return DataContext.Open(provider, lockTimeout);
        }

        public static DataContext Open(IDataPageProvider provider, int lockTimeout)
        {
            DataContext ctx = new DataContext(provider);

            byte[] page = provider.ReadLock(DataContext.RootPage);
            ctx.ctrl = DataContextControlInfo.Get(page, provider.PageHeaderSize, out ctx.description);
            provider.ReadUnlock(DataContext.RootPage);

            if (ctx.ctrl.Signature != DataContext.ControlSignature)
                throw new DataException("Invalid context control signature");

            ctx.index = DataIndexing.Open(ctx.ctrl.IndexPage, provider, lockTimeout);
            ctx.symbols = DataSymbols.Open(ctx.ctrl.SymbolPage, provider, lockTimeout);

            return ctx;
        }

        private IndexEntry Seek(long id)
        {
            ulong l;
            if (this.index.TryGetValue(id, out l))
                return new IndexEntry(l);
            return new IndexEntry(0);
        }

        private void UpdateIndex(long id, IndexEntry entry)
        {
            this.index.Insert(id, entry.Pack());
        }

        private long NextId()
        {
            long id = 0;
            byte[] root = this.provider.WriteLock(DataContext.RootPage);
            try
            {
                id = this.ctrl.LastId++;
                this.ctrl.Put(root, this.provider.PageHeaderSize, this.provider.PageSize, null);
            }
            finally
            {
                this.provider.WriteUnlock(DataContext.RootPage, true);
            }
            return id;
        }

        public DataValue GetRoot()
        {
            DataValue v = DataValue.Null;
            lock (this.sync)
            {
                if (this.ctrl.RootPage != 0)
                    v = this.DeserializeValue(this.ctrl.RootPage);
            }
            return v;
        }

        public DataValue Get(long id)
        {
            IndexEntry e = this.Seek(id);
            if (e.Empty)
                return DataValue.Null;
            return this.DeserializeValue(e.Page);
        }

        public long Add(DataValue value)
        {
            long id = this.NextId();
            IndexEntry e = new IndexEntry();
            e.Version = 1;
            e.Page = this.SerializeValue(0, id, value);
            this.UpdateIndex(id, e);
            value.Id = id;
            return id;
        }

        public int Update(DataValue value)
        {
            long id = value.Id;
            IndexEntry e = this.Seek(id);
            if (e.Empty)
                return -1;
            this.SerializeValue(e.Page, id, value);
            e = this.Seek(id);
            e.Version++;
            this.UpdateIndex(id, e);
            return e.Version;
        }

        public int GetVersion(long id)
        {
            ulong l;
            if (this.index.TryGetValue(id, out l))
            {
                return new IndexEntry(l).Version;
            }
            return -1;
        }

        public bool Delete(long id)
        {
            ulong l;
            if (!this.index.TryGetValue(id, out l))
                return false;
            IndexEntry e = new IndexEntry(l);
            this.index.Remove(id);

            int p = e.Page;
            int firstLocked = 0;
            int cnt = 0;

            while (p > 0)
            {
                PageHeaderData h;
                if (cnt == 0)
                {
                    h = this.Free(p, true);
                    firstLocked = p;
                }
                else
                {
                    h = this.Free(p, false);
                }
                ++cnt;
                p = h.NextPage;
            }
            if (firstLocked > 0)
                this.provider.WriteUnlock(firstLocked, true);

            return true;
        }

        public void UpdateRoot(DataValue value)
        {
            lock (this.sync)
            {
                int p = this.SerializeValue(this.ctrl.RootPage, 0, value);
                if (p != this.ctrl.RootPage)
                {
                    byte[] buf = this.provider.WriteLock(DataContext.RootPage);
                    this.ctrl.RootPage = p;
                    this.ctrl.Put(buf, this.provider.PageHeaderSize, this.provider.PageSize, null);
                    this.provider.WriteUnlock(DataContext.RootPage, true);
                }
            }
        }

        private int SerializeValue(int startPage, long id, DataValue value)
        {

            if (value.Type != DataValueType.Record)
                throw new ArgumentException("Record type expected");
            int p = 0;
            DataContextWriteStream ws = null;
            try
            {
                ws = new DataContextWriteStream(this, startPage);
                DataWriteStream stream = new DataWriteStream(ws);
                stream.Write(DataContext.ObjectSignature);
                stream.Write(id);
                if (value.Class == null)
                {
                    stream.Write((int)0);
                }
                else
                {
                    stream.Write(this.symbols.Get(value.Class));
                }
                value.Serialize(stream, this.symbols);
                stream.Flush();
                stream.Dispose();
                p = ws.StartPage;
                this.UpdateStats(0, ws.Count);
                ws.Dispose();
                ws = null;
            }
            finally
            {
                if (ws != null) ws.Dispose();
            }
            return p;
        }

        private DataValue DeserializeValue(int startPage)
        {
            DataValue v = null;
            DataContextReadStream rs = null;
            try
            {
                rs = new DataContextReadStream(this, startPage);
                DataReadStream stream = new DataReadStream(rs);
                if (stream.ReadLong() != DataContext.ObjectSignature)
                    throw new DataException("Invalid object signature");
                long id = stream.ReadLong();
                int sym = stream.ReadInt();
                string className = null;
                if (sym != 0)
                    className = this.symbols.Get(sym);
                v = DataValue.Null;
                v.Deserialize(stream, this.symbols);
                stream.Dispose();
                this.UpdateStats(rs.Count, 0);
                rs.Dispose();
                rs = null;
                v.Id = id;
                v.Class = className;
            }
            finally
            {
                if (rs != null) rs.Dispose();
            }
            return v;
        }

        public int Allocate()
        {
            int page = -1;
            bool updated = false;
            byte[] root = this.provider.WriteLock(DataContext.RootPage);
            try
            {
                if (this.ctrl.FreePage == 0)
                {
                    page = this.provider.Allocate();
                }
                else
                {
                    page = this.ctrl.FreePage;

                    byte[] p = this.provider.ReadLock(page);
                    PageHeaderData h = this.provider.GetPageHeader(p);
                    this.provider.ReadUnlock(page);

                    if (h.Data0 != DataContext.FreePageSignature)
                        throw new DataException("Invalid storage free page signature");
                    this.ctrl.FreePage = h.PrevPage;

                    this.ctrl.Put(root, this.provider.PageHeaderSize, this.provider.PageSize, null);

                    updated = true;
                }
            }
            finally
            {
                this.provider.WriteUnlock(DataContext.RootPage, updated);
            }
            return page;
        }

        public PageHeaderData Free(int page, bool keepLocked)
        {
            PageHeaderData r;
            byte[] root = this.provider.WriteLock(DataContext.RootPage);
            try
            {
                byte[] p = this.provider.WriteLock(page);
                PageHeaderData h = this.provider.GetPageHeader(p);
                r = h;
                h.Data0 = DataContext.FreePageSignature;
                h.PrevPage = this.ctrl.FreePage;
                this.provider.SetPageHeader(page, p, h);

                if (!keepLocked)
                    this.provider.WriteUnlock(page, true);

                this.ctrl.FreePage = page;
                this.ctrl.Put(root, this.provider.PageHeaderSize, this.provider.PageSize, null);

            }
            finally
            {
                this.provider.WriteUnlock(DataContext.RootPage, true);
            }
            return r;
        }
    }

    struct DataContextControlInfo
    {
        public long Signature;
        public int FreePage;
        public int IndexPage;
        public int SymbolPage;
        public int RootPage;
        public long LastId;

        public static unsafe DataContextControlInfo Get(byte[] page, int headerSize, out string text)
        {
            text = null;
            char[] buf = null;
            DataContextControlInfo c = new DataContextControlInfo();

            fixed (byte* p = page)
            {
                byte* pp = p + headerSize;
                c.Signature = *(long*)pp;
                pp += 8;
                c.FreePage = *(int*)pp;
                pp += 4;
                c.IndexPage = *(int*)pp;
                pp += 4;
                c.SymbolPage = *(int*)pp;
                pp += 4;
                c.RootPage = *(int*)pp;
                pp += 4;
                c.LastId = *(long*)pp;
                pp += 8;
                int l = *(int*)pp;
                pp += 4;
                if (l > 0)
                {
                    buf = new char[l];
                    fixed (char* pc = buf)
                    {
                        char* ppp = (char*)pp;
                        char* ppc = pc;
                        for (int i = 0; i < l; i++)
                            *(ppc++) = *(ppp++);
                    }
                }
            }
            if (buf != null)
                text = new String(buf, 0, buf.Length);
            return c;
        }


        public unsafe void Put(byte[] page, int headerSize, int pageSize, string text)
        {
            int l = -1;
            if (text != null)
            {
                int size = (pageSize - headerSize - 64) >> 1;
                l = text.Length;
                if (l > size) l = size;
            }
            fixed (byte* p = page)
            {
                byte* pp = p + headerSize;
                *(long*)pp = this.Signature;
                pp += 8;
                *(int*)pp = this.FreePage;
                pp += 4;
                *(int*)pp = this.IndexPage;
                pp += 4;
                *(int*)pp = this.SymbolPage;
                pp += 4;
                *(int*)pp = this.RootPage;
                pp += 4;
                *(long*)pp = this.LastId;
                pp += 8;
                if (l >= 0)
                {
                    *(int*)pp = l;
                    pp += 4;
                    if (l > 0)
                    {
                        char* pc = (char*)pp;
                        fixed (char* ps = text)
                        {
                            char* pps = ps;
                            for (int i = 0; i < l; i++)
                                *(pc++) = *(pps++);
                        }
                    }
                }
            }
        }

    }

    struct IndexEntry
    {
        public int Page;
        public int Version;

        public unsafe IndexEntry(ulong l)
        {
            int* p = (int*)&l;
            this.Page = *(p++);
            this.Version = *p;
        }

        public bool Empty
        {
            get { return (this.Page == 0) && (this.Version == 0); }
        }

        public unsafe ulong Pack()
        {
            ulong l = 0;
            int* p = (int*)&l;
            *(p++) = this.Page;
            *p = this.Version;
            return l;
        }
    }

}