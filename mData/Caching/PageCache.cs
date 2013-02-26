using System;
using System.Threading;
using mData.Storage;
using mData.Threading;
using mData.DataStructures;
using mData.Testing;
using mData.Utils;

namespace mData.Caching
{

    sealed class PageCache : IDataPageProvider
    {
        private IDataPageStorage file;
        private int pageSize;
        private object sync;
        private int count;
        private PageCacheEntry[] pages;
        private ArrayTree<int, int> locked;
        private ArrayTree<int, int> unlocked;
        private ArrayTree<int, int> unlockedModified;
        private int timeout;
        private DataPageProviderFlags flags;
        private CommonStatistics stats;
        
        public const int MinCacheSize = 0x80;
        public const int MaxCacheSize = 0x200000;

        PageCache(IDataPageStorage file, int cacheSize, int lockTimeout, DataPageProviderFlags flags)
        {
            this.file = file;
            this.pageSize = this.file.PageSize;
            int size = cacheSize / this.pageSize;
            if (size < PageCache.MinCacheSize) size = PageCache.MinCacheSize;
            if (size > PageCache.MaxCacheSize) size = PageCache.MaxCacheSize;
            this.pages = new PageCacheEntry[size];
            this.sync = new Object();
            this.count = 0;
            this.locked = new ArrayTree<int, int>(PageCache.Compare, -1, size);
            this.unlocked = new ArrayTree<int, int>(PageCache.Compare, -1, size);
            this.unlockedModified = new ArrayTree<int, int>(PageCache.Compare, -1, size);
            this.timeout = lockTimeout;
            this.flags = flags;
            this.stats = new CommonStatistics();
        }

        public string Uid
        {
            get { return this.file.Uid; }
        }

        public static IDataPageProvider Create(IDataPageStorage file, int cacheSize, int lockTimeout, DataPageProviderFlags flags)
        {
            return new PageCache(file, cacheSize, lockTimeout, flags);
        }

        public PageHeaderData GetPageHeader(byte[] page)
        {
            return this.file.GetPageHeader(page);
        }

        public void SetPageHeader(int pageNo, byte[] page, PageHeaderData header)
        {
            this.file.SetPageHeader(pageNo, page, header);
        }

        public int PageHeaderSize
        {
            get { return this.file.PageHeaderSize; }
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

        private static int Compare(int x, int y)
        {
            return x.CompareTo(y);
        }

        public int PageSize
        {
            get { return this.pageSize; }
        }

        public void Flush()
        {
            lock (this.sync)
            {
                
                int[] list = this.unlockedModified.AllKeys();
                if (list != null)
                {
                    for (int i = 0; i < list.Length; i++)
                    {
                        int k = list[i];
                        int p = this.unlockedModified[k];
                        if (this.pages[p].Modified)
                        {
                            this.file.WritePage(this.pages[p].PageNo, this.pages[p].Data);
                            ++this.stats.PagesWritten;
                            this.pages[p].Modified = false;
                        }
                        this.unlockedModified.Remove(k);
                        this.unlocked.Insert(k, p);
                    }
                }
            }
            this.file.Flush();
        }

        public int Allocate()
        {
            ProgressiveIdle idle = new ProgressiveIdle("CacheAllocate", this.timeout);
            int pageNo = this.file.NewPage();
            while (true)
            {
                bool ok = false;
                lock (this.sync)
                {
                    ok = this.TryAllocate(pageNo);
                }
                if (ok) return pageNo;
                if (!idle.Started) idle.Start();
                idle.Tick();
            }
        }

        public byte[] ReadLock(int pageNo)
        {
            ProgressiveIdle idle = new ProgressiveIdle("CacheReadLock", this.timeout);
            while (true)
            {
                byte[] b = null;
                lock (this.sync)
                {
                    b = this.TryReadLock(pageNo);
                }
                if (b != null)
                {
                    return b;
                }
                if (!idle.Started) idle.Start();
                idle.Tick();
            }
        }

        public byte[] WriteLock(int pageNo)
        {
            ProgressiveIdle idle = new ProgressiveIdle("CacheWriteLock", this.timeout);
            while (true)
            {
                byte[] b = null;
                lock (this.sync)
                {
                    b = this.TryWriteLock(pageNo);
                }
                if (b != null)
                {
                    return b;
                }
                if (!idle.Started) idle.Start();
                idle.Tick();
            }
        }

        public void ReadUnlock(int pageNo)
        {
            lock (this.sync)
            {
                this.TryReadUnlock(pageNo);
            }
        }

        public void WriteUnlock(int pageNo, bool markAsUpdated)
        {
            lock (this.sync)
            {
                this.TryWriteUnlock(pageNo, markAsUpdated);
            }
        }

        private bool TryAllocate(int pageNo)
        {
            int p = -1;
            if (this.count < this.pages.Length)
            {
                p = this.count++;

                this.pages[p] = new PageCacheEntry(pageNo, this.pageSize);
                this.file.Initialize(pageNo, this.pages[p].Data);
                this.pages[p].Modified = true;
                this.unlockedModified.Insert(pageNo, p);

                return true;
            }
            if (!this.unlocked.Empty)
            {
                TreeKeyValue<int, int> root = this.unlocked.Root;
                int pageToFree = root.Key;
                p = root.Value;
                this.unlocked.Remove(pageToFree);
            }
            else if (!this.unlockedModified.Empty)
            {
                TreeKeyValue<int, int> root = this.unlockedModified.Root;
                int pageToFree = root.Key;
                p = root.Value;
                this.unlockedModified.Remove(pageToFree);
            }

            if (p == -1) return false;

            if (pages[p].Modified == true)
                this.WritePage(p);

            this.pages[p].Reset(pageNo);
            this.file.Initialize(pageNo, this.pages[p].Data);
            this.pages[p].Modified = true;
            this.unlockedModified.Insert(pageNo, p);

            return true;
        }

        private PageCacheEntry Find(int pageNo)
        {
            for (int i = 0; i < this.count; i++)
            {
                if (this.pages[i].PageNo == pageNo)
                    return this.pages[i];
            }
            return new PageCacheEntry();
        }

        private int FindOrLoadPage(int pageNo)
        {
            int p;
            if (this.locked.TryGetValue(pageNo, out p))
            {
                ++this.stats.HitCount;
                return p;
            }
            if (this.unlocked.TryGetValue(pageNo, out p))
            {
                this.unlocked.Remove(pageNo);
                this.locked.Insert(pageNo, p);
                ++this.stats.HitCount;
                return p;
            }
            if (this.unlockedModified.TryGetValue(pageNo, out p))
            {
                this.unlockedModified.Remove(pageNo);
                this.locked.Insert(pageNo, p);
                ++this.stats.HitCount;
                return p;
            }
            p = -1;
            ++this.stats.MissCount;
            if (this.count < this.pages.Length)
            {
                p = this.count++;
                this.pages[p] = new PageCacheEntry(pageNo, this.pageSize);
                this.file.ReadPage(pageNo, this.pages[p].Data);
                this.locked.Insert(pageNo, p);
                ++this.stats.PagesRead;
            }
            else
            {
                if (!this.unlocked.Empty)
                {
                    TreeKeyValue<int,int> root = this.unlocked.Root;
                    int pageToFree = root.Key;
                    p = root.Value;
                    this.unlocked.Remove(pageToFree);
                }
                if (!this.unlockedModified.Empty)
                {
                    TreeKeyValue<int, int> root = this.unlockedModified.Root;
                    int pageToFree = root.Key;
                    p = root.Value;
                    this.unlockedModified.Remove(pageToFree);
                    if (this.pages[p].Modified)
                        this.WritePage(p);
                }
                if (p >= 0)
                {
                    ++this.stats.MissCount;
                    this.pages[p].Reset(pageNo);
                    this.file.ReadPage(pageNo, this.pages[p].Data);
                    this.locked.Insert(pageNo, p);
                    ++this.stats.PagesRead;
                }
            }
            return p;
        }

        private byte[] TryReadLock(int pageNo)
        {
            int p = this.FindOrLoadPage(pageNo);
            if (p == -1) return null;
            if (this.pages[p].TryReadLock())
                return this.pages[p].Data;
            return null;
        }

        private byte[] TryWriteLock(int pageNo)
        {

            int p = this.FindOrLoadPage(pageNo);
            if (p == -1) return null;
            if (this.pages[p].TryWriteLock())
                return this.pages[p].Data;
            return null;
        }

        private void TryReadUnlock(int pageNo)
        {
            int p;
            if (!this.locked.TryGetValue(pageNo, out p))
                throw new DataException("Page #{0} is not locked", pageNo);
            if (this.pages[p].ReadUnlock())
            {
                this.locked.Remove(pageNo);
                if (this.pages[p].Modified)
                {
                    this.unlockedModified.Insert(pageNo, p);
                }
                else
                {
                    this.unlocked.Insert(pageNo, p);
                }
            }
        }

        private void WritePage(int p)
        {
            this.file.WritePage(this.pages[p].PageNo, this.pages[p].Data);
            ++this.stats.PagesWritten;
            if ((this.flags & DataPageProviderFlags.FlushWrites) != 0)
                this.file.Flush();
            this.pages[p].Modified = false;
        }

        private void TryWriteUnlock(int pageNo, bool markAsModified)
        {
            int p;
            if (!this.locked.TryGetValue(pageNo, out p))
                throw new DataException("Page #{0} is not locked", pageNo);
            if (this.pages[p].WriteUnlock(markAsModified))
            {
                this.locked.Remove(pageNo);
                if (this.pages[p].Modified && ((this.flags & DataPageProviderFlags.WriteThrough) != 0))
                {
                    this.WritePage(p);
                }
                if (this.pages[p].Modified)
                {
                    this.unlockedModified.Insert(pageNo, p);
                }
                else
                {
                    this.unlocked.Insert(pageNo, p);
                }
            }
        }

        public void Dispose()
        {
            if (this.file != null)
            {
                this.Flush();
                this.file.Flush();
                this.file.Dispose();
                this.file = null;
            }
        }

    }

    enum PageEntryLocation : byte
    {
        Undefined = 0,
        Locked,
        Unlocked,
        UnlockedModified,
        New
    }

    struct PageCacheEntry
    {
        public int PageNo;
        private int lockCount;
        private ReadWriteLockState lockState;
        public bool Modified;
        public byte[] Data;

        public PageCacheEntry(int pageNo, int pageSize)
        {
            this.PageNo = pageNo;
            this.lockCount = 0;
            this.lockState = ReadWriteLockState.ReadLock;
            this.Modified = false;
            this.Data = new byte[pageSize];
        }

        public void Reset(int pageNo)
        {
            this.PageNo = pageNo;
            this.lockCount = 0;
            this.lockState = ReadWriteLockState.ReadLock;
            this.Modified = false;
        }

        public bool Locked
        {
            get
            {
                if (this.lockState != ReadWriteLockState.ReadLock)
                    return true;
                return (this.lockCount > 0);
            }
        }

        public bool TryReadLock()
        {
            if (this.lockState != ReadWriteLockState.ReadLock)
                return false;
            ++this.lockCount;
            return true;
        }

        public bool TryWriteLock()
        {
            switch (this.lockState)
            {
                case ReadWriteLockState.ReadLock:
                    this.lockState = ReadWriteLockState.WriteLockRequest;
                    break;

                case ReadWriteLockState.WriteLockRequest:
                    if (this.lockCount == 0)
                    {
                        this.lockState = ReadWriteLockState.WriteLock;
                        return true;
                    }
                    break;

            }
            return false;
        }

        public bool ReadUnlock()
        {
            if (this.lockState == ReadWriteLockState.WriteLock)
                throw new DataException("Invalid mode while read unlock");
            if (this.lockCount == 0)
                throw new DataException("Lock count undeflow while read unlock");
            --this.lockCount;
            return (this.lockCount == 0);
        }

        public bool WriteUnlock(bool markAsModified)
        {
            if (this.lockState != ReadWriteLockState.WriteLock)
                throw new DataException("Invalid mode while write unlock");
            this.lockState = ReadWriteLockState.ReadLock;
            if (this.Modified)
                return true;
            this.Modified = markAsModified;
            return true;
        }
    }

}