using System;
using mData.Storage;
using mData.DataStructures;
using mData.Serialization;

namespace mData.Context
{

    sealed class DataContextReadStream : IPageReadStream, IDisposable
    {
        private DataContext context;
        private int startPage;
        private int locked;
        private int nextPage;
        private int count;

        public DataContextReadStream(DataContext context, int startPage)
        {
            this.context = context;
            this.startPage = startPage;
            this.locked = 0;
            this.nextPage = 0;
            this.count = 0;
        }

        public int PageSize
        {
            get { return this.context.PageProvider.PageSize; }
        }

        public int PageHeaderSize
        {
            get { return this.context.PageProvider.PageHeaderSize; }
        }

        public void Read(byte[] page)
        {
            if (this.count == 0)
            {
                byte[] buf = this.context.PageProvider.ReadLock(this.startPage);
                this.locked = this.startPage;
                PageHeaderData h = this.context.PageProvider.GetPageHeader(buf);
                if (h.Data0 != DataContextWriteStream.FirstStreamPageSignature)
                {
                    this.context.PageProvider.ReadUnlock(this.startPage);
                    throw new DataException("Invalid stream data page signature");
                }
                this.nextPage = h.NextPage;
                DataContextWriteStream.Copy(buf, page, this.context.PageProvider);
            }
            else
            {
                int p = this.nextPage;
                if (p == 0)
                    throw new DataException("Unexpected end of stream");
                byte[] buf = this.context.PageProvider.ReadLock(p);
                PageHeaderData h = this.context.PageProvider.GetPageHeader(buf);
                if (h.Data0 != DataContextWriteStream.NextStreamPageSignature)
                {
                    this.context.PageProvider.ReadUnlock(p);
                    throw new DataException("Invalid stream data page signature");
                }
                this.nextPage = h.NextPage;
                DataContextWriteStream.Copy(buf, page, this.context.PageProvider);
                this.context.PageProvider.ReadUnlock(p);
            }
            ++this.count;
        }

        public int Count
        {
            get { return this.count; }
        }

        public void Dispose()
        {
            if (this.locked > 0)
            {
                this.context.PageProvider.ReadUnlock(this.locked);
                this.locked = 0;
            }
            this.count = 0;
        }
    }

    sealed class DataContextWriteStream : IPageWriteStream, IDisposable
    {

        private DataContext context;
        private int count;
        private int startPage;
        private int prevPage;
        private int nextPage;
        private int locked;
        private byte[] pageLocked;

        public const ulong FirstStreamPageSignature = 0x30656761506a624f;
        public const ulong NextStreamPageSignature = 0x31656761506a624f;

        public DataContextWriteStream(DataContext context, int startPage)
        {
            this.context = context;
            this.startPage = startPage;
            this.count = 0;
            this.prevPage = 0;
            this.nextPage = 0;
            this.locked = 0;
            this.pageLocked = null;
        }

        public int PageSize
        {
            get { return this.context.PageProvider.PageSize; }
        }

        public int PageHeaderSize
        {
            get { return this.context.PageProvider.PageHeaderSize; }
        }

        public void Flush()
        {
        }

        public int StartPage
        {
            get { return this.startPage; }
        }

        public int Count
        {
            get { return this.count; }
        }

        private byte[] LockPage(int pageToLock, int prevPage, out int lockedPage, ulong signature)
        {
            lockedPage = 0;
            byte[] buf = null;
            if (pageToLock == 0)
            {
                int p = this.context.Allocate();
                buf = this.context.PageProvider.WriteLock(p);
                lockedPage = p;
                PageHeaderData h = new PageHeaderData();
                h.Data0 = signature;
                h.PrevPage = prevPage;
                h.NextPage = 0;
                this.context.PageProvider.SetPageHeader(p, buf, h);
            }
            else
            {
                buf = this.context.PageProvider.WriteLock(pageToLock);
                lockedPage = pageToLock;
                PageHeaderData h = this.context.PageProvider.GetPageHeader(buf);
                if (h.Data0 != signature)
                {
                    this.context.PageProvider.WriteUnlock(pageToLock, false);
                    throw new DataException("Invalid stream data page signature");
                }
                this.nextPage = h.NextPage;
                h.PrevPage = prevPage;
                h.NextPage = 0;
                this.context.PageProvider.SetPageHeader(pageToLock, buf, h);
            }
            return buf;
        }

        public void Write(byte[] page)
        {
            if (this.count == 0)
            {
                this.pageLocked = this.LockPage(this.startPage, 0, out this.startPage, DataContextWriteStream.FirstStreamPageSignature);
                DataContextWriteStream.Copy(page, this.pageLocked, this.context.PageProvider);
                this.locked = this.startPage;
                this.prevPage = this.locked;
            }
            else
            {
                int p;
                byte[] buf = this.LockPage(this.nextPage, this.prevPage, out p, DataContextWriteStream.NextStreamPageSignature);
                DataContextWriteStream.Copy(page, buf, this.context.PageProvider);
                this.context.PageProvider.WriteUnlock(p, true);
                if (this.prevPage == this.locked)
                {
                    PageHeaderData h = this.context.PageProvider.GetPageHeader(this.pageLocked);
                    h.NextPage = p;
                    this.context.PageProvider.SetPageHeader(this.prevPage, this.pageLocked, h);
                }
                else
                {
                    buf = this.context.PageProvider.WriteLock(this.prevPage);
                    PageHeaderData h = this.context.PageProvider.GetPageHeader(buf);
                    h.NextPage = p;
                    this.context.PageProvider.SetPageHeader(this.prevPage, buf, h);
                    this.context.PageProvider.WriteUnlock(this.prevPage, true);
                }
                this.prevPage = p;
            }
            ++this.count;
        }

        public void Dispose()
        {
            if (this.locked > 0)
            {
                this.context.PageProvider.WriteUnlock(this.locked, true);
                this.locked = 0;
                this.pageLocked = null;
            }
            while (this.nextPage > 0)
            {
                int p = this.nextPage;
                byte[] buf = this.context.PageProvider.ReadLock(p);
                PageHeaderData h = this.context.PageProvider.GetPageHeader(buf);
                this.nextPage = h.NextPage;
                this.context.PageProvider.ReadUnlock(p);
                this.context.Free(p, false);
            }
            this.count = 0;
        }

        public static unsafe void Copy(byte[] source, byte[] dest, IDataPageProvider provider)
        {
            int offset = provider.PageHeaderSize;
            int count = provider.PageSize - offset;
            fixed (byte* src = source, dst = dest)
            {
                long* p0 = (long*)(src + offset);
                long* p1 = (long*)(dst + offset);
                while (count > 7)
                {
                    *(p1++) = *(p0++);
                    count -= 8;
                }
                byte* pp0 = (byte*)p0;
                byte* pp1 = (byte*)p1;
                while (count > 0)
                {
                    *(pp1++) = *(pp0++);
                    --count;
                }
            }
        }
    }

}