using System;
using System.IO;
using System.Threading;
using System.Runtime.InteropServices;
using mData.Utils;

namespace mData.Storage
{



    sealed class FileStorage : IDataPageStorage
    {

        public const long PageSignature = 0x6567615061746144;
        public const long FileSignature = 0x656c694661746144;
        public const int MinPageSize = 0x100;
        public const int MaxPageSize = 0x8000;

        private FileHeader header;
        private FileStream file;
        private object sync;
        private long bytesRead;
        private long bytesWritten;

        FileStorage()
        {
            this.file = null;
            this.header = new FileHeader();
            this.sync = new Object();
            this.bytesRead = 0;
            this.bytesWritten = 0;
        }

        public PageHeaderData GetPageHeader(byte[] page)
        {
            return FileStorage.GetPageHeaderData(page);
        }

        public void SetPageHeader(int pageNo, byte[] page, PageHeaderData header)
        {
            FileStorage.UpdatePageHeaderData(pageNo, page, header);
        }

        public int PageHeaderSize
        {
            get { return PageHeader.Size; }
        }

        public string Uid
        {
            get { return Char32.Convert130Bits(this.header.UidHigh, this.header.UidLow); }
        }

        public int PageSize
        {
            get { return this.header.PageSize; }
        }

        public int NewPage()
        {
            int p;
            lock (this.sync)
            {
                p = this.header.LastPage++;
                byte[] buf = new byte[FileHeader.Size];
                FileHeader.Set(buf, this.header);
                this.file.Position = 0;
                this.file.Write(buf, 0, FileHeader.Size);
            }
            return p;
        }

        public void Initialize(int pageNo, byte[] page)
        {
            PageHeader h = new PageHeader();
            h.Signature = FileStorage.PageSignature;
            h.PageNo = pageNo;
            h.PageNoCheck = pageNo ^ 0x7fffffff;
            PageHeader.Set(page, h);
        }

        public void WritePage(int pageNo, byte[] page)
        {
            if (pageNo >= this.header.LastPage)
                throw new ArgumentException("Invalid page number");
            if (page.Length != this.PageSize)
                throw new ArgumentException("Invalid page size");

            PageHeader h = PageHeader.Get(page);
            FileStorage.CheckPageHeader(h);
            if (h.PageNo != pageNo)
                throw new DataException("Page numbers mismatch while writing a page {0}", pageNo);

            lock (this.sync)
            {
                long p = (pageNo * this.PageSize) + FileHeader.Size;
                while (p > this.file.Length)
                {
                    this.file.Position = this.file.Length;
                    this.file.Write(page, 0, this.PageSize);
                    this.bytesWritten += this.PageSize;
                }
                this.file.Position = p;
                this.file.Write(page, 0, this.PageSize);
                this.bytesWritten += this.PageSize;
            }
        }

        public void ReadPage(int pageNo, byte[] page)
        {
            if (pageNo >= this.header.LastPage)
                throw new ArgumentException("Invalid page number");
            if (page.Length != this.PageSize)
                throw new ArgumentException("Invalid page size");
            int b = 0;
            lock (this.sync)
            {
                long p = (pageNo * this.PageSize) + FileHeader.Size;
                if (this.file.Length < p)
                    throw new DataException("Data file is corrupted (file too short)");
                this.file.Position = p;
                b = this.file.Read(page, 0, this.PageSize);
            }
            this.bytesRead += b;
            if (b != this.PageSize)
                throw new DataException("Data file is corrupted (invalid page size)");
            PageHeader h = PageHeader.Get(page);
            FileStorage.CheckPageHeader(h);
            if (h.PageNo != pageNo)
                throw new DataException("Page numbers mismatch while reading a page {0}", pageNo);
        }

        public static FileStorage Open(string fileName)
        {
            FileStorage f = new FileStorage();

            f.file = File.Open(fileName, FileMode.Open, FileAccess.ReadWrite);
            byte[] buf = new byte[FileHeader.Size];
            int r = f.file.Read(buf, 0, FileHeader.Size);
            if (r != FileHeader.Size)
                throw new DataException("Invalid file header size");
            f.header = FileHeader.Get(buf);
            if (f.header.Signature != FileStorage.FileSignature)
                throw new DataException("Invalid file signature {0} (0x{1})", f.header.Signature, f.header.Signature.ToString("x16"));
            return f;
        }

        public static FileStorage Create(string fileName, ulong uidHigh, ulong uidLow, short hash, int pageSize)
        {
            if ((pageSize < FileStorage.MinPageSize) || (pageSize > FileStorage.MaxPageSize))
                throw new ArgumentOutOfRangeException("Page size out of range");
            if ((pageSize & 0xff) != 0)
                throw new ArgumentException("Page size must be 255 bytes aligned");


            FileHeader h = new FileHeader();
            h.Signature = FileStorage.FileSignature;
            h.UidHigh = uidHigh;
            h.UidLow = uidLow;
            h.Hash = hash;
            h.PageSize = (short)pageSize;

            byte[] buf = new byte[FileHeader.Size];
            FileHeader.Set(buf, h);

            Stream f = File.Create(fileName);
            f.Write(buf, 0, FileHeader.Size);
            f.Close();
            f = null;

            return FileStorage.Open(fileName);
        }

        private static void UpdatePageHeaderData(int pageNo, byte[] page, PageHeaderData data)
        {
            PageHeader h = new PageHeader();
            h.Signature = FileStorage.PageSignature;
            h.PageNo = pageNo;
            h.PageNoCheck = pageNo ^ 0x7fffffff;
            h.Data = data;
            PageHeader.Set(page, h);
        }

        private static void CheckPageHeader(PageHeader h)
        {
            if (h.Signature != FileStorage.PageSignature)
                throw new DataException("Invalid page signature {0} (0x{1})", h.Signature, h.Signature.ToString("x16"));
            if ((h.PageNo ^ 0x7fffffff) != h.PageNoCheck)
                throw new DataException("Page number check failed");
        }

        private static PageHeaderData GetPageHeaderData(byte[] page)
        {
            PageHeader h = PageHeader.Get(page);
            FileStorage.CheckPageHeader(h);
            return h.Data;
        }

        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern int FlushFileBuffers(Microsoft.Win32.SafeHandles.SafeFileHandle fileHandle);

        public void Flush()
        {
            lock (this.sync)
            {
                if (this.file != null)
                    this.TryFlush();
            }
        }

        private void TryFlush()
        {
            this.file.Flush();
            FlushFileBuffers(this.file.SafeFileHandle);
        }

        public void Dispose()
        {
            lock (this.sync)
            {
                if (this.file != null)
                {
                    this.TryFlush();
                    this.file.Close();
                    this.file = null;
                    this.header = new FileHeader();
                }
            }
        }
    }

    struct FileHeader
    {
        public long Signature;
        public ulong UidHigh;
        public ulong UidLow;
        public short Hash;
        public short PageSize;
        public int LastPage;

        public const int Size = 64;

        public static unsafe FileHeader Get(byte[] buffer)
        {
            FileHeader h = new FileHeader();
            fixed (byte* p = buffer)
            {
                byte* pp = p;
                h.Signature = *((long*)pp);
                pp += 8;
                h.UidHigh = *((ulong*)pp);
                pp += 8;
                h.UidLow = *((ulong*)pp);
                pp += 8;
                h.Hash = *((short*)pp);
                pp += 2;
                h.PageSize = *((short*)pp);
                pp += 2;
                h.LastPage = *((int*)pp);
            }
            return h;
        }

        public static unsafe void Set(byte[] buffer, FileHeader h)
        {
            fixed (byte* p = buffer)
            {
                byte* pp = p;
                *((long*)pp) = h.Signature;
                pp += 8;
                *((ulong*)pp) = h.UidHigh;
                pp += 8;
                *((ulong*)pp) = h.UidLow;
                pp += 8;
                *((short*)pp) = h.Hash;
                pp += 2;
                *((short*)pp) = h.PageSize;
                pp += 2;
                *((int*)pp) = h.LastPage;
            }
        }
    }

    struct PageHeader
    {
        public long Signature;
        public int PageNo;
        public int PageNoCheck;
        public PageHeaderData Data;

        public const int Size = 64;

        public static unsafe PageHeader Get(byte[] buffer)
        {
            PageHeader h = new PageHeader();
            fixed (byte* p = buffer)
            {
                byte* pp = p;
                h.Signature = *((long*)pp);
                pp += 8;
                h.PageNo = *((int*)pp);
                pp += 4;
                h.PageNoCheck = *((int*)pp);
                pp += 4;
                h.Data.PrevPage = *((int*)pp);
                pp += 4;
                h.Data.NextPage = *((int*)pp);
                pp += 4;
                h.Data.Data0 = *((ulong*)pp);
                pp += 8;
                h.Data.Data1 = *((ulong*)pp);
                pp += 8;
                h.Data.Data2 = *((ulong*)pp);
                pp += 8;
                h.Data.Data3 = *((ulong*)pp);
            }
            return h;
        }

        public static unsafe void Set(byte[] buffer, PageHeader h)
        {
            fixed (byte* p = buffer)
            {
                byte* pp = p;
                *((long*)pp) = h.Signature;
                pp += 8;
                *((int*)pp) = h.PageNo;
                pp += 4;
                *((int*)pp) = h.PageNoCheck;
                pp += 4;
                *((int*)pp) = h.Data.PrevPage;
                pp += 4;
                *((int*)pp) = h.Data.NextPage;
                pp += 4;
                *((ulong*)pp) = h.Data.Data0;
                pp += 8;
                *((ulong*)pp) = h.Data.Data1;
                pp += 8;
                *((ulong*)pp) = h.Data.Data2;
                pp += 8;
                *((ulong*)pp) = h.Data.Data3;
            }
        }

    }

}