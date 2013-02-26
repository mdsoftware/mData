using System;
using System.Collections.Generic;

namespace mData.Storage
{
    [Flags]
    public enum DataPageProviderFlags : ushort
    {
        Empty = 0x0000,
        WriteThrough = 0x0001,
        FlushWrites = 0x0002
    }

    /// <summary>
    /// Generic page header
    /// </summary>
    public struct PageHeaderData
    {
        public int PrevPage;
        public int NextPage;
        public ulong Data0;
        public ulong Data1;
        public ulong Data2;
        public ulong Data3;
    }

    /// <summary>
    /// Generic data base provider
    /// </summary>
    public interface IDataPageProvider : IDisposable
    {
        /// <summary>
        /// Return page header from page buffer
        /// </summary>
        /// <param name="page"></param>
        /// <returns></returns>
        PageHeaderData GetPageHeader(byte[] page);

        /// <summary>
        /// Update page header for a specified page
        /// </summary>
        /// <param name="pageNo"></param>
        /// <param name="page"></param>
        /// <param name="header"></param>
        void SetPageHeader(int pageNo, byte[] page, PageHeaderData header); 

        /// <summary>
        /// Return page header size
        /// </summary>
        int PageHeaderSize { get; }

        /// <summary>
        /// Return page size
        /// </summary>
        int PageSize { get; }

        /// <summary>
        /// Read lock the page. Several read locks can be put on the same page.
        /// </summary>
        /// <param name="pageNo"></param>
        /// <returns></returns>
        byte[] ReadLock(int pageNo);

        /// <summary>
        /// Read unlock the page
        /// </summary>
        /// <param name="pageNo"></param>
        void ReadUnlock(int pageNo);

        /// <summary>
        /// Write lock the page
        /// </summary>
        /// <param name="pageNo"></param>
        /// <returns></returns>
        byte[] WriteLock(int pageNo);

        /// <summary>
        /// Write unlock the page optionally mark the page as modified.
        /// </summary>
        /// <param name="pageNo"></param>
        /// <param name="markAsModified"></param>
        void WriteUnlock(int pageNo, bool markAsModified);

        /// <summary>
        /// Allocate new page
        /// </summary>
        /// <returns></returns>
        int Allocate();

        /// <summary>
        /// Flush modified data
        /// </summary>
        void Flush();

        /// <summary>
        /// Get UID of the storage. This UID is usually taken from undelying storage.
        /// </summary>
        string Uid { get; }

    }

    /// <summary>
    /// Generic data page storage.
    /// </summary>
    public interface IDataPageStorage : IDisposable
    {
        /// <summary>
        /// Get page header from page data
        /// </summary>
        /// <param name="page"></param>
        /// <returns></returns>
        PageHeaderData GetPageHeader(byte[] page);

        /// <summary>
        /// Set page header for a page data
        /// </summary>
        /// <param name="pageNo"></param>
        /// <param name="page"></param>
        /// <param name="header"></param>
        void SetPageHeader(int pageNo, byte[] page, PageHeaderData header); 

        /// <summary>
        /// Returns page size
        /// </summary>
        int PageSize { get; }

        /// <summary>
        /// Returns page header size
        /// </summary>
        int PageHeaderSize { get; }

        /// <summary>
        /// Read page from storage
        /// </summary>
        /// <param name="pageNo"></param>
        /// <param name="page"></param>
        void ReadPage(int pageNo, byte[] page);

        /// <summary>
        /// Write page to storage
        /// </summary>
        /// <param name="pageNo"></param>
        /// <param name="page"></param>
        void WritePage(int pageNo, byte[] page);

        /// <summary>
        /// Allocate new page
        /// </summary>
        /// <returns></returns>
        int NewPage();

        /// <summary>
        /// Flush modified data
        /// </summary>
        void Flush();

        /// <summary>
        /// Initialize new page
        /// </summary>
        /// <param name="pageNo"></param>
        /// <param name="page"></param>
        void Initialize(int pageNo, byte[] page);

        /// <summary>
        /// Get storage UID. This UID is assigned while storage creation.
        /// </summary>
        string Uid { get; }

    }
}