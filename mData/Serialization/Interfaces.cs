using System;

namespace mData.Serialization
{

    /// <summary>
    /// Generic data read stream
    /// </summary>
    public interface IDataReadStream : IDisposable
    {
        /// <summary>
        /// Read 4 bytes integer
        /// </summary>
        /// <returns></returns>
        int ReadInt();

        /// <summary>
        /// Read 8 bytes integer
        /// </summary>
        /// <returns></returns>
        long ReadLong();

        /// <summary>
        /// Read string, string can be null
        /// </summary>
        /// <returns></returns>
        string ReadString();

        /// <summary>
        /// Read array of bytes, array can be null
        /// </summary>
        /// <returns></returns>
        byte[] ReadBytes();
    }

    /// <summary>
    /// Generic data write stream
    /// </summary>
    public interface IDataWriteStream : IDisposable
    {
        /// <summary>
        /// Write 4 bytes integer
        /// </summary>
        /// <param name="i"></param>
        void Write(int i);

        /// <summary>
        /// Write 8 bytes integer
        /// </summary>
        /// <param name="l"></param>
        void Write(long l);

        /// <summary>
        /// Write Unicode string, string can be null
        /// </summary>
        /// <param name="s"></param>
        void Write(string s);

        /// <summary>
        /// Write array of bytes, array can be null
        /// </summary>
        /// <param name="b"></param>
        void Write(byte[] b);

        /// <summary>
        /// Flush write stream
        /// </summary>
        void Flush();
    }

    /// <summary>
    /// Stream for reading data pages
    /// </summary>
    public interface IPageReadStream
    {
        /// <summary>
        /// Read page in a specified buffer
        /// </summary>
        /// <param name="page"></param>
        void Read(byte[] page);

        /// <summary>
        /// Specify a size of the data page
        /// </summary>
        int PageSize { get; }

        /// <summary>
        /// Specify a size of the page header
        /// </summary>
        int PageHeaderSize { get; }
    }

    /// <summary>
    /// Stream for writing data pages
    /// </summary>
    public interface IPageWriteStream
    {
        /// <summary>
        /// Write specified data from buffer
        /// </summary>
        /// <param name="page"></param>
        void Write(byte[] page);

        /// <summary>
        /// Specify a page size
        /// </summary>
        int PageSize { get; }

        /// <summary>
        /// Specify page header size
        /// </summary>
        int PageHeaderSize { get; }

        /// <summary>
        /// Flush write stream
        /// </summary>
        void Flush();
    }

    /// <summary>
    /// Generic symbol provider. Symbol is just a string containing only letters, digits and/or underscore
    /// and started from letter. Every symbol equivalent to its id.
    /// </summary>
    public interface ISymbolProvider
    {
        /// <summary>
        /// Get id of the symbol
        /// </summary>
        /// <param name="symbol"></param>
        /// <returns></returns>
        int Get(string symbol);

        /// <summary>
        /// Get symbol string by its id
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        string Get(int id);
    }
}