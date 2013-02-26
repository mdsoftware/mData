using System;
using mData.Value;
using mData.Utils;

namespace mData.Context
{

    /// <summary>
    /// Main data access interface.
    /// </summary>
    public interface IDataContext : IDisposable
    {
        /// <summary>
        /// Database description (assigned when database is created)
        /// </summary>
        string Description { get; }

        /// <summary>
        /// Database UID (assigned when database is created)
        /// </summary>
        string Uid { get; }

        /// <summary>
        /// Database statistics
        /// </summary>
        CommonStatistics Statistics { get; }

        /// <summary>
        /// Root data instance. 
        /// </summary>
        /// <returns></returns>
        DataValue GetRoot();

        /// <summary>
        /// Update root data instance
        /// </summary>
        /// <param name="value"></param>
        void UpdateRoot(DataValue value);

        /// <summary>
        /// Load object from the database by id
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        DataValue Get(long id);

        /// <summary>
        /// Returns object version. Version is autoincremented value increasing by 1 every time the object is updated.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        int GetVersion(long id);

        /// <summary>
        /// Add new object to database returning its id.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        long Add(DataValue value);

        /// <summary>
        /// Update object in database and increment its version.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        int Update(DataValue value);

        /// <summary>
        /// Delete database object by its id.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        bool Delete(long id);

        /// <summary>
        /// Lock id for reading. Several read locks can be applied to particular id.
        /// NOTE: Nested locks can result a deadlock if locked ids will appear in the same bucket.
        /// </summary>
        /// <param name="id"></param>
        /// <param name="timeout"></param>
        void ReadLock(long id, int timeout);

        /// <summary>
        /// Unlock id for reading.
        /// </summary>
        /// <param name="id"></param>
        void ReadUnlock(long id);

        /// <summary>
        /// Write lock id. Only one write lock can be applied to id.
        /// NOTE: Nested locks can result a deadlock if locked ids will appear in the same bucket.
        /// </summary>
        /// <param name="id"></param>
        /// <param name="timeout"></param>
        void WriteLock(long id, int timeout);

        /// <summary>
        /// Write unlock specified id and optionally read lock it.
        /// </summary>
        /// <param name="id"></param>
        /// <param name="readLock"></param>
        void WriteUnlock(long id, bool readLock);

        /// <summary>
        /// Put a read lock on a semaphore name.
        /// </summary>
        /// <param name="semaphore"></param>
        /// <param name="timeout"></param>
        void ReadLock(string semaphore, int timeout);

        /// <summary>
        /// Read unlock semaphore name.
        /// </summary>
        /// <param name="semaphore"></param>
        void ReadUnlock(string semaphore);

        /// <summary>
        /// Write lock semaphore name.
        /// </summary>
        /// <param name="semaphore"></param>
        /// <param name="timeout"></param>
        void WriteLock(string semaphore, int timeout);

        /// <summary>
        /// Write unlock semaphore name and put optional read lock on it.
        /// </summary>
        /// <param name="semaphore"></param>
        /// <param name="readLock"></param>
        void WriteUnlock(string semaphore, bool readLock);
    }

}