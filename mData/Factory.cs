using System;
using mData.Storage;
using mData.Utils;
using mData.Caching;
using mData.Context;

namespace mData
{

    public static class Factory
    {

        public static IDataPageStorage OpenFileStorage(string fileName)
        {
            return FileStorage.Open(fileName);
        }

        public static IDataPageStorage CreateFileStorage(string fileName, short hash, int pageSize)
        {
            return FileStorage.Create(fileName, Uid.NextHash(), Uid.NextHash(), hash, pageSize);
        }

        public static IDataPageProvider CachedPageProvider(IDataPageStorage file, int cacheSize, int lockTimeout, DataPageProviderFlags flags)
        {
            return PageCache.Create(file, cacheSize, lockTimeout, flags);
        }

        public static IDataContext CreateDataContext(IDataPageProvider provider, int lockTimeout, string description)
        {
            return DataContext.Create(provider, lockTimeout, description);
        }

        public static IDataContext OpenDataContext(IDataPageProvider provider, int lockTimeout)
        {
            return DataContext.Open(provider, lockTimeout);
        }

    }
}