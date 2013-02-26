using System;
using mData.Testing;
using mData.DataStructures;
using mData.Caching;
using mData.Storage;
using mData.Utils;
using mData.Serialization;

namespace mData.Testing
{

    public static class TestFactory
    {
        public static ITreeTesting<long, ulong> TestLeftLeanRedBlackTree()
        {
            return new LeftLeanRedBlackTree<long, ulong>(TestFactory.CompareKeys);
        }

        public static ITreeTesting<long, ulong> TestArrayTree(int size)
        {
            return new ArrayTree<long, ulong>(TestFactory.CompareKeys, 0, size);
        }

        public static IDataWriteStream TestDataWriteStream(IPageWriteStream stream)
        {
            return new DataWriteStream(stream);
        }

        public static IDataReadStream TestDataReadStream(IPageReadStream stream)
        {
            return new DataReadStream(stream);
        }

        private static int CompareKeys(int x, int y)
        {
            return x.CompareTo(y);
        }

        private static int CompareKeys(long x, long y)
        {
            return x.CompareTo(y);
        }

    }

}