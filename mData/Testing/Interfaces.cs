using System;
using System.Collections.Generic;

namespace mData.Testing
{
    public sealed class TreeKeyValue<TKey, TValue>
    {
        public TKey Key;
        public TValue Value;

        public TreeKeyValue(TKey key, TValue value)
        {
            this.Key = key;
            this.Value = value;
        }

        public override string ToString()
        {
            return String.Format("{0}:{1}", this.Key.ToString(), this.Value.ToString());
        }
    }

    public sealed class TreeNodeInfo<TKey, TValue>
    {
        public TKey Key;
        public TKey Left;
        public TKey Right;
        public TValue Value;
        public bool IsBlack;

        public override string ToString()
        {
            return String.Format("{0} ({1}) L:{2} R:{3} '{4}'", this.Key, this.IsBlack ? "Black" : "Red", this.Left, this.Right, this.Value);
        }
    }

    public interface ITreeTesting<TKey, TValue>
    {

        void Insert(TKey key, TValue value);

        void Remove(TKey key); 

        List<TreeKeyValue<TKey, TValue>> Content();

        int Count { get; }

        TreeNodeInfo<TKey, TValue>[] GetNodes();

        bool TryGetValue(TKey key, out TValue value);

    }
}