using System;
using System.Collections.Generic;
using mData.Value;
using mData.DataStructures;
using mData.Utils;

namespace mData.Query
{

    /// <summary>
    /// Query parameter operation kind
    /// </summary>
    public enum QueryParamOperation : byte
    {
        /// <summary>
        /// Simple column
        /// </summary>
        Default = 0,
        /// <summary>
        /// Sort ascending
        /// </summary>
        SortAscending,
        /// <summary>
        /// Sort descending
        /// </summary>
        SortDescending,
        /// <summary>
        /// MAX(x)
        /// </summary>
        Max,
        /// <summary>
        /// MIN(x)
        /// </summary>
        Min,
        /// <summary>
        /// SUM(x)
        /// </summary>
        Sum,
        /// <summary>
        /// COUNT(if nor null)
        /// </summary>
        Count,
        /// <summary>
        /// AVG(x)
        /// </summary>
        Average,
        /// <summary>
        /// Collect items
        /// </summary>
        Collect
    }

    /// <summary>
    /// Query parameter, actually a string and operation type
    /// </summary>
    public struct QueryParameter
    {
        private string name;
        private QueryParamOperation operation;

        public QueryParameter(string name, QueryParamOperation operation)
        {
            this.name = name;
            this.operation = operation;
        }

        public QueryParameter(string name)
        {
            this.name = name;
            this.operation = QueryParamOperation.Default;
        }

        public string Name
        {
            get { return this.name; }
        }

        public QueryParamOperation Operation
        {
            get { return this.operation; }
        }
    }

    sealed class KeyCollection
    {
        private ArrayTree<DataValue, int> keys;

        public KeyCollection(int size, PageFactor factor)
        {
            this.keys = new ArrayTree<DataValue, int>(KeyCollection.Compare, 0, size, factor);
        }

        public int Count
        {
            get { return this.keys.Count; }
        }

        public void Clear()
        {
            this.keys.Clear();
        }

        public void Set(DataValue key, int value)
        {
            this.keys.Insert(key, value);
        }

        public bool TryGetValue(DataValue key, out int value)
        {
            return this.keys.TryGetValue(key, out value);
        }

        public bool Contains(DataValue key)
        {
            int v;
            return this.keys.TryGetValue(key, out v); 
        }

        public int[] Values
        {
            get { return this.keys.AllValues(); }
        }

        public static int Compare(DataValue x, DataValue y)
        {
            if (x.Length != y.Length)
                throw new DataException("Keys must have the same size");
            return KeyCollection.Compare(x, y, x.AllNames);
        }

        public static int Compare(DataValue x, DataValue y, QueryParameter[] parameters)
        {
            for (int i = 0; i < parameters.Length; i++)
            {
                string n = parameters[i].Name;
                if (!x.ContainsName(n) || !y.ContainsName(n))
                    throw new DataException("Field '{0}' is not found in a key record", n);
                int r = DataValue.Compare(x[n], y[n]);
                if (r == 0) continue;
                if (parameters[i].Operation == QueryParamOperation.SortDescending) r = -r;
                return r;
            }
            return 0;
        }

        public static int Compare(DataValue x, DataValue y, string[] names)
        {
            for (int i = 0; i < names.Length; i++)
            {
                string n = names[i];
                if (!x.ContainsName(n) || !y.ContainsName(n))
                    throw new DataException("Field '{0}' is not found in a key record", n);
                int r = DataValue.Compare(x[n], y[n]);
                if (r == 0) continue;
                return r;
            }
            return 0;
        }
    }


}