using System;
using mData.Value;
using mData.Utils;
using mData.DataStructures;

namespace mData.Query
{

    struct ColumnAggregateInfo
    {
        public int Id;
        public string Name;
        public QueryParamOperation Operation;
        public int TargetId;

        public ColumnAggregateInfo(int id, string name)
        {
            this.Id = id;
            this.Name = name;
            this.Operation = QueryParamOperation.Default;
            this.TargetId = 0;
        }

        public ColumnAggregateInfo(int id, string name, bool desc)
        {
            this.Id = id;
            this.Name = name;
            this.Operation = desc ? QueryParamOperation.SortDescending : QueryParamOperation.SortAscending;
            this.TargetId = 0;
        }

        public ColumnAggregateInfo(int id, string name, QueryParamOperation op)
        {
            this.Id = id;
            this.Name = name;
            this.Operation = op;
            this.TargetId = 0;
        }

        public bool Descending
        {
            get { return this.Operation == QueryParamOperation.SortDescending; }
        }

        public override string ToString()
        {
            return String.Format("#{0} '{1}' ({2}) [{3}]", this.Id, this.Name, this.Operation, this.TargetId);
        }
    }

    sealed class QueryResultRow
    {
        private QueryRowEntry[] values;
        private int count;

        public int[] Children;
        public KeyCollection Keys;

        public const int GrowSize = 8;

        public QueryResultRow(int size)
        {
            this.values = new QueryRowEntry[size <= 0 ? QueryResultRow.GrowSize : size];
            this.count = 0;
            this.Keys = null;
            this.Children = null;
        }

        public int Count
        {
            get { return this.count; }
        }

        public DataValue GetRecord(ColumnAggregateInfo[] param)
        {
            DataValue rec = DataValue.Record(param.Length);
            for (int i = 0; i < param.Length; i++)
            {
                int p;
                if (!this.Find(param[i].Id, out p))
                {
                    rec[param[i].Name] = DataValue.Null;
                    continue;
                }
                rec[param[i].Name] = this.values[p].Value.DataValue;
            }
            return rec;
        }

        public static int Compare(QueryResultRow x, QueryResultRow y, ColumnAggregateInfo[] param)
        {
            for (int i = 0; i < param.Length; i++)
            {
                int p;
                QueryRowValue vx = QueryRowValue.Null;
                if (x.Find(param[i].Id, out p))
                    vx = x.values[p].Value;
                QueryRowValue vy = QueryRowValue.Null;
                if (y.Find(param[i].Id, out p))
                    vy = y.values[p].Value;
                int r = QueryRowValue.Compare(vx, vy);
                if (r == 0)
                    continue;
                return param[i].Descending ? -r : r;
            }
            return 0;
        }

        public int GetTag(int id)
        {
            int p;
            if (this.Find(id, out p))
                return this.values[p].Value.Tag;
            return 0;
        }

        public void SetTag(int id, int tag)
        {
            int p;
            if (this.Find(id, out p))
                this.values[p].Value.Tag = tag;
        }

        public bool Contains(int id)
        {
            int p;
            return this.Find(id, out p);
        }

        public void FinalizeAggregate(int id, QueryParamOperation op)
        {
            int p;
            if (this.Find(id, out p))
                this.values[p].Value.FinalizeAggregate(op);
        }

        public void Addregate(int id, QueryParamOperation op, QueryRowValue value)
        {
            if (value.IsNull) return;
            int p;
            if (!this.Find(id, out p))
            {
                this[id] = QueryRowValue.Null;
                this.Find(id, out p);
            }
            if (op == QueryParamOperation.Collect)
            {
                this.values[p].Value.Collect(value);
                return;
            }
            if (value.IsNumeric)
            {
                this.values[p].Value.Aggregate(op, value.ToNumber);
                return;
            }
            if (value.IsDateTime)
            {
                this.values[p].Value.Aggregate(op, value.DateTime);
                return;
            }
            throw new ArgumentException(String.Format("Value of type {0} cannot be aggregated", value.Type));
        }

        public QueryRowValue this[int id]
        {
            get
            {
                int p;
                if (this.Find(id, out p))
                    return this.values[p].Value;
                return QueryRowValue.Null;
            }
            set
            {
                int p;
                if (this.Find(id, out p))
                {
                    this.values[p].Value = value;
                    return;
                }
                int i;
                if (this.count >= this.values.Length)
                {
                    QueryRowEntry[] v = new QueryRowEntry[this.values.Length + QueryResultRow.GrowSize];
                    for (i = 0; i < this.count; i++)
                        v[i] = this.values[i];
                    this.values = null;
                    this.values = v;
                    v = null;
                }
                i = this.count - 1;
                while (i >= p)
                {
                    this.values[i + 1] = this.values[i];
                    i--;
                }
                this.values[p].Value = value;
                this.values[p].Id = id;
                ++this.count;
            }
        }

        private bool Find(int id, out int position)
        {
            position = 0;
            if (this.count == 0)
                return false;
            int min = 0;
            int max = this.count - 1;
            while (true)
            {
                int mid = (min + max) >> 1;
                int r = id.CompareTo(this.values[mid].Id);
                if (r == 0)
                {
                    position = mid;
                    return true;
                }
                if (r > 0)
                {
                    min = mid + 1;
                }
                else
                {
                    max = mid - 1;
                }
                if (min > max) break;
            }
            position = min;
            return false;
        }

        struct QueryRowEntry
        {
            public int Id;
            public QueryRowValue Value;

            public override string ToString()
            {
                return String.Format("#{0}: {1}", this.Id, this.Value.ToDebugString());
            }
        }
    }
}