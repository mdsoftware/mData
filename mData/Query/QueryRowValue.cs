using System;
using mData.Value;
using mData.Utils;
using mData.DataStructures;

namespace mData.Query
{
    struct QueryRowValue
    {
        public int Tag;

        public int Count;
        private string s;
        private long l;
        private double f;
        private DataValueType type;
        private QueryRowValueArray array;

        public static QueryRowValue Null
        {
            get
            {
                QueryRowValue v = new QueryRowValue();
                v.s = null;
                v.l = 0;
                v.Count = 0;
                v.f = Double.NaN;
                v.type = DataValueType.Null;
                v.array = null;
                return v;
            }
        }

        public string ToDebugString()
        {
            return String.Format("T:{0} S:{1} L:{2} F:{3} CNT:{4}", this.type,
                this.s == null ? "null" : (this.s.Length > 60 ? (this.s.Substring(0, 60) + "...") : this.s),
                this.l, this.f, this.Count);
        }

        public bool IsScalar
        {
            get
            {
                switch (this.type)
                {
                    case DataValueType.Array:
                    case DataValueType.Binary:
                    case DataValueType.Record:
                    case DataValueType.Undefined:
                    case DataValueType.Null:
                        return false;
                }
                return true;
            }
        }

        public bool IsNull
        {
            get { return this.type == DataValueType.Null; }
        }

        public bool IsNumeric
        {
            get { return (this.type == DataValueType.Float) || (this.type == DataValueType.Integer); }
        }

        public bool IsDateTime
        {
            get { return this.type == DataValueType.DateTime; }
        }

        public double ToNumber
        {
            get
            {
                if (this.type == DataValueType.Integer)
                    return (double)this.l;
                if (this.type == DataValueType.Float)
                    return this.f;
                throw new InvalidCastException("Value cannot be converted to number");
            }
        }

        public void Collect(QueryRowValue value)
        {
            if ((this.Count == 0) && (this.type != DataValueType.Array))
            {
                this.type = DataValueType.Array;
                this.array = new QueryRowValueArray(QueryRowValueArray.GrowSize);
            }
            switch (value.type)
            {
                case DataValueType.Array:
                    for (int i = 0; i < value.array.Count; i++)
                    {
                        if (value.array[i].IsNull) continue;
                        if (!value.array[i].IsScalar)
                            throw new ArgumentException("Only scalar values can be collected from array");
                        this.array.Add(value.array[i]);
                        ++this.Count;
                    }
                    break;

                case DataValueType.Record:
                case DataValueType.Binary:
                    throw new ArgumentException("Records or binaries cannot be collected");

                default:
                    this.array.Add(value);
                    ++this.Count;
                    break;
            }
        }

        public void Aggregate(QueryParamOperation op, double value)
        {
            if (this.Count == 0)
            {
                this.CheckType(DataValueType.Float);
                this.f = value;
                this.Count = 1;
                return;
            }
            if (this.type != DataValueType.Float)
                throw new DataException("Error aggregating values of a different types");

            switch (op)
            {
                case QueryParamOperation.Min:
                    if (this.f > value) this.f = value;
                    break;

                case QueryParamOperation.Max:
                    if (this.f < value) this.f = value;
                    break;

                default:
                    this.f += value;
                    break;
            }
            this.Count++;

        }

        public void Aggregate(QueryParamOperation op, DateTime value)
        {
            if (op == QueryParamOperation.Sum)
                throw new ArgumentException("Sum cannot be applied to dates");
            long sec = DateTimeParser.DateToSeconds(value);
            if (this.Count == 0)
            {
                this.CheckType(DataValueType.DateTime);
                this.l = sec;
                this.Count = 1;
                return;
            }
            if (this.type != DataValueType.DateTime)
                throw new DataException("Error aggregating values of a different types");

            switch (op)
            {
                case QueryParamOperation.Min:
                    if (this.l > sec) this.l = sec;
                    break;

                case QueryParamOperation.Max:
                    if (this.l < sec) this.l = sec;
                    break;

                default:
                    this.l += sec;
                    break;
            }
            this.Count++;
        }

        public void FinalizeAggregate(QueryParamOperation op)
        {
            if (this.Count == 0)
            {
                if (op == QueryParamOperation.Count)
                {
                    this.CheckType(DataValueType.Integer);
                    this.l = 0;
                }
                else
                {
                    this.CheckType(DataValueType.Null);
                }
                return;
            }
            switch (op)
            {
                case QueryParamOperation.Min:
                case QueryParamOperation.Max:
                    if (this.type == DataValueType.DateTime)
                        this.l = DateTimeParser.SecondsToDate(this.l).Ticks;
                    break;

                case QueryParamOperation.Count:
                    {
                        int c = this.Count;
                        this.CheckType(DataValueType.Integer);
                        this.l = (long)c;
                    }
                    break;

                case QueryParamOperation.Sum:
                    break;

                case QueryParamOperation.Average:
                    if (this.type == DataValueType.DateTime)
                    {
                        this.l = DateTimeParser.SecondsToDate(this.l / (long)this.Count).Ticks;
                    }
                    else if (this.type == DataValueType.Float)
                    {
                        this.f = this.f / (double)this.Count;
                    }
                    break;

            }
            this.Count = 0;
        }
        

        public static int Compare(QueryRowValue x, QueryRowValue y)
        {
            switch (x.type)
            {
                case DataValueType.Null:
                    return y.type == DataValueType.Null ? 0 : -1;

                case DataValueType.DateTime:
                    if (y.type == DataValueType.DateTime) return x.l.CompareTo(y.l);
                    break;

                case DataValueType.Integer:
                    if (y.type == DataValueType.Integer) return x.l.CompareTo(y.l);
                    if (y.type == DataValueType.Float) return x.f.CompareTo((double)y.l);
                    break;

                case DataValueType.Float:
                    if (y.type == DataValueType.Integer) return x.f.CompareTo((double)y.l);
                    if (y.type == DataValueType.Float) return x.f.CompareTo(y.f);
                    break;

                case DataValueType.Logic:
                    if (y.type == DataValueType.Logic) return (x.l & 0x1).CompareTo(y.l & 0x1);
                    break;

                case DataValueType.String:
                    if (y.type == DataValueType.String) return String.Compare(x.s, y.s, true);
                    break;

                case DataValueType.Array:
                    if (y.type == DataValueType.Array) return x.array.Count.CompareTo(y.array.Count);
                    break;
            }
            if (y.type == DataValueType.Null)
                return x.type == DataValueType.Null ? 0 : 1;
            throw new ArgumentException(String.Format("Cannot compare {0} and {1}", x.type, y.type));
        }

        public static QueryRowValue Array(int size)
        {
            QueryRowValue v = QueryRowValue.Null;
            v.type = DataValueType.Array;
            v.array = new QueryRowValueArray(size);
            return v;
        }

        public static QueryRowValue New()
        {
            return QueryRowValue.Null;
        }

        public static QueryRowValue New(DateTime value)
        {
            QueryRowValue v = QueryRowValue.Null;
            v.DateTime = value;
            return v;
        }

        public static QueryRowValue New(double value)
        {
            QueryRowValue v = QueryRowValue.Null;
            v.Float = value;
            v.l = 1L;
            return v;
        }

        public static QueryRowValue New(string value)
        {
            QueryRowValue v = QueryRowValue.Null;
            v.String = value;
            return v;
        }

        public static QueryRowValue New(bool value)
        {
            QueryRowValue v = QueryRowValue.Null;
            v.Logic = value;
            return v;
        }

        public static QueryRowValue New(DataValue value)
        {
            QueryRowValue v = QueryRowValue.Null;
            v.Assign(value);
            return v;
        }

        public QueryRowValue Copy()
        {
            QueryRowValue v = QueryRowValue.Null;
            v.s = this.s;
            v.l = this.l;
            v.f = this.f;
            v.type = this.type;
            v.array = this.array;
            v.Count = this.Count;
            return v;
        }

        public void Assign(QueryRowValue v)
        {
            this.s = v.s;
            this.l = v.l;
            this.f = v.f;
            this.Count = v.Count;
            this.type = v.type;
            this.array = v.array;
        }

        public void Assign(DataValue v)
        {
            switch (v.Type)
            {
                case DataValueType.DateTime:
                    this.DateTime = v.DateTime;
                    break;

                case DataValueType.Logic:
                    this.Logic = v.Logic;
                    break;

                case DataValueType.Integer:
                    this.Integer = v.Integer;
                    break;

                case DataValueType.Float:
                    this.Float = v.Float;
                    break;

                case DataValueType.String:
                    this.String = v.String;
                    break;

                case DataValueType.Reference:
                    this.CheckType(DataValueType.Reference);
                    this.l = v.Reference;
                    break;

                case DataValueType.Null:
                    break;

                default:
                    throw new ArgumentException(String.Format("Type {0} is not supported", v.Type));
            }
        }

        public QueryRowValue this[int i]
        {
            get
            {
                if (this.type != DataValueType.Array)
                    throw new DataException("{0} type expected", DataValueType.Array);
                return this.array[i];
            }
            set
            {
                if (this.type != DataValueType.Array)
                    throw new DataException("{0} type expected", DataValueType.Array);
                this.array[i] = value;
            }
        }

        public void Add(QueryRowValue v)
        {
            if (this.type != DataValueType.Array)
                throw new DataException("{0} type expected", DataValueType.Array);
            this.array.Add(v);
        }

        public int Length
        {
            get
            {
                switch (this.type)
                {
                    case DataValueType.Array:
                        return this.array.Count;

                    case DataValueType.String:
                        return (this.s == null) ? -1 : this.s.Length;

                }
                return -1;
            }
        }

        public DateTime DateTime
        {
            get
            {
                if (this.type != DataValueType.DateTime)
                    throw new DataException("{0} type expected", DataValueType.DateTime);
                return new DateTime(this.l);
            }
            set
            {
                this.CheckType(DataValueType.DateTime);
                this.l = value.Ticks;
            }
        }

        public double Float
        {
            get
            {
                if (this.type != DataValueType.Float)
                    throw new DataException("{0} type expected", DataValueType.Float);
                return this.f;
            }
            set
            {
                this.CheckType(DataValueType.Float);
                this.f = value;
            }
        }

        public string String
        {
            get
            {
                if (this.type != DataValueType.String)
                    throw new DataException("{0} type expected", DataValueType.String);
                return this.s;
            }
            set
            {
                this.CheckType(DataValueType.String);
                this.s = value;
            }
        }

        public long Reference
        {
            get
            {
                if (this.type != DataValueType.Reference)
                    throw new DataException("{0} type expected", DataValueType.Reference);
                return this.l;
            }
        }

        public long Integer
        {
            get
            {
                if (this.type != DataValueType.Integer)
                    throw new DataException("{0} type expected", DataValueType.Integer);
                return this.l;
            }
            set
            {
                this.CheckType(DataValueType.Integer);
                this.l = value;
            }
        }

        public bool Logic
        {
            get
            {
                if (this.type != DataValueType.Logic)
                    throw new DataException("{0} type expected", DataValueType.Logic);
                return this.l == 1L;
            }
            set
            {
                this.CheckType(DataValueType.Logic);
                this.l = value ? 1L : 0L;
            }
        }

        private void CheckType(DataValueType t)
        {
            if (this.type == t) return;
            this.s = null;
            this.l = 0;
            this.f = Double.NaN;
            this.array = null;
            this.type = t;
            this.Count = 0;
        }

        public DataValue DataValue
        {
            get
            {
                switch (this.type)
                {
                    case DataValueType.DateTime:
                    case DataValueType.Float:
                    case DataValueType.Integer:
                    case DataValueType.Logic:
                    case DataValueType.String:
                    case DataValueType.Null:
                        return this.ScalarValue;

                    case DataValueType.Array:
                        {
                            DataValue a = DataValue.Array(this.array.Count);
                            for (int i = 0; i < this.array.Count; i++)
                                a.Add(this.array[i].ScalarValue);
                            return a;
                        }
                }
                throw new DataException("Unsupported type {0}", this.type);
            }
        }

        private DataValue ScalarValue
        {
            get
            {
                switch (this.type)
                {
                    case DataValueType.DateTime:
                        return DataValue.New(this.DateTime);

                    case DataValueType.Float:
                        return DataValue.New(this.Float);

                    case DataValueType.Integer:
                        return DataValue.New(this.Integer);

                    case DataValueType.Logic:
                        return DataValue.New(this.Logic);

                    case DataValueType.String:
                        return DataValue.New(this.String);

                    case DataValueType.Null:
                        return DataValue.Null;

                    default:
                        throw new DataException("Type {0} is not a scalar type", this.type);
                }
            }
        }

        public DataValueType Type
        {
            get { return this.type; }
        }

        public override string ToString()
        {
            switch (this.type)
            {
                case DataValueType.Null:
                    return "null";

                case DataValueType.Array:
                    return String.Format("array[{0}]", this.array.Count);

                case DataValueType.DateTime:
                    return this.DateTime.ToString(DataValue.DateTimeFormat);

                case DataValueType.Float:
                    return this.Float.ToString();

                case DataValueType.Integer:
                    return String.Format("{0} (0x{1})", this.Integer, this.Integer.ToString("x"));

                case DataValueType.Logic:
                    return this.Logic ? "true" : "false";

                case DataValueType.Reference:
                    return String.Format("@{0}", this.l);

                case DataValueType.String:
                    return this.s;
            }
            return "?";
        }
    }

    sealed class QueryRowValueArray
    {
        public int Count;
        public QueryRowValue[] Items;

        public const int GrowSize = 32;

        public QueryRowValueArray(int size)
        {
            this.Count = 0;
            this.Items = new QueryRowValue[size];
        }

        public QueryRowValue this[int i]
        {
            get
            {
                if ((i < 0) || (i >= this.Count))
                    throw new IndexOutOfRangeException("Index is out of array bound");
                return this.Items[i];
            }
            set
            {
                if ((i < 0) || (i >= this.Count))
                    throw new IndexOutOfRangeException("Index is out of array bound");
                this.Items[i] = value;
            }
        }

        public void Add(QueryRowValue v)
        {
            if (this.Count >= this.Items.Length)
            {
                QueryRowValue[] l = new QueryRowValue[this.Items.Length + QueryRowValueArray.GrowSize];
                for (int i = 0; i < this.Count; i++)
                    l[i] = this.Items[i];
                this.Items = null;
                this.Items = l;
                l = null;
            }
            this.Items[this.Count++] = v;
        }
    }
}