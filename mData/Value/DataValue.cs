using System;
using mData;
using mData.Serialization;
using mData.Utils;

namespace mData.Value
{

    public enum DataValueType
    {
        Undefined = 0,
        Null = 1,
        Integer = 2,
        DateTime = 3,
        Logic = 4,
        String = 5,
        Float = 6,
        Reference = 7,
        Array = 8,
        Record = 9,
        Binary = 10
    }

    public sealed class DataValue
    {
        private DataValueType type;
        private long l;
        private string s;
        private DataValueRefValue refValue;

        public const double SecondsPerDay = 86400f;

        public const string DateTimeFormat = "yyyy'/'MM'/'dd' 'HH':'mm':'ss";

        private const int Signature = 0x6c615658;

        DataValue()
        {
            this.type = DataValueType.Null;
            this.l = 0;
            this.s = null;
            this.refValue = null;
        }

        public bool IsNull
        {
            get { return this.type == DataValueType.Null; }
        }

        public static DataValue Add(DataValue x, DataValue y)
        {
            switch (x.type)
            {
                case DataValueType.DateTime:
                    switch (y.type)
                    {
                        case DataValueType.Integer:
                            return DataValue.New(new DateTime(x.l + (y.l * TimeSpan.TicksPerDay)));

                        case DataValueType.Float:
                            return DataValue.New(new DateTime(x.l + (long)(DataValue.LongToDouble(y.l) * DataValue.SecondsPerDay) * TimeSpan.TicksPerSecond));
                    }
                    break;

                case DataValueType.Integer:
                    switch (y.type)
                    {
                        case DataValueType.Integer:
                            return DataValue.New(x.l + y.l);

                        case DataValueType.Float:
                            return DataValue.New((double)x.l + DataValue.LongToDouble(y.l));
                    }
                    break;

                case DataValueType.Float:
                    switch (y.type)
                    {
                        case DataValueType.Integer:
                            return DataValue.New(DataValue.LongToDouble(x.l) + (double)y.l);

                        case DataValueType.Float:
                            return DataValue.New(DataValue.LongToDouble(x.l) + DataValue.LongToDouble(y.l));
                    }
                    break;

                case DataValueType.String:
                    if (y.type == DataValueType.String)
                        return DataValue.New(x.s + y.s);
                    break;

            }
            throw new DataException("Add cannot be applied to {0} and {1}", x.type, y.type);
        }

        public bool ContainsName(string name)
        {
            if (this.type != DataValueType.Record)
                throw new DataException("{0} type expected", DataValueType.Record);
            return this.refValue.Contains(name);
        }

        public void Add(DataValue value)
        {
            if (this.type != DataValueType.Array)
                throw new DataException("{0} type expected", DataValueType.Array);
            this.refValue.Add(value);
        }

        public static DataValue Sub(DataValue x, DataValue y)
        {
            switch (x.type)
            {
                case DataValueType.DateTime:
                    switch (y.type)
                    {
                        case DataValueType.Integer:
                            return DataValue.New(new DateTime(x.l - (y.l * TimeSpan.TicksPerDay)));

                        case DataValueType.Float:
                            return DataValue.New(new DateTime(x.l - (long)(DataValue.LongToDouble(y.l) * DataValue.SecondsPerDay) * TimeSpan.TicksPerSecond));
                    }
                    break;

                case DataValueType.Integer:
                    switch (y.type)
                    {
                        case DataValueType.Integer:
                            return DataValue.New(x.l - y.l);

                        case DataValueType.Float:
                            return DataValue.New((double)x.l - DataValue.LongToDouble(y.l));
                    }
                    break;

                case DataValueType.Float:
                    switch (y.type)
                    {
                        case DataValueType.Integer:
                            return DataValue.New(DataValue.LongToDouble(x.l) - (double)y.l);

                        case DataValueType.Float:
                            return DataValue.New(DataValue.LongToDouble(x.l) - DataValue.LongToDouble(y.l));
                    }
                    break;

            }
            throw new DataException("Sub cannot be applied to {0} and {1}", x.type, y.type);
        }

        public static DataValue Mul(DataValue x, DataValue y)
        {
            switch (x.type)
            {

                case DataValueType.Integer:
                    switch (y.type)
                    {
                        case DataValueType.Integer:
                            return DataValue.New(x.l * y.l);

                        case DataValueType.Float:
                            return DataValue.New((double)x.l * DataValue.LongToDouble(y.l));
                    }
                    break;

                case DataValueType.Float:
                    switch (y.type)
                    {
                        case DataValueType.Integer:
                            return DataValue.New(DataValue.LongToDouble(x.l) * (double)y.l);

                        case DataValueType.Float:
                            return DataValue.New(DataValue.LongToDouble(x.l) * DataValue.LongToDouble(y.l));
                    }
                    break;

            }
            throw new DataException("Mul cannot be applied to {0} and {1}", x.type, y.type);
        }

        public static DataValue Div(DataValue x, DataValue y)
        {
            switch (x.type)
            {

                case DataValueType.Integer:
                    switch (y.type)
                    {
                        case DataValueType.Integer:
                            return DataValue.New(x.l / y.l);

                        case DataValueType.Float:
                            return DataValue.New((double)x.l / DataValue.LongToDouble(y.l));
                    }
                    break;

                case DataValueType.Float:
                    switch (y.type)
                    {
                        case DataValueType.Integer:
                            return DataValue.New(DataValue.LongToDouble(x.l) / (double)y.l);

                        case DataValueType.Float:
                            return DataValue.New(DataValue.LongToDouble(x.l) / DataValue.LongToDouble(y.l));
                    }
                    break;

            }
            throw new DataException("Div cannot be applied to {0} and {1}", x.type, y.type);
        }

        public static DataValue BitOr(DataValue x, DataValue y)
        {
            switch (x.type)
            {
                case DataValueType.Integer:
                    if (y.type == DataValueType.Integer)
                        return DataValue.New(x.l | y.l);

                    break;

            }
            throw new DataException("Bit OR cannot be applied to {0} and {1}", x.type, y.type);
        }

        public static DataValue BitAnd(DataValue x, DataValue y)
        {
            switch (x.type)
            {
                case DataValueType.Integer:
                    if (y.type == DataValueType.Integer)
                        return DataValue.New(x.l & y.l);

                    break;

            }
            throw new DataException("Bit AND cannot be applied to {0} and {1}", x.type, y.type);
        }

        public static DataValue BitXor(DataValue x, DataValue y)
        {
            switch (x.type)
            {
                case DataValueType.Integer:
                    if (y.type == DataValueType.Integer)
                        return DataValue.New(x.l ^ y.l);

                    break;

            }
            throw new DataException("Bit XOR cannot be applied to {0} and {1}", x.type, y.type);
        }

        public static DataValue LOr(DataValue x, DataValue y)
        {
            switch (x.type)
            {
                case DataValueType.Logic:
                    if (y.type == DataValueType.Logic)
                        return DataValue.New(((x.l | y.l) & 0x1) > 0);

                    break;

            }
            throw new DataException("Logic OR cannot be applied to {0} and {1}", x.type, y.type);
        }

        public static DataValue LAnd(DataValue x, DataValue y)
        {
            switch (x.type)
            {
                case DataValueType.Logic:
                    if (y.type == DataValueType.Logic)
                        return DataValue.New(((x.l & y.l) & 0x1) > 0);

                    break;

            }
            throw new DataException("Logic AND cannot be applied to {0} and {1}", x.type, y.type);
        }

        public static DataValue LNot(DataValue x)
        {
            if (x.type == DataValueType.Logic)
                return DataValue.New((x.l & 0x1) == 0);
            throw new DataException("Logic NOT cannot be applied to {0}", x.type);
        }

        public static DataValue Negative(DataValue x)
        {
            if (x.type == DataValueType.Integer)
                return DataValue.New(-x.l);
            if (x.type == DataValueType.Float)
                return DataValue.New(-DataValue.LongToDouble(x.l));
            throw new DataException("Negative cannot be applied to {0}", x.type);
        }

        public static DataValue Positive(DataValue x)
        {
            if (x.type == DataValueType.Integer)
                return DataValue.New(x.l);
            if (x.type == DataValueType.Float)
                return DataValue.New(DataValue.LongToDouble(x.l));
            throw new DataException("Positive cannot be applied to {0}", x.type);
        }

        public new ulong GetHashCode()
        {
            return this.AddCrc64(Crc64.InitialCrc);
        }

        private ulong AddCrc64(ulong crc)
        {
            crc = Crc64.Update((int)this.type, crc);
            switch (this.type)
            {
                case DataValueType.Array:
                    for (int i = 0; i < this.refValue.Count; i++)
                        crc = this.refValue.Array[i].Value.AddCrc64(crc);
                    break;

                case DataValueType.Record:
                    for (int i = 0; i < this.refValue.Count; i++)
                    {
                        crc = Crc64.Update(this.refValue.Array[i].Name, crc);
                        crc = this.refValue.Array[i].Value.AddCrc64(crc);
                    }
                    break;

                case DataValueType.Binary:
                    if (this.refValue.Binary != null)
                        crc = Crc64.Update(this.refValue.Binary, crc);
                    break;

                case DataValueType.String:
                    if (this.s != null)
                        crc = Crc64.Update(this.s, crc);
                    break;

                case DataValueType.DateTime:
                case DataValueType.Float:
                case DataValueType.Integer:
                case DataValueType.Reference:
                    crc = Crc64.Update((ulong)this.l, crc);
                    break;

            }
            return crc;
        }

        public void Serialize(IDataWriteStream stream)
        {
            this.Serialize(stream, null);
        }

        public void Serialize(IDataWriteStream stream, ISymbolProvider symbols)
        {
            stream.Write(DataValue.Signature);
            stream.Write((int)this.type);
            switch (this.type)
            {
                case DataValueType.Array:
                    stream.Write(this.refValue.Count);
                    for (int i = 0; i < this.refValue.Count; i++)
                        this.refValue.Array[i].Value.Serialize(stream, symbols);
                    break;

                case DataValueType.Binary:
                    stream.Write(this.refValue.Binary);
                    break;

                case DataValueType.DateTime:
                case DataValueType.Float:
                case DataValueType.Integer:
                case DataValueType.Reference:
                    stream.Write(this.l);
                    break;

                case DataValueType.Logic:
                    {
                        int x = this.l == 1L ? 1 : 0;
                        stream.Write(x);
                    }
                    break;

                case DataValueType.String:
                    stream.Write(this.s);
                    break;

                case DataValueType.Record:
                    stream.Write(this.refValue.Count);
                    if (symbols == null)
                    {
                        for (int i = 0; i < this.refValue.Count; i++)
                        {
                            stream.Write(this.refValue.Array[i].Name);
                            this.refValue.Array[i].Value.Serialize(stream, symbols);
                        }
                    }
                    else
                    {
                        for (int i = 0; i < this.refValue.Count; i++)
                        {
                            stream.Write(symbols.Get(this.refValue.Array[i].Name));
                            this.refValue.Array[i].Value.Serialize(stream, symbols);
                        }
                    }
                    break;
            }
        }

        public void Deserialize(IDataReadStream stream)
        {
            this.Deserialize(stream, null);
        }

        public void Deserialize(IDataReadStream stream, ISymbolProvider symbols)
        {
            this.type = DataValueType.Undefined;
            this.s = null;
            this.l = 0;
            this.refValue = null;
            int x = stream.ReadInt();
            if (x != DataValue.Signature)
                throw new DataException("Invalid data value serialization signature");
            x = stream.ReadInt();
            switch (x)
            {
                case (int)DataValueType.Array:
                    {
                        this.type = DataValueType.Array;
                        int l = stream.ReadInt();
                        this.refValue = new DataValueRefValue(l);
                        this.refValue.Count = l;
                        DataValueArrayItem[] arr = this.refValue.Array;
                        for (int i = 0; i < l; i++)
                        {
                            if (arr[i].Value == null)
                                arr[i].Value = DataValue.Null;
                            arr[i].Value.Deserialize(stream, symbols);
                        }
                    }
                    break;

                case (int)DataValueType.Binary:
                    this.type = DataValueType.Binary;
                    this.refValue = new DataValueRefValue();
                    this.refValue.Binary = stream.ReadBytes();
                    break;

                case (int)DataValueType.DateTime:
                    this.type = DataValueType.DateTime;
                    this.l = stream.ReadLong();
                    break;

                case (int)DataValueType.Float:
                    this.type = DataValueType.Float;
                    this.l = stream.ReadLong();
                    break;

                case (int)DataValueType.Integer:
                    this.type = DataValueType.Integer;
                    this.l = stream.ReadLong();
                    break;

                case (int)DataValueType.Reference:
                    this.type = DataValueType.Reference;
                    this.l = stream.ReadLong();
                    break;

                case (int)DataValueType.Null:
                    this.type = DataValueType.Null;
                    break;

                case (int)DataValueType.Logic:
                    this.type = DataValueType.Logic;
                    this.l = (stream.ReadInt() == 1) ? 1L : 0L;
                    break;

                case (int)DataValueType.String:
                    this.type = DataValueType.String;
                    this.s = stream.ReadString();
                    break;

                case (int)DataValueType.Record:
                    {
                        this.type = DataValueType.Record;
                        int l = stream.ReadInt();
                        this.refValue = new DataValueRefValue(l);
                        this.refValue.Count = l;
                        DataValueArrayItem[] arr = this.refValue.Array;
                        if (symbols == null)
                        {
                            for (int i = 0; i < l; i++)
                            {
                                if (arr[i].Value == null)
                                    arr[i].Value = DataValue.Null;
                                arr[i].Name = stream.ReadString();
                                arr[i].Value.Deserialize(stream, symbols);
                            }
                        }
                        else
                        {
                            for (int i = 0; i < l; i++)
                            {
                                if (arr[i].Value == null)
                                    arr[i].Value = DataValue.Null;
                                arr[i].Name = symbols.Get(stream.ReadInt());
                                arr[i].Value.Deserialize(stream, symbols);
                            }
                        }
                    }
                    break;

                default:
                    throw new DataException("Invalid data value type {0}", x);
            }
        }

        public static DataValue Null
        {
            get
            {
                return new DataValue();
            }
        }

        public static DataValue New(long value)
        {
            DataValue v = new DataValue();
            v.type = DataValueType.Integer;
            v.l = value;
            return v;
        }

        public static DataValue New(DateTime value)
        {
            DataValue v = new DataValue();
            v.type = DataValueType.DateTime;
            v.l = value.Ticks;
            return v;
        }

        public static DataValue New(bool value)
        {
            DataValue v = new DataValue();
            v.type = DataValueType.Logic;
            v.l = value ? 1L : 0L;
            return v;
        }

        public static DataValue New(string value)
        {
            DataValue v = new DataValue();
            v.type = DataValueType.String;
            v.s = value;
            return v;
        }

        public static DataValue New(double value)
        {
            DataValue v = new DataValue();
            v.type = DataValueType.Float;
            v.l = DataValue.DoubleToLong(value);
            return v;
        }

        public static DataValue New(byte[] value)
        {
            DataValue v = new DataValue();
            v.type = DataValueType.Binary;
            v.refValue = new DataValueRefValue();
            v.refValue.Binary = value;
            return v;
        }

        public static DataValue Array(int size)
        {
            DataValue v = new DataValue();
            v.type = DataValueType.Array;
            v.refValue = new DataValueRefValue(size);
            return v;
        }

        public static DataValue Record()
        {
            return DataValue.Record(0);
        }

        public static DataValue Record(int size)
        {
            DataValue v = new DataValue();
            v.type = DataValueType.Record;
            v.refValue = new DataValueRefValue(size);
            return v;
        }

        public static DataValue Record(long reference, string className)
        {
            return DataValue.Record(reference, className, 0);
        }

        public static DataValue Record(long id, string className, int size)
        {
            DataValue v = new DataValue();
            v.type = DataValueType.Record;
            v.refValue = new DataValueRefValue(size);
            v.refValue.Id = id;
            v.refValue.Class = className;
            return v;
        }

        public string[] AllNames
        {
            get
            {
                if (this.type != DataValueType.Record)
                    throw new DataException("{0} type expected", DataValueType.Record);
                string[] n = new string[this.refValue.Count];
                for (int i = 0; i < n.Length; i++)
                    n[i] = this.refValue.Array[i].Name;
                return n;
            }
        }

        public DataValue Subrecord(string[] names)
        {
            if (this.type != DataValueType.Record)
                throw new DataException("{0} type expected", DataValueType.Record);
            DataValue v = DataValue.Record(names.Length);
            for (int i = 0; i < names.Length; i++)
                v[names[i]] = this[names[i]];
            return v;
        }

        public DataValueType Type
        {
            get { return this.type; }
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

        public double Float
        {
            get
            {
                if (this.type != DataValueType.Float)
                    throw new DataException("{0} type expected", DataValueType.Float);
                return DataValue.LongToDouble(this.l);
            }
            set
            {
                this.CheckType(DataValueType.Float);
                this.l = DataValue.DoubleToLong(value);
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

        public byte[] Binary
        {
            get
            {
                if (this.type != DataValueType.Binary)
                    throw new DataException("{0} type expected", DataValueType.Binary);
                return this.refValue.Binary;
            }
            set
            {
                if (this.type != DataValueType.Binary)
                {
                    this.CheckType(DataValueType.Binary);
                    this.refValue = new DataValueRefValue();
                }
                this.refValue.Binary = value;
            }
        }

        public long Reference
        {
            get
            {
                if (this.type == DataValueType.Reference)
                    return this.l;
                throw new DataException("Not a reference type");
            }
            set
            {
                this.CheckType(DataValueType.Reference);
                this.l = value;
            }
        }

        public DataValue GetReference()
        {
            if (this.type != DataValueType.Record)
                throw new DataException("Record type expected");
            DataValue v = DataValue.Null;
            v.Reference = this.refValue.Id;
            return v;
        }


        public string Class
        {
            get
            {
                if (this.type != DataValueType.Record)
                    throw new DataException("Record type expected");
                return this.refValue.Class;
            }
            set
            {
                if (this.type != DataValueType.Record)
                    throw new DataException("Record type expected");
                this.refValue.Class = value;
            }
        }

        public long Id
        {
            get
            {
                if (this.type != DataValueType.Record)
                    throw new DataException("Record type expected");
                return this.refValue.Id;
            }
            set
            {
                if (this.type != DataValueType.Record)
                    throw new DataException("Record type expected");
                this.refValue.Id = value;
            }
        }

        public int Length
        {
            get
            {
                switch (this.type)
                {
                    case DataValueType.String:
                        return (this.s == null) ? -1 : s.Length;

                    case DataValueType.Binary:
                        return (this.refValue.Binary == null) ? -1 : this.refValue.Binary.Length;

                    case DataValueType.Array:
                    case DataValueType.Record:
                        return this.refValue.Count;
                }
                return -1;
            }
        }

        public DataValue this[int i]
        {
            get
            {
                if (this.type != DataValueType.Array)
                    throw new DataException("{0} type expected", DataValueType.Array);
                if ((i < 0) || (i >= this.refValue.Count))
                    throw new IndexOutOfRangeException("Out of array index");
                return this.refValue.Array[i].Value;
            }
            set
            {
                if (this.type != DataValueType.Array)
                    throw new DataException("{0} type expected", DataValueType.Array);
                if ((i < 0) || (i >= this.refValue.Count))
                    throw new IndexOutOfRangeException("Out of array index");
                this.refValue.Array[i].Value.Assign(value);
            }
        }

        public DataValue this[string name]
        {
            get
            {
                if (this.type != DataValueType.Record)
                    throw new DataException("{0} type expected", DataValueType.Record);
                return this.refValue.Get(name);
            }
            set
            {
                if (this.type != DataValueType.Record)
                    throw new DataException("{0} type expected", DataValueType.Record);
                this.refValue.Set(name, value);
            }
        }

        public void Assign(DataValue value)
        {
            this.type = value.type;
            this.l = value.l;
            this.s = value.s;
            this.refValue = value.refValue;
        }

        public DataValue Copy()
        {
            DataValue v = new DataValue();
            v.Assign(this);
            return v;
        }

        public override string ToString()
        {
            switch (this.type)
            {
                case DataValueType.Array:
                    return String.Format("Array [{0}]", this.refValue.Count);

                case DataValueType.Binary:
                    return String.Format("Binary {0}", this.refValue.Binary == null ? "null" : String.Format("[{0}]", this.refValue.Binary.Length));

                case DataValueType.DateTime:
                    return this.DateTime.ToString();

                case DataValueType.Float:
                    return this.Float.ToString();

                case DataValueType.Integer:
                    return this.l.ToString();

                case DataValueType.Logic:
                    return this.Logic ? "true" : "false";

                case DataValueType.Null:
                    return "null";

                case DataValueType.Record:
                    return String.Format("Record ({0})", this.refValue.Count);

                case DataValueType.Reference:
                    return String.Format("@{0}", this.l);

                case DataValueType.String:
                    return this.s;

            }
            return "?";
        }

        public static unsafe double LongToDouble(long l)
        {
            return *((double*)(&l));
        }

        public static unsafe long DoubleToLong(double d)
        {
            return *((long*)(&d));
        }

        public static int Compare(DataValue x, DataValue y)
        {
            switch (x.type)
            {
                case DataValueType.Null:
                    return (y.type == DataValueType.Null) ? 0 : 1;

                case DataValueType.String:
                    if (y.type == DataValueType.String)
                        return String.Compare(x.s, y.s);
                    break;

                case DataValueType.Array:
                    if (y.type == DataValueType.Array) return x.refValue.Count.CompareTo(y.refValue.Count);
                    break;

                case DataValueType.Record:
                    if (y.type == DataValueType.Record) return x.refValue.Count.CompareTo(y.refValue.Count);
                    break;

                case DataValueType.Binary:
                    if (y.type == DataValueType.Binary)
                    {
                        int xl = (x.refValue.Binary == null) ? -1 : x.refValue.Binary.Length;
                        int yl = (y.refValue.Binary == null) ? -1 : y.refValue.Binary.Length;
                        return xl.CompareTo(yl);
                    }
                    break;

                case DataValueType.DateTime:
                    if (y.type == DataValueType.DateTime)
                        return x.l.CompareTo(y.l);
                    break;

                case DataValueType.Logic:
                    if (y.type == DataValueType.Logic)
                        return x.l.CompareTo(y.l);
                    break;

                case DataValueType.Float:
                    if (y.type == DataValueType.Float)
                    {
                        return DataValue.LongToDouble(x.l).CompareTo(DataValue.LongToDouble(y.l));
                    }
                    else if (y.type == DataValueType.Integer)
                    {
                        return DataValue.LongToDouble(x.l).CompareTo((double)y.l);
                    }
                    break;

                case DataValueType.Integer:
                    if (y.type == DataValueType.Float)
                    {
                        return ((double)x.l).CompareTo(DataValue.LongToDouble(y.l));
                    }
                    else if (y.type == DataValueType.Integer)
                    {
                        return x.l.CompareTo(y.l);
                    }
                    break;

                case DataValueType.Reference:
                    if (y.type == DataValueType.Reference)
                        return x.l.CompareTo(y.l);
                    break;
            }
            if (y.type == DataValueType.Null)
                return (x.type == DataValueType.Null) ? 0 : -1;
            throw new DataException("Types {0} and {1} cannot be compared", x.type, y.type);
        }

        private void CheckType(DataValueType t)
        {
            if (this.type == t) return;
            this.l = 0;
            this.s = null;
            this.refValue = null;
            this.type = t;
        }
    }

    struct DataValueArrayItem
    {
        public string Name;
        public DataValue Value;

        public override string ToString()
        {
            if (this.Name == null)
                return this.Value.ToString();
            return String.Format("'{0}' = {1}", this.Name, this.Value);
        }
    }

    sealed class DataValueRefValue
    {
        public long Id;
        public string Class;
        public DataValueArrayItem[] Array;
        public byte[] Binary;
        public int Count;

        private const int MinArraySize = 32;
        private const int ArrayGrowSize = 32;

        public DataValueRefValue()
        {
            this.Array = null;
            this.Binary = null;
            this.Count = 0;
            this.Class = null;
            this.Id = 0;
        }

        public DataValueRefValue(int size)
        {
            this.Class = null;
            this.Id = 0;
            this.Array = new DataValueArrayItem[size <= 0 ? DataValueRefValue.MinArraySize : size];
            this.Binary = null;
            this.Count = 0;
        }

        public DataValue Get(string name)
        {
            int p;
            if (this.Find(name, out p))
                return this.Array[p].Value;
            return DataValue.Null;
        }

        public void Add(DataValue value)
        {
            if (this.Count >= this.Array.Length) this.ResizeArray(this.Array.Length + DataValueRefValue.ArrayGrowSize);
            this.Array[this.Count++].Value = value;
        }

        public bool Contains(string name)
        {
            int p;
            return this.Find(name, out p);
        }

        public void Set(string name, DataValue value)
        {
            int p;
            if (this.Find(name, out p))
            {
                this.Array[p].Value.Assign(value);
                return;
            }
            if (this.Count >= this.Array.Length) this.ResizeArray(this.Array.Length + DataValueRefValue.ArrayGrowSize);
            int i = this.Count;
            while (i > p)
            {
                this.Array[i] = this.Array[i - 1];
                i--;
            }
            this.Array[p].Name = name;
            this.Array[p].Value = DataValue.Null;
            this.Array[p].Value.Assign(value);
            ++this.Count;
        }

        private void ResizeArray(int size)
        {
            DataValueArrayItem[] a = new DataValueArrayItem[size];
            int i = 0;
            while ((i < a.Length) && (i < this.Array.Length))
            {
                a[i] = this.Array[i];
                i++;
            }
            this.Array = null;
            this.Array = a;
            a = null;
        }

        private bool Find(string name, out int position)
        {
            position = 0;
            if (this.Count == 0)
                return false;
            int min = 0;
            int max = this.Count - 1;
            while (true)
            {
                int mid = (min + max) >> 1;
                int r = String.Compare(name, this.Array[mid].Name, true);
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


    }

}