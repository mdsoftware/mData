using System;
using mData.Value;

namespace mData.Expressions
{

    public struct FuncCallContext
    {
        private DataValue[] stack;
        private int stackTop;
        private int paramCount;

        public FuncCallContext(DataValue[] stack, int stackTop, int paramCount)
        {
            this.stack = stack;
            this.stackTop = stackTop;
            this.paramCount = paramCount;
        }

        public int Count
        {
            get { return this.paramCount; }
        }

        public DataValue this[int param]
        {
            get
            {
                if (param >= this.paramCount)
                    throw new ArgumentException(System.String.Format("Parameter {0} is not supplies", param));
                return this.stack[this.stackTop + this.paramCount - param - 1];
            }
        }

        public DataValue[] Params
        {
            get
            {
                if (this.paramCount == 0) return null;
                DataValue[] p = new DataValue[this.paramCount];
                for (int i = 0; i < this.paramCount; i++)
                    p[i] = this.stack[this.stackTop + this.paramCount - i - 1];
                return p;
            }
        }

        public long Integer(int param)
        {
            return this[param].Integer;
        }

        public double Float(int param)
        {
            DataValue p = this[param];
            if (p.Type == DataValueType.Integer)
                return (double)p.Integer;
            return p.Float;
        }

        public bool Logic(int param)
        {
            return this[param].Logic;
        }

        public string String(int param)
        {
            return this[param].String;
        }

        public DateTime DateTime(int param)
        {
            return this[param].DateTime;
        }

        public long this[int param, long nullValue]
        {
            get
            {
                DataValue p = this[param];
                if (p.IsNull)
                    return nullValue;
                return p.Integer;
            }
        }

        public double this[int param, double nullValue]
        {
            get
            {
                DataValue p = this[param];
                if (p.IsNull)
                    return nullValue;
                if (p.Type == DataValueType.Integer)
                    return (double)p.Integer;
                return p.Float;
            }
        }

        public bool this[int param, bool nullValue]
        {
            get
            {
                DataValue p = this[param];
                if (p.IsNull)
                    return nullValue;
                return p.Logic;
            }
        }

        public DateTime this[int param, DateTime nullValue]
        {
            get
            {
                DataValue p = this[param];
                if (p.IsNull)
                    return nullValue;
                return p.DateTime;
            }
        }

        public string this[int param, string nullValue]
        {
            get
            {
                DataValue p = this[param];
                if (p.IsNull)
                    return nullValue;
                return p.String;
            }
        }
    }
}