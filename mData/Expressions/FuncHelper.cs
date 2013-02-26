using System;
using System.Text;
using mData.Value;
using mData.DataStructures;

namespace mData.Expressions
{
    [Flags]
    public enum FuncParamCheckFlags : ushort
    {
        Empty = 0x0000,
        String = 0x0001,
        Integer = 0x0002,
        Float = 0x0004,
        Numeric = 0x0008,
        DateTime = 0x0010,
        Logic = 0x0020,
        Binary = 0x0040,
        Record = 0x0080,
        Array = 0x0100,
        Reference = 0x0200,
        NotNull = 0x1000,
        MustBeSupplied = 0x2000,
        AnyType = 0x4000
    }

    public sealed class FuncHelper
    {
        private ArrayTree<string, Func<DataValue[], DataValue>> funcs;
        private FuncHelper previous;

        public FuncHelper(FuncHelper previous)
        {
            this.previous = previous;
            this.funcs = new ArrayTree<string, Func<DataValue[], DataValue>>(FuncHelper.Compare, null, 2048, PageFactor.Page128);
        }

        private static int Compare(string x, string y)
        {
            return String.Compare(x, y, true);
        }

        public void Add(string name, Func<DataValue[], DataValue> func)
        {
            this.funcs.Insert(name, func);
        }

        public DataValue Call(string name, FuncCallContext context)
        {
            Func<DataValue[], DataValue> f;
            if (this.funcs.TryGetValue(name, out f))
                return f(context.Params);
            if (this.previous == null)
                throw new ArgumentException(String.Format("Function '{0}' is not defined", name));
            return this.previous.Call(name, context);
        }

        public static FuncHelper Default()
        {
            FuncHelper h = new FuncHelper(null);

            h.Add("print", FuncHelper.FuncPrint);
            h.Add("str", FuncHelper.FuncStr);
            h.Add("concat", FuncHelper.FuncConcat);
            h.Add("strlen", FuncHelper.FuncStrlen);
            h.Add("substr", FuncHelper.FuncSubstr);
            h.Add("year", FuncHelper.FuncYear);
            h.Add("month", FuncHelper.FuncMonth);
            h.Add("day", FuncHelper.FuncDay);
            h.Add("int", FuncHelper.FuncInt);

            return h;
        }

        private static DataValue FuncYear(DataValue[] param)
        {
            FuncHelper.CheckFuncParams(param, FuncParamCheckFlags.DateTime | FuncParamCheckFlags.MustBeSupplied);
            return DataValue.New((long)param[0].DateTime.Year);
        }

        private static DataValue FuncMonth(DataValue[] param)
        {
            FuncHelper.CheckFuncParams(param, FuncParamCheckFlags.DateTime | FuncParamCheckFlags.MustBeSupplied);
            return DataValue.New((long)param[0].DateTime.Month);
        }

        private static DataValue FuncDay(DataValue[] param)
        {
            FuncHelper.CheckFuncParams(param, FuncParamCheckFlags.DateTime | FuncParamCheckFlags.MustBeSupplied);
            return DataValue.New((long)param[0].DateTime.Day);
        }

        private static DataValue FuncInt(DataValue[] param)
        {
            FuncHelper.CheckFuncParams(param, FuncParamCheckFlags.Numeric | FuncParamCheckFlags.MustBeSupplied);
            if (param[0].Type == DataValueType.Integer) return param[0];
            return DataValue.New((long)param[0].Float);
        }

        private static DataValue FuncSubstr(DataValue[] param)
        {
            FuncHelper.CheckFuncParams(param, 
                FuncParamCheckFlags.String | FuncParamCheckFlags.MustBeSupplied,
                FuncParamCheckFlags.Integer | FuncParamCheckFlags.MustBeSupplied,
                FuncParamCheckFlags.Integer);

            string s = param[0].String;
            if (s != null)
            {
                int p = (int)param[1].Integer;
                int l = 0;
                if (param.Length > 2)
                {
                    l = (int)param[2].Integer;
                }
                else
                {
                    l = s.Length - p;
                }
                s = s.Substring(p, l);
            }
            return DataValue.New(s);
        }

        private static DataValue FuncStrlen(DataValue[] param)
        {
            FuncHelper.CheckFuncParams(param, FuncParamCheckFlags.String | FuncParamCheckFlags.MustBeSupplied);
            string s = param[0].String;
            if (s == null) return DataValue.New(-1L);
            return DataValue.New((long)s.Length);
        }

        private static DataValue FuncPrint(DataValue[] param)
        {
            if (param != null)
            {
                for (int i = 0; i < param.Length; i++)
                    Console.WriteLine(param[i].ToString());
            }
            return DataValue.Null;
        }

        private static DataValue FuncConcat(DataValue[] param)
        {
            if (param == null) return DataValue.Null;
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < param.Length; i++)
            {
                if (param[i].IsNull) continue;
                if (param[i].Type != DataValueType.String)
                    throw new ArgumentException("Only strings can be concatenated");
                sb.Append(param[i].String);
            }
            return DataValue.New(sb.ToString());
        }

        private static DataValue FuncStr(DataValue[] param)
        {
            FuncHelper.CheckFuncParams(param, 
                FuncParamCheckFlags.String | FuncParamCheckFlags.Numeric | FuncParamCheckFlags.Logic | FuncParamCheckFlags.DateTime | FuncParamCheckFlags.NotNull, 
                FuncParamCheckFlags.String);
            string fmt = null;
            if (param.Length > 1)
                if (param[1].Type == DataValueType.String)
                    fmt = param[1].String;
            string s = null;
            switch (param[0].Type)
            {
                case DataValueType.Float:
                    s = (fmt == null) ? param[0].Float.ToString() : param[0].Float.ToString(fmt);
                    break;

                case DataValueType.Integer:
                    s = (fmt == null) ? param[0].Integer.ToString() : param[0].Integer.ToString(fmt);
                    break;

                case DataValueType.DateTime:
                    s = (fmt == null) ? param[0].DateTime.ToString() : param[0].DateTime.ToString(fmt);
                    break;

                case DataValueType.String:
                    s = param[0].String;
                    break;
                    
                case DataValueType.Logic:
                    s = param[0].Logic ? "true" : "false";
                    break;
            }
            return DataValue.New(s);
        }

        public static void CheckFuncParams(DataValue[] param, FuncParamCheckFlags p1)
        {
            FuncHelper.CheckFuncParams(param, new FuncParamCheckFlags[1] { p1 });
        }

        public static void CheckFuncParams(DataValue[] param, FuncParamCheckFlags p1, FuncParamCheckFlags p2)
        {
            FuncHelper.CheckFuncParams(param, new FuncParamCheckFlags[2] { p1, p2 });
        }

        public static void CheckFuncParams(DataValue[] param, FuncParamCheckFlags p1, FuncParamCheckFlags p2, FuncParamCheckFlags p3)
        {
            FuncHelper.CheckFuncParams(param, new FuncParamCheckFlags[3] { p1, p2, p3 });
        }

        public static void CheckFuncParams(DataValue[] param, FuncParamCheckFlags p1, FuncParamCheckFlags p2, FuncParamCheckFlags p3, FuncParamCheckFlags p4)
        {
            FuncHelper.CheckFuncParams(param, new FuncParamCheckFlags[4] { p1, p2, p3, p4 });
        }

        public static void CheckFuncParams(DataValue[] param, FuncParamCheckFlags[] flags)
        {
            if (param.Length > flags.Length)
                throw new ArgumentException("Unexpected parameter(s)");
            for (int i = 0; i < flags.Length; i++)
            {
                FuncParamCheckFlags f = flags[i];
                if (i >= param.Length)
                {
                    if ((f & FuncParamCheckFlags.MustBeSupplied) != 0)
                        throw new ArgumentException(String.Format("Parameter ({0}) must be supplied", i + 1));
                    continue;
                }
                if (param[i].IsNull)
                {
                    if ((f & FuncParamCheckFlags.NotNull) != 0)
                        throw new ArgumentException(String.Format("Parameter ({0}) must not be null", i + 1));
                    continue;
                }
                if ((f & FuncParamCheckFlags.AnyType) != 0)
                    continue;
                switch (param[i].Type)
                {
                    case DataValueType.String:
                        if ((f & FuncParamCheckFlags.String) == 0)
                            throw new ArgumentException(String.Format("Parameter ({0}) must be string", i + 1));
                        break;

                    case DataValueType.Array:
                        if ((f & FuncParamCheckFlags.Array) == 0)
                            throw new ArgumentException(String.Format("Parameter ({0}) must be array", i + 1));
                        break;

                    case DataValueType.Binary:
                        if ((f & FuncParamCheckFlags.Binary) == 0)
                            throw new ArgumentException(String.Format("Parameter ({0}) must be binary", i + 1));
                        break;

                    case DataValueType.DateTime:
                        if ((f & FuncParamCheckFlags.DateTime) == 0)
                            throw new ArgumentException(String.Format("Parameter ({0}) must be date/time", i + 1));
                        break;

                    case DataValueType.Float:
                        if (((f & FuncParamCheckFlags.Float) == 0) && ((f & FuncParamCheckFlags.Numeric) == 0))
                            throw new ArgumentException(String.Format("Parameter ({0}) must be float/numeric", i + 1));
                        break;

                    case DataValueType.Integer:
                        if (((f & FuncParamCheckFlags.Integer) == 0) && ((f & FuncParamCheckFlags.Numeric) == 0))
                            throw new ArgumentException(String.Format("Parameter ({0}) must be integer/numeric", i + 1));
                        break;

                    case DataValueType.Logic:
                        if ((f & FuncParamCheckFlags.Logic) == 0)
                            throw new ArgumentException(String.Format("Parameter ({0}) must be logic", i + 1));
                        break;

                    case DataValueType.Record:
                        if ((f & FuncParamCheckFlags.Record) == 0)
                            throw new ArgumentException(String.Format("Parameter ({0}) must be record", i + 1));
                        break;

                    case DataValueType.Reference:
                        if ((f & FuncParamCheckFlags.Reference) == 0)
                            throw new ArgumentException(String.Format("Parameter ({0}) must be reference", i + 1));
                        break;
                }
            }
        }

    }
}