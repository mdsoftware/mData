using System;
using System.IO;
using mData.Value;

namespace mData.Expressions
{

    public interface IExpressionReader
    {
        string Next();
    }

    public interface ITextWriter
    {
        void Write(string s);
        void WriteLine(string s);
    }

    public sealed class Expression
    {
        private byte[] code;
        private string[] strings;

        public const int StackSize = 0x200;

        Expression(byte[] code, string[] strings)
        {
            this.code = code;
            this.strings = strings;
        }

        public DataValue Execute(DataValue context, Func<string, FuncCallContext, DataValue> functions)
        {
            DataValue[] stack = new DataValue[Expression.StackSize];
            return this.Execute(context, stack, stack.Length, functions);
        }

        public static bool IsSymbol(string s)
        {
            return Parser.IsSymbol(s);
        }

        public DataValue Execute(DataValue context, DataValue[] stack, int sp, Func<string, FuncCallContext, DataValue> functions)
        {
            int p = 0;
            while (true)
            {
                byte b = this.code[p];

                if ((b < 100) || (b >= 200))
                    throw new DataException("Invalid opcode {0}: {1} (0x{2})", p.ToString("d6"), b, b.ToString("x2"));

                p++;

                bool stop = false;

                switch ((OpCode)b)
                {
                    case OpCode.Call:
                        {
                            string s = this.strings[this.GetShort(p)];
                            p += 2;
                            int args = (int)this.GetShort(p);
                            p += 2;
                            int spp = sp + args - 1;
                            stack[spp] = functions(s, new FuncCallContext(stack, sp, args));
                            sp = spp;
                        }
                        break;

                    case OpCode.Jmp:
                        p = p + (int)this.GetShort(p);
                        break;

                    case OpCode.JmpIfTrue:
                        if (stack[sp++].Logic)
                        {
                            p = p + (int)this.GetShort(p);
                        }
                        else
                        {
                            p += 2;
                        }
                        break;

                    case OpCode.JmpIfFalse:
                        if (!stack[sp++].Logic)
                        {
                            p = p + (int)this.GetShort(p);
                        }
                        else
                        {
                            p += 2;
                        }
                        break;

                    case OpCode.PushFloat:
                        stack[--sp] = DataValue.New(this.GetDouble(p));
                        p += 8;
                        break;

                    case OpCode.PushInt:
                        stack[--sp] = DataValue.New(this.GetLong(p));
                        p += 8;
                        break;

                    case OpCode.PushLogic:
                        stack[--sp] = DataValue.New(this.code[p++] == 0x1);
                        break;

                    case OpCode.PushNull:
                        stack[--sp] = DataValue.Null;
                        break;

                    case OpCode.PushString:
                        stack[--sp] = DataValue.New(this.strings[this.GetShort(p)]);
                        p += 2;
                        break;

                    case OpCode.PushSymbol:
                        stack[--sp] = context[this.strings[this.GetShort(p)]].Copy();
                        p += 2;
                        break;

                    case OpCode.Stop:
                        stop = true;
                        break;

                    case OpCode.Add:
                        sp++;
                        stack[sp] = DataValue.Add(stack[sp], stack[sp - 1]);
                        break;

                    case OpCode.BitAnd:
                        sp++;
                        stack[sp] = DataValue.BitAnd(stack[sp], stack[sp - 1]);
                        break;

                    case OpCode.BitOr:
                        sp++;
                        stack[sp] = DataValue.BitOr(stack[sp], stack[sp - 1]);
                        break;

                    case OpCode.BitXor:
                        sp++;
                        stack[sp] = DataValue.BitXor(stack[sp], stack[sp - 1]);
                        break;

                    case OpCode.Div:
                        sp++;
                        stack[sp] = DataValue.Div(stack[sp], stack[sp - 1]);
                        break;

                    case OpCode.Equal:
                        sp++;
                        stack[sp] = DataValue.New(DataValue.Compare(stack[sp], stack[sp - 1]) == 0);
                        break;

                    case OpCode.Great:
                        sp++;
                        stack[sp] = DataValue.New(DataValue.Compare(stack[sp], stack[sp - 1]) == 1);
                        break;

                    case OpCode.GreatEqual:
                        sp++;
                        stack[sp] = DataValue.New(DataValue.Compare(stack[sp], stack[sp - 1]) >= 0);
                        break;

                    case OpCode.LAnd:
                        sp++;
                        stack[sp] = DataValue.LAnd(stack[sp], stack[sp - 1]);
                        break;

                    case OpCode.Less:
                        sp++;
                        stack[sp] = DataValue.New(DataValue.Compare(stack[sp], stack[sp - 1]) == -1);
                        break;

                    case OpCode.LessEqual:
                        sp++;
                        stack[sp] = DataValue.New(DataValue.Compare(stack[sp], stack[sp - 1]) <= 0);
                        break;

                    case OpCode.LNot:
                        stack[sp] = DataValue.LNot(stack[sp]);
                        break;

                    case OpCode.LOr:
                        sp++;
                        stack[sp] = DataValue.LOr(stack[sp], stack[sp - 1]);
                        break;

                    case OpCode.Mul:
                        sp++;
                        stack[sp] = DataValue.Mul(stack[sp], stack[sp - 1]);
                        break;

                    case OpCode.NotEqual:
                        sp++;
                        stack[sp] = DataValue.New(DataValue.Compare(stack[sp], stack[sp - 1]) != 0);
                        break;

                    case OpCode.Sub:
                        sp++;
                        stack[sp] = DataValue.Sub(stack[sp], stack[sp - 1]);
                        break;

                    case OpCode.UnaryMinus:
                        stack[sp] = DataValue.Negative(stack[sp]);
                        break;

                    case OpCode.UnaryPlus:
                        stack[sp] = DataValue.Positive(stack[sp]);
                        break;

                    case OpCode.MemberAccess:
                        {
                            DataValue rec = stack[sp];
                            if (rec.Type != DataValueType.Record)
                                throw new InvalidCastException("Record type expected for member access");
                            string s = this.strings[this.GetShort(p)];
                            p += 2;
                            if (!rec.ContainsName(s))
                                throw new DataException("Field '{0}' not found", s);
                            stack[sp] = rec[s];
                        }
                        break;

                    case OpCode.ToDate:
                        if (stack[sp].Type != DataValueType.String)
                            throw new InvalidCastException("Invalid ToDate parameter, string expected");
                        stack[sp].DateTime = this.ParseDate(stack[sp].String);
                        break;

                    default:
                        throw new DataException("Unsupported opcode {0}: {1} (0x{2})", p.ToString("d6"), b, b.ToString("x2"));
                }
                if (stop) break;
            }
            DataValue r = stack[sp];
            stack = null;
            return r;
        }

        private DateTime ParseDate(string s)
        {
            DateTime d;
            if (!DateTime.TryParse(s, out d))
                throw new ArgumentException(String.Format("String '{0}' cannot be converted to DateTime", s));
            return d;
        }

        public static DataValue DefaultFunctions(string name, DataValue[] stack, int stackTop, int paramCount)
        {
            throw new DataException("Function '{0}' is not implemented");
        }

        public static Expression Compile(string expression, out string errorMessage)
        {
            return Expression.Compile(new SimpleReader(expression), out errorMessage);
        }

        public static Expression Compile(IExpressionReader reader, out string errorMessage)
        {
            ParsedItem[] parsed = Parser.Parse(reader, out errorMessage);
            if (parsed == null) return null;
            errorMessage = null;
            Expression code = null;
            try
            {
                Compiler compiler = new Compiler();
                if (!compiler.Compile(parsed))
                {
                    errorMessage = compiler.ErrorMessage;
                    return null;
                }

                code = new Expression(compiler.Code(), compiler.Strings());

                compiler = null;
            }
            catch (Exception e)
            {
                code = null;
                errorMessage = "FATAL ERROR: " + e.GetBaseException().Message;
            }

            return code;
        }

        public void Disassemble(ITextWriter writer)
        {
            int p = 0;
            while (p < this.code.Length)
            {
                byte b = this.code[p];

                if ((b < 100) || (b >= 200))
                    throw new DataException("Invalid opcode {0}: {1} (0x{2})", p.ToString("d6"), b, b.ToString("x2"));

                writer.Write(String.Format("{0}: {1}", p.ToString("d6"), (OpCode)b));

                p++;

                switch ((OpCode)b)
                {
                    case OpCode.Call:
                        {
                            string s = this.strings[this.GetShort(p)];
                            p += 2;
                            int args = (int)this.GetShort(p);
                            p += 2;
                            writer.WriteLine(String.Format(" {0}({1})", s, args));
                        }
                        break;

                    case OpCode.MemberAccess:
                        {
                            string s = this.strings[this.GetShort(p)];
                            p += 2;
                            writer.WriteLine(String.Format(" {0}", s));
                        }
                        break;

                    case OpCode.Jmp:
                    case OpCode.JmpIfTrue:
                    case OpCode.JmpIfFalse:
                        writer.WriteLine(String.Format(" [{0}]", (p + (int)this.GetShort(p)).ToString("d6")));
                        p += 2;
                        break;

                    case OpCode.PushFloat:
                        {
                            double v = this.GetDouble(p);
                            writer.WriteLine(String.Format(" {0}", v.ToString()));
                            p += 8;
                        }
                        break;

                    case OpCode.PushInt:
                        {
                            long v = this.GetLong(p);
                            writer.WriteLine(String.Format(" {0} (0x{1})", v, v.ToString("x")));
                            p += 8;
                        }
                        break;

                    case OpCode.PushLogic:
                        {
                            byte v = this.code[p++];
                            writer.WriteLine(String.Format(" {0}", v == 1 ? "true" : "false"));
                        }
                        break;

                    case OpCode.PushNull:
                        writer.WriteLine(null);
                        break;

                    case OpCode.PushString:
                        {
                            string s = this.strings[this.GetShort(p)];
                            p += 2;
                            writer.WriteLine(String.Format(" '{0}'", s));
                        }
                        break;

                    case OpCode.PushSymbol:
                        {
                            string s = this.strings[this.GetShort(p)];
                            p += 2;
                            writer.WriteLine(String.Format(" {0}", s));
                        }
                        break;

                    default:
                        writer.WriteLine(null);
                        break;
                }
            }
        }

        private unsafe int GetInt(int offset)
        {
            int v;
            fixed (byte* p = this.code)
            {
                v = *(int*)(p + offset);
            }
            return v;
        }

        private unsafe short GetShort(int offset)
        {
            short v;
            fixed (byte* p = this.code)
            {
                v = *(short*)(p + offset);
            }
            return v;
        }

        private unsafe long GetLong(int offset)
        {
            long v;
            fixed (byte* p = this.code)
            {
                v = *(long*)(p + offset);
            }
            return v;
        }

        private unsafe double GetDouble(int offset)
        {
            double v;
            fixed (byte* p = this.code)
            {
                v = *(double*)(p + offset);
            }
            return v;
        }

    }

    sealed class SimpleReader : IExpressionReader
    {
        private string s;

        public SimpleReader(string s)
        {
            this.s = s;
        }

        public string Next()
        {
            string r = this.s;
            this.s = null;
            return r;
        }
    }

}