using System;
using System.Collections.Generic;
using mData.DataStructures;
using mData.Serialization;

namespace mData.Expressions
{

    enum StackItemType : byte
    {
        OperationResult = 0,
        Constant,
        Symbol,
        FunctionResult
    }

    struct StackItem
    {
        public string Symbol;
        public StackItemType Type;
    }

    sealed class Compiler
    {
        private static ArrayTree<OpCode, short> precedence = null;

        public const int StackSize = 512;
        public const int CodeSize = 0x8000;
        public const int StringsSize = 4096;

        private string errorMessage;
        private CompilerStack operators;
        private byte[] code;
        private int codePos;
        private List<string> strings;
        private ArrayTree<string, int> symbols;
        private int stackTop;
        private StackItem[] stack;
        private string symbolName;
        private int savedPos;

        public Compiler()
        {
            if (Compiler.precedence == null)
            {
                Compiler.precedence = new ArrayTree<OpCode, short>(Compiler.Compare, -1, 32);

                Compiler.precedence.Insert(OpCode.MemberAccess, 5);

                Compiler.precedence.Insert(OpCode.UnaryMinus, 10);
                Compiler.precedence.Insert(OpCode.UnaryPlus, 10);
                Compiler.precedence.Insert(OpCode.LNot, 10);
                Compiler.precedence.Insert(OpCode.ToDate, 10);

                Compiler.precedence.Insert(OpCode.Div, 20);
                Compiler.precedence.Insert(OpCode.Mul, 20);

                Compiler.precedence.Insert(OpCode.Add, 30);
                Compiler.precedence.Insert(OpCode.Sub, 30);

                Compiler.precedence.Insert(OpCode.Less, 40);
                Compiler.precedence.Insert(OpCode.LessEqual, 40);
                Compiler.precedence.Insert(OpCode.Great, 40);
                Compiler.precedence.Insert(OpCode.GreatEqual, 40);

                Compiler.precedence.Insert(OpCode.Equal, 50);
                Compiler.precedence.Insert(OpCode.NotEqual, 50);

                Compiler.precedence.Insert(OpCode.BitAnd, 60);

                Compiler.precedence.Insert(OpCode.BitXor, 70);

                Compiler.precedence.Insert(OpCode.BitOr, 80);

                Compiler.precedence.Insert(OpCode.LAnd, 90);

                Compiler.precedence.Insert(OpCode.LOr, 100);

                Compiler.precedence.Insert(OpCode.Question, 110);
                Compiler.precedence.Insert(OpCode.Colon, 110);

            }
            this.errorMessage = null;
            this.operators = new CompilerStack(Compiler.StackSize);
            this.stack = new StackItem[Compiler.StackSize];
            this.code = new byte[Compiler.CodeSize];
            this.codePos = 0;
            this.stackTop = 0;
            this.strings = new List<string>(Compiler.StringsSize);
            this.symbols = new ArrayTree<string, int>(Compiler.Compare, -1, Compiler.StringsSize);
            this.savedPos = 0;
            this.symbolName = null;
        }

        private bool Operator(OpCode op, CompilingFlags flags)
        {
            switch (op)
            {
                case OpCode.LPar:
                    if ((flags & CompilingFlags.LastSymbol) > 0)
                    {
                        this.codePos = savedPos;
                        --stackTop;
                        this.operators.Push(OpCode.Call, this.symbolName, this.stackTop);
                    }
                    else
                    {
                        this.operators.Push(OpCode.LPar, 1000, 0);
                    }
                    return true;


                case OpCode.RPar:
                    return this.DropLPar();

                case OpCode.Comma:
                    return this.DropComma();

                case OpCode.Add:
                    if ((flags & CompilingFlags.LastOperator) > 0)
                        op = OpCode.UnaryPlus;
                    break;

                case OpCode.Sub:
                    if ((flags & CompilingFlags.LastOperator) > 0)
                        op = OpCode.UnaryMinus;
                    break;

                case OpCode.MemberAccess:
                    {
                        if (this.operators.Count > 0)
                        {
                            CompilerStackItem o = this.operators.Pop();
                            if (o.Code == OpCode.MemberAccess)
                            {
                                this.DropOperator(o);
                            }
                            else
                            {
                                this.operators.Push(o);
                            }
                        }
                    }
                    break;

            }

            short precedence = Compiler.Precedence(op);

            if (!this.DropToPrecedence(precedence)) return false;

            this.operators.Push(op, precedence, this.codePos);

            switch (op)
            {
                case OpCode.Question:
                    this.Put(OpCode.JmpIfFalse, (short)0);
                    break;

                case OpCode.Colon:
                    this.Put(OpCode.Jmp, (short)0);
                    break;

            }

            return true;
        }


        private bool DropLPar()
        {
            if (this.operators.Count == 0)
                throw new DataException("Paranthesis mismatch");
            while (true)
            {
                CompilerStackItem o = this.operators.Pop();
                bool stop = false;
                switch (o.Code)
                {
                    case OpCode.LPar:
                        stop = true;
                        break;

                    case OpCode.Call:
                        {
                            int pars = (stackTop - o.Offset);
                            this.Put(OpCode.Call, this.AddSymbol(o.Text), (short)pars);
                            this.stackTop -= pars;
                            this.stackTop++;
                            this.stack[this.stackTop].Type = StackItemType.FunctionResult;
                            stop = true;
                        }
                        break;


                    case OpCode.RPar:
                    case OpCode.Comma:
                        {
                            this.errorMessage = "Right paranthesis or comma mismatch";
                            return false;
                        }

                    default:
                        if (!this.DropOperator(o)) return false;
                        break;
                }
                if (stop)
                    break;
            }
            return true;
        }

        private bool DropToPrecedence(short precedence)
        {
            while (true)
            {
                if(this.operators.Count == 0)
                    break;
                CompilerStackItem o = this.operators.Pop();
                if (o.Precedence >= precedence)
                {
                    this.operators.Push(o);
                    break;
                }
                if (!this.DropOperator(o)) return false;
            }
            return true;
        }

        private bool DropComma()
        {
            while (true)
            {
                CompilerStackItem o = this.operators.Pop();
                bool stop = false;
                switch (o.Code)
                {

                    case OpCode.Call:
                        this.operators.Push(o);
                        stop = true;
                        break;


                    case OpCode.RPar:
                    case OpCode.Comma:
                        {
                            this.errorMessage = "Right paranthesis or comma mismatch";
                            return false;
                        }

                    default:
                        this.DropOperator(o);
                        break;
                }
                if (stop)
                    break;
            }
            return true;
        }

        private bool DropOperator(CompilerStackItem op)
        {
            switch (op.Code)
            {
                case OpCode.LNot:
                case OpCode.UnaryMinus:
                case OpCode.UnaryPlus:
                case OpCode.ToDate:
                    if (this.stackTop < 1)
                    {
                        this.errorMessage = "Expression stack underflow";
                        return false;
                    }
                    this.stack[this.stackTop - 1].Type = StackItemType.OperationResult;
                    this.Put(op.Code);
                    break;

                case OpCode.LPar:
                    break;

                case OpCode.Question:
                    this.errorMessage = "Invalid ?: control structure";
                    return false;

                case OpCode.Colon:
                    if (this.stackTop < 1)
                    {
                        this.errorMessage = "Expression stack underflow";
                        return false;
                    }
                    --this.stackTop;

                    int colonOfs = op.Offset;
                    this.Put(colonOfs + 1, (short)(this.codePos - colonOfs - 1));

                    if (this.operators.Count == 0)
                    {
                        this.errorMessage = "Invalid ?: control structure";
                        return false;
                    }

                    CompilerStackItem q = this.operators.Pop();
                    if (q.Code != OpCode.Question)
                    {
                        this.errorMessage = "Invalid ?: control structure";
                        return false;
                    }

                    if (this.stackTop < 1)
                    {
                        this.errorMessage = "Expression stack underflow";
                        return false;
                    }
                    --this.stackTop;

                    this.Put(q.Offset + 1, (short)(colonOfs - q.Offset + 2));

                    break;

                case OpCode.Call:
                    {
                        this.errorMessage = "Function call mismatch";
                        return false;
                    }

                case OpCode.MemberAccess:
                    if (this.stackTop < 2)
                    {
                        this.errorMessage = "Expression stack underflow";
                        return false;
                    }
                    StackItem s = this.stack[--this.stackTop];
                    if (s.Type != StackItemType.Symbol)
                    {
                        this.errorMessage = "Member name must be a symbol";
                        return false;
                    }
                    this.codePos -= 3;
                    this.Put(op.Code, this.AddSymbol(s.Symbol));
                    break;

                default:
                    if (this.stackTop < 2)
                    {
                        this.errorMessage = "Expression stack underflow";
                        return false;
                    }
                    this.Put(op.Code);
                    --this.stackTop;
                    this.stack[this.stackTop - 1].Type = StackItemType.OperationResult;
                    break;
            }
            return true;
        }

        public bool Compile(ParsedItem[] parsed)
        {
            CompilingFlags flags = CompilingFlags.LastOperator;
            for (int i = 0; i < parsed.Length; i++)
            {

                switch (parsed[i].Type)
                {
                    case ParsedItemType.Operator:
                        {
                            OpCode op = Compiler.Operator(parsed[i].Text);
                            if (op == OpCode.Undefined)
                            {
                                this.errorMessage = String.Format("Unknown operator '{0}'", parsed[i].Text);
                                return false;
                            }
                            if (!this.Operator(op, flags)) return false;
                            if (op != OpCode.RPar)
                                flags = CompilingFlags.LastOperator;
                        }
                        break;

                    case ParsedItemType.String:
                        this.Put(OpCode.PushString, this.AddString(parsed[i].Text));
                        ++this.stackTop;
                        this.stack[this.stackTop - 1].Type = StackItemType.Constant;
                        flags = CompilingFlags.Empty;
                        break;

                    case ParsedItemType.Integer:
                        {
                            long v;
                            if (!Int64.TryParse(parsed[i].Text, out v))
                            {
                                this.errorMessage = String.Format("Invalid integer number '{0}'", parsed[i].Text);
                                return false;
                            }
                            this.Put(OpCode.PushInt, v);
                            ++this.stackTop;
                            this.stack[this.stackTop - 1].Type = StackItemType.Constant;
                            flags = CompilingFlags.Empty;
                        }
                        break;

                    case ParsedItemType.Float:
                        {
                            double v;
                            if (!Double.TryParse(parsed[i].Text, out v))
                            {
                                this.errorMessage = String.Format("Invalid float number '{0}'", parsed[i].Text);
                                return false;
                            }
                            this.Put(OpCode.PushFloat, v);
                            ++this.stackTop;
                            this.stack[this.stackTop - 1].Type = StackItemType.Constant;
                            flags = CompilingFlags.Empty;
                        }
                        break;

                    case ParsedItemType.Symbol:
                        {
                            switch (parsed[i].Text.ToLower())
                            {
                                case "null":
                                    this.Put(OpCode.PushNull);
                                    ++this.stackTop;
                                    this.stack[this.stackTop - 1].Type = StackItemType.Constant;
                                    flags = CompilingFlags.Empty;
                                    break;

                                case "true":
                                    this.Put(OpCode.PushLogic, (byte)0x01);
                                    ++this.stackTop;
                                    this.stack[this.stackTop - 1].Type = StackItemType.Constant;
                                    flags = CompilingFlags.Empty;
                                    break;

                                case "false":
                                    this.Put(OpCode.PushLogic, (byte)0x00);
                                    ++this.stackTop;
                                    this.stack[this.stackTop - 1].Type = StackItemType.Constant;
                                    flags = CompilingFlags.Empty;
                                    break;

                                default:
                                    this.savedPos = this.codePos;
                                    this.symbolName = parsed[i].Text;
                                    ++this.stackTop;
                                    this.stack[this.stackTop - 1].Type = StackItemType.Symbol;
                                    this.stack[this.stackTop - 1].Symbol = this.symbolName;
                                    this.Put(OpCode.PushSymbol, this.AddSymbol(this.symbolName));
                                    
                                    
                                    flags = CompilingFlags.LastSymbol;
                                    break;

                            }
                        }
                        break;

                    default:
                        this.errorMessage = String.Format("Unsupported parsed item type {0}", parsed[i].Type);
                        return false;
                }

            }

            if (!this.DropToPrecedence(10000)) return false;
            
            if (this.stackTop != 1)
            {
                this.errorMessage = "Invalid expression";
                return false;
            }
            
            this.Put(OpCode.Stop);

            return true;
        }

        public unsafe byte[] Code()
        {
            byte[] b = new byte[this.codePos];
            fixed (byte* src = this.code, dest = b)
            {
                DataWriteStream.Copy(src, 0, dest, 0, this.codePos);
            }
            return b;
        }

        public string[] Strings()
        {
            return this.strings.ToArray();
        }

        public string ErrorMessage
        {
            get { return this.errorMessage; }
        }

        private static short Precedence(OpCode code)
        {
            short p;
            if (Compiler.precedence.TryGetValue(code, out p))
                return p;
            throw new DataException("Precedence is not defined for '{0}'", code);
        }

        private static int Compare(OpCode x, OpCode y)
        {
            return ((byte)x).CompareTo((byte)y);
        }

        private static int Compare(string x, string y)
        {
            return String.Compare(x, y, true);
        }

        private static OpCode Operator(string token)
        {
            switch (token)
            {
                case "=":
                case "==":
                    return OpCode.Equal;

                case ">":
                    return OpCode.Great;

                case "<":
                    return OpCode.Less;

                case ">=":
                case "=>":
                    return OpCode.GreatEqual;

                case "<=":
                case "=<":
                    return OpCode.LessEqual;

                case "!=":
                case "<>":
                    return OpCode.NotEqual;

                case "!":
                    return OpCode.LNot;

                case "&&":
                    return OpCode.LAnd;

                case "||":
                    return OpCode.LOr;

                case "+":
                    return OpCode.Add;

                case "-":
                    return OpCode.Sub;

                case "/":
                    return OpCode.Div;

                case "*":
                    return OpCode.Mul;

                case "&":
                    return OpCode.BitAnd;

                case "|":
                    return OpCode.BitOr;

                case "^":
                    return OpCode.BitXor;

                case "(":
                    return OpCode.LPar;

                case ")":
                    return OpCode.RPar;

                case ",":
                    return OpCode.Comma;

                case "?":
                    return OpCode.Question;

                case ":":
                    return OpCode.Colon;

                case ".":
                    return OpCode.MemberAccess;

                case "@":
                    return OpCode.ToDate;
            }
            return OpCode.Undefined;
        }

        private void Put(OpCode opcode)
        {
            if (this.codePos >= Compiler.CodeSize)
                throw new DataException("Code buffer overflow");
            this.code[this.codePos++] = (byte)opcode;
        }

        private unsafe void Put(OpCode code, long param)
        {
            if ((this.codePos + 9) >= Compiler.CodeSize)
                throw new DataException("Code buffer overflow");
            fixed (byte* p = this.code)
            {
                byte* pp = p + this.codePos;
                *(pp++) = (byte)code;
                *(long*)pp = param;
            }
            this.codePos += 9;
        }

        private unsafe void Put(OpCode code, double param)
        {
            if ((this.codePos + 9) >= Compiler.CodeSize)
                throw new DataException("Code buffer overflow");
            fixed (byte* p = this.code)
            {
                byte* pp = p + this.codePos;
                *(pp++) = (byte)code;
                *(double*)pp = param;
            }
            this.codePos += 9;
        }

        private unsafe void Put(OpCode code, short param)
        {
            if ((this.codePos + 3) >= Compiler.CodeSize)
                throw new DataException("Code buffer overflow");
            fixed (byte* p = this.code)
            {
                byte* pp = p + this.codePos;
                *(pp++) = (byte)code;
                *(short*)pp = param;
            }
            this.codePos += 3;
        }

        private unsafe void Put(OpCode code, short param0, short param1)
        {
            if ((this.codePos + 5) >= Compiler.CodeSize)
                throw new DataException("Code buffer overflow");
            fixed (byte* p = this.code)
            {
                byte* pp = p + this.codePos;
                *(pp++) = (byte)code;
                *(short*)pp = param0;
                pp += 2;
                *(short*)pp = param1;
            }
            this.codePos += 5;
        }

        private unsafe void Put(OpCode code, byte param)
        {
            if ((this.codePos + 2) >= Compiler.CodeSize)
                throw new DataException("Code buffer overflow");
            this.code[this.codePos++] = (byte)code;
            this.code[this.codePos++] = param;
        }

        private unsafe void Put(int offset, short fixup)
        {
            fixed (byte* p = this.code)
            {
                *(short*)(p + offset) = fixup;
            }
        }

        private short AddString(string s)
        {
            if (this.strings.Count >= Compiler.StringsSize)
                throw new DataException("Strings buffer overflow");
            int p = this.strings.Count;
            this.strings.Add(s);
            return (short)p;
        }

        private short AddSymbol(string s)
        {
            int p;
            if (this.symbols.TryGetValue(s, out p))
                return (short)p;
            p = this.AddString(s);
            this.symbols.Insert(s, p);
            return (short)p;
        }
    }

    struct CompilerStack
    {
        private CompilerStackItem[] stack;
        private int top;

        public CompilerStack(int size)
        {
            this.stack = new CompilerStackItem[size];
            this.top = 0;
        }

        public CompilerStackItem Pop()
        {
            if (this.top == 0)
                throw new DataException("Compiler stack underflow");
            return this.stack[--this.top];
        }

        public void Push(OpCode code, short precedence, int offset)
        {
            if (this.top >= this.stack.Length)
                throw new DataException("Compiler stack overflow");
            this.stack[this.top++] = new CompilerStackItem(code, precedence, offset);
        }

        public void Push(CompilerStackItem item)
        {
            if (this.top >= this.stack.Length)
                throw new DataException("Compiler stack overflow");
            this.stack[this.top++] = item;
        }

        public void Push(OpCode code, string text, int stackTop)
        {
            if (this.top >= this.stack.Length)
                throw new DataException("Compiler stack overflow");
            this.stack[this.top++] = new CompilerStackItem(code, text, stackTop);
        }

        public CompilerStackItem Top()
        {
            if (this.top == 0)
                throw new DataException("Compiler stack underflow");
            return this.stack[this.top - 1];
        }

        public int Count
        {
            get { return this.top; }
        }
    }

    struct CompilerStackItem
    {
        public OpCode Code;
        public string Text;
        public short Precedence;
        public int Offset;


        public CompilerStackItem(OpCode code, short precedence, int offset)
        {
            this.Code = code;
            this.Text = null;
            this.Precedence = precedence;
            this.Offset = offset;
        }

        public CompilerStackItem(OpCode code, string text, int stackTop)
        {
            this.Code = code;
            this.Text = text;
            this.Precedence = 1000;
            this.Offset = stackTop;
        }

        public override string ToString()
        {
            if (this.Text == null) return this.Code.ToString();
            return String.Format("{0} '{1}' <{2}> [{3}]", this.Code, this.Text, this.Precedence, this.Offset);
        }
    }

    [Flags]
    enum CompilingFlags : byte
    {
        Empty = 0x00,
        LastSymbol = 0x01,
        LastOperator = 0x02
    }

    enum OpCode : byte
    {
        Undefined = 0,

        Nop = 100,
        Jmp = 101,
        JmpIfTrue = 102,
        JmpIfFalse = 103,
        Call = 104,
        PushSymbol = 105,
        PushString = 106,
        PushInt = 107,
        PushFloat = 108,
        PushNull = 109,
        PushLogic = 110,
        Add = 111,
        Sub = 112,
        Mul = 113,
        Div = 114,
        Equal = 115,
        NotEqual = 116,
        LessEqual = 117,
        GreatEqual = 118,
        Less = 119,
        Great = 120,
        LAnd = 121,
        LOr = 122,
        LNot = 123,
        BitAnd = 124,
        BitOr = 125,
        BitXor = 126,
        UnaryPlus = 127,
        UnaryMinus = 128,
        MemberAccess = 129,
        ToDate = 130,

        Stop = 199,

        LPar = 200,
        RPar = 201,
        Comma = 202,
        Question = 203,
        Colon = 204,
    }

}