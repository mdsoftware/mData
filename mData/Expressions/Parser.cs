using System;
using System.Collections.Generic;
using System.Text;
using mData.DataStructures;

namespace mData.Expressions
{

    enum ParsedItemType : short
    {
        Undefined = 0,
        String,
        Symbol,
        Operator,
        Float,
        Integer
    }

    struct ParsedItem
    {
        public ParsedItemType Type;
        public string Text;

        public ParsedItem(ParsedItemType type, string text)
        {
            this.Type = type;
            this.Text = text;
        }

        public ParsedItem(ParsedItemType type)
        {
            this.Type = type;
            this.Text = null;
        }

        public override string ToString()
        {
            if (this.Text == null) return this.Type.ToString();
            return String.Format("{0} '{1}'", this.Type, this.Text);
        }
    }

    static class Parser
    {
        private static ArrayTree<char, ExpressionCharType> chars = null;

        public const int MaxSymbolLength = 256;

        static Parser()
        {
            Parser.chars = new ArrayTree<char, ExpressionCharType>(Parser.Compare, ExpressionCharType.Undefined, 256);
            Parser.AddChars("qwertyuiopasdfghjklzxcvbnm_", ExpressionCharType.Symbol);
            Parser.AddChars("QWERTYUIOPASDFGHJKLZXCVBNM", ExpressionCharType.Symbol);
            Parser.AddChars("0123456789", ExpressionCharType.Number);
            Parser.AddChars("\"'", ExpressionCharType.String);
            Parser.AddChars(" \t\r\n", ExpressionCharType.BlankSpace);
            Parser.AddChars("!<>=*/+-^&|", ExpressionCharType.Operator);
            Parser.AddChars("(),?:.@", ExpressionCharType.SingleCharOperator);
        }

        public static bool IsSymbol(string s)
        {
            if (s == null) return false;
            int l = s.Length;
            if ((l == 0) || (l > Parser.MaxSymbolLength)) return false;
            for (int i = 0; i < l; i++)
            {
                switch (Parser.CharType(s[i])){
                    case ExpressionCharType.Symbol:
                        break;

                    case ExpressionCharType.Number:
                        if (i == 0) return false;
                        break;

                    default:
                        return false;
                }
            }
            return true;
        }

        public static ParsedItem[] Parse(IExpressionReader reader, out string errorMessage)
        {
            errorMessage = null;

            List<ParsedItem> items = new List<ParsedItem>();
            char prevCh = '\0';
            char ch = '\0';
            char[] buffer = new char[Parser.MaxSymbolLength];
            int count = 0;
            ExpressionCharType type = ExpressionCharType.BlankSpace;
            StringBuilder text = null;
            char stringChar = '\0';
            bool comment = false;
            bool dot = false;


            string s = String.Empty;
            ParserCharBuffer charBuf = new ParserCharBuffer(8);

            int i = 0;
            while (true)
            {
                prevCh = ch;

                if (charBuf.Mode == CharBufferMode.Reading)
                {
                    ch = charBuf.Next();
                }
                else
                {
                    if (i >= s.Length)
                    {
                        bool eos = false;
                        while (true)
                        {
                            s = reader.Next();
                            if (s == null)
                            {
                                eos = true;
                                break;
                            }
                            if (s.Length > 0) break;
                        }
                        if (eos)
                        {
                            if (charBuf.Mode == CharBufferMode.Empty)
                                break;
                            if (charBuf.Mode == CharBufferMode.Filling)
                                charBuf.Mode = CharBufferMode.Reading;
                            continue;
                        }
                        i = 0;
                    }
                    ch = s[i++];
                }
               

                if (charBuf.Mode == CharBufferMode.Filling)
                {
                    if (charBuf.Full)
                    {
                        errorMessage = "Char buffer is full";
                        return null;
                    }
                    charBuf.Add(ch);
                    if (charBuf.Length == 2)
                    {
                        if (charBuf.ToString() == "/*")
                        {
                            if (count > 0)
                            {
                                Parser.AddParsedItem(items, type, new String(buffer, 0, count), dot);
                                count = 0;
                            }
                            type = ExpressionCharType.BlankSpace;
                            comment = true;
                            charBuf.Clear();
                        }
                        else
                        {
                            charBuf.Mode = CharBufferMode.Reading;
                            continue;
                        }
                    }
                    else
                    {
                        continue;
                    }
                }

                if (comment)
                {
                    if ((prevCh == '*') && (ch == '/'))
                        comment = false;
                    continue;
                }

                if (type == ExpressionCharType.String)
                {
                    if (ch == '\\')
                        continue;
                    if (prevCh == '\\')
                    {
                        switch (ch)
                        {
                            case '\'':
                            case '"':
                            case '\\':
                                break;

                            case 'r':
                                ch = '\r';
                                break;

                            case 'n':
                                ch = '\n';
                                break;

                            case 't':
                                ch = '\t';
                                break;

                            default:
                                errorMessage = String.Format("Invalid string escape sequence '\\{0}'", ch);
                                return null;
                        }
                    }
                    else
                    {
                        if (ch == stringChar)
                        {
                            string s0 = (count == 0) ? String.Empty : new String(buffer, 0, count);
                            if (text != null)
                            {
                                s0 += text.ToString();
                                text = null;
                            }
                            items.Add(new ParsedItem(ParsedItemType.String, s0));
                            type = ExpressionCharType.BlankSpace;
                            continue;
                        }
                    }
                    if (count < buffer.Length)
                    {
                        buffer[count++] = ch;
                    }
                    else
                    {
                        if (text == null) text = new StringBuilder();
                        text.Append(ch);
                    }
                    continue;
                }

                ExpressionCharType t = Parser.CharType(ch);
                if (t == ExpressionCharType.Undefined)
                {
                    errorMessage = String.Format("Invalid expression char '{0}'", ch);
                    return null;
                }

                if ((ch == '/') && (charBuf.Mode == CharBufferMode.Empty))
                {
                    charBuf.Mode = CharBufferMode.Filling;
                    charBuf.Add(ch);
                    continue;
                }

                if (t != type)
                {
                    bool set = true;
                    if ((type == ExpressionCharType.Symbol) && (t == ExpressionCharType.Number))
                    {
                        set = false;
                    }
                    else if ((type == ExpressionCharType.Number) && (t == ExpressionCharType.Symbol))
                    {
                        errorMessage = String.Format("Invalid number '{0}'", Parser.Token(buffer, count, ch));
                        return null;
                    }
                    else if ((type == ExpressionCharType.Number) && (ch == '.'))
                    {
                        if (dot)
                        {
                            errorMessage = String.Format("Invalid number '{0}'", Parser.Token(buffer, count, ch));
                            return null;
                        }
                        dot = true;
                        set = false;
                    }
                    if (set)
                    {
                        if (count > 0)
                        {
                            Parser.AddParsedItem(items, type, new String(buffer, 0, count), dot);
                            count = 0;
                        }
                        dot = false;
                        type = t;
                    }
                }

                if (type == ExpressionCharType.BlankSpace)
                    continue;

                if (type == ExpressionCharType.String)
                {
                    stringChar = ch;
                    continue;
                }

                if (type == ExpressionCharType.SingleCharOperator)
                {
                    Parser.AddParsedItem(items, ExpressionCharType.Operator, ch.ToString(), false);
                    type = ExpressionCharType.BlankSpace;
                    dot = false;
                    continue;
                }

                if (count >= buffer.Length)
                {
                    errorMessage = "Symbol is too long";
                    return null;
                }
                buffer[count++] = ch;
            }

            if (comment)
            {
                errorMessage = "Comment is not closed";
                return null;
            }

            if (type == ExpressionCharType.String)
            {
                errorMessage = "String is not closed";
                return null;
            }

            if (count > 0)
                Parser.AddParsedItem(items, type, new String(buffer, 0, count), dot);

            return items.ToArray();
        }

        private static string Token(char[] buffer, int count, char c)
        {
            if (count == 0)
                return c.ToString();
            return new String(buffer, 0, count) + c;
        }

        private static void AddParsedItem(List<ParsedItem> items, ExpressionCharType type, string t, bool dot)
        {
            switch (type)
            {
                case ExpressionCharType.Number:
                    items.Add(new ParsedItem(dot ? ParsedItemType.Float : ParsedItemType.Integer, t));
                    break;

                case ExpressionCharType.Operator:
                    items.Add(new ParsedItem(ParsedItemType.Operator, t));
                    break;

                case ExpressionCharType.Symbol:
                    if ((t.Length == 2) || (t.Length == 3))
                    {
                        switch (t.ToLower())
                        {
                            case "and":
                                items.Add(new ParsedItem(ParsedItemType.Operator, "&&"));
                                break;

                            case "or":
                                items.Add(new ParsedItem(ParsedItemType.Operator, "||"));
                                break;

                            case "not":
                                items.Add(new ParsedItem(ParsedItemType.Operator, "!"));
                                break;

                            default:
                                items.Add(new ParsedItem(ParsedItemType.Symbol, t));
                                break;
                        }
                    }
                    else
                    {
                        items.Add(new ParsedItem(ParsedItemType.Symbol, t));
                    }
                    break;

            }
        }

        private static ExpressionCharType CharType(char c)
        {
            ExpressionCharType f;
            if (Parser.chars.TryGetValue(c, out f))
                return f;
            return ExpressionCharType.Undefined;
        }

        private static void AddChars(string s, ExpressionCharType t)
        {
            for (int i = 0; i < s.Length; i++)
            {
                char c = s[i];
                if (Parser.chars.ContainsKey(c))
                {
                    Parser.chars[c] = t;
                }
                else
                {
                    Parser.chars.Insert(c, t);
                }
            }
        }

        private static int Compare(char x, char y)
        {
            return x.CompareTo(y);
        }

    }

    struct ParserCharBuffer
    {
        public CharBufferMode Mode;
        private char[] chars;
        private int head;
        private int tail;

        public ParserCharBuffer(int size)
        {
            this.Mode = CharBufferMode.Empty;
            this.chars = new char[size];
            this.head = this.tail = 0;
        }

        public void Clear()
        {
            this.Mode = CharBufferMode.Empty;
            this.head = this.tail = 0;
        }

        public bool Full
        {
            get { return this.head >= this.chars.Length; }
        }

        public void Add(char c)
        {
            this.chars[this.head++] = c;
        }

        public override string ToString()
        {
            if (this.head == 0) return String.Empty;
            return new String(this.chars, 0, this.head);
        }

        public char Next()
        {
            char c = this.chars[this.tail++];
            if (this.head == this.tail)
            {
                this.head = this.tail = 0;
                this.Mode = CharBufferMode.Empty;
            }
            return c;
        }

        public int Length
        {
            get { return this.head - this.tail; }
        }
    }


    enum CharBufferMode:byte
    {
        Empty = 0,
        Filling,
        Reading
    }

    enum ExpressionCharType : byte
    {
        Undefined = 0,
        Symbol,
        Number,
        String,
        Operator,
        SingleCharOperator,
        BlankSpace
    }

}
