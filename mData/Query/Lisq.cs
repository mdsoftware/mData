using System;
using mData.Expressions;
using mData.DataStructures;

namespace mData.Query
{
    enum LisqItemType : byte
    {
        List = 0,
        String,
        Symbol
    }

    sealed class LisqItem
    {
        private LisqItemType type;
        private string value;
        private LisqItem[] items;

        public const int MaxDepth = 128;

        LisqItem(LisqItemType type, string value)
        {
            this.type = type;
            this.value = value;
            this.items = null;
        }

        LisqItem(LisqItem[] items)
        {
            this.type = LisqItemType.List;
            this.value = null;
            this.items = items;
        }

        public bool CheckItem(int i, LisqItemType type)
        {
            if (this.items == null) return false;
            if ((i < 0) || (i >= this.items.Length)) return false;
            return (this.items[i].type == type);
        }

        public string[] ToSymbolArray()
        {
            if (this.type == LisqItemType.String) return null;
            if (this.type == LisqItemType.Symbol) return new string[1] { this.value };
            string[] l = new string[this.items.Length];
            for (int i = 0; i < this.items.Length; i++)
            {
                if (this.items[i].type != LisqItemType.Symbol) return null;
                l[i] = this.items[i].value;
            }
            return l;
        }

        public override string ToString()
        {
            if (this.type == LisqItemType.List) return String.Format("({0})", this.items.Length);
            return String.Format("{0} '{1}'", this.type, this.value);
        }

        public LisqItem this[int i]
        {
            get
            {
                if (this.type != LisqItemType.List)
                    throw new DataException("Lisq item is not of List type");
                if ((i < 0) || (i >= this.items.Length))
                    throw new IndexOutOfRangeException("List index is out of range");
                return this.items[i];
            }
        }

        public static LisqItem Parse(string query, out string errorMessage)
        {
            return LisqItem.Parse(new SimpleReader(query), out errorMessage);
        }

        public static LisqItem Parse(IExpressionReader reader, out string errorMessage)
        {
            ParsedItem[] list = Parser.Parse(reader, out errorMessage);
            if (list == null) return null;
            LisqParserStackItem[] stack = new LisqParserStackItem[LisqItem.MaxDepth];
            int sp = 0;
            LisqItem result = null;
            for (int i = 0; i < list.Length; i++)
            {
                ParsedItem p = list[i];
                LisqItem item = null;
                if (p.Type == ParsedItemType.Operator)
                {
                    switch (p.Text)
                    {
                        case "(":
                            if (sp >= stack.Length)
                            {
                                errorMessage = "Lisq query is too deep";
                                return null;
                            }
                            stack[sp++] = new LisqParserStackItem(PageFactor.Page32);
                            break;

                        case ")":
                            if (sp == 0)
                            {
                                errorMessage = "Lisq paranthesis not match";
                                return null;
                            }
                            sp--;
                            if (stack[sp].List.Count == 0)
                            {
                                errorMessage = "Emply lists are not allowed";
                                return null;
                            }
                            item = new LisqItem(stack[sp].List.ToArray());
                            stack[sp] = new LisqParserStackItem();
                            if (sp == 0)
                            {
                                if (i < (list.Length - 1))
                                {
                                    errorMessage = "Unexpected items after query end";
                                    return null;
                                }
                                result = item;
                            }
                            else
                            {
                                stack[sp - 1].List.Add(item);
                            }
                            break;

                        default:
                            errorMessage = String.Format("Unsupported operator '{0}'", p.Text);
                            return null;
                    }
                    continue;
                }
                if ((p.Type != ParsedItemType.String) && (p.Type != ParsedItemType.Symbol))
                {
                    errorMessage = String.Format("Unsupported item '{0}'", p.Type);
                    return null;
                }

                item = new LisqItem(p.Type == ParsedItemType.String ? LisqItemType.String : LisqItemType.Symbol, p.Text);
                if (sp == 0)
                {
                    if (i < (list.Length - 1))
                    {
                        errorMessage = "Unexpected items after query end";
                        return null;
                    }
                    result = item;
                    break;
                }
                stack[sp - 1].List.Add(item);                
            }
            return result;
        }

        public LisqItemType Type
        {
            get { return this.type; }
        }

        public string Value
        {
            get { return this.value; }
        }

        public int Count
        {
            get
            {
                if (this.type == LisqItemType.List) return this.items.Length;
                return 0;
            }
        }

        struct LisqParserStackItem
        {
            public PagedList<LisqItem> List;

            public LisqParserStackItem(PageFactor factor)
            {
                this.List = new PagedList<LisqItem>(factor, 4);
            }
        }
    }
}