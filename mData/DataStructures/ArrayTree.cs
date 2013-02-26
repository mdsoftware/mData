using System;
using System.Collections.Generic;
using mData.Testing;

namespace mData.DataStructures
{


    class ArrayTree<TKey, TValue> : ITreeTesting<TKey, TValue>
    {

        private Func<TKey, TKey, int> comparison;
        private TValue defaultValue;
        private int root;
        private int free;
        private ArrayTreePage[] pages;
        private int pageCount;
        private int count;
        private int mask;
        private int shift;

        public ArrayTree(Func<TKey, TKey, int> comparison, TValue defaultValue, int size)
            : this(comparison, defaultValue, size, PageFactor.Page1024)
        {
        }

        public ArrayTree(Func<TKey, TKey, int> comparison, TValue defaultValue, int size, PageFactor factor)
        {
            this.defaultValue = defaultValue;
            this.comparison = comparison;

            this.shift = (int)factor;
            this.mask = (0x1 << this.shift);
            this.mask--;

            size = (size >> this.shift) + ((size & this.mask) == 0 ? 0 : 1);

            this.pages = new ArrayTreePage[size];
            this.pageCount = 0;

            this.Clear();
        }

        public int Count
        {
            get { return this.count; }
        }

        public void Clear()
        {
            this.root = -1;
            this.free = -1;
            for (int i = 0; i < this.pageCount; i++) this.pages[i].Page = null;
            this.pageCount = 0;
            this.count = 0;
        }

        public TreeNodeInfo<TKey, TValue>[] GetNodes()
        {
            if (this.root == -1) return null;
            List<TreeNodeInfo<TKey, TValue>> l = new List<TreeNodeInfo<TKey, TValue>>();
            this.Iterate(this.root, l);
            l.Sort(this.CompareNodeInfo);
            return l.ToArray();
        }

        private void Iterate(int h, List<TreeNodeInfo<TKey, TValue>> list)
        {
            ArrayTreeItem n = this.GetNode(h);

            TreeNodeInfo<TKey, TValue> ni = new TreeNodeInfo<TKey, TValue>();
            ni.Key = n.Key;
            ni.Value = n.Value;
            ni.IsBlack = n.IsBlack;
            ni.Left = default(TKey);
            ni.Right = default(TKey);

            if (n.Left != -1) ni.Left = this.GetNode(n.Left).Key;
            if (n.Right != -1) ni.Right = this.GetNode(n.Right).Key;

            list.Add(ni);

            if (n.Left != -1) this.Iterate(n.Left, list);
            if (n.Right != -1) this.Iterate(n.Right, list);
        }

        public List<TreeKeyValue<TKey, TValue>> Content()
        {
            if (this.root == -1) return null;
            List<TreeKeyValue<TKey, TValue>> l = new List<TreeKeyValue<TKey, TValue>>();
            this.Iterate(this.root, l);
            return l;
        }

        private void Iterate(int h, List<TreeKeyValue<TKey, TValue>> list)
        {
            ArrayTreeItem n = this.GetNode(h);
            list.Add(new TreeKeyValue<TKey, TValue>(n.Key, n.Value));
            if (n.Left != -1) this.Iterate(n.Left, list);
            if (n.Right != -1) this.Iterate(n.Right, list);
        }

        public bool Empty
        {
            get { return this.root == -1; }
        }

        public TKey[] AllKeys()
        {
            if (this.root == -1) return null;
            TKey[] l = new TKey[this.count];
            int i = 0;
            this.Iterate(this.root, l, ref i);
            return l;
        }

        public TValue[] AllValues()
        {
            if (this.root == -1) return null;
            TValue[] l = new TValue[this.count];
            int i = 0;
            this.Iterate(this.root, l, ref i);
            return l;
        }

        public TreeKeyValue<TKey, TValue> Root
        {
            get
            {
                if (this.root == -1) return null;
                ArrayTreeItem n = this.GetNode(this.root);
                return new TreeKeyValue<TKey, TValue>(n.Key, n.Value);
            }
        }

        private void Iterate(int h, TKey[] list, ref int pos)
        {
            ArrayTreeItem n = this.GetNode(h);
            list[pos++] = n.Key;
            if (n.Left != -1) this.Iterate(n.Left, list, ref pos);
            if (n.Right != -1) this.Iterate(n.Right, list, ref pos);
        }

        private void Iterate(int h, TValue[] list, ref int pos)
        {
            ArrayTreeItem n = this.GetNode(h);
            list[pos++] = n.Value;
            if (n.Left != -1) this.Iterate(n.Left, list, ref pos);
            if (n.Right != -1) this.Iterate(n.Right, list, ref pos);
        }

        public TValue this[TKey key]
        {
            get
            {
                TValue v;
                if (this.TryGetValue(key, out v)) return v;
                throw new DataException("Key {0} not found", key.ToString());
            }
            set
            {
                this.Insert(key, value);
            }
        }

        public bool ContainsKey(TKey key)
        {
            TValue v;
            return this.TryGetValue(key, out v);
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            value = this.defaultValue;
            int node = this.root;
            while (node != -1)
            {
                ArrayTreeItem n = this.GetNode(node);
                int comparisonResult = this.comparison(key, n.Key);
                if (comparisonResult < 0)
                {
                    node = n.Left;
                }
                else if (0 < comparisonResult)
                {
                    node = n.Right;
                }
                else
                {
                    value = n.Value;
                    return true;
                }
            }
            return false;
        }

        public TValue Search(TKey key)
        {
            int node = this.root;
            while (node != -1)
            {
                ArrayTreeItem n = this.GetNode(node);
                int comparisonResult = this.comparison(key, n.Key);
                if (comparisonResult < 0)
                {
                    node = n.Left;
                }
                else if (0 < comparisonResult)
                {
                    node = n.Right;
                }
                else
                {
                    return n.Value;
                }
            }
            return this.defaultValue;
        }

        private void Free(int node)
        {
            ArrayTreeItem n = new ArrayTreeItem();
            n.Mode = ArrayTreeItemMode.Free;
            n.Next = this.free;
            this.SetNode(node, n);
            this.free = node;
        }

        private ArrayTreeItem this[int i]
        {
            get
            {
                int p = (i >> this.shift);
                if (this.pages[p].Page == null)
                    throw new DataException("Tree page is not initialised");
                return this.pages[p].Page[i & this.mask];
            }
        }

        private int New(TKey key, TValue value)
        {
            int i;
            if (this.free == -1)
            {
                if (this.pageCount >= this.pages.Length)
                    throw new OutOfMemoryException("Array tree is full");
                ArrayTreeItem[] page = new ArrayTreeItem[0x1 << this.shift];

                page[0].Mode = ArrayTreeItemMode.Free;
                page[0].Next = this.free;

                int p = this.pageCount << this.shift;
                for (i = 1; i < page.Length; i++)
                {
                    page[i].Mode = ArrayTreeItemMode.Free;
                    page[i].Next = p++;
                }
                this.free = p;
                this.pages[this.pageCount++].Page = page;
                page = null;
            }
            i = this.free;
            this.free = this[i].Next;

            ArrayTreeItem n = new ArrayTreeItem();

            n.Mode = ArrayTreeItemMode.Red;
            n.Left = -1;
            n.Right = -1;
            n.Key = key;
            n.Value = value;

            this.SetNode(i, n);

            return i;
        }


        public TreeNodeInfo<TKey, TValue>[] GetNodes(TKey defaultKey)
        {
            if (this.root == -1) return null;
            List<TreeNodeInfo<TKey, TValue>> l = new List<TreeNodeInfo<TKey, TValue>>();
            this.Iterate(this.root, l, defaultKey);
            l.Sort(this.CompareNodeInfo);
            return l.ToArray();
        }

        private int CompareNodeInfo(TreeNodeInfo<TKey, TValue> x, TreeNodeInfo<TKey, TValue> y)
        {
            return this.comparison(x.Key, y.Key);
        }

        private void Iterate(int h, List<TreeNodeInfo<TKey, TValue>> list, TKey defaultKey)
        {
            TreeNodeInfo<TKey, TValue> ni = new TreeNodeInfo<TKey, TValue>();
            ArrayTreeItem n = this.GetNode(h);
            ni.Key = n.Key;
            ni.Value = n.Value;
            ni.IsBlack = n.IsBlack;
            ni.Left = defaultKey;
            ni.Right = defaultKey;
            if (n.Left != -1) ni.Left = this.GetNode(n.Left).Key;
            if (n.Right != -1) ni.Right = this.GetNode(n.Right).Key;
            list.Add(ni);
            if (n.Left != -1) this.Iterate(n.Left, list, defaultKey);
            if (n.Right != -1) this.Iterate(n.Right, list, defaultKey);
        }

        public void Insert(TKey key, TValue value)
        {
            this.root = this.Add(root, key, value);
            ArrayTreeItem n = this.GetNode(this.root);
            if (!n.IsBlack)
            {
                n.IsBlack = true;
                this.SetNode(this.root, n);
            }
        }

        private bool IsRed(int node)
        {
            if (node == -1)
                return false;
            return !this.GetNode(node).IsBlack;
        }

        private void FlipColor(int node)
        {
            ArrayTreeItem n = this.GetNode(node);
            n.IsBlack = !n.IsBlack;
            if (n.Left != -1)
            {
                ArrayTreeItem n0 = this.GetNode(n.Left);
                n0.IsBlack = !n0.IsBlack;
                this.SetNode(n.Left, n0);
            }
            if (n.Right != -1)
            {
                ArrayTreeItem n0 = this.GetNode(n.Right);
                n0.IsBlack = !n0.IsBlack;
                this.SetNode(n.Right, n0);
            }
            this.SetNode(node, n);
        }

        private ArrayTreeItem GetNode(int i)
        {
            return this[i];
        }

        private void SetNode(int i, ArrayTreeItem node)
        {
            int p = i >> this.shift;
            this.pages[p].Page[i & this.mask] = node;
        }

        public void Remove(TKey key)
        {
            if (this.root != -1)
            {
                this.root = this.Remove(this.root, key);
                if (this.root != -1)
                {
                    ArrayTreeItem n = this.GetNode(this.root);
                    if (!n.IsBlack)
                    {
                        n.IsBlack = true;
                        this.SetNode(this.root, n);
                    }
                }
            }
        }

        private int Remove(int node, TKey key)
        {
            ArrayTreeItem n = this.GetNode(node);
            int x;
            int comparisonResult = this.comparison(key, n.Key);
            if (comparisonResult < 0)
            {
                if (n.Left != -1)
                {
                    if (!IsRed(n.Left) && !IsRed(this.GetNode(n.Left).Left))
                    {
                        node = this.MoveRedLeft(node);
                        n = this.GetNode(node);
                    }
                    x = n.Left;
                    n.Left = this.Remove(x, key);
                    if (n.Left != x)
                        this.SetNode(node, n);
                }
            }
            else
            {

                if (IsRed(n.Left))
                {
                    node = this.RotateRight(node);
                    n = this.GetNode(node);
                }

                

                if ((0 == this.comparison(key, n.Key)) && (n.Right == -1))
                {
                    this.count--;
                    this.Free(node);
                    return -1;
                }
                if (n.Right != -1)
                {
                    if (!IsRed(n.Right) && !IsRed(this.GetNode(n.Right).Left))
                    {
                        node = this.MoveRedRight(node);
                        n = this.GetNode(node);
                    }

                    if (0 == this.comparison(key, n.Key))
                    {
                        this.count--;

                        int m = GetExtreme(n.Right);
                        ArrayTreeItem mn = this.GetNode(m);
                        TKey k = mn.Key;
                        n.Key = mn.Key;
                        n.Value = mn.Value;

                        this.SetNode(node, n);

                        n.Right = this.DeleteMinimum(n.Right);

                        this.SetNode(node, n);

                        /*
                        if (this.Notify != null)
                            this.Notify(this.GetNodes(this.DefaultKey));
                        */
                        
                        if (m != node)
                            this.Free(m);
                        
                    }
                    else
                    {
                        x = n.Right;
                        n.Right = this.Remove(x, key);
                        if (n.Right != x)
                            this.SetNode(node, n);
                    }
                }
            }

            return this.FixUp(node);
        }

        public int NodesCount
        {
            get
            {
                int c = 0;
                int t = this.pageCount << this.shift;
                for (int i = 0; i < t; i++)
                    if (this[i].Mode != ArrayTreeItemMode.Free) c++;
                return c;
            }
        }

        private int DeleteMinimum(int node)
        {
            ArrayTreeItem n = this.GetNode(node);
            int x;
            if (n.Left == -1)
            {
                /*
                if (this.comparison(key, n.Key) == 0)
                    this.Free(node);
                 */
                return -1;
            }

            if (!IsRed(n.Left) && !IsRed(this.GetNode(n.Left).Left))
            {
                x = node;
                node = this.MoveRedLeft(x);
                n = this.GetNode(node);
            }

            x = n.Left;
            n.Left = DeleteMinimum(x);
            if (x != n.Left)
                this.SetNode(node, n);

            return this.FixUp(node);
        }

        private int GetExtreme(int node)
        {
            int r = -1;
            int n = node;
            while (n != -1)
            {
                r = n;
                n = this.GetNode(n).Left;
            }
            return r;
        }


        private int FixUp(int node)
        {
            ArrayTreeItem n = this.GetNode(node);
            int x;
            if (IsRed(n.Right))
            {
                node = this.RotateLeft(node);
                n = this.GetNode(node);
            }

            if (IsRed(n.Left) && IsRed(this.GetNode(n.Left).Left))
            {
                node = this.RotateRight(node);
                n = this.GetNode(node);
            }

            if (IsRed(n.Left) && IsRed(n.Right))
            {
                FlipColor(node);
                n = this.GetNode(node);
            }

            if ((n.Left != -1) && IsRed(this.GetNode(n.Left).Right) && !IsRed(this.GetNode(n.Left).Left))
            {
                x = n.Left;
                n.Left = this.RotateLeft(x);
                if (x != n.Left)
                    this.SetNode(node, n);
                if (IsRed(n.Left))
                {
                    node = this.RotateRight(node);
                }
            }
            return node;
        }

        private int MoveRedLeft(int node)
        {
            this.FlipColor(node);
            int x;
            ArrayTreeItem n = this.GetNode(node);
            if (IsRed(this.GetNode(n.Right).Left))
            {
                x = n.Right;
                n.Right = RotateRight(x);
                if (n.Right != x)
                    this.SetNode(node, n);
                node = this.RotateLeft(node);
                this.FlipColor(node);
                n = this.GetNode(node);

                if (IsRed(this.GetNode(n.Right).Right))
                {
                    x = n.Right;
                    n.Right = RotateLeft(x);
                    if (x != n.Right)
                        this.SetNode(node, n);
                }
            }
            return node;
        }

        private int MoveRedRight(int node)
        {
            this.FlipColor(node);
            ArrayTreeItem n = this.GetNode(node);
            if (IsRed(this.GetNode(n.Left).Left))
            {
                node = this.RotateRight(node);
                FlipColor(node);
            }
            return node;
        }

        private int Add(int node, TKey key, TValue value)
        {
            if (node == -1)
            {
                this.count++;
                return this.New(key, value);
            }

            if (IsRed(this[node].Left) && IsRed(this[node].Right))
            {
                this.FlipColor(node);
            }

            int comparisonResult = this.comparison(key, this.GetNode(node).Key);
            if (comparisonResult < 0)
            {
                ArrayTreeItem n = this.GetNode(node);
                n.Left = Add(n.Left, key, value);
                this.SetNode(node, n);
            }
            else if (0 < comparisonResult)
            {
                ArrayTreeItem n = this.GetNode(node);
                n.Right = Add(n.Right, key, value);
                this.SetNode(node, n);
            }
            else
            {
                ArrayTreeItem n = this.GetNode(node);
                n.Value = value;
                this.SetNode(node, n);
            }

            if (IsRed(this.GetNode(node).Right))
            {
                node = this.RotateLeft(node);
            }

            if (IsRed(this.GetNode(node).Left) && IsRed(this.GetNode(this.GetNode(node).Left).Left))
            {
                node = this.RotateRight(node);
            }

            return node;
        }

        private int RotateLeft(int node)
        {
            ArrayTreeItem n = this.GetNode(node);
            int x = n.Right;
            ArrayTreeItem nx = this.GetNode(x);
            n.Right = nx.Left;
            nx.Left = node;
            nx.IsBlack = n.IsBlack;
            n.IsBlack = false;
            this.SetNode(node, n);
            this.SetNode(x, nx);
            return x;
        }

        private int RotateRight(int node)
        {
            ArrayTreeItem n = this.GetNode(node);
            int x = n.Left;
            ArrayTreeItem nx = this.GetNode(x);
            n.Left = nx.Right;
            nx.Right = node;
            nx.IsBlack = n.IsBlack;
            n.IsBlack = false;
            this.SetNode(node, n);
            this.SetNode(x, nx);
            return x;
        }

        struct ArrayTreePage
        {
            public ArrayTreeItem[] Page;

            public ArrayTreePage(int size)
            {
                this.Page = new ArrayTreeItem[size];
            }
        }

        struct ArrayTreeItem
        {
            public ArrayTreeItemMode Mode;
            public int Left;
            public int Right;
            public TKey Key;
            public TValue Value;

            public bool IsBlack
            {
                get { return this.Mode == ArrayTreeItemMode.Black; }
                set { this.Mode = value ? ArrayTreeItemMode.Black : ArrayTreeItemMode.Red; }
            }

            public int Next
            {
                get { return this.Left; }
                set { this.Left = value; }
            }

            public override string ToString()
            {
                if (this.Mode == ArrayTreeItemMode.Free)
                    return String.Format("Free, next [{0}]", this.Left);
                return String.Format("{0} {3} '{4}'{1}{2}", this.Mode,
                    (this.Left == -1) ? String.Empty : String.Format(" (Left [{0}])", this.Left),
                    (this.Right == -1) ? String.Empty : String.Format(" (Right [{0}])", this.Right),
                    this.Key.ToString(), this.Value.ToString());
            }
        }

        enum ArrayTreeItemMode:byte
        {
            Free = 0,
            Red,
            Black
        }
    }
}