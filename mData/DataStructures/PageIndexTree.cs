using System;
using System.Collections.Generic;
using mData.Threading;
using mData.Storage;
using mData.Testing;

namespace mData.DataStructures
{

    public struct IndexItemHeader
    {

        public uint Flags;
        public IndexItemReference Left;
        public IndexItemReference Right;

        public const int Size = 16;

        public const int FreeItemSignature = 0x6d657449;
        public const ulong PageSignature = 0x6567615078646e49;

        public bool IsBlack
        {
            get { return (this.Flags & 0x1) == 0x1; }
            set
            {
                if (value)
                {
                    this.Flags |= 0x1;
                }
                else
                {
                    this.Flags &= 0xfffffffe;
                }
            }
        }

    }

    public struct IndexItemReference
    {
        public int Page;
        public short Item;

        public IndexItemReference(int page, short item)
        {
            this.Page = page;
            this.Item = item;
        }

        public static IndexItemReference Null
        {
            get
            {
                IndexItemReference x = new IndexItemReference();
                x.Page = -1;
                x.Item = -1;
                return x;
            }
        }

        public bool IsNull
        {
            get { return (this.Page == -1) && (this.Item == -1); }
        }

        public bool NotEqual(IndexItemReference x)
        {
            return (this.Page != x.Page) || (this.Item != x.Item);
        }

        public static unsafe IndexItemReference Unpack(ulong v)
        {
            IndexItemReference r = new IndexItemReference();
            int* p = (int*)&v;
            r.Page = *p;
            p++;
            r.Item = *((short*)p);
            return r;
        }

        public unsafe ulong Pack()
        {
            ulong v;
            int* p = (int*)&v;
            *p = this.Page;
            p++;
            *((short*)p) = this.Item;
            return v;
        }

        public override string ToString()
        {
            return String.Format("{0}[{1}]", this.Page, this.Item);
        }
    }

    public interface IPageIndexItemSupport<TKey, TValue>
    {
        int ItemSize { get; }

        TKey GetKey(byte[] page, int offset);

        TValue GetValue(byte[] page, int offset);

        IndexItemHeader GetHeader(byte[] page, int offset);

        void Update(byte[] page, int offset, IndexItemHeader hdr);

        void Update(byte[] page, int offset, IndexItemHeader hdr, TKey key, TValue value);

        int CompareKeys(TKey x, TKey y);

        TValue EmptyValue { get; }

        TKey EmptyKey { get; }
    }

    public class PageTreeIndex<TKey, TValue> : ITreeTesting<TKey, TValue>
    {
        private IDataPageProvider provider;
        private IPageIndexItemSupport<TKey, TValue> support;
        private int rootPage;
        private short maxPageItem;
        private int headerSize;
        private int itemSize;
        private IndexItemReference root;
        private IndexItemReference free;
        private ReadWriteLock sync;
        private int count;

        PageTreeIndex(IDataPageProvider provider, IPageIndexItemSupport<TKey, TValue> support, int timeout)
        {
            this.support = support;
            this.provider = provider;
            this.root = IndexItemReference.Null;
            this.free = IndexItemReference.Null;
            this.headerSize = this.provider.PageHeaderSize;
            this.itemSize = this.support.ItemSize;
            this.maxPageItem = (short)((this.provider.PageSize - this.headerSize) / this.itemSize);
            this.rootPage = -1;
            this.sync = new ReadWriteLock("PageTreeLock", timeout);
            this.count = 0;
        }

        public int RootPage
        {
            get { return this.rootPage; }
        }

        public static PageTreeIndex<TKey, TValue> Create(IDataPageProvider provider, IPageIndexItemSupport<TKey, TValue> support, int lockTimeout)
        {
            PageTreeIndex<TKey, TValue> idx = new PageTreeIndex<TKey, TValue>(provider, support, lockTimeout);
            idx.rootPage = idx.AllocatePage();
            idx.UpdateRootPage();
            return idx;
        }

        public static PageTreeIndex<TKey, TValue> Open(int rootPage, IDataPageProvider provider, IPageIndexItemSupport<TKey, TValue> support, int lockTimeout)
        {
            byte[] page = provider.ReadLock(rootPage);
            PageHeaderData h = provider.GetPageHeader(page);
            provider.ReadUnlock(rootPage);
            if (h.Data0 != IndexItemHeader.PageSignature)
                throw new DataException("Invalid index page signature");

            PageTreeIndex<TKey, TValue> idx = new PageTreeIndex<TKey, TValue>(provider, support, lockTimeout);
            idx.rootPage = rootPage;
            idx.root = IndexItemReference.Unpack(h.Data1);
            idx.free = IndexItemReference.Unpack(h.Data2);
            idx.count = (int)h.Data3;

            return idx;
        }

        public int Count
        {
            get { return this.count; }
        }


        public List<TreeKeyValue<TKey, TValue>> Content()
        {
            List<TreeKeyValue<TKey, TValue>> l = null;
            this.sync.ReadLock();
            try
            {
                if (!this.root.IsNull)
                {
                    l = new List<TreeKeyValue<TKey, TValue>>();
                    this.Iterate(this.root, l);
                }
            }
            finally
            {
                this.sync.ReadUnlock();
            }
            return l;
        }


        public TreeNodeInfo<TKey, TValue>[] GetNodes()
        {
            List<TreeNodeInfo<TKey, TValue>> l = null;
            this.sync.ReadLock();
            try
            {
                if (!this.root.IsNull)
                {
                    l = new List<TreeNodeInfo<TKey, TValue>>();
                    this.Iterate(this.root, l);
                    l.Sort(this.CompareNodeInfo);
                }
            }
            finally
            {
                this.sync.ReadUnlock();
            }
            if (l == null) return null;
            return l.ToArray();
        }

        private int CompareNodeInfo(TreeNodeInfo<TKey, TValue> x, TreeNodeInfo<TKey, TValue> y)
        {
            return this.support.CompareKeys(x.Key, y.Key);
        }

        private void Iterate(IndexItemReference h, List<TreeNodeInfo<TKey, TValue>> list)
        {
            TreeNodeInfo<TKey, TValue> ni = new TreeNodeInfo<TKey, TValue>();
            IndexItem n = this.GetNode(h, true);
            ni.Key = n.Key;
            ni.Value = n.Value;
            ni.IsBlack = n.Header.IsBlack;
            ni.Left = ni.Right = this.support.EmptyKey;
            if (!n.Header.Left.IsNull) ni.Left = this.GetNode(n.Header.Left).Key;
            if (!n.Header.Right.IsNull) ni.Right = this.GetNode(n.Header.Right).Key;
            list.Add(ni);
            if (!n.Header.Left.IsNull) this.Iterate(n.Header.Left, list);
            if (!n.Header.Right.IsNull) this.Iterate(n.Header.Right, list);
        }

        private void Iterate(IndexItemReference h, List<TreeKeyValue<TKey, TValue>> list)
        {
            IndexItem n = this.GetNode(h, true);
            list.Add(new TreeKeyValue<TKey, TValue>(n.Key, n.Value));
            if (!n.Header.Left.IsNull) this.Iterate(n.Header.Left, list);
            if (!n.Header.Right.IsNull) this.Iterate(n.Header.Right, list);
        }


        public void Insert(TKey key, TValue value)
        {
            this.sync.WriteLock();
            try
            {
                this.root = this.Add(root, key, value);
                IndexItem r = this.GetNode(this.root);
                if (!r.Header.IsBlack)
                {
                    r.Header.IsBlack = true;
                    this.SetNode(this.root, r);
                }
                this.UpdateRootPage();
            }
            finally
            {
                this.sync.WriteUnlock(false);
            }
        }

        public void Remove(TKey key)
        {
            this.sync.WriteLock();
            try
            {
                if (!this.root.IsNull)
                {
                    this.root = this.Remove(this.root, key);
                    if (!this.root.IsNull)
                    {
                        IndexItem n = this.GetNode(this.root);
                        if (!n.Header.IsBlack)
                        {
                            n.Header.IsBlack = true;
                            this.SetNode(this.root, n);
                        }
                    }
                    this.UpdateRootPage();
                }
            }
            finally
            {
                this.sync.WriteUnlock(false);
            }
        }

        private IndexItemReference Remove(IndexItemReference node, TKey key)
        {
            IndexItem n = this.GetNode(node);
            IndexItemReference x;
            int comparisonResult = this.support.CompareKeys(key, n.Key);
            if (comparisonResult < 0)
            {
                if (!n.Header.Left.IsNull)
                {
                    if (!IsRed(n.Header.Left) && !IsRed(this.GetNode(n.Header.Left).Header.Left))
                    {
                        node = this.MoveRedLeft(node);
                        n = this.GetNode(node);
                    }
                    x = n.Header.Left;
                    n.Header.Left = this.Remove(x, key);
                    if (n.Header.Left.NotEqual(x))
                        this.SetNode(node, n);
                }
            }
            else
            {

                if (IsRed(n.Header.Left))
                {
                    node = this.RotateRight(node);
                    n = this.GetNode(node);
                }

                if ((0 == this.support.CompareKeys(key, n.Key)) && (n.Header.Right.IsNull))
                {
                    this.count--;
                    this.Free(node);
                    return IndexItemReference.Null;
                }
                if (!n.Header.Right.IsNull)
                {
                    if (!IsRed(n.Header.Right) && !IsRed(this.GetNode(n.Header.Right).Header.Left))
                    {
                        node = this.MoveRedRight(node);
                        n = this.GetNode(node);
                    }

                    if (0 == this.support.CompareKeys(key, n.Key))
                    {
                        this.count--;

                        IndexItemReference m = this.GetExtreme(n.Header.Right);
                        IndexItem mn = this.GetNode(m, true);

                        this.SetNode(node, n, mn.Key, mn.Value);

                        n.Header.Right = this.DeleteMinimum(n.Header.Right);

                        this.SetNode(node, n);

                        if (m.NotEqual(node))
                            this.Free(m);

                    }
                    else
                    {
                        x = n.Header.Right;
                        n.Header.Right = this.Remove(x, key);
                        if (n.Header.Right.NotEqual(x))
                            this.SetNode(node, n);
                    }
                }
            }

            return this.FixUp(node);
        }

        private IndexItemReference DeleteMinimum(IndexItemReference node)
        {
            IndexItem n = this.GetNode(node);
            IndexItemReference x;
            if (n.Header.Left.IsNull)
            {
                /*
                if (this.comparison(key, n.Key) == 0)
                    this.Free(node);
                 */
                return IndexItemReference.Null;
            }

            if (!IsRed(n.Header.Left) && !IsRed(this.GetNode(n.Header.Left).Header.Left))
            {
                x = node;
                node = this.MoveRedLeft(x);
                n = this.GetNode(node);
            }

            x = n.Header.Left;
            n.Header.Left = DeleteMinimum(x);
            if (x.NotEqual(n.Header.Left))
                this.SetNode(node, n);

            return this.FixUp(node);
        }

        private IndexItemReference Add(IndexItemReference node, TKey key, TValue value)
        {
            if (node.IsNull)
            {
                this.count++;
                return this.New(key, value);
            }

            IndexItem n = this.GetNode(node);
            if (this.IsRed(n.Header.Left) && this.IsRed(n.Header.Right))
            {
                this.FlipColor(node);
                n = this.GetNode(node);
            }

            int comparisonResult = this.support.CompareKeys(key, n.Key);
            if (comparisonResult < 0)
            {
                n.Header.Left = this.Add(n.Header.Left, key, value);
                this.SetNode(node, n);
            }
            else if (0 < comparisonResult)
            {
                n.Header.Right = this.Add(n.Header.Right, key, value);
                this.SetNode(node, n);
            }
            else
            {
                this.SetNode(node, n, key, value);
            }

            if (IsRed(this.GetNode(node).Header.Right))
            {
                node = this.RotateLeft(node);
            }

            if (IsRed(this.GetNode(node).Header.Left) && IsRed(this.GetNode(this.GetNode(node).Header.Left).Header.Left))
            {
                node = this.RotateRight(node);
            }

            return node;
        }

        private IndexItem GetNode(IndexItemReference node)
        {
            return this.GetNode(node, false);
        }

        private IndexItem GetNode(IndexItemReference node, bool value)
        {
            byte[] page = this.provider.ReadLock(node.Page);
            PageHeaderData h = this.provider.GetPageHeader(page);
            if (h.Data0 != IndexItemHeader.PageSignature)
            {
                this.provider.ReadUnlock(node.Page);
                throw new DataException("Invalid tree page signature");
            }
            IndexItem item = new IndexItem();
            int ofs = this.ItemOffset(node.Item);
            item.Header = this.support.GetHeader(page, ofs);
            item.Key = this.support.GetKey(page, ofs);
            if (value)
                item.Value = this.support.GetValue(page, ofs);
            this.provider.ReadUnlock(node.Page);
            return item;
        }

        private void SetNode(IndexItemReference node, IndexItem item)
        {
            byte[] page = this.provider.WriteLock(node.Page);
            PageHeaderData h = this.provider.GetPageHeader(page);
            if (h.Data0 != IndexItemHeader.PageSignature)
            {
                this.provider.ReadUnlock(node.Page);
                throw new DataException("Invalid tree page signature");
            }
            this.support.Update(page, this.ItemOffset(node.Item), item.Header);
            this.provider.WriteUnlock(node.Page, true);
        }

        private void SetNode(IndexItemReference node, IndexItem item, TKey key, TValue value)
        {
            byte[] page = this.provider.WriteLock(node.Page);
            PageHeaderData h = this.provider.GetPageHeader(page);
            if (h.Data0 != IndexItemHeader.PageSignature)
            {
                this.provider.ReadUnlock(node.Page);
                throw new DataException("Invalid tree page signature");
            }
            this.support.Update(page, this.ItemOffset(node.Item), item.Header, key, value);
            this.provider.WriteUnlock(node.Page, true);
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            value = this.support.EmptyValue;
            bool found = false;
            this.sync.ReadLock();
            try
            {
                IndexItemReference node = this.root;
                while (!node.IsNull)
                {
                    IndexItem n = this.GetNode(node, true);
                    int comparisonResult = this.support.CompareKeys(key, n.Key);
                    if (comparisonResult < 0)
                    {
                        node = n.Header.Left;
                    }
                    else if (0 < comparisonResult)
                    {
                        node = n.Header.Right;
                    }
                    else
                    {
                        value = n.Value;
                        found = true;
                        break;
                    }
                }
            }
            finally
            {
                this.sync.ReadUnlock();
            }
            return found;
        }

        private int ItemOffset(short i)
        {
            return this.headerSize + (i * this.itemSize);
        }

        private int AllocatePage()
        {
            int p = this.provider.Allocate();
            byte[] page = this.provider.WriteLock(p);
            IndexItemHeader h = new IndexItemHeader();
            h.Right.Page = IndexItemHeader.FreeItemSignature;
            h.Left = this.free;
            for (short i = 0; i < this.maxPageItem; i++)
            {
                this.support.Update(page, this.ItemOffset(i), h);
                h.Left.Page = p;
                h.Left.Item = i;
            }
            PageHeaderData ph = this.provider.GetPageHeader(page);
            ph.Data0 = IndexItemHeader.PageSignature;
            this.provider.SetPageHeader(p, page, ph);
            this.provider.WriteUnlock(p, true);
            this.free.Page = p;
            this.free.Item = (short)(this.maxPageItem - 1);
            return p;
        }

        private void UpdateRootPage()
        {
            byte[] page = this.provider.WriteLock(this.rootPage);
            PageHeaderData h = this.provider.GetPageHeader(page);
            h.Data1 = this.root.Pack();
            h.Data2 = this.free.Pack();
            h.Data3 = (ulong)this.count;
            this.provider.SetPageHeader(this.rootPage, page, h);
            this.provider.WriteUnlock(this.rootPage, true);
        }

        private bool IsRed(IndexItemReference node)
        {
            if (node.IsNull)
                return false;
            return !this.GetNode(node).Header.IsBlack;
        }

        private void FlipColor(IndexItemReference node)
        {
            IndexItem n = this.GetNode(node);
            n.Header.IsBlack = !n.Header.IsBlack;
            if (!n.Header.Left.IsNull)
            {
                IndexItem n0 = this.GetNode(n.Header.Left);
                n0.Header.IsBlack = !n0.Header.IsBlack;
                this.SetNode(n.Header.Left, n0);
            }
            if (!n.Header.Right.IsNull)
            {
                IndexItem n0 = this.GetNode(n.Header.Right);
                n0.Header.IsBlack = !n0.Header.IsBlack;
                this.SetNode(n.Header.Right, n0);
            }
            this.SetNode(node, n);
        }

        private IndexItemReference GetExtreme(IndexItemReference node)
        {
            IndexItemReference r = IndexItemReference.Null;
            IndexItemReference n = node;
            while (!n.IsNull)
            {
                r = n;
                n = this.GetNode(n).Header.Left;
            }
            return r;
        }

        private IndexItemReference FixUp(IndexItemReference node)
        {
            IndexItem n = this.GetNode(node);
            IndexItemReference x;
            if (IsRed(n.Header.Right))
            {
                node = this.RotateLeft(node);
                n = this.GetNode(node);
            }

            if (IsRed(n.Header.Left) && IsRed(this.GetNode(n.Header.Left).Header.Left))
            {
                node = this.RotateRight(node);
                n = this.GetNode(node);
            }

            if (IsRed(n.Header.Left) && IsRed(n.Header.Right))
            {
                FlipColor(node);
                n = this.GetNode(node);
            }

            if ((!n.Header.Left.IsNull) && IsRed(this.GetNode(n.Header.Left).Header.Right) && !IsRed(this.GetNode(n.Header.Left).Header.Left))
            {
                x = n.Header.Left;
                n.Header.Left = this.RotateLeft(x);
                if (x.NotEqual(n.Header.Left))
                    this.SetNode(node, n);
                if (IsRed(n.Header.Left))
                {
                    node = this.RotateRight(node);
                }
            }
            return node;
        }

        private IndexItemReference MoveRedLeft(IndexItemReference node)
        {
            this.FlipColor(node);
            IndexItemReference x;
            IndexItem n = this.GetNode(node);
            if (IsRed(this.GetNode(n.Header.Right).Header.Left))
            {
                x = n.Header.Right;
                n.Header.Right = RotateRight(x);
                if (n.Header.Right.NotEqual(x))
                    this.SetNode(node, n);
                node = this.RotateLeft(node);
                this.FlipColor(node);
                n = this.GetNode(node);

                if (IsRed(this.GetNode(n.Header.Right).Header.Right))
                {
                    x = n.Header.Right;
                    n.Header.Right = this.RotateLeft(x);
                    if (x.NotEqual(n.Header.Right))
                        this.SetNode(node, n);
                }
            }
            return node;
        }

        private IndexItemReference MoveRedRight(IndexItemReference node)
        {
            this.FlipColor(node);
            IndexItem n = this.GetNode(node);
            if (IsRed(this.GetNode(n.Header.Left).Header.Left))
            {
                node = this.RotateRight(node);
                this.FlipColor(node);
            }
            return node;
        }

        private IndexItemReference RotateLeft(IndexItemReference node)
        {
            IndexItem n = this.GetNode(node);
            IndexItemReference x = n.Header.Right;
            IndexItem nx = this.GetNode(x);
            n.Header.Right = nx.Header.Left;
            nx.Header.Left = node;
            nx.Header.IsBlack = n.Header.IsBlack;
            n.Header.IsBlack = false;
            this.SetNode(node, n);
            this.SetNode(x, nx);
            return x;
        }

        private IndexItemReference RotateRight(IndexItemReference node)
        {
            IndexItem n = this.GetNode(node);
            IndexItemReference x = n.Header.Left;
            IndexItem nx = this.GetNode(x);
            n.Header.Left = nx.Header.Right;
            nx.Header.Right = node;
            nx.Header.IsBlack = n.Header.IsBlack;
            n.Header.IsBlack = false;
            this.SetNode(node, n);
            this.SetNode(x, nx);
            return x;
        }

        private void Free(IndexItemReference node)
        {
            IndexItem n = new IndexItem();
            n.Header.Right.Page = IndexItemHeader.FreeItemSignature;
            n.Header.Left = this.free;
            this.SetNode(node, n);
            this.free = node;
        }

        private IndexItemReference New(TKey key, TValue value)
        {
            if (this.free.IsNull)
                this.AllocatePage();
            IndexItemReference i = this.free;
            IndexItem n = this.GetNode(i);
            if (n.Header.Right.Page != IndexItemHeader.FreeItemSignature)
                throw new DataException("Invalid free index item signature");
            this.free = n.Header.Left;

            n.Header.IsBlack = false;
            n.Header.Left = IndexItemReference.Null;
            n.Header.Right = IndexItemReference.Null;

            this.SetNode(i, n, key, value);

            return i;
        }

        struct IndexItem
        {
            public IndexItemHeader Header;
            public TKey Key;
            public TValue Value;
        }
    }


}