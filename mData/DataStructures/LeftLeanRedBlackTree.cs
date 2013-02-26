using System;
using System.Collections.Generic;
using mData.DataStructures;
using mData.Testing;

namespace mData.DataStructures
{

    sealed class LeftLeanRedBlackTree<TKey, TValue> : ITreeTesting<TKey, TValue>
    {
        private Func<TKey, TKey, int> comparison;
        private LeftLeanRedBlackTreeNode root;

        public LeftLeanRedBlackTree(Func<TKey, TKey, int> comparison)
        {
            this.comparison = comparison;
            this.root = null;
        }

        public List<TreeKeyValue<TKey, TValue>> Content()
        {
            if (this.root == null) return null;
            List<TreeKeyValue<TKey, TValue>> l = new List<TreeKeyValue<TKey, TValue>>();
            LeftLeanRedBlackTree<TKey, TValue>.Iterate(this.root, l);
            return l;
        }

        public TreeNodeInfo<TKey, TValue>[] GetNodes()
        {
            if (this.root == null) return null;
            List<TreeNodeInfo<TKey, TValue>> l = new List<TreeNodeInfo<TKey, TValue>>();
            LeftLeanRedBlackTree<TKey, TValue>.Iterate(this.root, l);
            l.Sort(this.CompareNodeInfo);
            return l.ToArray();
        }

        private static void Iterate(LeftLeanRedBlackTreeNode h, List<TreeNodeInfo<TKey, TValue>> list)
        {
            TreeNodeInfo<TKey, TValue> ni = new TreeNodeInfo<TKey, TValue>();
            ni.Key = h.Key;
            ni.Value = h.Value;
            ni.IsBlack = h.IsBlack;
            ni.Left = default(TKey);
            ni.Right = default(TKey);
            if (h.Left != null) ni.Left = h.Left.Key;
            if (h.Right != null) ni.Right = h.Right.Key;
            list.Add(ni);
            if (h.Left != null) LeftLeanRedBlackTree<TKey, TValue>.Iterate(h.Left, list);
            if (h.Right != null) LeftLeanRedBlackTree<TKey, TValue>.Iterate(h.Right, list);
        }

        private int CompareNodeInfo(TreeNodeInfo<TKey, TValue> x, TreeNodeInfo<TKey, TValue> y)
        {
            return this.comparison(x.Key, y.Key);
        }

        private static void Iterate(LeftLeanRedBlackTreeNode h, List<TreeKeyValue<TKey, TValue>> list)
        {
            list.Add(new TreeKeyValue<TKey, TValue>(h.Key, h.Value));
            if (h.Left != null) LeftLeanRedBlackTree<TKey, TValue>.Iterate(h.Left, list);
            if (h.Right != null) LeftLeanRedBlackTree<TKey, TValue>.Iterate(h.Right, list);
        }

        public int MaxDepth()
        {
            if (this.root == null) return 0;
            int d = 0;
            LeftLeanRedBlackTree<TKey, TValue>.Iterate(this.root, 1, ref d);
            return d;
        }

        private static void Iterate(LeftLeanRedBlackTreeNode h, int depth, ref int maxDepth)
        {
            if (depth > maxDepth) maxDepth = depth;
            if (h.Left != null) LeftLeanRedBlackTree<TKey, TValue>.Iterate(h.Left, depth + 1, ref maxDepth);
            if (h.Right != null) LeftLeanRedBlackTree<TKey, TValue>.Iterate(h.Right, depth + 1, ref maxDepth);
        }

        private sealed class LeftLeanRedBlackTreeNode
        {
            public TKey Key;
            public TValue Value;
            public LeftLeanRedBlackTreeNode Left;
            public LeftLeanRedBlackTreeNode Right;
            public bool IsBlack;

            public override string ToString()
            {
                return String.Format("{0} {3} '{4}'{1}{2}", this.IsBlack ? "Black" : "Red", 
                    (this.Left==null) ? String.Empty : String.Format(" Left({0} {1} '{2}')", 
                    this.Left.IsBlack ? "Black" : "Red", this.Left.Key, this.Left.Value),
                    (this.Right == null) ? String.Empty : String.Format(" Right({0} {1} '{2}')",
                    this.Right.IsBlack ? "Black" : "Red", this.Right.Key, this.Right.Value),
                    this.Key, this.Value);
            }
        }

        public void Insert(TKey key, TValue value)
        {
            root = Add(root, key, value);
            root.IsBlack = true;
        }

        public void Remove(TKey key)
        {
            int initialCount = Count;
            if (null != root)
            {
                root = Remove(root, key);
                if (null != root)
                    root.IsBlack = true;
            }
        }

        public void Clear()
        {
            root = null;
            Count = 0;
        }

        public TValue Search(TKey key)
        {
            LeftLeanRedBlackTreeNode node = GetNodeForKey(key);
            if (null != node)
            {
                return node.Value;
            }
            else
            {
                throw new Exception("Key not found");
            }
        }

        public bool TryGetValue(TKey key, out TValue value)
        {
            value = default(TValue);
            LeftLeanRedBlackTreeNode node = GetNodeForKey(key);
            if (null != node)
            {
                value = node.Value;
                return true;
            }
            return false;
        }

        public int Count { get; private set; }

        private static bool IsRed(LeftLeanRedBlackTreeNode node)
        {
            if (null == node)
            {
                return false;
            }
            return !node.IsBlack;
        }

        private LeftLeanRedBlackTreeNode Add(LeftLeanRedBlackTreeNode node, TKey key, TValue value)
        {
            if (null == node)
            {
                Count++;
                return new LeftLeanRedBlackTreeNode { Key = key, Value = value };
            }

            if (IsRed(node.Left) && IsRed(node.Right))
            {
                FlipColor(node);
            }

            int comparisonResult = KeyComparison(key, node.Key);
            if (comparisonResult < 0)
            {
                node.Left = Add(node.Left, key, value);
            }
            else if (0 < comparisonResult)
            {
                node.Right = Add(node.Right, key, value);
            }
            else
            {
                node.Value = value;
            }

            if (IsRed(node.Right))
            {
                node = RotateLeft(node);
            }

            if (IsRed(node.Left) && IsRed(node.Left.Left))
            {
                node = RotateRight(node);
            }

            return node;
        }

        private LeftLeanRedBlackTreeNode Remove(LeftLeanRedBlackTreeNode node, TKey key)
        {
            int comparisonResult = KeyComparison(key, node.Key);
            if (comparisonResult < 0)
            {
                if (null != node.Left)
                {
                    if (!IsRed(node.Left) && !IsRed(node.Left.Left))
                    {
                        node = MoveRedLeft(node);
                    }

                    node.Left = Remove(node.Left, key);
                }
            }
            else
            {
                if (IsRed(node.Left))
                {
                    node = RotateRight(node);
                }

                


                if ((0 == KeyComparison(key, node.Key)) && (null == node.Right))
                {
                    Count--;
                    return null;
                }
                if (null != node.Right)
                {
                    if (!IsRed(node.Right) && !IsRed(node.Right.Left))
                    {
                        node = MoveRedRight(node);
                    }

                    if (0 == KeyComparison(key, node.Key))
                    {
                        Count--;
                        LeftLeanRedBlackTreeNode m = GetExtreme(node.Right);
                        node.Key = m.Key;
                        node.Value = m.Value;
                        node.Right = DeleteMinimum(node.Right);
                    }
                    else
                    {
                        node.Right = Remove(node.Right, key);
                    }
                }
            }

            return FixUp(node);
        }

        private static void FlipColor(LeftLeanRedBlackTreeNode node)
        {
            node.IsBlack = !node.IsBlack;
            node.Left.IsBlack = !node.Left.IsBlack;
            node.Right.IsBlack = !node.Right.IsBlack;
        }

        private static LeftLeanRedBlackTreeNode RotateLeft(LeftLeanRedBlackTreeNode node)
        {
            LeftLeanRedBlackTreeNode x = node.Right;
            node.Right = x.Left;
            x.Left = node;
            x.IsBlack = node.IsBlack;
            node.IsBlack = false;
            return x;
        }

        private static LeftLeanRedBlackTreeNode RotateRight(LeftLeanRedBlackTreeNode node)
        {
            LeftLeanRedBlackTreeNode x = node.Left;
            node.Left = x.Right;
            x.Right = node;
            x.IsBlack = node.IsBlack;
            node.IsBlack = false;
            return x;
        }

        private static LeftLeanRedBlackTreeNode MoveRedLeft(LeftLeanRedBlackTreeNode node)
        {
            FlipColor(node);
            if (IsRed(node.Right.Left))
            {
                node.Right = RotateRight(node.Right);
                node = RotateLeft(node);
                FlipColor(node);

                if (IsRed(node.Right.Right))
                {
                    node.Right = RotateLeft(node.Right);
                }
            }
            return node;
        }

        private static LeftLeanRedBlackTreeNode MoveRedRight(LeftLeanRedBlackTreeNode node)
        {
            FlipColor(node);
            if (IsRed(node.Left.Left))
            {
                node = RotateRight(node);
                FlipColor(node);
            }
            return node;
        }

        private LeftLeanRedBlackTreeNode DeleteMinimum(LeftLeanRedBlackTreeNode node)
        {
            if (null == node.Left)
            {
                return null;
            }

            if (!IsRed(node.Left) && !IsRed(node.Left.Left))
            {
                node = MoveRedLeft(node);
            }

            node.Left = DeleteMinimum(node.Left);

            return FixUp(node);
        }

        private static LeftLeanRedBlackTreeNode FixUp(LeftLeanRedBlackTreeNode node)
        {
            if (IsRed(node.Right))
            {
                node = RotateLeft(node);
            }

            if (IsRed(node.Left) && IsRed(node.Left.Left))
            {
                node = RotateRight(node);
            }

            if (IsRed(node.Left) && IsRed(node.Right))
            {
                FlipColor(node);
            }

            if ((null != node.Left) && IsRed(node.Left.Right) && !IsRed(node.Left.Left))
            {
                node.Left = RotateLeft(node.Left);
                if (IsRed(node.Left))
                {
                    node = RotateRight(node);
                }
            }

            return node;
        }

        private LeftLeanRedBlackTreeNode GetNodeForKey(TKey key)
        {
            LeftLeanRedBlackTreeNode node = root;
            while (null != node)
            {
                int comparisonResult = comparison(key, node.Key);
                if (comparisonResult < 0)
                {
                    node = node.Left;
                }
                else if (0 < comparisonResult)
                {
                    node = node.Right;
                }
                else
                {
                    return node;
                }
            }

            return null;
        }

        private static LeftLeanRedBlackTreeNode GetExtreme(LeftLeanRedBlackTreeNode node)
        {
            LeftLeanRedBlackTreeNode r = null;
            LeftLeanRedBlackTreeNode n = node;
            while (n != null)
            {
                r = n;
                n = n.Left;
            }
            return r;
        }

        private int KeyComparison(TKey leftKey, TKey rightKey)
        {
            return this.comparison(leftKey, rightKey);
        }

    }

}