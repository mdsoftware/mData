using System;
using mData.Value;
using mData.Utils;
using mData.Expressions;
using mData.DataStructures;

namespace mData.Query
{

    /// <summary>
    /// Results of the query
    /// </summary>
    public sealed class QueryResult
    {

        private int lastColumnId;
        private ArrayTree<QueryResultColumnInfo, int> columns;
        private PagedList<QueryResultRow> rows;
        private ColumnAggregateInfo[] sortParam;
        private ColumnAggregateInfo[] defaultColumns;

        public const int MaxColumn = 1000;

        QueryResult()
        {
            this.lastColumnId = 1000;
            this.columns = QueryResult.NewColumnList(QueryResult.MaxColumn);
            this.rows = QueryResult.NewRows(100);
            this.sortParam = null;
            this.defaultColumns = null;
        }

        private static PagedList<QueryResultRow> NewRows(int pages)
        {
            return new PagedList<QueryResultRow>(PageFactor.Page1024, pages);
        }

        public static QueryResult Create()
        {
            return new QueryResult();
        }

        public void Query(string query, Func<string, FuncCallContext, DataValue> functions)
        {
            string msg;
            LisqItem q = LisqItem.Parse(query, out msg);
            if (q == null)
                throw new ArgumentException(msg);
            if (q.Type != LisqItemType.List)
                throw new ArgumentException("List expected");
            if (q.Count == 0)
                return;

            PivotGroupParam param = null;

            for (int i = 0; i < q.Count; i++)
            {
                LisqItem qq = q[i];
                if (!qq.CheckItem(0, LisqItemType.Symbol))
                    throw new ArgumentException("Invalid subquery");

                switch (qq[0].Value.ToLower())
                {
                    case "calc":
                        if (qq.CheckItem(1, LisqItemType.Symbol) && qq.CheckItem(2, LisqItemType.String))
                        {
                            if (param != null)
                            {
                                this.DoGroupPivot(param);
                                param = null;
                            }
                            this.Calculate(qq[1].Value, qq[2].Value, functions);
                        }
                        else
                        {
                            throw new ArgumentException("Invalid calc arguments");
                        }
                        break;

                    case "filter":
                        if (qq.CheckItem(1, LisqItemType.String))
                        {
                            if (param != null)
                            {
                                this.DoGroupPivot(param);
                                param = null;
                            }
                            this.Filter(qq[1].Value, functions);
                        }
                        else
                        {
                            throw new ArgumentException("Invalid calc arguments");
                        }
                        break;

                    case "rename":
                        if (qq.CheckItem(1, LisqItemType.String))
                        {
                            if (param != null)
                            {
                                this.DoGroupPivot(param);
                                param = null;
                            }
                            this.RenameColumns(qq[1].Value, functions);
                        }
                        else
                        {
                            throw new ArgumentException("Invalid calc arguments");
                        }
                        break;

                    case "sort":
                        if (param != null)
                        {
                            this.DoGroupPivot(param);
                            param = null;
                        }
                        this.QuerySort(qq);
                        break;

                    case "group":
                        {
                            PivotGroupParam p = this.GroupPivotParams(qq, false);
                            if (param == null)
                            {
                                param = p;
                            }
                            else
                            {
                                if (param.Pivot)
                                {
                                    this.GroupPivot(p.Columns, p.Aggregates, param.Columns, param.Aggregates);
                                    param = null;
                                }
                                else
                                {
                                    this.DoGroupPivot(param);
                                    param = p;
                                }
                            }
                        }
                        break;

                    case "pivot":
                        {
                            PivotGroupParam p = this.GroupPivotParams(qq, true);
                            if (param == null)
                            {
                                param = p;
                            }
                            else
                            {
                                if (param.Pivot)
                                {
                                    this.DoGroupPivot(param);
                                    param = p;
                                }
                                else
                                {
                                    this.GroupPivot(param.Columns, param.Aggregates, p.Columns, p.Aggregates);
                                    param = null;
                                }
                            }
                        }
                        break;

                    default:
                        throw new ArgumentException(String.Format("Unsupported function '{0}'", qq[0].Value));
                }
            }
            if (param != null)
                this.DoGroupPivot(param);
        }

        private void DoGroupPivot(PivotGroupParam param)
        {
            if (param.Pivot)
            {
                this.GroupPivot(null, null, param.Columns, param.Aggregates);
            }
            else
            {
                this.GroupPivot(param.Columns, param.Aggregates, null, null);
            }
        }

        private PivotGroupParam GroupPivotParams(LisqItem item, bool pivot)
        {
            if (item.Count < 3)
                throw new ArgumentException("Invalid group/pivot arguments");
            string[] columns = item[1].ToSymbolArray();
            if (columns == null)
                throw new ArgumentException("Invalid group/pivot column list");

            PagedList<QueryParameter> aggregates = new PagedList<QueryParameter>(PageFactor.Page16, 4);
            for (int i = 2; i < item.Count; i++)
            {
                string[] sym = item[i].ToSymbolArray();
                if (sym == null)
                    throw new ArgumentException("Invalid group/pivot aggregates");
                if (sym.Length < 2)
                    throw new ArgumentException("Invalid group/pivot aggregates");

                QueryParamOperation op = QueryParamOperation.Default;
                switch (sym[0].ToLower())
                {
                    case "sum":
                        op = QueryParamOperation.Sum;
                        break;

                    case "avg":
                        op = QueryParamOperation.Average;
                        break;

                    case "min":
                        op = QueryParamOperation.Min;
                        break;

                    case "max":
                        op = QueryParamOperation.Max;
                        break;

                    case "count":
                        op = QueryParamOperation.Count;
                        break;

                    case "collect":
                        op = QueryParamOperation.Collect;
                        break;

                }
                if (op == QueryParamOperation.Default)
                    throw new ArgumentException("Invalid group/pivot aggregate function");

                for (int j = 1; j < sym.Length; j++)
                    aggregates.Add(new QueryParameter(sym[j], op));
            }
            return new PivotGroupParam(pivot, columns, aggregates.ToArray());
        }

        private void QuerySort(LisqItem item)
        {
            int c = item.Count - 1;
            if (c > 0)
            {
                QueryParameter[] param = new QueryParameter[c];
                for (int j = 0; j < c; j++)
                {
                    string[] sym = item[j + 1].ToSymbolArray();
                    if (sym == null)
                        throw new ArgumentException("Invalid sort argument");
                    if (sym.Length > 2)
                        throw new ArgumentException("Invalid sort argument");
                    if (sym.Length > 1)
                    {
                        QueryParamOperation op = QueryParamOperation.SortAscending;
                        switch (sym[0].ToLower())
                        {
                            case "asc":
                                op = QueryParamOperation.SortAscending;
                                break;

                            case "desc":
                                op = QueryParamOperation.SortDescending;
                                break;

                            default:
                                throw new ArgumentException("Invalid sort function");
                        }
                        param[j] = new QueryParameter(sym[1], op);
                    }
                    else
                    {
                        param[j] = new QueryParameter(sym[0], QueryParamOperation.SortAscending);
                    }
                }
                this.Sort(param);
            }
        }

        public int Count
        {
            get { return this.rows.Count; }
        }

        public DataValue this[int row, int columnId]
        {
            get
            {
                if ((row < 0) || (row >= this.rows.Count))
                    throw new IndexOutOfRangeException("Row is out of range");
                return this.rows[row][columnId].DataValue;
            }
            set
            {
                if ((row < 0) || (row >= this.rows.Count))
                    throw new IndexOutOfRangeException("Row is out of range");
                this.rows[row][columnId] = QueryRowValue.New(value);
            }
        }

        public static string ColumnNameById(int id)
        {
            return String.Format("column_{0}", id);
        }

        public DataValue AllColumns(int row)
        {
            if ((row < 0) || (row >= this.rows.Count))
                throw new IndexOutOfRangeException("Row is out of range");
            ColumnAggregateInfo[] cols = new ColumnAggregateInfo[this.columns.Count];
            QueryResultColumnInfo[] all = this.columns.AllKeys();
            for (int i = 0; i < all.Length; i++)
                cols[i] = new ColumnAggregateInfo(all[i].Id, QueryResult.ColumnNameById(all[i].Id));
            return this.rows[row].GetRecord(cols);
        }

        public DataValue this[int row]
        {
            get
            {
                if ((row < 0) || (row >= this.rows.Count))
                    throw new IndexOutOfRangeException("Row is out of range");
                if (!this.CheckDefaultColumns())
                    return null;
                return this.rows[row].GetRecord(this.defaultColumns);
            }
            set
            {
                if ((row < 0) || (row >= this.rows.Count))
                    throw new IndexOutOfRangeException("Row is out of range");
                this.rows[row] = this.ToRow(value);
            }
        }

        private bool CheckDefaultColumns()
        {
            if (this.defaultColumns != null) return true;
            QueryResultColumnInfo[] c = this.columns.AllKeys();
            int cnt = 0;
            for (int i = 0; i < c.Length; i++)
                if (c[i].Aggregate == QueryParamOperation.Default) cnt++;
            if (cnt == 0)
                return false;
            this.defaultColumns = new ColumnAggregateInfo[cnt];
            int p = 0;
            for (int i = 0; i < c.Length; i++)
            {
                if (c[i].Aggregate == QueryParamOperation.Default)
                    this.defaultColumns[p++] = new ColumnAggregateInfo(c[i].Id, c[i].Name);
            }
            c = null;
            return true;
        }

        private ColumnAggregateInfo[] Convert(string[] columns)
        {
            if (columns == null)
                throw new ArgumentException("Columns list must be specified");
            if (columns.Length == 0)
                throw new ArgumentException("Columns list must not be empty");
            ColumnAggregateInfo[] a = new ColumnAggregateInfo[columns.Length];
            for (int i = 0; i < columns.Length; i++)
            {
                string name = columns[i];
                int id = this.FindAddColumn(name, QueryParamOperation.Default, null, false);
                if (id == -1)
                    throw new ArgumentException(String.Format("Column '{0}' not found", name));
                a[i] = new ColumnAggregateInfo(id, name);
            }
            return a;
        }

        private ColumnAggregateInfo[] Convert(QueryParameter[] aggregates, bool pivot)
        {
            if (aggregates == null)
                throw new ArgumentException("Columns list must be specified");
            if (aggregates.Length == 0)
                throw new ArgumentException("Columns list must not be empty");
            ColumnAggregateInfo[] a = new ColumnAggregateInfo[aggregates.Length];
            for (int i = 0; i < aggregates.Length; i++)
            {
                switch (aggregates[i].Operation)
                {
                    case QueryParamOperation.Average:
                    case QueryParamOperation.Collect:
                    case QueryParamOperation.Count:
                    case QueryParamOperation.Max:
                    case QueryParamOperation.Min:
                    case QueryParamOperation.Sum:
                        break;

                    case QueryParamOperation.Default:
                        if (pivot) break;
                        throw new ArgumentException("Invalid aggregate operation");

                    default:
                        throw new ArgumentException("Invalid aggregate operation");
                }
                string name = aggregates[i].Name;
                int id = this.FindAddColumn(name, QueryParamOperation.Default, null, false);
                if (id == -1)
                    throw new ArgumentException(String.Format("Column '{0}' not found", name));
                a[i] = new ColumnAggregateInfo(id, name, aggregates[i].Operation);
            }
            return a;
        }

        private int[] ColumnsToKeep(KeyAggregate group, KeyAggregate pivot)
        {

            ArrayTree<int, string> columnList = new ArrayTree<int, string>(Comparers.Compare, null, this.columns.Count, PageFactor.Page32);

            if (group == null)
            {
                QueryResultColumnInfo[] cols = this.columns.AllKeys();
                for (int i = 0; i < cols.Length; i++)
                    if (cols[i].Aggregate == QueryParamOperation.Default)
                        columnList.Insert(cols[i].Id, cols[i].Name);
            }
            else
            {
                for (int i = 0; i < group.Keys.Length; i++)
                    columnList.Insert(group.Keys[i].Id, group.Keys[i].Name);
            }

            if (pivot != null)
            {
                for (int i = 0; i < pivot.Keys.Length; i++)
                    if (columnList.ContainsKey(pivot.Keys[i].Id))
                        columnList.Remove(pivot.Keys[i].Id);

                for (int i = 0; i < pivot.Aggregates.Length; i++)
                    if (columnList.ContainsKey(pivot.Aggregates[i].Id))
                        columnList.Remove(pivot.Aggregates[i].Id);
            }

            if (columnList.Count == 0) return null;

            return columnList.AllKeys();
        }

        private static ArrayTree<QueryResultColumnInfo, int> NewColumnList(int count)
        {
            return new ArrayTree<QueryResultColumnInfo, int>(QueryResultColumnInfo.Compare, 0, count, PageFactor.Page64);
        }

        public void RenameColumns(Func<QueryResultColumnInfo, string> func)
        {
            ArrayTree<QueryResultColumnInfo, int> cols = QueryResult.NewColumnList(this.columns.Count);

            QueryResultColumnInfo[] list = this.columns.AllKeys();
            for (int i = 0; i < list.Length; i++)
            {
                string name = func(list[i]);
                if (name == null)
                {
                    cols.Insert(list[i], list[i].Id);
                    continue;
                }
                if (!Expression.IsSymbol(name))
                    throw new DataException("Invalid column name '{0}'", name);
                QueryResultColumnInfo c = new QueryResultColumnInfo(list[i].Id, name, QueryParamOperation.Default, null);
                if (cols.ContainsKey(c))
                    throw new DataException("Column '{0}' already exists", name);
                cols.Insert(c, c.Id);
            }

            list = null;
            this.columns = null;
            this.columns = cols;
            this.defaultColumns = null;
            cols = null;
        }

        public void RenameColumns(string expression, Func<string, FuncCallContext, DataValue> functions)
        {
            string msg;
            Expression exp = Expression.Compile(expression, out msg);
            if (exp == null)
                throw new ArgumentException(String.Format("Expression error: " + msg));

            ArrayTree<QueryResultColumnInfo, int> cols = QueryResult.NewColumnList(this.columns.Count);
            QueryResultColumnInfo[] list = this.columns.AllKeys();

            DataValue[] stack = new DataValue[Expression.StackSize];

            for (int i = 0; i < list.Length; i++)
            {
                QueryResultColumnInfo c = list[i];
                DataValue col = DataValue.Record(16);

                col["Name"] = DataValue.New(c.Name);
                col["IsAggregate"] = DataValue.New(c.IsAggregate);
                col["IsSum"] = DataValue.New(c.IsSum);
                col["IsAverage"] = DataValue.New(c.IsAverage);
                col["IsMax"] = DataValue.New(c.IsMax);
                col["IsMin"] = DataValue.New(c.IsMin);
                col["IsCollection"] = DataValue.New(c.IsCollection);
                if (c.PivotKey == null)
                {
                    col["IsPivot"] = DataValue.New(false);
                }
                else
                {
                    col["IsPivot"] = DataValue.New(true);
                    col["PivotKey"] = c.PivotKey;
                }

                DataValue v = exp.Execute(col, stack, stack.Length, functions);
                if (v.IsNull)
                {
                    cols.Insert(c, c.Id);
                    continue;
                }
                string name = v.String;
                if (!Expression.IsSymbol(name))
                    throw new DataException("Invalid column name '{0}'", name);
                c = new QueryResultColumnInfo(list[i].Id, name, QueryParamOperation.Default, null);
                if (cols.ContainsKey(c))
                    throw new DataException("Column '{0}' already exists", name);
                cols.Insert(c, c.Id);
            }

            list = null;
            this.columns = null;
            this.columns = cols;
            this.defaultColumns = null;
            cols = null;
        }

        public void GroupPivot(string[] groupColumns, QueryParameter[] groupAggregates, string[] pivotColumns, QueryParameter[] pivotAggregates)
        {
            KeyAggregate group = null;
            KeyAggregate pivot = null;
            KeyCollection groupKeys = null;

            if (groupColumns != null)
                group = new KeyAggregate(this.Convert(groupColumns), this.Convert(groupAggregates, false));
            if (pivotColumns != null)
                pivot = new KeyAggregate(this.Convert(pivotColumns), this.Convert(pivotAggregates, true));

            if ((group == null) && (pivot == null))
                return;

            PagedList<QueryResultRow> result = QueryResult.NewRows(this.rows.PageCount);

            int[] keep = this.ColumnsToKeep(group, pivot);

            ArrayTree<int, int> columnList = new ArrayTree<int, int>(Comparers.Compare, -1, QueryResult.MaxColumn, PageFactor.Page32);

            int j;

            if (keep != null)
                for (j = 0; j < keep.Length; j++)
                    columnList.Insert(keep[j], keep[j]);

            for (int i = 0; i < this.rows.Count; i++)
            {
                QueryResultRow src = this.rows[i];
                QueryResultRow dest = null;
                bool newrow = true;
                if (group == null)
                {
                    dest = new QueryResultRow(src.Count);
                    result.Add(dest);
                }
                else
                {
                    DataValue key = src.GetRecord(group.Keys);
                    if (groupKeys == null) groupKeys = new KeyCollection(this.rows.Count, PageFactor.Page256);
                    if (groupKeys.TryGetValue(key, out j))
                    {
                        dest = result[j];
                        newrow = false;
                    }
                    else
                    {
                        dest = new QueryResultRow(src.Count);
                        groupKeys.Set(key, result.Count);
                        result.Add(dest);
                    }
                }
                if (newrow && (keep != null))
                {
                    for (j = 0; j < keep.Length; j++)
                        dest[keep[j]] = src[keep[j]];
                }
                if (group != null)
                {
                    for (j = 0; j < group.Aggregates.Length; j++)
                    {
                        ColumnAggregateInfo ai = group.Aggregates[j];
                        int colId = this.FindAddColumn(ai.Name, ai.Operation, null, true);
                        if (!columnList.ContainsKey(colId)) columnList.Insert(colId, colId);
                        dest.Addregate(colId, ai.Operation, src[ai.Id]);
                    }
                }
                if (pivot != null)
                {
                    DataValue key = src.GetRecord(pivot.Keys);
                    for (j = 0; j < pivot.Aggregates.Length; j++)
                    {
                        ColumnAggregateInfo ai = pivot.Aggregates[j];
                        int colId = this.FindAddColumn(ai.Name, ai.Operation, key, true);
                        if (!columnList.ContainsKey(colId)) columnList.Insert(colId, colId);
                        dest.Addregate(colId, ai.Operation, src[ai.Id]);
                    }
                }
            }

            ArrayTree<QueryResultColumnInfo, int> cols = QueryResult.NewColumnList(columnList.Count);
            QueryResultColumnInfo[] all = this.columns.AllKeys();
            ColumnAggregateInfo[] aggregates = new ColumnAggregateInfo[columnList.Count];
            int k = 0;
            for (j = 0; j < all.Length; j++)
            {
                if (columnList.ContainsKey(all[j].Id))
                {
                    cols.Insert(all[j], all[j].Id);
                    if (all[j].IsAggregate)
                        aggregates[k++] = new ColumnAggregateInfo(all[j].Id, null, all[j].Aggregate);
                }
            }

            for (int i = 0; i < result.Count; i++)
            {
                QueryResultRow r = result[i];
                for (j = 0; j < k; j++)
                {
                    r.FinalizeAggregate(aggregates[j].Id, aggregates[j].Operation);
                }
            }

            this.columns = null;
            this.columns = cols;
            this.defaultColumns = null;
            cols = null;
            this.rows = null;
            this.rows = result;
            result = null;
        }

        public void Filter(Func<DataValue, bool> expression)
        {
            PagedList<QueryResultRow> r = QueryResult.NewRows(this.rows.PageCount);

            for (int i = 0; i < this.rows.Count; i++)
                if (expression(this[i])) r.Add(this.rows[i]);

            this.rows = null;
            this.rows = r;
            r = null;
        }

        public void Calculate(string columnName, Func<DataValue, DataValue> function)
        {
            int id = this.FindAddColumn(columnName, QueryParamOperation.Default, null, true);
            for (int i = 0; i < this.rows.Count; i++)
            {
                DataValue v = function(this[i]);
                this.rows[i][id] = QueryRowValue.New(v);
            }
        }

        public void Calculate(string columnName, string expression, Func<string, FuncCallContext, DataValue> functions)
        {
            string msg;
            Expression exp = Expression.Compile(expression, out msg);
            if (exp == null)
                throw new ArgumentException(String.Format("Expression error: " + msg));
            int id = this.FindAddColumn(columnName, QueryParamOperation.Default, null, true);

            DataValue[] stack = new DataValue[Expression.StackSize];

            for (int i = 0; i < this.rows.Count; i++)
            {
                DataValue v = exp.Execute(this[i], stack, stack.Length, functions);
                this.rows[i][id] = QueryRowValue.New(v);
            }
        }

        public void Filter(string expression, Func<string, FuncCallContext, DataValue> functions)
        {
            string msg;
            Expression exp = Expression.Compile(expression, out msg);
            if (exp == null)
                throw new ArgumentException(String.Format("Expression error: " + msg));

            PagedList<QueryResultRow> r = QueryResult.NewRows(this.rows.PageCount);

            DataValue[] stack = new DataValue[Expression.StackSize];

            for (int i = 0; i < this.rows.Count; i++)
            {
                DataValue v = exp.Execute(this[i], stack, stack.Length, functions);
                if (v.Type != DataValueType.Logic)
                    throw new InvalidCastException("Expression result must be boolean");
                if (v.Logic)
                    r.Add(this.rows[i]);
            }

            this.rows = null;
            this.rows = r;
            r = null;
        }

        private QueryResultRow ToRow(DataValue value)
        {
            if (value.Type != DataValueType.Record)
                throw new ArgumentException("Record type expected");
            string[] names = value.AllNames;
            QueryResultRow row = new QueryResultRow(names.Length);
            for (int i = 0; i < names.Length; i++)
            {
                int id = this.FindAddColumn(names[i], QueryParamOperation.Default, null, true);
                row[id] = QueryRowValue.New(value[names[i]]);
            }
            return row;
        }

        public int Add(DataValue value)
        {
            QueryResultRow row = this.ToRow(value);
            this.rows.Add(row);
            return this.rows.Count - 1;
        }

        private int AddRow()
        {
            this.rows.Add(new QueryResultRow(this.columns.Count));
            return this.rows.Count - 1;
        }

        public bool RenameColumn(string oldName, string newName, QueryParamOperation operation)
        {
            return this.RenameColumn(oldName, newName, operation, null);
        }

        public bool RenameColumn(string oldName, string newName, QueryParamOperation operation, DataValue pivotKey)
        {
            int id;
            if (this.columns.TryGetValue(new QueryResultColumnInfo(0, newName, QueryParamOperation.Default, null), out id))
                return false;
            if (!this.columns.TryGetValue(new QueryResultColumnInfo(0, oldName, operation, pivotKey), out id))
                return false;
            if (!Parser.IsSymbol(newName))
                throw new ArgumentException("Column name must be a symbol");
            this.columns.Remove(new QueryResultColumnInfo(0, oldName, operation, pivotKey));
            this.columns.Insert(new QueryResultColumnInfo(id, newName, QueryParamOperation.Default, null), id);
            this.defaultColumns = null;
            return true;
        }

        public int GetColumnId(string name)
        {
            return this.FindAddColumn(name, QueryParamOperation.Default, null, true);
        }

        public int GetColumnId(string name, QueryParamOperation operation)
        {
            return this.FindAddColumn(name, operation, null, true);
        }

        public int GetColumnId(string name, QueryParamOperation operation, DataValue pivotKey)
        {
            return this.FindAddColumn(name, operation, pivotKey, true);
        }

        public int FindColumn(string name)
        {
            return this.FindAddColumn(name, QueryParamOperation.Default, null, false);
        }

        public int FindColumn(string name, QueryParamOperation operation)
        {
            return this.FindAddColumn(name, operation, null, false);
        }

        public int FindColumn(string name, QueryParamOperation operation, DataValue pivotKey)
        {
            return this.FindAddColumn(name, operation, pivotKey, false);
        }

        public QueryResultColumnInfo[] Columns
        {
            get { return this.columns.AllKeys(); }
        }

        private int FindAddColumn(string name, QueryParamOperation operation, DataValue pivotKey, bool add)
        {
            int id;
            if (this.columns.TryGetValue(new QueryResultColumnInfo(0, name, operation, pivotKey), out id))
                return id;
            if (!add) return -1;
            if (!Parser.IsSymbol(name))
                throw new ArgumentException("Column name must be a symbol");
            switch (operation)
            {
                case QueryParamOperation.Average:
                case QueryParamOperation.Count:
                case QueryParamOperation.Max:
                case QueryParamOperation.Min:
                case QueryParamOperation.Default:
                case QueryParamOperation.Collect:
                case QueryParamOperation.Sum:
                    break;

                default:
                    throw new ArgumentException("Invalid column aggregate");
            }
            id = this.lastColumnId++;
            if ((operation == QueryParamOperation.Default) && (pivotKey == null))
            {
                this.defaultColumns = null;
            }
            this.columns.Insert(new QueryResultColumnInfo(id, name, operation, pivotKey), id);
            return id;
        }

        public void Sort(string column, bool descending)
        {
            QueryParameter[] p = new QueryParameter[1];
            p[0] = new QueryParameter(column, descending ? QueryParamOperation.SortDescending : QueryParamOperation.SortAscending);
            this.Sort(p);
        }

        public void Sort(string column1, bool descending1, string column2, bool descending2)
        {
            QueryParameter[] p = new QueryParameter[2];
            p[0] = new QueryParameter(column1, descending1 ? QueryParamOperation.SortDescending : QueryParamOperation.SortAscending);
            p[1] = new QueryParameter(column2, descending2 ? QueryParamOperation.SortDescending : QueryParamOperation.SortAscending);
            this.Sort(p);
        }

        public void Sort(QueryParameter[] param)
        {
            this.sortParam = new ColumnAggregateInfo[param.Length];
            for (int i = 0; i < param.Length; i++)
            {
                int id;
                if (!this.columns.TryGetValue(new QueryResultColumnInfo(0, param[i].Name, QueryParamOperation.Default, null), out id))
                    throw new ArgumentException(String.Format("Column '{0}' not found", param[i].Name));
                this.sortParam[i] = new ColumnAggregateInfo(id, param[i].Name, param[i].Operation == QueryParamOperation.SortDescending);
            }
            this.rows.Sort(this.Compare);
            this.sortParam = null;
        }

        private int Compare(QueryResultRow x, QueryResultRow y)
        {
            return QueryResultRow.Compare(x, y, this.sortParam);
        }

        sealed class KeyAggregate
        {
            public ColumnAggregateInfo[] Keys;
            public ColumnAggregateInfo[] Aggregates;

            public KeyAggregate(ColumnAggregateInfo[] keys, ColumnAggregateInfo[] aggr)
            {
                this.Keys = keys;
                this.Aggregates = aggr;
            }
        }

        sealed class PivotGroupParam
        {
            public bool Pivot;
            public string[] Columns;
            public QueryParameter[] Aggregates;

            public PivotGroupParam(bool pivot, string[] columns, QueryParameter[] aggregates)
            {
                this.Pivot = pivot;
                this.Columns = columns;
                this.Aggregates = aggregates;
            }
        }

    }

    public struct QueryResultColumnInfo
    {
        private int id;
        private string name;
        private QueryParamOperation aggregate;
        private DataValue pivotKey;

        public QueryResultColumnInfo(int id, string name, QueryParamOperation aggregate, DataValue pivotKey)
        {
            this.id = id;
            this.aggregate = aggregate;
            this.name = name;
            this.pivotKey = pivotKey;
        }

        public int Id
        {
            get { return this.id; }
        }

        public string Name
        {
            get { return this.name; }
        }

        public QueryParamOperation Aggregate
        {
            get { return this.aggregate; }
        }

        public DataValue PivotKey
        {
            get { return this.pivotKey; }
        }

        public bool IsPivot
        {
            get { return this.pivotKey != null; }
        }

        public bool IsAggregate
        {
            get { return this.aggregate != QueryParamOperation.Default; }
        }

        public bool IsMin
        {
            get { return this.aggregate == QueryParamOperation.Min; }
        }

        public bool IsMax
        {
            get { return this.aggregate == QueryParamOperation.Max; }
        }

        public bool IsSum
        {
            get { return this.aggregate == QueryParamOperation.Sum; }
        }

        public bool IsAverage
        {
            get { return this.aggregate == QueryParamOperation.Average; }
        }

        public bool IsCollection
        {
            get { return this.aggregate == QueryParamOperation.Collect; }
        }

        public static int Compare(QueryResultColumnInfo x, QueryResultColumnInfo y)
        {
            int r = String.Compare(x.name, y.name);
            if (r == 0)
            {
                r = x.aggregate.CompareTo(y.aggregate);
                if (r == 0)
                {
                    if ((x.pivotKey != null) || (y.pivotKey != null))
                    {
                        if (x.pivotKey == null)
                        {
                            r = 1;
                        }
                        else if (y.pivotKey == null)
                        {
                            r = -1;
                        }
                        else
                        {
                            r = KeyCollection.Compare(x.pivotKey, y.pivotKey);
                        }
                    }
                }
            }
            return r;
        }

        public override string ToString()
        {
            return String.Format("#{0} '{1}' {2}{3}", this.id, this.name, this.aggregate,
                this.pivotKey == null ? String.Empty : (" Pivot:" + this.pivotKey.ToString()));
        }
    }

}