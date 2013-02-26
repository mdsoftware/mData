using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using mData.Utils;
using mData.Serialization;
using mData.Value;
using mData.DataStructures;
using mData.Threading;
using mData.Storage;
using mData.Caching;
using mData.Services;
using mData.Testing;
using mData.Expressions;
using mData.Query;
using mData.Context;

namespace Tests
{
    class Program
    {
        static void Main(string[] args)
        {
            // Simple test for Expression

            string expr = "(1 + 2 * 3) > 10 ? 'More than 10!!!' : 'Looks less or equal than 10...'";
            string msg;
            Expression e = Expression.Compile(expr, out msg);
            if (e == null)
            {
                Console.WriteLine("Error compiling expression: " + msg);
            }
            else
            {
                DataValue result = e.Execute(null, null);
                Console.WriteLine("'{0}' = {1}", expr, result.ToString());
            }

            Console.Write("PRESS ENTER >>>");
            Console.ReadLine();

            IDataPageStorage f = null;
            IDataPageProvider cache = null;
            IDataContext data = null;
            string fn = "test.dat";

            // If we want a database to be creareated
            bool create = true;

            if (create)
            {
                // Create file storage instance
                f = mData.Factory.CreateFileStorage(fn, 0, 1024);

                // Create cached page provider instance using created file instance
                cache = mData.Factory.CachedPageProvider(f, 500 * 1024 * 1024, 60, DataPageProviderFlags.Empty);

                // Create data context providing a description
                data = mData.Factory.CreateDataContext(cache, 60, "This is a text data context...");

                // Create root DataValue record
                DataValue root = DataValue.Record();

                // Add values to it
                root["Operations"] = DataValue.Array(10000);
                root["Message"] = DataValue.New("This is a test message");

                // Initialize random value
                RandomValue rv = new RandomValue();

                // Update root value in a data context
                data.UpdateRoot(root);

                DateTime started = DateTime.Now;
                DateTime d = new DateTime(2012, 1, 1);

                int count = 0;
                double sec;

                string[] persons = new string[25] {
"Chung, Kristina H.",
"Chen, Paige H.",
"Melton, Sherri E.",
"Hill, Gretchen I.",
"Puckett, Karen U.",
"Song, Patrick O.",
"Hamilton, Elsie A.",
"Bender, Hazel E.",
"Wagner, Malcolm A.",
"McLaughlin, Dolores C.",
"McNamara, Francis C.",
"Raynor, Sandy A.",
"Moon, Marion O.",
"Woodard, Beth O.",
"Desai, Julia E.",
"Wallace, Jerome A.",
"Lawrence, Neal A.",
"Griffin, Jean R.",
"Dougherty, Kristine O.",
"Powers, Crystal O.",
"May, Alex A.",
"Steele, Eric T.",
"Teague, Wesley E.",
"Vick, Franklin I.",
"Gallagher, Claire A."
                };


                string[] products = new string[20] {
"Structured Derivatives",
"Corporate Loan Book",
"Portfolio Management",
"CIB Non Core Activities",
"Acquisition Financing",
"Leveraged Finance",
"Media Telecom",
"Real Estate Financing",
"Loan Syndication",
"Commercial Loans (domestic retail)",
"Factoring & Credit Insurance (domestic retail)",
"Means of Payment / Cash Mgt & Services (domestic retail)",
"Mortgage Financing (domestic retail)",
"Stake Acquisition / Capital Development (domestic retail)",
"Structured Finance (domestic retail)",
"Trade Finance (domestic retail)",
"International Retail Banking Products",
"Consumer Credit",
"Externalisation",
"Finance Leasing",
                };

                string[] currencies = new string[4]{
                    "USD", "EUR", "UAH", "RUR"
                };

                for (int i = 0; i < 10000; i++)
                {
                    DataValue ti = DataValue.Record(0, "Operation");

                    ti["Date"] = DataValue.New(d.AddDays(rv.Random * 360f));
                    ti["Amount"] = DataValue.New(Math.Round(1000f + (rv.Random * 100000f), 2));
                    ti["Description"] = rv.Next(DataValueType.String);
                    ti["Reference"] = DataValue.New("#" + rv.RandomInt(10000).ToString("d6"));
                    ti["Currency"] = DataValue.New(currencies[rv.RandomInt(currencies.Length)]);
                    ti["Person"] = DataValue.New(persons[rv.RandomInt(persons.Length)]);
                    ti["Product"] = DataValue.New(products[rv.RandomInt(products.Length)]);

                    data.Add(ti);

                    root["Operations"].Add(ti.GetReference());

                    if ((count > 0) && ((count % 500) == 0))
                    {
                        sec = DateTime.Now.Subtract(started).TotalSeconds;
                        Console.WriteLine("Write {0} ({1} per sec.)", count, ((double)count / sec).ToString("########0.00##"));
                    }

                    count++;
                }

                // Update root with latest version of data
                data.UpdateRoot(root);

                // Dispose data context
                data.Dispose();
                data = null;

            }
            {
                // Open storage file
                f = mData.Factory.OpenFileStorage(fn);

                // Create pages cache based on supplied file
                cache = mData.Factory.CachedPageProvider(f, 500 * 1024 * 1024, 60, DataPageProviderFlags.Empty);

                // Opening data context
                data = mData.Factory.OpenDataContext(cache, 60);

                // Show database info
                Console.WriteLine("Database UID:{0} '{1}'", data.Uid, data.Description);

                // Read operation list from database
                QueryResult qr = Program.ReadOperations(data);

                DateTime started = DateTime.Now;
                int count = qr.Count;

                /*
                 * This LISQ query doing following (step by step):
                 * 1. Filters results by Product
                 * 2. Calculate new field Period as year of Date * 100 + Month of Date
                 * 3. Calc amount in USD using Amount and fixed rate (2.0)
                 * 4. Group data by Period, Person and Country, collecting field Reference for each group in array
                 * 5. Rename summary columns UsdAmount and Amount with UsdAmount_Sum and Amount_Sum respectively
                 * 6. Sorts by Period, Currency and Person
                 */
                string query = @"(
(filter ""Product = 'Real Estate Financing'"")

(calc Period ""year(Date) * 100 + month(Date)"")

(calc UsdAmount ""Currency = 'USD' ? Amount : (Amount / 2.0)"")

(group (Period Person Currency) (sum UsdAmount) (sum Amount) (collect Reference))

(rename ""!IsPivot and IsSum ? (Name + '_Sum') : null"")

(sort Period Currency Person)
)";

                // Execute query
                qr.Query(query, FuncHelper.Default().Call);

                double sec = DateTime.Now.Subtract(started).TotalSeconds;
                Console.WriteLine("Processing {0} sec. {1} per sec. ({2})", sec, (double)count / sec, qr.Count);



                Program.SaveToText("results1.txt", qr);

                // Repeat with initial data set
                qr = Program.ReadOperations(data);

                started = DateTime.Now;
                count = qr.Count;

                /*
                 * This LISQ query doing following (step by step):
                 * 1. Filters results by Person
                 * 2. Calculate new field Period as year of Date * 100 + Month of Date
                 * 3. Calc amount in USD using Amount and fixed rate (2.0)
                 * 4. Applying pivot to result for a period for UsdAmount. It means that UsdAmount for a different periods
                 *    appears in a different columns.
                 * 5. Group data by Product and Currency
                 * 6. Rename summary columns UsdAmount and Amount with UsdAmount_Sum and Amount_Sum respectively
                 * 7. Rename pivot columnt to convert them to usual columns with a name UsdAmount_period_Sum
                 * 8. Sorts by UsdAmount summery (descending) and Product
                 */
                query = @"(
(filter ""Person = 'McNamara, Francis C.'"")

(calc Period ""year(Date) * 100 + month(Date)"")

(calc UsdAmount ""Currency = 'USD' ? Amount : (Amount / 2.0)"")

(pivot Period (sum UsdAmount))
(group (Product Currency) (sum UsdAmount) (sum Amount))

(rename ""!IsPivot and IsSum ? (Name + '_Sum') : null"")

(rename ""IsPivot && IsSum ? (Name + '_' + str(PivotKey.Period) + '_Sum') : null"")

(sort (desc UsdAmount_Sum) Product)
)";

                // Execute query
                qr.Query(query, FuncHelper.Default().Call);

                sec = DateTime.Now.Subtract(started).TotalSeconds;
                Console.WriteLine("Processing {0} sec. {1} per sec. ({2})", sec, (double)count / sec, qr.Count);



                Program.SaveToText("results2.txt", qr);


                data.Dispose();
                data = null;
            }
        Done:

            Console.Write("PRESS ENTER >>>");
            Console.ReadLine();
        }

        private static QueryResult ReadOperations(IDataContext data)
        {
            DateTime started = DateTime.Now;

            DataValue root = data.GetRoot();

            DataValue arr = root["Operations"];

            int count = 0;

            QueryResult qr = QueryResult.Create();

            if (arr.Type == DataValueType.Array)
            {
                for (int i = 0; i < arr.Length; i++)
                {
                    if (arr[i].Type == DataValueType.Reference)
                    {
                        DataValue ti = data.Get(arr[i].Reference);
                        if (!ti.IsNull)
                        {
                            qr.Add(ti);
                        }
                        count++;
                    }
                }
            }

            double sec = DateTime.Now.Subtract(started).TotalSeconds;
            Console.WriteLine("Read {0} sec. {1} per sec.", sec, (double)count / sec);

            return qr;
        }

        private static void SaveToText(string fileName, QueryResult qr)
        {
            StreamWriter w = File.CreateText(fileName);
            QueryResultColumnInfo[] cols = qr.Columns;

            Utils.Sort(cols, QueryResultColumnInfo.Compare);

            for (int i = 0; i < cols.Length; i++)
            {
                if (i > 0) w.Write('\t');
                w.Write(cols[i].Name);
                if (cols[i].IsAggregate)
                    w.Write(" ({0})", cols[i].Aggregate);
                if (cols[i].PivotKey != null)
                {
                    w.Write(" PivotKey:(");
                    string[] names = cols[i].PivotKey.AllNames;
                    for (int j = 0; j < names.Length; j++)
                    {
                        if (j > 0) w.Write(",");
                        w.Write(names[j]);
                        w.Write(":");
                        w.Write(cols[i].PivotKey[names[j]].ToString());
                    }
                    w.Write(")");
                }
            }


            for (int i = 0; i < qr.Count; i++)
            {
                w.WriteLine();
                DataValue vv = qr.AllColumns(i);
                for (int j = 0; j < cols.Length; j++)
                {
                    if (j > 0) w.Write('\t');
                    DataValue v = vv[QueryResult.ColumnNameById(cols[j].Id)];
                    if (!v.IsNull)
                    {
                        string s = v.ToString();
                        if (s != null)
                            if (s.Length > 30) s = s.Substring(0, 30) + "...";
                        w.Write(s);
                    }
                }
            }

            w.Close();
            w = null;

        }

        private static bool Inspector(int k, string v)
        {
            if (k == 4) return true;
            return false;
        }

        private static int Compare(long x, long y)
        {
            return x.CompareTo(y);
        }

        private static DataValue CallFunc(string name, FuncCallContext context)
        {
            switch (name.ToLower())
            {
                case "str":
                    if (context.Count == 1)
                        return DataValue.New(context[0].ToString());
                    break;

                case "print":
                    for (int i = 0; i < context.Count; i++)
                        Console.WriteLine("arg[{0}]: {1}", i, context[i].ToString());
                    return DataValue.Null;
            }
            throw new NotImplementedException(String.Format("Function '{0}' not implemented or invalid argument(s) count", name));
        }

        private static bool Compare(TreeNodeInfo<int, string>[] a, TreeNodeInfo<int, string>[] b)
        {
            if ((a == null) || (b == null)) return true;
            int i = 0;
            bool ok = true;
            while (true)
            {
                if (i >= a.Length) break;
                if (i >= b.Length) break;

                TreeNodeInfo<int, string> aa = a[i];
                TreeNodeInfo<int, string> bb = b[i];

                if ((aa.Key != bb.Key) || (aa.IsBlack != bb.IsBlack) || (aa.Left != bb.Left) || (aa.Right != bb.Right))
                {
                    Console.WriteLine("{0} <> {1}", aa, bb);
                    ok = false;
                }

                i++;
            }
            return ok;
        }

        private static bool Compare(TreeNodeInfo<long, ulong>[] a, TreeNodeInfo<long, ulong>[] b)
        {
            if ((a == null) || (b == null)) return true;
            int i = 0;
            bool ok = true;
            while (true)
            {
                if (i >= a.Length) break;
                if (i >= b.Length) break;

                TreeNodeInfo<long, ulong> aa = a[i];
                TreeNodeInfo<long, ulong> bb = b[i];

                if ((aa.Key != bb.Key) || (aa.IsBlack != bb.IsBlack) || (aa.Left != bb.Left) || (aa.Right != bb.Right))
                {
                    Console.WriteLine("{0} <> {1}", aa, bb);
                    ok = false;
                }

                i++;
            }
            return ok;
        }

        private static void ShowNums(long[] t, List<long> v)
        {
            for (int i = 0; i < t.Length; i++)
            {
                Console.Write(" {0}:", t[i]);
                for (int j = 0; j < v.Count; j++)
                {
                    if (v[j] == t[i])
                        Console.Write("[{0}]", j);
                }
                Console.WriteLine();
            }
        }

        private static int CompareKeys(int x, int y)
        {
            return x.CompareTo(y);
        }

        private static int CompareKeys(long x, long y)
        {
            return x.CompareTo(y);
        }

        private static string EmptyValue()
        {
            return null;
        }

    }

    class FileBufferStream : IPageReadStream, IPageWriteStream, IDisposable
    {
        private Stream stream;
        public long Bytes;

        public FileBufferStream(string file, bool write)
        {
            if (file != null)
            {
                if (write)
                {
                    this.stream = File.Create(file);
                }
                else
                {
                    this.stream = File.OpenRead(file);
                }
            }
            this.Bytes = 0;
        }

        public void Flush()
        {
            if (this.stream != null)
                this.stream.Flush();
        }

        public int PageSize
        {
            get { return 512; }
        }

        public int PageHeaderSize
        {
            get { return 32; }
        }

        public void Reset()
        {
            this.stream.Position = 0;
        }

        public void Read(byte[] buffer)
        {
            int c = this.stream.Read(buffer, 0, this.PageSize);
            this.Bytes += c;
        }

        public void Write(byte[] buffer)
        {
            int count = this.PageSize;
            if (this.stream != null)
                this.stream.Write(buffer, 0, this.PageSize);
            this.Bytes += count;
        }

        public void Close()
        {
            if (this.stream != null)
            {
                this.stream.Close();
                this.stream = null;
            }
        }

        public void Dispose()
        {
            if (this.stream != null)
            {
                this.stream.Dispose();
                this.stream = null;
            }
        }
    }

    public class TestNotify
    {
        public TreeNodeInfo<int, string>[] t0;
        public TreeNodeInfo<int, string>[] t1;

        public int NotifyT0(TreeNodeInfo<int, string>[] l)
        {
            this.t0 = l;
            return 0;
        }

        public int NotifyT1(TreeNodeInfo<int, string>[] l)
        {
            this.t1 = l;
            return 0;
        }
    }

    public class TestSymbols : ISymbolProvider
    {
        private SortedDictionary<int, string> idIndex;
        private SortedDictionary<string, int> symbolIndex;
        private int lastId;

        public TestSymbols()
        {
            this.idIndex = new SortedDictionary<int, string>();
            this.symbolIndex = new SortedDictionary<string, int>();
            this.lastId = 100;
        }

        public int Get(string symbol)
        {
            symbol = symbol.ToLower();
            int x;
            if (this.symbolIndex.TryGetValue(symbol, out x))
                return x;
            x = this.lastId++;
            this.idIndex.Add(x, symbol);
            this.symbolIndex.Add(symbol, x);
            return x;
        }

        public string Get(int id)
        {
            string s;
            if (this.idIndex.TryGetValue(id, out s))
                return s;
            return null;
        }
    }

    public class TextExpReader : IExpressionReader, ITextWriter
    {
        private List<string> lines;
        private int count;

        public long CharCount;

        public TextExpReader(List<string> lines)
        {
            this.lines = lines;
            this.count = 0;
            this.CharCount = 0;
        }

        public string Next()
        {
            if (this.count >= this.lines.Count)
                return null;
            string s = this.lines[this.count++];
            this.CharCount += s.Length;
            return s;
        }

        public void Reset()
        {
            this.count = 0;
        }

        public void Write(string s)
        {
            Console.Write(s);
        }

        public void WriteLine(string s)
        {
            Console.WriteLine(s);
        }

        public void WriteLine()
        {
            Console.WriteLine();
        }
    }

}
