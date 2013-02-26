using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using mData.Value;

namespace Tests
{
    public sealed class RandomValue
    {
        private Random rnd;

        public RandomValue()
        {
            this.rnd = new Random();
        }

        public int RandomInt(int max)
        {
            return this.rnd.Next(max);
        }

        public float Random
        {
            get { return (float)this.rnd.Next(999999) / 1000000f; }
        }

        public DataValue NextRecord(string className)
        {
            int c = 10 + this.rnd.Next(50);
            DataValue v = DataValue.Record(-1, className, c);
            for (int i = 0; i < c; i++)
                v[String.Format("Field_{0}", i)] = this.Next(false);
            return v;
        }

        public DataValue Next(DataValueType type)
        {
            DataValue v = DataValue.Null;

            switch (type)
            {
                case DataValueType.Integer:
                    v.Integer = rnd.Next();
                    break;

                case DataValueType.Logic:
                    v.Logic = rnd.Next(10) > 5;
                    break;

                case DataValueType.Float:
                    v.Float = (double)rnd.Next(100000) / 1000f;
                    break;

                case DataValueType.String:
                    v.String = this.RandomString();
                    break;

                case DataValueType.DateTime:
                    v.DateTime = DateTime.Now.AddDays((double)rnd.Next(100000) / 1000f);
                    break;

                case DataValueType.Reference:
                    v.Reference = 1000000 + rnd.Next(1000000);
                    break;

                case DataValueType.Binary:
                    {
                        int l = 100 + rnd.Next(200);
                        byte[] b = new byte[l];
                        for (int i = 0; i < l; i++) b[i] = (byte)(i & 0xff);
                        v.Binary = b;
                    }
                    break;

                case DataValueType.Array:
                    {
                        int l = 20 + rnd.Next(30);
                        v = DataValue.Array(l);
                        for (int i = 0; i < l; i++)
                            v[i] = this.Next(true);
                    }
                    break;

                case DataValueType.Record:
                    {
                        int l = 5 + rnd.Next(20);
                        v = DataValue.Record();
                        for (int i = 0; i < l; i++)
                            v[String.Format("Field_{0}", i + 1)] = this.Next(true);
                    }
                    break;
            }
            return v;
        }

        public DataValue Next(bool scalar)
        {
            int x = rnd.Next(scalar ? 6 : 10);

            switch (x)
            {
                case 0:
                    return this.Next(DataValueType.Integer);

                case 1:
                    return this.Next(DataValueType.Logic);

                case 2:
                    return this.Next(DataValueType.Float);

                case 3:
                    return this.Next(DataValueType.String);

                case 4:
                    return this.Next(DataValueType.DateTime);

                case 6:
                    return this.Next(DataValueType.Reference);

                case 7:
                    return this.Next(DataValueType.Binary);

                case 8:
                    return this.Next(DataValueType.Array);

                case 9:
                    return this.Next(DataValueType.Record);
            }

            return DataValue.Null;
        }

        public const string Chars = "1234567890qwertyuiopasdfghjklzxcvbnm.,QWERTYUIOPASDFGHJKLZXCVBNM!?";

        private string RandomString()
        {
            if (this.rnd.Next(100) < 3) return null;

            int l = 40 + rnd.Next(500);
            char[] b = new char[l];

            bool space = true;

            for (int i = 0; i < l; i++)
            {
                if ((!space) && (rnd.Next(100) < 30))
                {
                    b[i] = ' ';
                    space = true;
                }
                else
                {
                    b[i] = RandomValue.Chars[rnd.Next(RandomValue.Chars.Length)];
                    space = false;
                }
            }
            return new String(b);
        }
    }
}