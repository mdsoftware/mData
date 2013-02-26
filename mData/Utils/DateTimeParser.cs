using System;
using System.Globalization;
using mData.DataStructures;

namespace mData.Utils
{

    public sealed class DateTimeParser
    {
        private ArrayTree<string, int> months = null;
        private PagedList<DateTimeTemplate> templates = null;

        private static int[] monthDays = new int[12] { 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };

        private static long StartTicks = new DateTime(1900, 1, 1).Ticks;

        public const int MaxTokenSize = 64;

        public DateTimeParser()
        {
            this.months = new ArrayTree<string, int>(DateTimeParser.Compare, 0, 1, PageFactor.Page32);
            CultureInfo en = null;
            try
            {
                en = CultureInfo.GetCultureInfo("en-US");
            }
            catch { en = null; }
            for (int m = 1; m <= 12; m++)
            {
                this.AddMonthString(m, CultureInfo.CurrentCulture.DateTimeFormat.GetMonthName(m));
                this.AddMonthString(m, CultureInfo.CurrentCulture.DateTimeFormat.GetAbbreviatedMonthName(m));
                if (en == null) continue;
                this.AddMonthString(m, en.DateTimeFormat.GetMonthName(m));
                this.AddMonthString(m, en.DateTimeFormat.GetAbbreviatedMonthName(m));
            }
            this.templates = new PagedList<DateTimeTemplate>(PageFactor.Page16, 4);
        }

        public static long DateToSeconds(DateTime d)
        {
            return (d.Ticks - DateTimeParser.StartTicks) / TimeSpan.TicksPerSecond;
        }

        public static DateTime SecondsToDate(long sec)
        {
            return new DateTime((sec * TimeSpan.TicksPerSecond) + DateTimeParser.StartTicks);
        }

        public bool TryParse(string s, string template, out DateTime dateTime)
        {
            dateTime = DateTime.MinValue;
            string msg;
            DateTimePart[] parts = DateTimeParser.ParseString(s, out msg);
            if (parts == null)
                return false;
            DateTimePart[] tmp = DateTimeParser.ParseTemplate(template);
            if (tmp == null)
                return false;
            DateTimeParsed parsed;
            if (this.MatchCheck(parts, tmp, out parsed))
            {
                dateTime = new DateTime(parsed.Year, parsed.Month, parsed.Day, parsed.Hour, parsed.Min, parsed.Sec);
                return true;
            }
            return false;
        }

        public bool TryParse(string s, out DateTime dateTime)
        {
            dateTime = DateTime.MinValue;
            if (this.templates.Count == 0) return false;
            string msg;
            DateTimePart[] parts = DateTimeParser.ParseString(s, out msg);
            if (parts==null)
                return false;
            for (int i = 0; i < this.templates.Count; i++)
            {
                DateTimeParsed parsed;
                if (!this.MatchCheck(parts, this.templates[i].Template, out parsed)) continue;
                dateTime = new DateTime(parsed.Year, parsed.Month, parsed.Day, parsed.Hour, parsed.Min, parsed.Sec);
                return true;
            }
            return false;
        }

        public void AddTemplate(string template)
        {
            this.templates.Add(new DateTimeTemplate(Crc64.Calculate(template), DateTimeParser.ParseTemplate(template)));
        }

        public void AddMonthString(int month, string s)
        {
            if ((month < 1) || (month > 12))
                throw new ArgumentException("Invalid month range");
            int m;
            if (this.months.TryGetValue(s, out m))
            {
                if (m != month)
                    throw new ArgumentException("Duplicate month names");
                return;
            }
            this.months.Insert(s, month);
        }

        private static int Compare(string x, string y)
        {
            return String.Compare(x, y, true);
        }

        private int FindMonth(string s)
        {
            int m;
            if (this.months.TryGetValue(s, out m)) return m;
            return 0;
        }

        private bool MatchCheck(DateTimePart[] dateTime, DateTimePart[] template, out DateTimeParsed parsed)
        {
            parsed = new DateTimeParsed();
            parsed.Month = 1;
            parsed.Day = 1;

            if (dateTime.Length != template.Length)
                return false;

            for (int i = 0; i < template.Length; i++)
            {
                if (dateTime[i].Type != template[i].Type)
                    return false;
                if (template[i].Type == DateTimePartType.Separator)
                {
                    if (template[i].Value == "?")
                        continue;
                    if (template[i].Value != dateTime[i].Value) return false;
                    continue;
                }
                DateTimePartFlags f = template[i].Flags;
                if (template[i].Type == DateTimePartType.String)
                {
                    if (template[i].Value == "?")
                        continue;
                    if ((f & DateTimePartFlags.Month) != 0)
                    {
                        int m = this.FindMonth(dateTime[i].Value);
                        if (m == 0) return false;
                        parsed.Month = m;
                        parsed.Flags |= DateTimePartFlags.Month;
                    }
                    else if ((f & DateTimePartFlags.Am) != 0)
                    {
                        switch (dateTime[i].Value.ToLower())
                        {
                            case "am":
                                parsed.Flags |= DateTimePartFlags.Am;
                                break;

                            case "pm":
                                parsed.Flags |= DateTimePartFlags.Pm;
                                break;

                            default:
                                return false;
                        }
                    }
                    else
                    {
                        return false;
                    }
                    continue;
                }

                int n = dateTime[i].Number;

                if ((f & DateTimePartFlags.Year) != 0)
                {
                    if ((n >= 0) && (n < 100))
                    {
                        if (n < 50)
                        {
                            n = 2000 + n;
                        }
                        else
                        {
                            n = 1900 + n;
                        }
                    }
                    parsed.Year = n;
                    parsed.Flags |= DateTimePartFlags.Year;
                    continue;
                }
                if ((f & DateTimePartFlags.Month) != 0)
                {
                    if ((n < 1) || (n > 12)) return false;
                    parsed.Month = n;
                    parsed.Flags |= DateTimePartFlags.Month;
                    continue;
                }
                if ((f & DateTimePartFlags.Day) != 0)
                {
                    if ((n < 1) || (n > 31)) return false;
                    parsed.Day = n;
                    parsed.Flags |= DateTimePartFlags.Day;
                    continue;
                }
                if ((f & DateTimePartFlags.Hours) != 0)
                {
                    if ((n < 0) || (n > 23)) return false;
                    parsed.Hour = n;
                    parsed.Flags |= DateTimePartFlags.Hours;
                    continue;
                }
                if ((f & DateTimePartFlags.Minutes) != 0)
                {
                    if ((n < 0) || (n > 59)) return false;
                    parsed.Min = n;
                    parsed.Flags |= DateTimePartFlags.Minutes;
                    continue;
                }
                if ((f & DateTimePartFlags.Seconds) != 0)
                {
                    if ((n < 0) || (n > 59)) return false;
                    parsed.Sec = n;
                    parsed.Flags |= DateTimePartFlags.Seconds;
                    continue;
                }
            }

            if (((parsed.Flags & DateTimePartFlags.Hours) != 0) & ((parsed.Flags & (DateTimePartFlags.Am | DateTimePartFlags.Pm)) != 0))
            {
                if (parsed.Hour == 12) parsed.Hour = 0;
                if ((parsed.Flags & DateTimePartFlags.Pm) != 0) parsed.Hour = (parsed.Hour % 12) + 12;
                if ((parsed.Hour < 0) || (parsed.Hour > 23)) return false;
            }

            if ((parsed.Flags & DateTimePartFlags.Day) != 0)
            {
                if (parsed.Month == 2)
                {
                    if (DateTimeParser.IsLeapYear(parsed.Year))
                    {
                        if (parsed.Day > 29) return false;
                    }
                    else
                    {
                        if (parsed.Day > 28) return false;
                    }
                }
                else
                {
                    if (parsed.Day > DateTimeParser.monthDays[parsed.Month - 1]) return false;
                }
            }

            return true;
        }

        public static bool IsLeapYear(int year)
        {
            if ((year % 400) == 0)
            {
                return true;
            }
            else if ((year % 100) == 0)
            {
                return false;
            }
            else if ((year % 4) == 0)
            {
                return true;
            }
            return false;
        }

        private static DateTimePart[] ParseTemplate(string s)
        {
            string msg;
            DateTimePart[] parts = DateTimeParser.ParseString(s, out msg);
            if (parts == null)
                throw new ArgumentException("Error parsing DateTime template");

            for (int i = 0; i < parts.Length; i++)
            {
                if (parts[i].Type == DateTimePartType.Number)
                    throw new ArgumentException("Numbers are not allowed in DateTime template");
                if (parts[i].Type == DateTimePartType.Separator)
                    continue;

                switch (parts[i].Value.ToLower())
                {
                    case "yy":
                    case "yyyy":
                    case "year":
                        parts[i].Type = DateTimePartType.Number;
                        parts[i].Flags = DateTimePartFlags.Year;
                        break;

                    case "mo":
                        parts[i].Type = DateTimePartType.Number;
                        parts[i].Flags = DateTimePartFlags.Month;
                        break;

                    case "month":
                        parts[i].Flags = DateTimePartFlags.Month;
                        break;

                    case "d":
                    case "dd":
                    case "day":
                        parts[i].Type = DateTimePartType.Number;
                        parts[i].Flags = DateTimePartFlags.Day;
                        break;

                    case "h":
                    case "hh":
                    case "hour":
                        parts[i].Type = DateTimePartType.Number;
                        parts[i].Flags = DateTimePartFlags.Hours;
                        break;

                    case "m":
                    case "mm":
                    case "min":
                    case "minute":
                        parts[i].Type = DateTimePartType.Number;
                        parts[i].Flags = DateTimePartFlags.Minutes;
                        break;

                    case "s":
                    case "ss":
                    case "sec":
                    case "second":
                        parts[i].Type = DateTimePartType.Number;
                        parts[i].Flags = DateTimePartFlags.Seconds;
                        break;

                    case "am":
                    case "pm":
                        parts[i].Flags = DateTimePartFlags.Am;
                        break;

                    case "x":
                        parts[i].Value = "?";
                        break;

                    default:
                        throw new ArgumentException("Unsupported format token in DateTime template");

                }
            }

            DateTimePartFlags f = DateTimePartFlags.Empty;
            for (int i = 0; i < parts.Length; i++)
            {
                DateTimePartFlags ff = parts[i].Flags;
                if ((f & ff) != 0)
                    throw new ArgumentException("Ambiguous DateTime template");
                f |= ff;
            }

            if ((f & DateTimePartFlags.Year) == 0)
                throw new ArgumentException("Invalid DateTime template (year is missing)");

            if (((f & DateTimePartFlags.Day) != 0) && ((f & DateTimePartFlags.Month) == 0))
                throw new ArgumentException("Invalid DateTime template (month is missing)");

            if (((f & DateTimePartFlags.Minutes) != 0) && ((f & DateTimePartFlags.Hours) == 0))
                throw new ArgumentException("Invalid DateTime template (hour is missing)");

            if (((f & DateTimePartFlags.Seconds) != 0) &&
                (((f & DateTimePartFlags.Hours) == 0) || ((f & DateTimePartFlags.Minutes) == 0)))
                throw new ArgumentException("Invalid DateTime template (hour and/or minute is missing)");

            return parts;
        }

        private static DateTimePart[] ParseString(string s, out string errorMessage)
        {
            errorMessage = null;
            if (String.IsNullOrEmpty(s))
            {
                errorMessage = "DateTime string must not be empty";
                return null;
            }
            char[] token = new char[DateTimeParser.MaxTokenSize];
            int pos = 0;
            DateTimePartType type = DateTimePartType.Undefined;
            PagedList<DateTimePart> list = new PagedList<DateTimePart>(PageFactor.Page32, 1);
            for (int i = 0; i < s.Length; i++)
            {
                char c = s[i];
                if (Char.IsDigit(c))
                {
                    if (type != DateTimePartType.Number)
                    {
                        if (pos > 0)
                        {
                            list.Add(new DateTimePart(type, new String(token, 0, pos)));
                            pos = 0;
                        }
                        type = DateTimePartType.Number;
                    }
                    if (pos >= token.Length)
                    {
                        errorMessage = "DateTime token too long";
                        return null;
                    }
                    token[pos++] = c;
                    continue;
                }
                if (Char.IsLetter(c))
                {
                    if (type != DateTimePartType.String)
                    {
                        if (pos > 0)
                        {
                            list.Add(new DateTimePart(type, new String(token, 0, pos)));
                            pos = 0;
                        }
                        type = DateTimePartType.String;
                    }
                    if (pos >= token.Length)
                    {
                        errorMessage = "DateTime token too long";
                        return null;
                    }
                    token[pos++] = c;
                    continue;
                }
                switch (c)
                {
                    case ' ':
                    case '\t':
                    case '\r':
                    case '\n':
                        if ((type != DateTimePartType.Undefined) && (pos > 0))
                        {
                            list.Add(new DateTimePart(type, new String(token, 0, pos)));
                            pos = 0;
                        }
                        type = DateTimePartType.Undefined;
                        break;

                    default:
                        if (type != DateTimePartType.Separator)
                        {
                            if (pos > 0)
                            {
                                list.Add(new DateTimePart(type, new String(token, 0, pos)));
                                pos = 0;
                            }
                            type = DateTimePartType.Separator;
                        }
                        if (pos >= token.Length)
                        {
                            errorMessage = "DateTime token too long";
                            return null;
                        }
                        token[pos++] = c;
                        break;
                }
            }
            if ((type != DateTimePartType.Undefined) && (pos > 0))
                list.Add(new DateTimePart(type, new String(token, 0, pos)));

            return list.ToArray();
        }

        struct DateTimeTemplate
        {
            public ulong Hash;
            public DateTimePart[] Template;

            public DateTimeTemplate(ulong hash, DateTimePart[] template)
            {
                this.Hash = hash;
                this.Template = template;
            }
        }

    }

    public struct DateTimeParsed
    {
        public int Year;
        public int Month;
        public int Day;
        public int Hour;
        public int Min;
        public int Sec;
        public DateTimePartFlags Flags;
    }

    public struct DateTimePart
    {
        public DateTimePartFlags Flags;
        public string Value;
        public DateTimePartType Type;
        public int Number;

        public DateTimePart(DateTimePartType type, string value)
        {
            this.Type = type;
            if (this.Type == DateTimePartType.Number)
            {
                if (value.Length > 10)
                    throw new ArgumentException("DateTime number token too long");
                this.Value = null;
                this.Number = Int32.Parse(value);
            }
            else
            {
                this.Value = value;
                this.Number = 0;
            }
            this.Flags = DateTimePartFlags.Empty;
        }

        public override string ToString()
        {
            return String.Format("{0} '{1}'{2}", this.Type,
                (this.Type == DateTimePartType.Number) ? this.Number.ToString() : this.Value,
                (this.Flags == DateTimePartFlags.Empty) ? "" : " " + this.Flags.ToString());
        }
    }

    

    [Flags]
    public enum DateTimePartFlags : short
    {
        Empty = 0x0,
        Year = 0x1,
        Month = 0x2,
        Day = 0x4,
        MonthString = 0x8,
        Hours = 0x10,
        Minutes = 0x20,
        Seconds = 0x40,
        Am = 0x80,
        Pm = 0x100
    }

    public enum DateTimePartType : byte
    {
        Undefined = 0,
        Separator,
        Number,
        String
    }

}