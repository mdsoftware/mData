using System;
using System.IO;

namespace mData.Serialization
{

    sealed class DataWriteStream : IDataWriteStream
    {
        private IPageWriteStream stream;
        private byte[] buffer;
        private int pos;
        private int length;

        public DataWriteStream(IPageWriteStream stream)
        {
            this.stream = stream;
            this.buffer = new byte[this.stream.PageSize];
            this.length = this.buffer.Length;
            this.pos = this.stream.PageHeaderSize;
        }

        public unsafe void Write(int i)
        {
            if (pos >= this.length)
            {
                this.stream.Write(this.buffer);
                this.pos = this.stream.PageHeaderSize;
            }
            fixed (byte* p = this.buffer)
            {
                byte* pp = p + this.pos;
                *(int*)pp = i;
            }
            this.pos += 4;
        }

        public unsafe void Write(long l)
        {
            if ((this.pos + 8) <= this.length)
            {
                fixed (byte* p = this.buffer)
                {
                    byte* pp = p + this.pos;
                    *(long*)pp = l;
                }
                this.pos += 8;
            }
            else
            {
                int* p = (int*)&l;
                this.Write(*p);
                ++p;
                this.Write(*p);
            }
        }

        public unsafe void Write(string s)
        {
            if (s == null)
            {
                this.Write((int)-1);
                return;
            }
            int l = s.Length;
            this.Write(l);
            int ofs = 0;
            while (l > 0)
            {
                if (pos >= this.length)
                {
                    this.stream.Write(this.buffer);
                    this.pos = this.stream.PageHeaderSize;
                }
                int c = (this.length - this.pos) >> 1;
                if (c > l) c = l;
                int bytes = c << 1;
                fixed (char* p0 = s)
                {
                    fixed (byte* p1 = this.buffer)
                    {
                        DataWriteStream.Copy((byte*)p0, ofs, p1, this.pos, bytes);
                    }
                }
                this.pos += bytes;
                ofs += bytes;
                l -= c;
            }
            while ((this.pos & 0x3) != 0) ++this.pos;
        }

        public unsafe void Write(byte[] b)
        {
            if (b == null)
            {
                this.Write((int)-1);
                return;
            }
            int l = b.Length;
            this.Write(l);
            int ofs = 0;
            while (l > 0)
            {
                if (pos >= this.length)
                {
                    this.stream.Write(this.buffer);
                    this.pos = this.stream.PageHeaderSize;
                }
                int bytes = this.length - this.pos;
                if (bytes > l) bytes = l;
                fixed (byte* p0 = b, p1 = this.buffer)
                {
                    DataWriteStream.Copy((byte*)p0, ofs, p1, this.pos, bytes);
                }
                this.pos += bytes;
                ofs += bytes;
                l -= bytes;
            }
            while ((this.pos & 0x3) != 0) ++this.pos;
        }

        public static unsafe void Copy(byte* src, int srcOffset, byte* dest, int destOffset, int count)
        {
            long* p0 = (long*)(src + srcOffset);
            long* p1 = (long*)(dest + destOffset);
            while (count > 7)
            {
                *(p1++) = *(p0++);
                count -= 8;
            }
            byte* pp0 = (byte*)p0;
            byte* pp1 = (byte*)p1;
            while (count > 0)
            {
                *(pp1++) = *(pp0++);
                --count;
            }
        }

        public void Flush()
        {
            if (this.pos > this.stream.PageHeaderSize)
            {
                this.stream.Write(this.buffer);
                this.pos = this.stream.PageHeaderSize;
                this.stream.Flush();
            }
        }

        public void Dispose()
        {
            this.stream = null;
            this.buffer = null;
            this.pos = 0;
            this.length = 0;
        }
    }

    sealed class DataReadStream : IDataReadStream
    {
        private IPageReadStream stream;
        private byte[] buffer;
        private int pos;
        private int length;

        public DataReadStream(IPageReadStream stream)
        {
            this.stream = stream;
            this.buffer = new byte[this.stream.PageSize];
            this.length = 0;
            this.pos = this.stream.PageHeaderSize;
        }

        public unsafe int ReadInt()
        {
            if (this.pos >= this.length)
                this.ReadBuffer();
            int x;
            fixed (byte* p = this.buffer)
            {
                x = *(int*)(p + this.pos);
            }
            this.pos += 4;
            return x;
        }

        public unsafe long ReadLong()
        {
            long l;
            if ((this.pos + 8) <= this.length)
            {
                fixed (byte* p = this.buffer)
                {
                    l = *(long*)(p + this.pos);
                }
                this.pos += 8;
            }
            else
            {
                int* p = (int*)&l;
                *p = this.ReadInt();
                p++;
                *p = this.ReadInt();
            }
            return l;
        }

        public unsafe string ReadString()
        {
            int l = this.ReadInt();
            if (l == -1) return null;
            if (l == 0) return String.Empty;
            char[] buf = new char[l];
            int ofs = 0;
            while (l > 0)
            {
                if (this.pos >= this.length)
                    this.ReadBuffer();
                int c = (this.length - this.pos) >> 1;
                if (c > l) c = l;
                int bytes = (c << 1);
                fixed (byte* p0 = this.buffer)
                {
                    fixed (char* p1 = buf)
                    {
                        DataWriteStream.Copy(p0, this.pos, (byte*)p1, ofs, bytes);
                    }
                }
                this.pos += bytes;
                ofs += bytes;
                l -= c;
            }
            while ((this.pos & 0x3) != 0) ++this.pos;
            return new String(buf);
        }

        public unsafe byte[] ReadBytes()
        {
            int l = this.ReadInt();
            if (l == -1) return null;
            if (l == 0) return new byte[0];
            byte[] buf = new byte[l];
            int ofs = 0;
            while (l > 0)
            {
                if (this.pos >= this.length)
                    this.ReadBuffer();
                int bytes = this.length - this.pos;
                if (bytes > l) bytes = l;
                fixed (byte* p0 = this.buffer, p1 = buf)
                {
                    DataWriteStream.Copy(p0, this.pos, (byte*)p1, ofs, bytes);
                }
                this.pos += bytes;
                ofs += bytes;
                l -= bytes;
            }
            while ((this.pos & 0x3) != 0) ++this.pos;
            return buf;
        }

        private void ReadBuffer()
        {
            this.stream.Read(this.buffer);
            this.length = this.stream.PageSize;
            this.pos = this.stream.PageHeaderSize;
        }

        public void Dispose()
        {
            this.stream = null;
            this.buffer = null;
            this.pos = 0;
            this.length = 0;
        }
    }

}