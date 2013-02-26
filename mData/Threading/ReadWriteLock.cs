using System;
using System.Threading;

namespace mData.Threading
{

    sealed class ReadWriteLock
    {
        private int timeout;
        private int count;
        private ReadWriteLockState state;
        private object sync;
        private float waitSeconds;
        private long lockCount;
        private long unlockCount;
        private string name;

        public ReadWriteLock(string name, int timeout)
        {
            this.name = name;
            this.timeout = timeout < 0 ? 0 : timeout;
            this.count = 0;
            this.state = ReadWriteLockState.ReadLock;
            this.sync = new Object();
            this.waitSeconds = 0f;
            this.lockCount = 0;
            this.unlockCount = 0;
        }

        public string Name
        {
            get { return this.name; }
        }

        public float WaitSeconds
        {
            get
            {
                Monitor.Enter(this.sync);
                float f = this.waitSeconds;
                Monitor.Exit(this.sync);
                return f;
            }
        }

        public long LockCount
        {
            get
            {
                Monitor.Enter(this.sync);
                long l = this.lockCount;
                Monitor.Exit(this.sync);
                return l;
            }
        }

        public long UnlockCount
        {
            get
            {
                Monitor.Enter(this.sync);
                long l = this.unlockCount;
                Monitor.Exit(this.sync);
                return l;
            }
        }

        public void ReadLock()
        {
            this.ReadLock(this.timeout);
        }

        public void ReadLock(int timeout)
        {
            ProgressiveIdle idle = new ProgressiveIdle();
            while (true)
            {
                bool ok = false;
                Monitor.Enter(this.sync);
                if (this.state == ReadWriteLockState.ReadLock)
                {
                    ++this.count;
                    ++this.lockCount;
                    ok = true;
                }
                Monitor.Exit(this.sync);
                if (ok) break;
                if (!idle.Started) idle.Start();
                idle.Tick();
                if ((timeout <= 0) || !idle.Waiting) continue;
                float f = idle.WaitSeconds;
                if ((int)f > timeout)
                {
                    this.AddWaitSeconds(f);
                    throw new DataException("Read lock {0} timeout exceeded", this.name);
                }
            }
            if (idle.Started && idle.Waiting)
                this.AddWaitSeconds(idle.WaitSeconds);
        }

        public void ReadUnlock()
        {
            string msg = null;
            Monitor.Enter(this.sync);
            if (this.state == ReadWriteLockState.WriteLock)
            {
                msg = "Invalid state for ReadUnlock ({0})";
            }
            else
            {
                if (this.count == 0)
                {
                    msg = "Read lock count underflow ({0})";
                }
                else
                {
                    --this.count;
                    ++this.unlockCount;
                }
            }
            Monitor.Exit(this.sync);
            if (msg != null)
                throw new DataException(msg, this.name);
        }

        public void WriteUnlock(bool readLock)
        {
            string msg = null;
            Monitor.Enter(this.sync);
            if (this.state != ReadWriteLockState.WriteLock)
            {
                msg = "Invalid state for WriteUnlock ({0})";
            }
            else
            {
                this.state = ReadWriteLockState.ReadLock;
                ++this.unlockCount;
                if (readLock)
                {
                    ++this.lockCount;
                    ++this.count;
                }
            }
            Monitor.Exit(this.sync);
            if (msg != null)
                throw new DataException(msg, this.name);
        }

        public void WriteLock()
        {
            this.WriteLock(this.timeout);
        }

        public void WriteLock(int timeout)
        {
            ProgressiveIdle idle = new ProgressiveIdle();
            while (true)
            {
                bool ok = false;
                Monitor.Enter(this.sync);
                switch (this.state)
                {
                    case ReadWriteLockState.WriteLockRequest:
                        if (this.count == 0)
                        {
                            this.state = ReadWriteLockState.WriteLock;
                            ++this.lockCount;
                            ok = true;
                        }
                        break;

                    case ReadWriteLockState.ReadLock:
                        this.state = ReadWriteLockState.WriteLockRequest;
                        break;

                }
                Monitor.Exit(this.sync);
                if (ok) break;
                if (!idle.Started) idle.Start();
                idle.Tick();
                if ((timeout <= 0) || !idle.Waiting) continue;
                float f = idle.WaitSeconds;
                if ((int)f > timeout)
                {
                    this.AddWaitSeconds(f);
                    throw new DataException("Write lock {0} timeout exceeded", this.name);
                }
            }
            if (idle.Started && idle.Waiting)
                this.AddWaitSeconds(idle.WaitSeconds);
        }

        private void AddWaitSeconds(float f)
        {
            Monitor.Enter(this.sync);
            this.waitSeconds += f;
            Monitor.Exit(this.sync);
        }
    }

    public enum ReadWriteLockState : byte
    {
        ReadLock = 0,
        WriteLockRequest,
        WriteLock
    }
}