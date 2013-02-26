using System;
using System.Threading;

namespace mData.Threading
{

    struct ProgressiveIdle
    {
        private int count;
        private bool waiting;
        private long ticks;
        private bool started;
        private int timeout;
        private string name;

        public const int NoWaitCount = 100;
        public const int Wait0Count = 500;
        public const int Wait5Count = 1000;
        public const int Wait10Count = 2000;

        public ProgressiveIdle(string name, int timeout)
        {
            this.count = 0;
            this.waiting = false;
            this.ticks = 0;
            this.started = false;
            this.timeout = timeout;
            this.name = name;
        }

        public void Start()
        {
            this.count = 0;
            this.waiting = false;
            this.ticks = 0;
            this.started = true;
        }

        public bool Started
        {
            get { return this.started; }
        }

        public bool Waiting
        {
            get { return this.waiting; }
        }

        public void Tick()
        {
            ++this.count;
            if (this.count < ProgressiveIdle.NoWaitCount)
                return;
            if (this.count < ProgressiveIdle.Wait0Count)
            {
                Thread.Sleep(0);
                return;
            }
            if (!this.waiting)
            {
                this.waiting = true;
                this.ticks = DateTime.Now.Ticks;
            }
            else if (this.timeout > 0)
            {
                if ((int)this.WaitSeconds > this.timeout)
                    throw new DataException("Idle timeout exceeded ({0})", this.name);
            }
            if (this.count < ProgressiveIdle.Wait5Count)
            {
                Thread.Sleep(5);
                return;
            }
            if (this.count < ProgressiveIdle.Wait10Count)
            {
                Thread.Sleep(10);
                return;
            }
            Thread.Sleep(50);
        }

        public float WaitSeconds
        {
            get
            {
                if (!this.waiting) return 0f;
                if (this.ticks == 0) return 0f;
                return (float)((DateTime.Now.Ticks - this.ticks) / TimeSpan.TicksPerMillisecond) / 1000f;
            }
        }

        public float Stop()
        {
            if (this.started)
            {
                this.started = false;
                if (this.waiting)
                {
                    float sec = this.WaitSeconds;
                    return sec;
                }
            }
            return Single.NaN;
        }
    }

}