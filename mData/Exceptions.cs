using System;

namespace mData
{
    /// <summary>
    /// Base mData exception
    /// </summary>
    public class DataException : Exception
    {

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="msg"></param>
        public DataException(string msg)
            : base(msg)
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="format"></param>
        /// <param name="arg0"></param>
        public DataException(string format, object arg0)
            : base(String.Format(format, arg0))
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="format"></param>
        /// <param name="arg"></param>
        public DataException(string format, params object[] arg)
            : base(String.Format(format, arg))
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="format"></param>
        /// <param name="arg0"></param>
        /// <param name="arg1"></param>
        public DataException(string format, object arg0, object arg1)
            : base(String.Format(format, arg0, arg1))
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="format"></param>
        /// <param name="arg0"></param>
        /// <param name="arg1"></param>
        /// <param name="arg2"></param>
        public DataException(string format, object arg0, object arg1, object arg2)
            : base(String.Format(format, arg0, arg1, arg2))
        {
        }

        /// <summary>
        /// Constructor
        /// </summary>
        /// <param name="format"></param>
        /// <param name="arg0"></param>
        /// <param name="arg1"></param>
        /// <param name="arg2"></param>
        /// <param name="arg3"></param>
        public DataException(string format, object arg0, object arg1, object arg2, object arg3)
            : base(String.Format(format, arg0, arg1, arg2, arg3))
        {
        }

    }

}