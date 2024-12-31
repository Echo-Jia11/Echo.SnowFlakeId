namespace Echo.SnowFlakeId
{
    /// <summary>
    /// Snowflake ID 生成器
    /// </summary>
    public class SnowflakeIdBuilder
    {
        /// <summary>
        /// 开始时间截 (2024-01-01)
        /// </summary>
        private const long twepoch = 638396640000000000;

        /// <summary>
        /// 机器id所占的位数
        /// </summary>
        private const int workerIdBits = 5;

        /// <summary>
        /// 数据中心id所占的位数
        /// </summary>
        private const int datacenterIdBits = 5;

        /// <summary>
        /// 支持的最大机器id，结果是31
        /// </summary>
        private const long maxWorkerId = -1 ^ (-1 << workerIdBits);

        /// <summary>
        /// 支持的最大数据中心id，结果是31
        /// </summary>
        private const long maxDatacenterId = -1 ^ (-1 << datacenterIdBits);

        /// <summary>
        /// 生成时间戳的掩码，这里为18014398509473792 (0000 0000 0011 1111 1111 1111 1111 1111 1111 1111 1111 1111 1110 0000 0000 0000)
        /// </summary>
        private const long timestampMask = 18014398509473792;

        /// <summary>
        /// 生成序列的掩码，这里为4095 (1111 1111 1111)
        /// </summary>
        private const long sequenceMask = 4095;

        /// <summary>
        /// 机械码(0~1023)
        /// </summary>
        private readonly long machineId;

        private readonly object lockObj = new object();

        /// <summary>
        /// 序列(0~4095)
        /// </summary>
        private long sequence = 0L;

        /// <summary>
        /// 上次生成ID的时间截
        /// </summary>
        private long lastTimestamp = -1L;

        /// <summary>
        /// 构造函数
        /// </summary>
        /// <param name="workerId">工作ID (0~31)</param>
        /// <param name="datacenterId">数据中心ID (0~31)</param>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        public SnowflakeIdBuilder(long workerId, long datacenterId)
        {
            if (workerId > maxWorkerId || workerId < 0)
            {
                throw new ArgumentOutOfRangeException($"worker Id can't be greater than {maxWorkerId} or less than 0");
            }
            if (datacenterId > maxDatacenterId || datacenterId < 0)
            {
                throw new ArgumentOutOfRangeException($"datacenter Id can't be greater than {maxDatacenterId} or less than 0");
            }
            this.machineId = (datacenterId << datacenterIdBits) | workerId;
        }

        /// <summary>
        /// 获得下一个ID (该方法是线程安全的)
        /// </summary>
        /// <returns>雪花Id</returns>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        public long NextId()
        {
            lock (lockObj)
            {
                var timestamp = TimeGen();

                //如果当前时间小于上一次ID生成的时间戳，说明系统时钟回退过这个时候应当抛出异常
                if (timestamp < lastTimestamp)
                {
                    throw new ArgumentOutOfRangeException($"Clock moved backwards.  Refusing to generate id for {lastTimestamp - timestamp} milliseconds");
                }

                //如果是同一时间生成的，则进行毫秒内序列
                if (lastTimestamp == timestamp)
                {
                    sequence = (sequence + 1) & sequenceMask;
                    //毫秒内序列溢出
                    if (sequence == 0)
                    {
                        //阻塞到下一个毫秒,获得新的时间戳
                        timestamp = TilNextMillis(lastTimestamp);
                    }
                }
                //时间戳改变，毫秒内序列重置
                else
                {
                    sequence = 0;
                }

                //上次生成ID的时间截
                lastTimestamp = timestamp;
                return BuildId(timestamp, 9, machineId, 12, sequence);
            }
        }

        /// <summary>
        /// 构建ID
        /// </summary>
        /// <param name="timestamp">时间戳</param>
        /// <param name="timestampLeftShift">时间戳偏移量</param>
        /// <param name="machineCode">机械码</param>
        /// <param name="machineCodeLeftShift">机械码偏移量</param>
        /// <param name="sequence">序列</param>
        /// <returns></returns>
        protected static long BuildId(long timestamp, int timestampLeftShift, long machineCode, int machineCodeLeftShift, long sequence)
        {
            long id = (timestamp << timestampLeftShift);
            id |= (machineCode << machineCodeLeftShift);
            id |= sequence;
            return id;
        }

        /// <summary>
        /// 阻塞到下一个序列，直到获得新的时间戳
        /// </summary>
        /// <param name="lastTimestamp">上次生成ID的时间截</param>
        /// <returns>当前时间戳</returns>
        protected long TilNextMillis(long lastTimestamp)
        {
            long timestamp = TimeGen();
            while (timestamp <= lastTimestamp)
            {
                timestamp = TimeGen();
            }
            return timestamp;
        }

        /// <summary>
        ///返回当前时间戳
        /// </summary>
        /// <returns>时间戳</returns>
        protected long TimeGen()
        {
            var ticks = DateTime.UtcNow.Ticks;
            ticks -= twepoch;
            var timestamp = ticks & timestampMask;
            return timestamp;
        }
    }
}
