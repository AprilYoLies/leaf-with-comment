package com.sankuai.inf.leaf.snowflake;

import com.google.common.base.Preconditions;
import com.sankuai.inf.leaf.IDGen;
import com.sankuai.inf.leaf.common.Result;
import com.sankuai.inf.leaf.common.Status;
import com.sankuai.inf.leaf.common.Utils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class SnowflakeIDGenImpl implements IDGen {

    @Override
    public boolean init() {
        return true;
    }

    static private final Logger LOGGER = LoggerFactory.getLogger(SnowflakeIDGenImpl.class);

    private final long twepoch = 1288834974657L;    // 时间元时刻
    private final long workerIdBits = 10L;  // worker id 所占的位数
    private final long maxWorkerId = -1L ^ (-1L << workerIdBits);//最大能够分配的workerid =1023
    private final long sequenceBits = 12L;  // 序列号所占的位长度
    private final long workerIdShift = sequenceBits;    // worker id 所应该移动的位数
    private final long timestampLeftShift = sequenceBits + workerIdBits;    // 时间戳应该移动的位数
    private final long sequenceMask = -1L ^ (-1L << sequenceBits);  // 序列号掩码
    private long workerId;
    private long sequence = 0L; // 初始序列号
    private long lastTimestamp = -1L;   // 初始时间戳
    public boolean initFlag = false;    // 初始化标志
    private static final Random RANDOM = new Random();
    private int port;
    // 主要是构建了 SnowflakeZookeeperHolder，负责和 zookeeper 的交互，然后尝试从 zookeeper 获取 worker id，失败的情况下从本地获取 worker id
    public SnowflakeIDGenImpl(String zkAddress, int port) {
        this.port = port;   //  缓存 port
        SnowflakeZookeeperHolder holder = new SnowflakeZookeeperHolder(Utils.getIp(), String.valueOf(port), zkAddress);
        initFlag = holder.init();   // 这里就是一个尝试从 zookeeper 中获取 worker id 的过程，如果在获取 worker id 的过程中出现错误，那么就使用本地缓存的 worker id
        if (initFlag) { // 如果成功的拿到了 worker id
            workerId = holder.getWorkerID();    // 缓存 worker id
            LOGGER.info("START SUCCESS USE ZK WORKERID-{}", workerId);  // 日志记录当前启动的 worker id
        } else {
            Preconditions.checkArgument(initFlag, "Snowflake Id Gen is not init ok");
        }   // worker id 是有效的
        Preconditions.checkArgument(workerId >= 0 && workerId <= maxWorkerId, "workerID must gte 0 and lte 1023");
    }
    // 获取时间戳，和上一个 id 的时间戳比对，如果不一致，跟 worker id 和序列号拼凑成 id 返回，否则变更序列号，再拼凑出 id 返回，如果变更序列号可能导致 id 重复，那么人为将时间戳等待到之后的时刻，再拼凑 id 返回
    public synchronized Result get(String key) {
        long timestamp = timeGen(); // 获取当前的系统时间
        if (timestamp < lastTimestamp) {    // 如果时间发生了回拨
            long offset = lastTimestamp - timestamp;
            if (offset <= 5) {  // 如果回拨值小于 5
                try {
                    wait(offset << 1);  // 那么就人为的等待两倍的回拨值
                    timestamp = timeGen();  // 重新获取系统时间
                    if (timestamp < lastTimestamp) {    // 如果人为调整后，时间还是有回拨现象，返回异常
                        return new Result(-1, Status.EXCEPTION);    // 返回错误 result
                    }
                } catch (InterruptedException e) {  // 如果人为等待的过程中，线程被中断了，那么日志记录错误，返回错误 result
                    LOGGER.error("wait interrupted");
                    return new Result(-2, Status.EXCEPTION);
                }
            } else {
                return new Result(-3, Status.EXCEPTION);    // 如果回拨时长超过 5，那么就返回错误 result
            }
        }
        if (lastTimestamp == timestamp) {   // 如果时间戳一致，也就是在 1ms 内多次请求，唯一 id 就需要通过序列号来区分
            sequence = (sequence + 1) & sequenceMask;   // 计算此时的序列号
            if (sequence == 0) {    // 如果序列号为 0
                //seq 为0的时候表示是下一毫秒时间开始对seq做随机
                sequence = RANDOM.nextInt(100); // 随机获得序列值
                timestamp = tilNextMillis(lastTimestamp);   // 自旋直到下一个 lastTimestamp 时刻之后，返回此时的时间戳（这里是为了防止在 1ms 时间内，sequence 递增太多，导致重复，从而用时间来区分）
            }
        } else {    // 执行到这里，说明时刻发生了变化，已经确保了 id 的唯一性，所以 sequence 随机一个就行
            //如果是新的ms开始
            sequence = RANDOM.nextInt(100);
        }
        lastTimestamp = timestamp;  // 记录下当前时间戳
        long id = ((timestamp - twepoch) << timestampLeftShift) | (workerId << workerIdShift) | sequence;   // 根据时间戳、worker id、序列号来构建唯一 id
        return new Result(id, Status.SUCCESS);  // 返回构建出来的 id 值

    }
    // 自旋直到下一个 lastTimestamp 时刻之后，返回此时的时间戳
    protected long tilNextMillis(long lastTimestamp) {
        long timestamp = timeGen(); // 当前时间戳
        while (timestamp <= lastTimestamp) {    // 直到时间戳变为 lastTimestamp 时刻之后的时间戳
            timestamp = timeGen();
        }
        return timestamp;
    }
    // 获取当前时间戳
    protected long timeGen() {
        return System.currentTimeMillis();
    }

    public long getWorkerId() {
        return workerId;
    }

}
