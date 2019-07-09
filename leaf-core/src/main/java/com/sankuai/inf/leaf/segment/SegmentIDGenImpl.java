package com.sankuai.inf.leaf.segment;

import com.sankuai.inf.leaf.IDGen;
import com.sankuai.inf.leaf.common.Result;
import com.sankuai.inf.leaf.common.Status;
import com.sankuai.inf.leaf.segment.dao.IDAllocDao;
import com.sankuai.inf.leaf.segment.model.*;
import org.perf4j.StopWatch;
import org.perf4j.slf4j.Slf4JStopWatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

public class SegmentIDGenImpl implements IDGen {
    private static final Logger logger = LoggerFactory.getLogger(SegmentIDGenImpl.class);

    /**
     * IDCache未初始化成功时的异常码
     */
    private static final long EXCEPTION_ID_IDCACHE_INIT_FALSE = -1;
    /**
     * key不存在时的异常码
     */
    private static final long EXCEPTION_ID_KEY_NOT_EXISTS = -2;
    /**
     * SegmentBuffer中的两个Segment均未从DB中装载时的异常码
     */
    private static final long EXCEPTION_ID_TWO_SEGMENTS_ARE_NULL = -3;
    /**
     * 最大步长不超过100,0000
     */
    private static final int MAX_STEP = 1000000;
    /**
     * 一个Segment维持时间为15分钟
     */
    private static final long SEGMENT_DURATION = 15 * 60 * 1000L;
    private ExecutorService service = new ThreadPoolExecutor(5, Integer.MAX_VALUE, 60L, TimeUnit.SECONDS, new SynchronousQueue<Runnable>(), new UpdateThreadFactory());
    private volatile boolean initOK = false;
    private Map<String, SegmentBuffer> cache = new ConcurrentHashMap<String, SegmentBuffer>();
    private IDAllocDao dao;

    public static class UpdateThreadFactory implements ThreadFactory {

        private static int threadInitNumber = 0;

        private static synchronized int nextThreadNum() {
            return threadInitNumber++;
        }

        @Override
        public Thread newThread(Runnable r) {
            return new Thread(r, "Thread-Segment-Update-" + nextThreadNum());
        }
    }

    @Override   // 初始化，主要是完成了从数据库中获取全部的 biz-tag 信息再更新缓存的过程，另外启动了一个每分钟从数据库更新缓存的任务
    public boolean init() {
        logger.info("Init ...");
        // 确保加载到kv后才初始化成功
        updateCacheFromDb();    // 主要是根据数据库中当前 biz-tag，来刷新缓存的 biz-tag 和 buffer（加入新增的，移除失效的）
        initOK = true;  // 更新状态
        updateCacheFromDbAtEveryMinute();   // 每隔 1 分钟执行一次更新 cache 缓存
        return initOK;
    }
    // 每隔 1 分钟执行一次更新 cache 缓存
    private void updateCacheFromDbAtEveryMinute() {
        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor(new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread t = new Thread(r);
                t.setName("check-idCache-thread");  // 线程名字
                t.setDaemon(true);  // 守护线程
                return t;
            }
        });
        service.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() { // 每隔 1 分钟执行一次更新缓存的操作
                updateCacheFromDb();    // 主要是根据数据库中当前 biz-tag，来刷新缓存的 biz-tag 和 buffer（加入新增的，移除失效的）
            }
        }, 60, 60, TimeUnit.SECONDS);
    }
    // 主要是根据数据库中当前 biz-tag，来刷新缓存的 biz-tag 和 buffer（加入新增的，移除失效的）
    private void updateCacheFromDb() {
        logger.info("update cache from db");
        StopWatch sw = new Slf4JStopWatch();
        try {
            List<String> dbTags = dao.getAllTags(); // 从数据库中查询所有的 biz_tag 字段的值
            if (dbTags == null || dbTags.isEmpty()) {   // 数据库中 biz-tag 字段为空，直接返回
                return;
            }
            List<String> cacheTags = new ArrayList<String>(cache.keySet()); // 获取已经缓存到 biz-tag 集合
            List<String> insertTags = new ArrayList<String>(dbTags);    // 这是从数据库中查出的 biz-tag 集合
            List<String> removeTags = new ArrayList<String>(cacheTags); // 缓存中的 biz-tag
            //db中新加的tags灌进cache
            insertTags.removeAll(cacheTags);    // 这里是相对于缓存中新增的 biz-tag
            for (String tag : insertTags) { // 大致就是构建新增 biz-tag 的 key value 对，然后进行缓存
                SegmentBuffer buffer = new SegmentBuffer(); // 构建了持有的两个内部 Segment，完成了相关参数的初始化
                buffer.setKey(tag); // SegmentBuffer 和 biz-tag 对应
                Segment segment = buffer.getCurrent();  // 获取当前使用的 Segment
                segment.setValue(new AtomicLong(0));    // 设置了 value（暂时不清粗用来干啥的）
                segment.setMax(0);  // 设置最大值和步进值（这里只是初始化，稍后应该是使用从数据库查出来的步进值和最大值）
                segment.setStep(0);
                cache.put(tag, buffer); // 缓存和 tag 对应的 buffer
                logger.info("Add tag {} from db to IdCache, SegmentBuffer {}", tag, buffer);
            }
            //cache中已失效的tags从cache删除
            removeTags.removeAll(dbTags);   // 得到缓存中存在，但是数据库中已经失效的 biz-tag
            for (String tag : removeTags) {
                cache.remove(tag);  // 移除失效的 biz-tag
                logger.info("Remove tag {} from IdCache", tag);
            }
        } catch (Exception e) {
            logger.warn("update cache from db exception", e);
        } finally {
            sw.stop("updateCacheFromDb");
        }
    }

    @Override
    public Result get(final String key) {
        if (!initOK) {  // 检查 SegmentIDGenImpl 的初始化状态，
            return new Result(EXCEPTION_ID_IDCACHE_INIT_FALSE, Status.EXCEPTION);   // 如果初始化未完成，那么返回错误结果，其实内部就是两个字段，id 和 status
        }
        if (cache.containsKey(key)) {   // 查看缓存中是否有 key 对应的值
            SegmentBuffer buffer = cache.get(key);  // 拿到 key 对应的 buffer
            if (!buffer.isInitOk()) {   // 检查 buffer 的状态（构建 buffer 时是被初始化为 false）
                synchronized (buffer) {
                    if (!buffer.isInitOk()) {   // 双重检查锁
                        try {
                            updateSegmentFromDb(key, buffer.getCurrent());
                            logger.info("Init buffer. Update leafkey {} {} from db", key, buffer.getCurrent());
                            buffer.setInitOk(true); // 设置 buffer 的状态
                        } catch (Exception e) {
                            logger.warn("Init buffer {} exception", buffer.getCurrent(), e);
                        }
                    }
                }
            }   // 尝试从当前 segment 获取 id，如果获取失败，就进行 segment 的切换，如果切换失败，就直接返回异常结果
            return getIdFromSegmentBuffer(cache.get(key));
        }
        return new Result(EXCEPTION_ID_KEY_NOT_EXISTS, Status.EXCEPTION);
    }
    // 如果 buffer 是未初始化态，从数据库中得到 key 对应的数据，设置 buffer 的 step 和 minStep 信息，如果是 update time 为 0，更新 update time 和 min step，
    public void updateSegmentFromDb(String key, Segment segment) {  // 否则根据上一次的 update time 时间差，来动态调整 next step，将此动态 step 存入数据库，最后更新 segment
        StopWatch sw = new Slf4JStopWatch();
        SegmentBuffer buffer = segment.getBuffer(); // 获取 segment 对应的 buffer
        LeafAlloc leafAlloc;
        if (!buffer.isInitOk()) {   // 再次验证 buffer 的状态
            leafAlloc = dao.updateMaxIdAndGetLeafAlloc(key);    // 就是将数据库中 tag 对应的项的 max_id 修改为 max_id + step 值，然后将这一条结果查出来，用 LeafAlloc 封装后返回
            buffer.setStep(leafAlloc.getStep());    // 将查询出来的 step 信息填充到 buffer 中
            buffer.setMinStep(leafAlloc.getStep()); //leafAlloc中的step为DB中的step
        } else if (buffer.getUpdateTimestamp() == 0) {
            leafAlloc = dao.updateMaxIdAndGetLeafAlloc(key);    // 就是将数据库中 tag 对应的项的 max_id 修改为 max_id + step 值，然后将这一条结果查出来，用 LeafAlloc 封装后返回
            buffer.setUpdateTimestamp(System.currentTimeMillis());  // 为 buffer 设置更新的时间戳
            buffer.setMinStep(leafAlloc.getStep());//leafAlloc中的step为DB中的step
        } else {
            long duration = System.currentTimeMillis() - buffer.getUpdateTimestamp();   // 距离上次更新 segment 的时长
            int nextStep = buffer.getStep();
            if (duration < SEGMENT_DURATION) {  // 如果持续的时长低于门限（说明一次缓存的不够多，可能导致缓存不够及时而降低 id 获取的时效性）
                if (nextStep * 2 > MAX_STEP) {  // 在不超过最大步长的情况下，将步长扩大为之前的两倍
                    //do nothing
                } else {
                    nextStep = nextStep * 2;
                }
            } else if (duration < SEGMENT_DURATION * 2) {   // 如果缓存时长也没高于门限的两倍，那么就说明当前的缓存时长比较合适
                //do nothing with nextStep
            } else {
                nextStep = nextStep / 2 >= buffer.getMinStep() ? nextStep / 2 : nextStep;   // 否则，在不低于最低步长的情况下将步长修改为原来的一半
            }
            logger.info("leafKey[{}], step[{}], duration[{}mins], nextStep[{}]", key, buffer.getStep(), String.format("%.2f",((double)duration / (1000 * 60))), nextStep);
            LeafAlloc temp = new LeafAlloc();
            temp.setKey(key);
            temp.setStep(nextStep);
            leafAlloc = dao.updateMaxIdByCustomStepAndGetLeafAlloc(temp);   // 根据新计算出来的步长来更新 max id，然后将当前这条更新的数据查出来
            buffer.setUpdateTimestamp(System.currentTimeMillis());  // 更新更新时间（这里差不多是进行了一个动态的调整）
            buffer.setStep(nextStep);   // 更新步长信息
            buffer.setMinStep(leafAlloc.getStep());//leafAlloc的step为DB中的step
        }
        // must set value before set max
        long value = leafAlloc.getMaxId() - buffer.getStep();   // 根据最大值和 step 值计算起始值
        segment.getValue().set(value);  // 设置 value 起始值
        segment.setMax(leafAlloc.getMaxId());   // 设置当前 segment 的 max id
        segment.setStep(buffer.getStep());  // 设置当前 segment 的 step
        sw.stop("updateSegmentFromDb", key + " " + segment);
    }
    // 尝试从当前 segment 获取 id，如果获取失败，就进行 segment 的切换，如果切换失败，就直接返回异常结果
    public Result getIdFromSegmentBuffer(final SegmentBuffer buffer) {
        while (true) {
            try {
                buffer.rLock().lock();  // 上读锁
                final Segment segment = buffer.getCurrent();    // 获取 buffer 中当前使用的 segment
                if (!buffer.isNextReady() && (segment.getIdle() < 0.9 * segment.getStep()) && buffer.getThreadRunning().compareAndSet(false, true)) {
                    service.execute(new Runnable() {    // 执行到这里说明，另外一个 segment 未准备就绪，而且当前 segment 已经使用了 10 %，并且线程状态从 false 修改为了 true（cas 保证只有一个线程能执行到这里）
                        @Override
                        public void run() {
                            Segment next = buffer.getSegments()[buffer.nextPos()];  // 获取下一个 segment，因为当前 segment 已经使用超过 10%，提前缓存另一个 segment 的 id
                            boolean updateOk = false;
                            try {
                                updateSegmentFromDb(buffer.getKey(), next); // 如果 buffer 是未初始化态或者时间戳为 0，那么就从数据库中查询数据来对 segment 完成初始化
                                updateOk = true;
                                logger.info("update segment {} from db {}", buffer.getKey(), next);
                            } catch (Exception e) {
                                logger.warn(buffer.getKey() + " updateSegmentFromDb exception", e);
                            } finally {
                                if (updateOk) {     // 如果 segment 更新成功
                                    buffer.wLock().lock();  // 上写锁
                                    buffer.setNextReady(true);  // 更新 segment 就绪的状态
                                    buffer.getThreadRunning().set(false);   // 设置线程的运行状态为 false
                                    buffer.wLock().unlock();    // 释放写锁
                                } else {
                                    buffer.getThreadRunning().set(false);   // 否则仅仅是设置线程的状态为 false
                                }
                            }
                        }
                    });
                }   // 从当前的 segment 中获取 id
                long value = segment.getValue().getAndIncrement();
                if (value < segment.getMax()) { // 健壮性，获得的 id 不会超过当前 segment 的最大值
                    return new Result(value, Status.SUCCESS);   // 返回结果
                }
            } finally {
                buffer.rLock().unlock();    // 释放读锁
            }   // 执行到这里，说明 segment 的 value 已经超过 segment 的最大值了
            waitAndSleep(buffer);   // 在更新另外一个 segment 的线程执行的情况下，自旋 + 睡眠 10 ms 后退出
            try {
                buffer.wLock().lock();  // 此时上写锁（如果有线程进出了写锁，它将会进入阻塞状态）
                final Segment segment = buffer.getCurrent();    // 拿到当前 segment
                long value = segment.getValue().getAndIncrement();
                if (value < segment.getMax()) { // 再一次尝试，看 segment 的 value 是否小于最大值
                    return new Result(value, Status.SUCCESS);   // 成立的情况下，返回获得的值
                }
                if (buffer.isNextReady()) { // 否则，如果下一个 segment 准备就绪
                    buffer.switchPos(); // 切换到另外一个 segment
                    buffer.setNextReady(false); // 更新下一个 segment 就绪状态的标志
                } else {    // 如果从当前 segment 获取失败，等待很久另外一个 segment 也没能就绪，就直接返回错误状态了（是两个 segment 都失效的情况）
                    logger.error("Both two segments in {} are not ready!", buffer);
                    return new Result(EXCEPTION_ID_TWO_SEGMENTS_ARE_NULL, Status.EXCEPTION);
                }
            } finally {
                buffer.wLock().unlock();    // 最终释放写锁
            }
        }
    }
    // 在更新另外一个 segment 的线程执行的情况下，自旋 + 睡眠 10 ms 后退出
    private void waitAndSleep(SegmentBuffer buffer) {
        int roll = 0;
        while (buffer.getThreadRunning().get()) {   // 如果另外一个线程的状态是正在执行
            roll += 1;  // 自旋计数
            if(roll > 10000) {
                try {
                    Thread.currentThread().sleep(10);   // 自旋 10000 次，睡眠 10 ms 后退出
                    break;
                } catch (InterruptedException e) {
                    logger.warn("Thread {} Interrupted",Thread.currentThread().getName());
                    break;
                }
            }
        }
    }

    public List<LeafAlloc> getAllLeafAllocs() {
        return dao.getAllLeafAllocs();
    }

    public Map<String, SegmentBuffer> getCache() {
        return cache;
    }

    public IDAllocDao getDao() {
        return dao;
    }

    public void setDao(IDAllocDao dao) {
        this.dao = dao;
    }
}
