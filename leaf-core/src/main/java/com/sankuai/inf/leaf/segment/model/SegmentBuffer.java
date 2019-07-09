package com.sankuai.inf.leaf.segment.model;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * 双buffer
 */
public class SegmentBuffer {
    private String key;
    private Segment[] segments; //双buffer
    private volatile int currentPos; //当前的使用的segment的index
    private volatile boolean nextReady; //下一个segment是否处于可切换状态
    private volatile boolean initOk; //是否初始化完成
    private final AtomicBoolean threadRunning; //线程是否在运行中
    private final ReadWriteLock lock;

    private volatile int step;
    private volatile int minStep;
    private volatile long updateTimestamp;
    // 构建了持有的两个内部 Segment，完成了相关参数的初始化
    public SegmentBuffer() {
        segments = new Segment[]{new Segment(this), new Segment(this)}; // 一个 SegmentBuffer 持有两个 Segment，代表两个 id 字段
        currentPos = 0; // 当前 Segment 的 pos
        nextReady = false;  // 标志位，用于标志下一个 Segment 是否准备就绪
        initOk = false; // 初始化的标志
        threadRunning = new AtomicBoolean(false);   // 这应该是线程启动的标志（该线程应该是用来获取备用 Segment 的）
        lock = new ReentrantReadWriteLock();    // 可冲入的读写锁
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public Segment[] getSegments() {
        return segments;
    }
    // 获取当前使用的 Segment
    public Segment getCurrent() {
        return segments[currentPos];
    }

    public int getCurrentPos() {
        return currentPos;
    }
    // 计算另外一个 pos 的索引
    public int nextPos() {
        return (currentPos + 1) % 2;
    }
    // 切换到另外一个 segment 的 pos
    public void switchPos() {
        currentPos = nextPos();
    }

    public boolean isInitOk() {
        return initOk;
    }

    public void setInitOk(boolean initOk) {
        this.initOk = initOk;
    }
    // 查看下一个 segment 是否准备就绪
    public boolean isNextReady() {
        return nextReady;
    }

    public void setNextReady(boolean nextReady) {
        this.nextReady = nextReady;
    }

    public AtomicBoolean getThreadRunning() {
        return threadRunning;
    }

    public Lock rLock() {
        return lock.readLock();
    }

    public Lock wLock() {
        return lock.writeLock();
    }

    public int getStep() {
        return step;
    }

    public void setStep(int step) {
        this.step = step;
    }

    public int getMinStep() {
        return minStep;
    }

    public void setMinStep(int minStep) {
        this.minStep = minStep;
    }

    public long getUpdateTimestamp() {
        return updateTimestamp;
    }

    public void setUpdateTimestamp(long updateTimestamp) {
        this.updateTimestamp = updateTimestamp;
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder("SegmentBuffer{");
        sb.append("key='").append(key).append('\'');
        sb.append(", segments=").append(Arrays.toString(segments));
        sb.append(", currentPos=").append(currentPos);
        sb.append(", nextReady=").append(nextReady);
        sb.append(", initOk=").append(initOk);
        sb.append(", threadRunning=").append(threadRunning);
        sb.append(", step=").append(step);
        sb.append(", minStep=").append(minStep);
        sb.append(", updateTimestamp=").append(updateTimestamp);
        sb.append('}');
        return sb.toString();
    }
}
