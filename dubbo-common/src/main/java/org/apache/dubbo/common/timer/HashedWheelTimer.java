/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.dubbo.common.timer;

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.ClassUtils;

import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link Timer} optimized for approximated I/O timeout scheduling.
 *
 * <h3>Tick Duration</h3>
 * <p>
 * As described with 'approximated', this timer does not execute the scheduled
 * {@link TimerTask} on time.  {@link HashedWheelTimer}, on every tick, will
 * check if there are any {@link TimerTask}s behind the schedule and execute
 * them.
 * <p>
 * You can increase or decrease the accuracy of the execution timing by
 * specifying smaller or larger tick duration in the constructor.  In most
 * network applications, I/O timeout does not need to be accurate.  Therefore,
 * the default tick duration is 100 milliseconds and you will not need to try
 * different configurations in most cases.
 *
 * <h3>Ticks per Wheel (Wheel Size)</h3>
 * <p>
 * {@link HashedWheelTimer} maintains a data structure called 'wheel'.
 * To put simply, a wheel is a hash table of {@link TimerTask}s whose hash
 * function is 'dead line of the task'.  The default number of ticks per wheel
 * (i.e. the size of the wheel) is 512.  You could specify a larger value
 * if you are going to schedule a lot of timeouts.
 *
 * <h3>Do not create many instances.</h3>
 * <p>
 * {@link HashedWheelTimer} creates a new thread whenever it is instantiated and
 * started.  Therefore, you should make sure to create only one instance and
 * share it across your application.  One of the common mistakes, that makes
 * your application unresponsive, is to create a new instance for every connection.
 *
 * <h3>Implementation Details</h3>
 * <p>
 * {@link HashedWheelTimer} is based on
 * <a href="http://cseweb.ucsd.edu/users/varghese/">George Varghese</a> and
 * Tony Lauck's paper,
 * <a href="http://cseweb.ucsd.edu/users/varghese/PAPERS/twheel.ps.Z">'Hashed
 * and Hierarchical Timing Wheels: data structures to efficiently implement a
 * timer facility'</a>.  More comprehensive slides are located
 * <a href="http://www.cse.wustl.edu/~cdgill/courses/cs6874/TimingWheels.ppt">here</a>.
 * 
 * HashedWheelTimer 是 Timer 接口的实现,它通过时间轮算法实现了一个定时器.
 * HashedWheelTimer 会根据当前时间轮指针选定对应的槽(HashedWheelBucket),
 * 从双向链表的头部开始迭代,对每个定时任务(HashedWheelTimeout)进行计算,
 * 属于当前时钟周期则取出运行,不属于则将其剩余的时钟周期数减一操作
 * 
 * Dubbo 中如何使用定时任务?
 *
 * 在 Dubbo 中,时间轮并不直接用于周期性操作,而是只向时间轮提交执行单次的定时任务,在上一次任务执行完成的时候,
 * 调用 newTimeout() 方法再次提交当前任务,这样就会在下个周期执行该任务.即使在任务执行过程中出现了 GC、I/O
 * 阻塞等情况,导致任务延迟或卡住,也不会有同样的任务源源不断地提交进来,导致任务堆积.
 *
 * Dubbo 中对时间轮的应用主要体现在如下两个方面
 *
 * 1) 失败重试, 例如,Provider 向注册中心进行注册失败时的重试操作,或是 Consumer 向注册中心订阅时的失败重试等.
 *
 * 2) 周期性定时任务, 例如,定期发送心跳请求,请求超时的处理,或是网络连接断开后的重连机制.
 */
public class HashedWheelTimer implements Timer {

    /**
     * may be in spi?
     */
    public static final String NAME = "hased";

    private static final Logger logger = LoggerFactory.getLogger(HashedWheelTimer.class);

    private static final AtomicInteger INSTANCE_COUNTER = new AtomicInteger();
    private static final AtomicBoolean WARNED_TOO_MANY_INSTANCES = new AtomicBoolean();
    private static final int INSTANCE_COUNT_LIMIT = 64;
    private static final AtomicIntegerFieldUpdater<HashedWheelTimer> WORKER_STATE_UPDATER =
            AtomicIntegerFieldUpdater.newUpdater(HashedWheelTimer.class, "workerState");

    /**
     * 真正执行定时任务的逻辑封装这个 Runnable 对象中
     */
    private final Worker worker = new Worker();

    /**
     * 时间轮内部真正执行定时任务的线程
     */
    private final Thread workerThread;

    private static final int WORKER_STATE_INIT = 0;
    private static final int WORKER_STATE_STARTED = 1;
    private static final int WORKER_STATE_SHUTDOWN = 2;

    /**
     * 0 - init, 1 - started, 2 - shut down
     * 时间轮当前所处状态,可选值有 init、started、shutdown.
     * 同时, 有相应的 AtomicIntegerFieldUpdater实现workerState的原子修改
     */
    @SuppressWarnings({"unused", "FieldMayBeFinal"})
    private volatile int workerState;

    /**
     * 时间指针每次加 1 所代表的实际时间,单位为纳秒
     */
    private final long tickDuration;

    /**
     * 该数组就是时间轮的环形队列, 每一个元素都是一个槽.
     * 当指定时间轮槽数为n时, 实际上会取大于且最靠近 n 的 2 的幂次方值
     */
    private final HashedWheelBucket[] wheel;

    /**
     * 掩码, mask = wheel.length - 1,执行 ticks & mask 便能定位到对应的时钟槽
     */
    private final int mask;

    private final CountDownLatch startTimeInitialized = new CountDownLatch(1);

    /**
     * timeouts 队列用于缓冲外部提交时间轮中的定时任务,cancelledTimeouts 队列用于暂存取消的定时任务
     * HashedWheelTimer 会在处理 HashedWheelBucket 的双向链表之前,先处理这两个队列中的数据
     */
    private final Queue<HashedWheelTimeout> timeouts = new LinkedBlockingQueue<>();
    private final Queue<HashedWheelTimeout> cancelledTimeouts = new LinkedBlockingQueue<>();

    /**
     * 当前时间轮剩余的定时任务总数
     */
    private final AtomicLong pendingTimeouts = new AtomicLong(0);

    private final long maxPendingTimeouts;

    /**
     * 当前时间轮的启动时间, 提交到该时间轮的定时任务的 deadline 字段值均以该时间戳为起点进行计算
     */
    private volatile long startTime;

    /**
     * Creates a new timer with the default thread factory
     * ({@link Executors#defaultThreadFactory()}), default tick duration, and
     * default number of ticks per wheel.
     */
    public HashedWheelTimer() {
        this(Executors.defaultThreadFactory());
    }

    /**
     * Creates a new timer with the default thread factory
     * ({@link Executors#defaultThreadFactory()}) and default number of ticks
     * per wheel.
     *
     * @param tickDuration the duration between tick
     * @param unit         the time unit of the {@code tickDuration}
     * @throws NullPointerException     if {@code unit} is {@code null}
     * @throws IllegalArgumentException if {@code tickDuration} is &lt;= 0
     */
    public HashedWheelTimer(long tickDuration, TimeUnit unit) {
        this(Executors.defaultThreadFactory(), tickDuration, unit);
    }

    /**
     * Creates a new timer with the default thread factory
     * ({@link Executors#defaultThreadFactory()}).
     *
     * @param tickDuration  the duration between tick
     * @param unit          the time unit of the {@code tickDuration}
     * @param ticksPerWheel the size of the wheel
     * @throws NullPointerException     if {@code unit} is {@code null}
     * @throws IllegalArgumentException if either of {@code tickDuration} and {@code ticksPerWheel} is &lt;= 0
     */
    public HashedWheelTimer(long tickDuration, TimeUnit unit, int ticksPerWheel) {
        this(Executors.defaultThreadFactory(), tickDuration, unit, ticksPerWheel);
    }

    /**
     * Creates a new timer with the default tick duration and default number of
     * ticks per wheel.
     *
     * @param threadFactory a {@link ThreadFactory} that creates a
     *                      background {@link Thread} which is dedicated to
     *                      {@link TimerTask} execution.
     * @throws NullPointerException if {@code threadFactory} is {@code null}
     */
    public HashedWheelTimer(ThreadFactory threadFactory) {
        this(threadFactory, 100, TimeUnit.MILLISECONDS);
    }

    /**
     * Creates a new timer with the default number of ticks per wheel.
     *
     * @param threadFactory a {@link ThreadFactory} that creates a
     *                      background {@link Thread} which is dedicated to
     *                      {@link TimerTask} execution.
     * @param tickDuration  the duration between tick
     * @param unit          the time unit of the {@code tickDuration}
     * @throws NullPointerException     if either of {@code threadFactory} and {@code unit} is {@code null}
     * @throws IllegalArgumentException if {@code tickDuration} is &lt;= 0
     */
    public HashedWheelTimer(
            ThreadFactory threadFactory, long tickDuration, TimeUnit unit) {
        this(threadFactory, tickDuration, unit, 512);
    }

    /**
     * Creates a new timer.
     *
     * @param threadFactory a {@link ThreadFactory} that creates a
     *                      background {@link Thread} which is dedicated to
     *                      {@link TimerTask} execution.
     * @param tickDuration  the duration between tick
     * @param unit          the time unit of the {@code tickDuration}
     * @param ticksPerWheel the size of the wheel
     * @throws NullPointerException     if either of {@code threadFactory} and {@code unit} is {@code null}
     * @throws IllegalArgumentException if either of {@code tickDuration} and {@code ticksPerWheel} is &lt;= 0
     */
    public HashedWheelTimer(
            ThreadFactory threadFactory,
            long tickDuration, TimeUnit unit, int ticksPerWheel) {
        this(threadFactory, tickDuration, unit, ticksPerWheel, -1);
    }

    /**
     * Creates a new timer.
     *
     * @param threadFactory      a {@link ThreadFactory} that creates a
     *                           background {@link Thread} which is dedicated to
     *                           {@link TimerTask} execution.
     * @param tickDuration       the duration between tick
     * @param unit               the time unit of the {@code tickDuration}
     * @param ticksPerWheel      the size of the wheel
     * @param maxPendingTimeouts The maximum number of pending timeouts after which call to
     *                           {@code newTimeout} will result in
     *                           {@link java.util.concurrent.RejectedExecutionException}
     *                           being thrown. No maximum pending timeouts limit is assumed if
     *                           this value is 0 or negative.
     * @throws NullPointerException     if either of {@code threadFactory} and {@code unit} is {@code null}
     * @throws IllegalArgumentException if either of {@code tickDuration} and {@code ticksPerWheel} is &lt;= 0
     */
    public HashedWheelTimer(
            ThreadFactory threadFactory,
            long tickDuration, TimeUnit unit, int ticksPerWheel,
            long maxPendingTimeouts) {

        if (threadFactory == null) {
            throw new NullPointerException("threadFactory");
        }
        if (unit == null) {
            throw new NullPointerException("unit");
        }
        if (tickDuration <= 0) {
            throw new IllegalArgumentException("tickDuration must be greater than 0: " + tickDuration);
        }
        if (ticksPerWheel <= 0) {
            throw new IllegalArgumentException("ticksPerWheel must be greater than 0: " + ticksPerWheel);
        }

        /**
         * Normalize ticksPerWheel to power of two and initialize the wheel.
         * 圆环上一共有多少个时间间隔, HashedWheelTimer对其正则化, 将其换算为大于等于该值的2^n
         */
        wheel = createWheel(ticksPerWheel);

        // 这用来快速计算任务应该呆的槽, 类似HashMap的寻址算法
        mask = wheel.length - 1;

        /**
         * Convert tickDuration to nanos.
         * 时间轮每个槽的时间间隔
         */
        this.tickDuration = unit.toNanos(tickDuration);

        // Prevent overflow.
        if (this.tickDuration >= Long.MAX_VALUE / wheel.length) {
            throw new IllegalArgumentException(String.format(
                    "tickDuration: %d (expected: 0 < tickDuration in nanos < %d",
                    tickDuration, Long.MAX_VALUE / wheel.length));
        }

        // threadFactory是创建线程的线程工厂对象, 每个时间轮对象持有一个线程
        workerThread = threadFactory.newThread(worker);

        // 最多允许多少个任务等待执行
        this.maxPendingTimeouts = maxPendingTimeouts;

        // 如果HashedWheelTimer实例太多, 那么就会打印一个error日志
        if (INSTANCE_COUNTER.incrementAndGet() > INSTANCE_COUNT_LIMIT &&
                WARNED_TOO_MANY_INSTANCES.compareAndSet(false, true)) {
            reportTooManyInstances();
        }
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            super.finalize();
        } finally {
            // This object is going to be GCed and it is assumed the ship has sailed to do a proper shutdown. If
            // we have not yet shutdown then we want to make sure we decrement the active instance count.
            if (WORKER_STATE_UPDATER.getAndSet(this, WORKER_STATE_SHUTDOWN) != WORKER_STATE_SHUTDOWN) {
                INSTANCE_COUNTER.decrementAndGet();
            }
        }
    }

    private static HashedWheelBucket[] createWheel(int ticksPerWheel) {
        if (ticksPerWheel <= 0) {
            throw new IllegalArgumentException(
                    "ticksPerWheel must be greater than 0: " + ticksPerWheel);
        }
        if (ticksPerWheel > 1073741824) {
            throw new IllegalArgumentException(
                    "ticksPerWheel may not be greater than 2^30: " + ticksPerWheel);
        }

        ticksPerWheel = normalizeTicksPerWheel(ticksPerWheel);

        // 初始化时间轮数组
        HashedWheelBucket[] wheel = new HashedWheelBucket[ticksPerWheel];
        for (int i = 0; i < wheel.length; i++) {
            wheel[i] = new HashedWheelBucket();
        }
        return wheel;
    }

    private static int normalizeTicksPerWheel(int ticksPerWheel) {
        int normalizedTicksPerWheel = ticksPerWheel - 1;
        normalizedTicksPerWheel |= normalizedTicksPerWheel >>> 1;
        normalizedTicksPerWheel |= normalizedTicksPerWheel >>> 2;
        normalizedTicksPerWheel |= normalizedTicksPerWheel >>> 4;
        normalizedTicksPerWheel |= normalizedTicksPerWheel >>> 8;
        normalizedTicksPerWheel |= normalizedTicksPerWheel >>> 16;
        return normalizedTicksPerWheel + 1;
    }

    /**
     * Starts the background thread explicitly.  The background thread will
     * start automatically on demand even if you did not call this method.
     * 
     * 启动时间轮. 这个方法其实不需要显示的主动调用, 因为在添加定时任务(newTimeout()方法)的时候会自动调用此方法.
     * 这个是合理的设计, 因为如果时间轮里根本没有定时任务, 启动时间轮也是空耗资源, 因此使用的类似于hashMap的懒加载的形式,
     * 只有当有任务提交之后才会启动这个调度器
     *
     * @throws IllegalStateException if this timer has been
     *                               {@linkplain #stop() stopped} already
     */
    public void start() {
        /**
         * 判断当前时间轮的状态
         * 1) 如果是初始化, 则启动worker线程, 启动整个时间轮
         * 2) 如果已经启动则略过
         * 3) 如果是已经停止,则报错
         *
         * 这里是一个Lock Free的设计. 因为可能有多个线程调用启动方法, 这里使用AtomicIntegerFieldUpdater原子的更新时间轮的状态
         * 因此使用这个方法来获取实例的允许状态,防止重入
         */
        switch (WORKER_STATE_UPDATER.get(this)) {
            case WORKER_STATE_INIT:
                // 使用cas来获取启动调度的权力, 只有竞争到的线程允许来进行实例启动
                if (WORKER_STATE_UPDATER.compareAndSet(this, WORKER_STATE_INIT, WORKER_STATE_STARTED)) {
                    workerThread.start();
                }
                break;
            case WORKER_STATE_STARTED:
                break;
            case WORKER_STATE_SHUTDOWN:
                throw new IllegalStateException("cannot be started once stopped");
            default:
                throw new Error("Invalid WorkerState");
        }

        // Wait until the startTime is initialized by the worker.
        // 等待worker线程初始化时间轮的启动时间
        while (startTime == 0) {
            try {
                // 这里使用countDownLatch来确保调度的线程已经被启动
                startTimeInitialized.await();
            } catch (InterruptedException ignore) {
                // Ignore - it will be ready very soon.
            }
        }
    }

    @Override
    public Set<Timeout> stop() {
        /**
         *  worker线程不能停止时间轮, 也就是加入的定时任务, 不能调用这个方法
         *  不然会有恶意的定时任务调用这个方法而造成大量定时任务失效
         */
        if (Thread.currentThread() == workerThread) {
            throw new IllegalStateException(
                    HashedWheelTimer.class.getSimpleName() +
                            ".stop() cannot be called from " +
                            TimerTask.class.getSimpleName());
        }

        if (!WORKER_STATE_UPDATER.compareAndSet(this, WORKER_STATE_STARTED, WORKER_STATE_SHUTDOWN)) {
            // workerState can be 0 or 2 at this moment - let it always be 2.
            if (WORKER_STATE_UPDATER.getAndSet(this, WORKER_STATE_SHUTDOWN) != WORKER_STATE_SHUTDOWN) {
                INSTANCE_COUNTER.decrementAndGet();
            }

            return Collections.emptySet();
        }

        try {
            boolean interrupted = false;
            /**
             * 中断worker线程, 尝试把正在进行任务的线程中断掉, 如果某些任务正在执行
             * 则会抛出interrupt异常, 并且任务会尝试立即中断
             */
            while (workerThread.isAlive()) {
                workerThread.interrupt();
                try {
                    //当前前程会等待stop的结果
                    workerThread.join(100);
                } catch (InterruptedException ignored) {
                    interrupted = true;
                }
            }

            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        } finally {
            INSTANCE_COUNTER.decrementAndGet();
        }
        // 返回未处理的任务
        return worker.unprocessedTimeouts();
    }

    @Override
    public boolean isStop() {
        return WORKER_STATE_SHUTDOWN == WORKER_STATE_UPDATER.get(this);
    }

    /**
     * 时间轮对外提供了一个 newTimeout() 接口用于提交定时任务
     *
     * 1) 在定时任务进入到 timeouts 队列之前会先调用 start() 方法启动时间轮, 其中会完成下面两个关键步骤
     *    a) 确定时间轮的 startTime 字段
     *    b) 启动 workerThread 线程, 开始执行 worker 任务
     *
     * 2) 之后根据 startTime 计算该定时任务的 deadline 字段
     *
     * 3) 最后才能将定时任务封装成 HashedWheelTimeout 并添加到 timeouts 队列
     */
    @Override
    public Timeout newTimeout(TimerTask task, long delay, TimeUnit unit) {
        if (task == null) {
            throw new NullPointerException("task");
        }
        if (unit == null) {
            throw new NullPointerException("unit");
        }

        long pendingTimeoutsCount = pendingTimeouts.incrementAndGet();

        if (maxPendingTimeouts > 0 && pendingTimeoutsCount > maxPendingTimeouts) {
            pendingTimeouts.decrementAndGet();
            throw new RejectedExecutionException("Number of pending timeouts ("
                    + pendingTimeoutsCount + ") is greater than or equal to maximum allowed pending "
                    + "timeouts (" + maxPendingTimeouts + ")");
        }

        // 启动时间轮
        start();

        // Add the timeout to the timeout queue which will be processed on the next tick.
        // During processing all the queued HashedWheelTimeouts will be added to the correct HashedWheelBucket.
        // 计算该定时任务的deadline
        long deadline = System.nanoTime() + unit.toNanos(delay) - startTime;

        // Guard against overflow.
        if (delay > 0 && deadline < 0) {
            deadline = Long.MAX_VALUE;
        }

        
        /**
         * 将定时任务封装成HashedWheelTimeOut并添加到timeouts队列
         * 这里定时任务不是直接加到对应的格子中, 而是先加入到一个队列里, 然后等到下一个tick的时候,
         * 会从队列里取出最多100000个任务加入到指定的格子中
         */
        HashedWheelTimeout timeout = new HashedWheelTimeout(this, task, deadline);
        timeouts.add(timeout);
        return timeout;
    }

    /**
     * Returns the number of pending timeouts of this {@link Timer}.
     */
    public long pendingTimeouts() {
        return pendingTimeouts.get();
    }

    private static void reportTooManyInstances() {
        String resourceType = ClassUtils.simpleClassName(HashedWheelTimer.class);
        logger.error("You are creating too many " + resourceType + " instances. " +
                resourceType + " is a shared resource that must be reused across the JVM," +
                "so that only a few instances are created.");
    }

    private final class Worker implements Runnable {
        private final Set<Timeout> unprocessedTimeouts = new HashSet<Timeout>();

        /**
         * 是时间轮的指针, 是一个步长为 1 的单调递增计数器
         */
        private long tick;

        @Override
        public void run() {
            /**
             * 初始化startTime, 所有任务的的deadline都是相对于这个时间点
             */
            startTime = System.nanoTime();

            /**
             * 由于System.nanoTime()可能返回0,甚至负数.
             * 并且0是一个标示符,用来判断startTime是否被初始化,所以当startTime=0的时候,重新赋值为1
             */
            if (startTime == 0) {
                // We use 0 as an indicator for the uninitialized value here, so make sure it's not 0 when initialized.
                startTime = 1;
            }

            /**
             * 唤醒阻塞在start()的线程
             */
            startTimeInitialized.countDown();

            // 只要时间轮的状态为WORKER_STATE_STARTED, 就循环的转动tick, 循环判断响应格子中的到期任务
            do {
                /**
                 * 时间轮转动, 时间轮周期开始
                 * waitForNextTick方法主要是计算下次tick的时间, 然后sleep到下次tick
                 * 返回值就是System.nanoTime() - startTime, 也就是Timer启动后到这次tick, 所过去的时间
                 */
                final long deadline = waitForNextTick();
                if (deadline > 0) {
                    // 获取tick对应的槽索引
                    int idx = (int) (tick & mask);

                    // 清理用户主动取消的定时任务, 这些定时任务在用户取消时, 会记录到 cancelledTimeouts 队列中. 在每次指针转动的时候,时间轮都会清理该队列
                    processCancelledTasks();

                    // 根据当前指针定位对应槽
                    HashedWheelBucket bucket = wheel[idx];

                    // 将缓存在 timeouts 队列中的定时任务转移到时间轮中对应的槽中
                    transferTimeoutsToBuckets();

                    // 处理该槽位的双向链表中的定时任务
                    bucket.expireTimeouts(deadline);
                    tick++;
                }
              //  检测时间轮的状态, 如果时间轮处于运行状态, 则循环执行上述步骤, 不断执行定时任务
            } while (WORKER_STATE_UPDATER.get(HashedWheelTimer.this) == WORKER_STATE_STARTED);

            // Fill the unprocessedTimeouts so we can return them from stop() method.
            // 这里应该是时间轮停止了, 清除所有槽中的任务, 并加入到未处理任务列表, 以供stop()方法返回
            for (HashedWheelBucket bucket : wheel) {
                bucket.clearTimeouts(unprocessedTimeouts);
            }
            
            // 将还没有加入到槽中的待处理定时任务队列中的任务取出, 如果是未取消的任务, 则加入到未处理任务队列中, 以供stop()方法返回
            for (; ; ) {
                HashedWheelTimeout timeout = timeouts.poll();
                if (timeout == null) {
                    break;
                }
                if (!timeout.isCancelled()) {
                    unprocessedTimeouts.add(timeout);
                }
            }
            // 最后再次清理 cancelledTimeouts 队列中用户主动取消的定时任务
            processCancelledTasks();
        }

        /**
         * 将newTimeout()方法中加入到待处理定时任务队列中的任务加入到指定的格子中
         */
        private void transferTimeoutsToBuckets() {
            /**
             * 每次tick只处理10w个任务, 以免阻塞worker线程
             */
            for (int i = 0; i < 100000; i++) {
                HashedWheelTimeout timeout = timeouts.poll();
                // 没有任务了直接跳出循环
                if (timeout == null) {
                    // all processed
                    break;
                }
                // 还没有放入到槽中就取消了, 直接略过
                if (timeout.state() == HashedWheelTimeout.ST_CANCELLED) {
                    // Was cancelled in the meantime.
                    continue;
                }

                // 计算任务需要经过多少个tick
                long calculated = timeout.deadline / tickDuration;
                // 计算任务的轮数
                timeout.remainingRounds = (calculated - tick) / wheel.length;

                /**
                 * Ensure we don't schedule for past.
                 * 如果任务在timeouts队列里面放久了, 以至于已经过了执行时间, 这个时候就使用当前tick, 也就是放到当前bucket, 此方法调用完后就会被执行.
                 */
                final long ticks = Math.max(calculated, tick);
                int stopIndex = (int) (ticks & mask);

                // 将任务加入到响应的槽中
                HashedWheelBucket bucket = wheel[stopIndex];
                bucket.addTimeout(timeout);
            }
        }

        /**
         * 将取消的任务取出, 并从槽中移除
         */
        private void processCancelledTasks() {
            for (; ; ) {
                HashedWheelTimeout timeout = cancelledTimeouts.poll();
                if (timeout == null) {
                    // all processed
                    break;
                }
                try {
                    timeout.remove();
                } catch (Throwable t) {
                    if (logger.isWarnEnabled()) {
                        logger.warn("An exception was thrown while process a cancellation task", t);
                    }
                }
            }
        }

        /**
         * calculate goal nanoTime from startTime and current tick number,
         * then wait until that goal has been reached.
         *
         * @return Long.MIN_VALUE if received a shutdown request,
         * current time otherwise (with Long.MIN_VALUE changed by +1)
         *
         * sleep, 直到下次tick到来, 然后返回该次tick和启动时间之间的时长
         */
        private long waitForNextTick() {
            // 下次tick的时间点, 用于计算需要sleep的时间
            long deadline = tickDuration * (tick + 1);

            for (; ; ) {
                
                /**
                 * 计算需要sleep的时间, 之所以加999999后再除10000000, 前面是1所以这里需要减去1, 才能计算准确,
                 * 还有通过这里可以看到, 其实线程是以睡眠一定的时候再来执行下一个ticket的任务的, 这样如果ticket
                 * 的间隔设置的太小的话, 系统会频繁的睡眠然后启动, 其实感觉影响部分的性能, 所以为了更好的利用系统
                 * 资源步长可以稍微设置大点
                 */
                final long currentTime = System.nanoTime() - startTime;
                long sleepTimeMs = (deadline - currentTime + 999999) / 1000000;

                //这里无需等待, 立即返回下一个指针时间
                if (sleepTimeMs <= 0) {
                    if (currentTime == Long.MIN_VALUE) {
                        return -Long.MAX_VALUE;
                    } else {
                        return currentTime;
                    }
                }

                /**
                 * See https://github.com/netty/netty/issues/356
                 * 这里是因为windows平台的定时调度最小单位为10ms, 如果不是10ms的倍数, 可能会引起sleep时间不准确
                 */
                if (isWindows()) {
                    sleepTimeMs = sleepTimeMs / 10 * 10;
                }

                try {
                    Thread.sleep(sleepTimeMs);
                } catch (InterruptedException ignored) {
                    // 调用HashedWheelTimer.stop()时优雅退出
                    if (WORKER_STATE_UPDATER.get(HashedWheelTimer.this) == WORKER_STATE_SHUTDOWN) {
                        return Long.MIN_VALUE;
                    }
                }
            }
        }

        Set<Timeout> unprocessedTimeouts() {
            return Collections.unmodifiableSet(unprocessedTimeouts);
        }
    }

    /**
     * HashedWheelTimeout是Timeout接口的唯一实现, 是HashedWheelTimer的内部类, HashedWheelTimeout扮演了两个角色
     *
     * 1) 时间轮中双向链表的节点, 即定时任务TimerTask在HashedWheelTimer中的容器
     *
     * 2) 定时任务TimerTask提交到HashedWheelTimer之后返回的句柄(Handle), 用于在时间轮外部查看和控制定时任务
     */
    private static final class HashedWheelTimeout implements Timeout {

        private static final int ST_INIT = 0;
        private static final int ST_CANCELLED = 1;
        private static final int ST_EXPIRED = 2;

        /**
         * 用于实现state状态变更的原子性
         */
        private static final AtomicIntegerFieldUpdater<HashedWheelTimeout> STATE_UPDATER =
                AtomicIntegerFieldUpdater.newUpdater(HashedWheelTimeout.class, "state");

        private final HashedWheelTimer timer;

        /**
         * 指实际被调度的任务
         */
        private final TimerTask task;

        /**
         * 指定时任务执行的时间. 这个时间是在创建 HashedWheelTimeout 时指定的,
         * 计算公式是: currentTime(创建 HashedWheelTimeout 的时间) + delay(任务延迟时间) - startTime(HashedWheelTimer 的启动时间),
         * 时间单位为纳秒
         */
        private final long deadline;

        /**
         * 指定当前任务状态
         */
        @SuppressWarnings({"unused", "FieldMayBeFinal", "RedundantFieldInitialization"})
        private volatile int state = ST_INIT;

        /**
         * RemainingRounds will be calculated and set by Worker.transferTimeoutsToBuckets() before the
         * HashedWheelTimeout will be added to the correct HashedWheelBucket.
         * 
         * 指当前任务剩余的时钟周期数. 时间轮所能表示的时间长度是有限的, 在任务到期时间与当前时刻
         * 的时间差超过时间轮单圈能表示的时长, 就出现了套圈的情况, 需要该字段值表示剩余的时钟周期
         */
        long remainingRounds;

        /**
         * This will be used to chain timeouts in HashedWheelTimerBucket via a double-linked-list.
         * As only the workerThread will act on it there is no need for synchronization / volatile.
         *
         * 分别对应当前定时任务在链表中的前驱节点和后继节点
         */
        HashedWheelTimeout next;
        HashedWheelTimeout prev;

        /**
         * The bucket to which the timeout was added
         */
        HashedWheelBucket bucket;

        HashedWheelTimeout(HashedWheelTimer timer, TimerTask task, long deadline) {
            this.timer = timer;
            this.task = task;
            this.deadline = deadline;
        }

        @Override
        public Timer timer() {
            return timer;
        }

        @Override
        public TimerTask task() {
            return task;
        }

        /**
         * 将状态更新为canceled, 并将当前HashedWheelTimeout添加到cancelledTimeouts队列中等待销毁
         */
        @Override
        public boolean cancel() {
            // only update the state it will be removed from HashedWheelBucket on next tick.
            if (!compareAndSetState(ST_INIT, ST_CANCELLED)) {
                return false;
            }
            // If a task should be canceled we put this to another queue which will be processed on each tick.
            // So this means that we will have a GC latency of max. 1 tick duration which is good enough. This way
            // we can make again use of our MpscLinkedQueue and so minimize the locking / overhead as much as possible.
            timer.cancelledTimeouts.add(this);
            return true;
        }

        /**
         * 将当前 HashedWheelTimeout 从时间轮中删除
         */
        void remove() {
            HashedWheelBucket bucket = this.bucket;
            if (bucket != null) {
                bucket.remove(this);
            } else {
                timer.pendingTimeouts.decrementAndGet();
            }
        }

        public boolean compareAndSetState(int expected, int state) {
            return STATE_UPDATER.compareAndSet(this, expected, state);
        }

        public int state() {
            return state;
        }

        @Override
        public boolean isCancelled() {
            return state() == ST_CANCELLED;
        }

        @Override
        public boolean isExpired() {
            return state() == ST_EXPIRED;
        }

        /**
         *  当任务到期时, 会调用该方法将当前 HashedWheelTimeout 设置为 EXPIRED 状态,
         *  然后调用其中的 TimerTask 的 run() 方法执行定时任务
         */
        public void expire() {
            if (!compareAndSetState(ST_INIT, ST_EXPIRED)) {
                return;
            }

            try {
                task.run(this);
            } catch (Throwable t) {
                if (logger.isWarnEnabled()) {
                    logger.warn("An exception was thrown by " + TimerTask.class.getSimpleName() + '.', t);
                }
            }
        }

        @Override
        public String toString() {
            final long currentTime = System.nanoTime();
            long remaining = deadline - currentTime + timer.startTime;
            String simpleClassName = ClassUtils.simpleClassName(this.getClass());

            StringBuilder buf = new StringBuilder(192)
                    .append(simpleClassName)
                    .append('(')
                    .append("deadline: ");
            if (remaining > 0) {
                buf.append(remaining)
                        .append(" ns later");
            } else if (remaining < 0) {
                buf.append(-remaining)
                        .append(" ns ago");
            } else {
                buf.append("now");
            }

            if (isCancelled()) {
                buf.append(", cancelled");
            }

            return buf.append(", task: ")
                    .append(task())
                    .append(')')
                    .toString();
        }
    }

    /**
     * Bucket that stores HashedWheelTimeouts. These are stored in a linked-list like datastructure to allow easy
     * removal of HashedWheelTimeouts in the middle. Also the HashedWheelTimeout act as nodes themself and so no
     * extra object creation is needed.
     * 
     * HashedWheelBucket 是时间轮中的一个槽,时间轮中的槽实际上就是一个用于缓存和管理双向链表的容器,
     * 双向链表中的每一个节点就是一个 HashedWheelTimeout 对象,也就关联了一个 TimerTask 定时任务
     *
     * HashedWheelBucket 持有双向链表的首尾两个节点,分别是 head 和 tail 两个字段,再加上每个
     * HashedWheelTimeout 节点均持有前驱和后继的引用,这样就可以正向或是逆向遍历整个双向链表了
     */
    private static final class HashedWheelBucket {

        /**
         * Used for the linked-list datastructure
         */
        private HashedWheelTimeout head;
        private HashedWheelTimeout tail;

        /**
         * Add {@link HashedWheelTimeout} to this bucket.
         * 新增 HashedWheelTimeout 到双向链表的尾部
         */
        void addTimeout(HashedWheelTimeout timeout) {
            assert timeout.bucket == null;
            timeout.bucket = this;
            if (head == null) {
                head = tail = timeout;
            } else {
                tail.next = timeout;
                timeout.prev = tail;
                tail = timeout;
            }
        }

        /**
         * Expire all {@link HashedWheelTimeout}s for the given {@code deadline}.
         * 遍历双向链表中的全部 HashedWheelTimeout 节点
         * 1) 在处理到期的定时任务时,会通过 remove() 方法取出,并调用其 expire() 方法执行
         * 2) 对于已取消的任务,通过 remove() 方法取出后直接丢弃
         * 3) 对于未到期的任务,会将 remainingRounds 字段(剩余时钟周期数)减一
         */
        void expireTimeouts(long deadline) {
            HashedWheelTimeout timeout = head;

            // process all timeouts
            while (timeout != null) {
                HashedWheelTimeout next = timeout.next;
                if (timeout.remainingRounds <= 0) {
                    next = remove(timeout);
                    if (timeout.deadline <= deadline) {
                        timeout.expire();
                    } else {
                        // The timeout was placed into a wrong slot. This should never happen.
                        throw new IllegalStateException(String.format(
                                "timeout.deadline (%d) > deadline (%d)", timeout.deadline, deadline));
                    }
                } else if (timeout.isCancelled()) {
                    next = remove(timeout);
                } else {
                    timeout.remainingRounds--;
                }
                timeout = next;
            }
        }

        /**
         * 从双向链表中移除指定的 HashedWheelTimeout 节点
         */
        public HashedWheelTimeout remove(HashedWheelTimeout timeout) {
            HashedWheelTimeout next = timeout.next;
            // remove timeout that was either processed or cancelled by updating the linked-list
            if (timeout.prev != null) {
                timeout.prev.next = next;
            }
            if (timeout.next != null) {
                timeout.next.prev = timeout.prev;
            }

            if (timeout == head) {
                // if timeout is also the tail we need to adjust the entry too
                if (timeout == tail) {
                    tail = null;
                    head = null;
                } else {
                    head = next;
                }
            } else if (timeout == tail) {
                // if the timeout is the tail modify the tail to be the prev node.
                tail = timeout.prev;
            }
            // null out prev, next and bucket to allow for GC.
            timeout.prev = null;
            timeout.next = null;
            timeout.bucket = null;
            timeout.timer.pendingTimeouts.decrementAndGet();
            return next;
        }

        /**
         * Clear this bucket and return all not expired / cancelled {@link Timeout}s.
         * 循环调用 pollTimeout() 方法处理整个双向链表, 并返回所有未超时或者未被取消的任务
         */
        void clearTimeouts(Set<Timeout> set) {
            for (; ; ) {
                HashedWheelTimeout timeout = pollTimeout();
                if (timeout == null) {
                    return;
                }
                if (timeout.isExpired() || timeout.isCancelled()) {
                    continue;
                }
                set.add(timeout);
            }
        }

        /**
         * 移除双向链表中的头结点, 并将其返回
         */
        private HashedWheelTimeout pollTimeout() {
            HashedWheelTimeout head = this.head;
            if (head == null) {
                return null;
            }
            HashedWheelTimeout next = head.next;
            if (next == null) {
                tail = this.head = null;
            } else {
                this.head = next;
                next.prev = null;
            }

            // null out prev and next to allow for GC.
            head.next = null;
            head.prev = null;
            head.bucket = null;
            return head;
        }
    }

    private boolean isWindows() {
        return System.getProperty("os.name", "").toLowerCase(Locale.US).contains("win");
    }
}
