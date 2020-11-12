/*
 * Copyright 2014 The Netty Project
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

package org.apache.dubbo.common.threadlocal;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Set;

/**
 * InternalThreadLocal
 * A special variant of {@link ThreadLocal} that yields higher access performance when accessed from a
 * {@link InternalThread}.
 * <p></p>
 * Internally, a {@link InternalThread} uses a constant index in an array, instead of using hash code and hash table,
 * to look for a variable.  Although seemingly very subtle, it yields slight performance advantage over using a hash
 * table, and it is useful when accessed frequently.
 * <p></p>
 * This design is learning from {@see io.netty.util.concurrent.FastThreadLocal} which is in Netty.
 * 
 * JDK 提供的 ThreadLocal 底层实现大致如下: 对于不同线程创建对应的 ThreadLocalMap, 用于存放线程绑定信息,
 * 当用户调用ThreadLocal.get() 方法获取变量时, 底层会先获取当前线程 Thread, 然后获取绑定到当前线程 Thread
 * 的 ThreadLocalMap, 最后将当前 ThreadLocal 对象作为 Key 去 ThreadLocalMap 表中获取线程绑定的数据. 
 * ThreadLocal.set() 方法的逻辑与之类似, 首先会获取绑定到当前线程的 ThreadLocalMap, 然后将 ThreadLocal 
 * 实例作为 Key、待存储的数据作为 Value 存储到 ThreadLocalMap 中. 
 *
 * Dubbo 的 InternalThreadLocal 与 JDK 提供的 ThreadLocal 功能类似, 只是底层实现略有不同, 其底层的 
 * InternalThreadLocalMap 采用数组结构存储数据, 直接通过 index 获取变量, 相较于 Map 方式计算 hash 值的性能更好
 */
public class InternalThreadLocal<V> {

    /**
     * InternalThreadLocal 的静态变量 VARIABLES_TO_REMOVE_INDEX 是调用InternalThreadLocalMap 的
     * nextVariableIndex 方法得到的一个索引值, 在 InternalThreadLocalMap 数组的对应位置保存的是
     * Set<InternalThreadLocal> 类型的集合, 也就是上面提到的“待删除集合”, 即绑定到当前线程所有的
     * InternalThreadLocal, 这样就可以方便管理对象及内存的释放
     */
    private static final int VARIABLES_TO_REMOVE_INDEX = InternalThreadLocalMap.nextVariableIndex();

    private final int index;

    public InternalThreadLocal() {
        index = InternalThreadLocalMap.nextVariableIndex();
    }

    /**
     * Removes all {@link InternalThreadLocal} variables bound to the current thread.  This operation is useful when you
     * are in a container environment, and you don't want to leave the thread local variables in the threads you do not
     * manage.
     */
    @SuppressWarnings("unchecked")
    public static void removeAll() {
        InternalThreadLocalMap threadLocalMap = InternalThreadLocalMap.getIfSet();
        if (threadLocalMap == null) {
            return;
        }

        try {
            Object v = threadLocalMap.indexedVariable(VARIABLES_TO_REMOVE_INDEX);
            if (v != null && v != InternalThreadLocalMap.UNSET) {
                Set<InternalThreadLocal<?>> variablesToRemove = (Set<InternalThreadLocal<?>>) v;
                InternalThreadLocal<?>[] variablesToRemoveArray =
                        variablesToRemove.toArray(new InternalThreadLocal[variablesToRemove.size()]);
                for (InternalThreadLocal<?> tlv : variablesToRemoveArray) {
                    tlv.remove(threadLocalMap);
                }
            }
        } finally {
            InternalThreadLocalMap.remove();
        }
    }

    /**
     * Returns the number of thread local variables bound to the current thread.
     */
    public static int size() {
        InternalThreadLocalMap threadLocalMap = InternalThreadLocalMap.getIfSet();
        if (threadLocalMap == null) {
            return 0;
        } else {
            return threadLocalMap.size();
        }
    }

    public static void destroy() {
        InternalThreadLocalMap.destroy();
    }

    @SuppressWarnings("unchecked")
    private static void addToVariablesToRemove(InternalThreadLocalMap threadLocalMap, InternalThreadLocal<?> variable) {
        Object v = threadLocalMap.indexedVariable(VARIABLES_TO_REMOVE_INDEX);
        Set<InternalThreadLocal<?>> variablesToRemove;
        if (v == InternalThreadLocalMap.UNSET || v == null) {
            variablesToRemove = Collections.newSetFromMap(new IdentityHashMap<InternalThreadLocal<?>, Boolean>());
            threadLocalMap.setIndexedVariable(VARIABLES_TO_REMOVE_INDEX, variablesToRemove);
        } else {
            variablesToRemove = (Set<InternalThreadLocal<?>>) v;
        }

        variablesToRemove.add(variable);
    }

    @SuppressWarnings("unchecked")
    private static void removeFromVariablesToRemove(InternalThreadLocalMap threadLocalMap, InternalThreadLocal<?> variable) {

        Object v = threadLocalMap.indexedVariable(VARIABLES_TO_REMOVE_INDEX);

        if (v == InternalThreadLocalMap.UNSET || v == null) {
            return;
        }

        Set<InternalThreadLocal<?>> variablesToRemove = (Set<InternalThreadLocal<?>>) v;
        variablesToRemove.remove(variable);
    }

    /**
     * Returns the current value for the current thread
     */
    @SuppressWarnings("unchecked")
    public final V get() {
        // 获取当前线程绑定的InternalThreadLocalMap
        InternalThreadLocalMap threadLocalMap = InternalThreadLocalMap.get();
        // 根据当前InternalThreadLocal对象的index字段, 从InternalThreadLocalMap中读取相应的数据
        Object v = threadLocalMap.indexedVariable(index);
        if (v != InternalThreadLocalMap.UNSET) {
            // 如果非UNSET, 则表示读取到了有效数据, 直接返回
            return (V) v;
        }

        // 读取到UNSET值, 则会调用initialize()方法进行初始化, 其中首先会调用
        // initialValue()方法进行初始化, 然后会调用前面介绍的setIndexedVariable()方法
        // 和addToVariablesToRemove()方法存储初始化得到的值
        return initialize(threadLocalMap);
    }

    private V initialize(InternalThreadLocalMap threadLocalMap) {
        V v = null;
        try {
            v = initialValue();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        threadLocalMap.setIndexedVariable(index, v);
        addToVariablesToRemove(threadLocalMap, this);
        return v;
    }

    /**
     * Sets the value for the current thread.
     * 在拿到 InternalThreadLocalMap 对象之后, 我们就可以调用其 setIndexedVariable() 方法和
     * indexedVariable() 方法读写, 这里我们得结合InternalThreadLocal进行讲解. 在
     * InternalThreadLocal 的构造方法中, 会使用 InternalThreadLocalMap.NEXT_INDEX
     * 初始化其 index 字段（int 类型）, 在 InternalThreadLocal.set() 方法中就会将传入
     * 的数据存储到 InternalThreadLocalMap.indexedVariables 集合中, 具体的下标位置就是这里的 index 字段值
     */
    public final void set(V value) {
        // 如果要存储的值为null或是UNSERT, 则直接清除
        if (value == null || value == InternalThreadLocalMap.UNSET) {
            remove();
        } else {
            // 获取当前线程绑定的InternalThreadLocalMap
            InternalThreadLocalMap threadLocalMap = InternalThreadLocalMap.get();
            // 将value存储到InternalThreadLocalMap.indexedVariables集合中
            if (threadLocalMap.setIndexedVariable(index, value)) {
                // 将当前InternalThreadLocal记录到待删除集合中
                addToVariablesToRemove(threadLocalMap, this);
            }
        }
    }

    /**
     * Sets the value to uninitialized; a proceeding call to get() will trigger a call to initialValue().
     */
    @SuppressWarnings("unchecked")
    public final void remove() {
        remove(InternalThreadLocalMap.getIfSet());
    }

    /**
     * Sets the value to uninitialized for the specified thread local map;
     * a proceeding call to get() will trigger a call to initialValue().
     * The specified thread local map must be for the current thread.
     */
    @SuppressWarnings("unchecked")
    public final void remove(InternalThreadLocalMap threadLocalMap) {
        if (threadLocalMap == null) {
            return;
        }

        Object v = threadLocalMap.removeIndexedVariable(index);
        removeFromVariablesToRemove(threadLocalMap, this);

        if (v != InternalThreadLocalMap.UNSET) {
            try {
                onRemoval((V) v);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * Returns the initial value for this thread-local variable.
     */
    protected V initialValue() throws Exception {
        return null;
    }

    /**
     * Invoked when this thread local variable is removed by {@link #remove()}.
     */
    protected void onRemoval(@SuppressWarnings("unused") V value) throws Exception {
    }
}
