/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.dubbo.common.bytecode;

import org.apache.dubbo.common.utils.ClassUtils;
import org.apache.dubbo.common.utils.ReflectUtils;

import java.lang.ref.Reference;
import java.lang.ref.WeakReference;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.dubbo.common.constants.CommonConstants.MAX_PROXY_COUNT;

/**
 * Proxy 中的 getProxy() 方法提供了动态创建代理类的核心实现
 */

public abstract class Proxy {
    public static final InvocationHandler RETURN_NULL_INVOKER = (proxy, method, args) -> null;
    public static final InvocationHandler THROW_UNSUPPORTED_INVOKER = new InvocationHandler() {
        @Override
        public Object invoke(Object proxy, Method method, Object[] args) {
            throw new UnsupportedOperationException("Method [" + ReflectUtils.getName(method) + "] unimplemented.");
        }
    };
    private static final AtomicLong PROXY_CLASS_COUNTER = new AtomicLong(0);
    private static final String PACKAGE_NAME = Proxy.class.getPackage().getName();

    /**
     * 其中第一层 Key 是 ClassLoader 对象, 第二层 Key 是上面整理得到的接口拼接而成的,
     * Value 是被缓存的代理类的 WeakReference弱引用
     */
    private static final Map<ClassLoader, Map<String, Object>> PROXY_CACHE_MAP = new WeakHashMap<ClassLoader, Map<String, Object>>();

    private static final Object PENDING_GENERATION_MARKER = new Object();

    protected Proxy() {
    }

    /**
     * Get proxy.
     *
     * @param ics interface class array.
     * @return Proxy instance.
     */
    public static Proxy getProxy(Class<?>... ics) {
        return getProxy(ClassUtils.getClassLoader(Proxy.class), ics);
    }

    /**
     * 1) 首先是查找 PROXY_CACHE_MAP 这个代理类缓存(new WeakHashMap<ClassLoader, Map<String, Object>>() 类型),
     * 其中第一层 Key 是 ClassLoader 对象, 第二层 Key 是上面整理得到的接口拼接而成的, Value 是被缓存的代理类的
     * WeakReference(弱引用). 查找缓存的结果有下面三个:
     *
     *   a) 如果缓存中查找不到任务信息, 则会在缓存中添加一个 PENDING_GENERATION_MARKER 占位符,
     *      当前线程后续创建生成代理类并最终替换占位符.
     *
     *   b) 如果在缓存中查找到了 PENDING_GENERATION_MARKER 占位符, 说明其他线程已经在生成相应的代理类了,
     *      当前线程会阻塞等待.
     *
     *   c) 如果缓存中查找到完整代理类, 则会直接返回, 不会再执行后续动态代理类的生成.
     *
     *   完成缓存的查找之后, 下面我们再来看代理类的生成过程.
     *
     * 2) 调用 ClassGenerator.newInstance() 方法创建 ClassLoader 对应的 ClassPool. ClassGenerator 中封装了
     *    Javassist 的基本操作, 还定义了很多字段用来暂存代理类的信息, 在其 toClass() 方法中会用这些暂存的信息来动态生成代理类
     *
     * 3) 从 PROXY_CLASS_COUNTER 字段(AtomicLong类型)中获取一个 id 值, 作为代理类的后缀, 这主要是为了避免类名重复发生冲突.
     *
     * 4) 遍历全部接口, 获取每个接口中定义的方法, 对每个方法进行如下处理:
     *
     *    a) 加入 worked 集合(Set<String> 类型)中, 用来判重.
     *
     *    b) 将方法对应的 Method 对象添加到 methods 集合(List<Method> 类型)中.
     *
     *    c) 获取方法的参数类型以及返回类型, 构建方法体以及 return 语句.
     *
     *    d) 将构造好的方法添加到 ClassGenerator 中的 mMethods 集合中进行缓存.
     *
     * 5) 开始创建代理实例类(ProxyInstance)和代理类. 这里我们先创建代理实例类, 需要向 ClassGenerator
     *    中添加相应的信息, 例如, 类名、默认构造方法、字段、父类以及一个 newInstance() 方法
     * 
     * 6) 也就是最后一步, 在 finally 代码块中, 会释放 ClassGenerator 的相关资源, 将生成的代理类添加到
     *    PROXY_CACHE_MAP 缓存中保存, 同时会唤醒所有阻塞在 PROXY_CACHE_MAP 缓存上的线程, 重新检测需要
     *    的代理类是否已经生成完毕
     */
    public static Proxy getProxy(ClassLoader cl, Class<?>... ics) {
        if (ics.length > MAX_PROXY_COUNT) {
            throw new IllegalArgumentException("interface limit exceeded");
        }

        StringBuilder sb = new StringBuilder();
        // 循环处理每个接口类
        for (int i = 0; i < ics.length; i++) {
            String itf = ics[i].getName();
            // 传入的必须是接口类, 否则直接报错
            if (!ics[i].isInterface()) {
                throw new RuntimeException(itf + " is not a interface.");
            }

            Class<?> tmp = null;
            try {
                // 加载接口类, 加载失败则直接报错
                tmp = Class.forName(itf, false, cl);
            } catch (ClassNotFoundException e) {
            }

            if (tmp != ics[i]) {
                throw new IllegalArgumentException(ics[i] + " is not visible from class loader");
            }

            // 将接口类的完整名称用分号连接起来
            sb.append(itf).append(';');
        }

        // use interface class name list as key.
        // 接口列表将会作为第二层集合的Key
        String key = sb.toString();

        // get cache by class loader.
        final Map<String, Object> cache;
        // 加锁同步
        synchronized (PROXY_CACHE_MAP) {
            cache = PROXY_CACHE_MAP.computeIfAbsent(cl, k -> new HashMap<>());
        }

        Proxy proxy = null;
        synchronized (cache) {
            do {
                Object value = cache.get(key);
                // 获取到WeakReference
                if (value instanceof Reference<?>) {
                    proxy = (Proxy) ((Reference<?>) value).get();
                    // 查找到缓存的代理类
                    if (proxy != null) {
                        return proxy;
                    }
                }

                // 获取到占位符
                if (value == PENDING_GENERATION_MARKER) {
                    try {
                        // 阻塞等待其他线程生成好代理类, 并添加到缓存中
                        cache.wait();
                    } catch (InterruptedException e) {
                    }
                } else {
                    // 设置占位符, 由当前线程生成代理类
                    cache.put(key, PENDING_GENERATION_MARKER);
                    // 退出当前循环
                    break;
                }
            }
            while (true);
        }

        // 后续生成动态代理类的逻辑
        // 从 PROXY_CLASS_COUNTER 字段(AtomicLong类型)中获取一个 id 值, 作为代理类的后缀, 这主要是为了避免类名重复发生冲突
        long id = PROXY_CLASS_COUNTER.getAndIncrement();
        String pkg = null;
        ClassGenerator ccp = null, ccm = null;
        try {
            ccp = ClassGenerator.newInstance(cl);

            Set<String> worked = new HashSet<>();
            List<Method> methods = new ArrayList<>();

            for (int i = 0; i < ics.length; i++) {
                if (!Modifier.isPublic(ics[i].getModifiers())) {
                    String npkg = ics[i].getPackage().getName();
                    // 如果接口不是public的, 则需要保证所有接口在一个包下
                    if (pkg == null) {
                        pkg = npkg;
                    } else {
                        if (!pkg.equals(npkg)) {
                            throw new IllegalArgumentException("non-public interfaces from different packages");
                        }
                    }
                }
                // 向ClassGenerator中添加接口
                ccp.addInterface(ics[i]);

                // 遍历接口中的每个方法
                for (Method method : ics[i].getMethods()) {
                    String desc = ReflectUtils.getDesc(method);
                    // 跳过已经重复方法以及static方法
                    if (worked.contains(desc) || Modifier.isStatic(method.getModifiers())) {
                        continue;
                    }
                    if (ics[i].isInterface() && Modifier.isStatic(method.getModifiers())) {
                        continue;
                    }
                    // 将方法描述添加到worked这个Set集合中, 进行去重
                    worked.add(desc);

                    int ix = methods.size();
                    // 获取方法的返回值
                    Class<?> rt = method.getReturnType();
                    // 获取方法的参数列表
                    Class<?>[] pts = method.getParameterTypes();

                    // 创建方法体
                    StringBuilder code = new StringBuilder("Object[] args = new Object[").append(pts.length).append("];");
                    for (int j = 0; j < pts.length; j++) {
                        code.append(" args[").append(j).append("] = ($w)$").append(j + 1).append(";");
                    }
                    code.append(" Object ret = handler.invoke(this, methods[").append(ix).append("], args);");
                    // 生成return语句
                    if (!Void.TYPE.equals(rt)) {
                        code.append(" return ").append(asArgument(rt, "ret")).append(";");
                    }

                    // 将生成好的方法添加到ClassGenerator中缓存
                    methods.add(method);
                    ccp.addMethod(method.getName(), method.getModifiers(), rt, pts, method.getExceptionTypes(), code.toString());
                }
            }

            if (pkg == null) {
                pkg = PACKAGE_NAME;
            }

            // 开始创建代理实例类(ProxyInstance)和代理类
            // create ProxyInstance class.
            // 生成并设置代理类类名
            String pcn = pkg + ".proxy" + id;
            ccp.setClassName(pcn);
            // 添加字段, 一个是前面生成的methods集合, 另一个是InvocationHandler对象
            ccp.addField("public static java.lang.reflect.Method[] methods;");
            ccp.addField("private " + InvocationHandler.class.getName() + " handler;");
            // 添加构造方法
            ccp.addConstructor(Modifier.PUBLIC, new Class<?>[]{InvocationHandler.class}, new Class<?>[0], "handler=$1;");
            // 默认构造方法
            ccp.addDefaultConstructor();
            Class<?> clazz = ccp.toClass();
            clazz.getField("methods").set(null, methods.toArray(new Method[0]));

            // create Proxy class.
            // 创建代理类
            String fcn = Proxy.class.getName() + id;
            ccm = ClassGenerator.newInstance(cl);
            ccm.setClassName(fcn);
            // 默认构造方法
            ccm.addDefaultConstructor();
            // 实现Proxy接口
            ccm.setSuperClass(Proxy.class);
            // 实现newInstance()方法, 返回上面创建的代理实例类的对象
            ccm.addMethod("public Object newInstance(" + InvocationHandler.class.getName() + " h){ return new " + pcn + "($1); }");
            Class<?> pc = ccm.toClass();
            proxy = (Proxy) pc.newInstance();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            /**
             * 在 finally 代码块中, 会释放 ClassGenerator 的相关资源, 将生成的代理类添加到 PROXY_CACHE_MAP 缓存中保存,
             * 同时会唤醒所有阻塞在 PROXY_CACHE_MAP 缓存上的线程, 重新检测需要的代理类是否已经生成完毕
             */
            if (ccp != null) {
                ccp.release();
            }
            if (ccm != null) {
                ccm.release();
            }
            synchronized (cache) {
                if (proxy == null) {
                    cache.remove(key);
                } else {
                    cache.put(key, new WeakReference<Proxy>(proxy));
                }
                cache.notifyAll();
            }
        }
        return proxy;
    }

    private static String asArgument(Class<?> cl, String name) {
        if (cl.isPrimitive()) {
            if (Boolean.TYPE == cl) {
                return name + "==null?false:((Boolean)" + name + ").booleanValue()";
            }
            if (Byte.TYPE == cl) {
                return name + "==null?(byte)0:((Byte)" + name + ").byteValue()";
            }
            if (Character.TYPE == cl) {
                return name + "==null?(char)0:((Character)" + name + ").charValue()";
            }
            if (Double.TYPE == cl) {
                return name + "==null?(double)0:((Double)" + name + ").doubleValue()";
            }
            if (Float.TYPE == cl) {
                return name + "==null?(float)0:((Float)" + name + ").floatValue()";
            }
            if (Integer.TYPE == cl) {
                return name + "==null?(int)0:((Integer)" + name + ").intValue()";
            }
            if (Long.TYPE == cl) {
                return name + "==null?(long)0:((Long)" + name + ").longValue()";
            }
            if (Short.TYPE == cl) {
                return name + "==null?(short)0:((Short)" + name + ").shortValue()";
            }
            throw new RuntimeException(name + " is unknown primitive type.");
        }
        return "(" + ReflectUtils.getName(cl) + ")" + name;
    }

    /**
     * get instance with default handler.
     *
     * @return instance.
     */
    public Object newInstance() {
        return newInstance(THROW_UNSUPPORTED_INVOKER);
    }

    /**
     * get instance with special handler.
     *
     * @return instance.
     */
    abstract public Object newInstance(InvocationHandler handler);
}
