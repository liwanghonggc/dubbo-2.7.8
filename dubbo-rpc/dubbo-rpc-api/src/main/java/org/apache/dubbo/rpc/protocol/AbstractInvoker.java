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
package org.apache.dubbo.rpc.protocol;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.Version;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.threadpool.ThreadlessExecutor;
import org.apache.dubbo.common.threadpool.manager.ExecutorRepository;
import org.apache.dubbo.common.utils.ArrayUtils;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.rpc.AsyncRpcResult;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.InvokeMode;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.protocol.dubbo.FutureAdapter;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * AbstractInvoker.
 */
public abstract class AbstractInvoker<T> implements Invoker<T> {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * 该 Invoker 对象封装的业务接口类型
     */
    private final Class<T> type;

    /**
     * 与当前 Invoker 关联的 URL 对象, 其中包含了全部的配置信息
     */
    private final URL url;

    /**
     * 当前 Invoker 关联的一些附加信息, 这些附加信息可以来自关联的 URL. 在
     * AbstractInvoker 的构造函数的某个重载中, 会调用 convertAttachment() 方法,
     * 其中就会从关联的 URL 对象获取指定的 KV 值记录到 attachment 集合中
     */
    private final Map<String, Object> attachment;

    /**
     * available和destroyed用来控制invoker状态
     */
    private volatile boolean available = true;

    private AtomicBoolean destroyed = new AtomicBoolean(false);

    public AbstractInvoker(Class<T> type, URL url) {
        this(type, url, (Map<String, Object>) null);
    }

    public AbstractInvoker(Class<T> type, URL url, String[] keys) {
        this(type, url, convertAttachment(url, keys));
    }

    public AbstractInvoker(Class<T> type, URL url, Map<String, Object> attachment) {
        if (type == null) {
            throw new IllegalArgumentException("service type == null");
        }
        if (url == null) {
            throw new IllegalArgumentException("service url == null");
        }
        this.type = type;
        this.url = url;
        this.attachment = attachment == null ? null : Collections.unmodifiableMap(attachment);
    }

    private static Map<String, Object> convertAttachment(URL url, String[] keys) {
        if (ArrayUtils.isEmpty(keys)) {
            return null;
        }
        Map<String, Object> attachment = new HashMap<>();
        for (String key : keys) {
            String value = url.getParameter(key);
            if (value != null && value.length() > 0) {
                attachment.put(key, value);
            }
        }
        return attachment;
    }

    @Override
    public Class<T> getInterface() {
        return type;
    }

    @Override
    public URL getUrl() {
        return url;
    }

    @Override
    public boolean isAvailable() {
        return available;
    }

    protected void setAvailable(boolean available) {
        this.available = available;
    }

    @Override
    public void destroy() {
        if (!destroyed.compareAndSet(false, true)) {
            return;
        }
        setAvailable(false);
    }

    public boolean isDestroyed() {
        return destroyed.get();
    }

    @Override
    public String toString() {
        return getInterface() + " -> " + (getUrl() == null ? "" : getUrl().toString());
    }

    /**
     * 在 AbstractInvoker 中实现了 Invoker 接口中的 invoke() 方法, 这里有点模板方法模式的感觉,
     * 其中先对 URL 中的配置信息以及 RpcContext 中携带的附加信息进行处理, 添加到 Invocation 中
     * 作为附加信息, 然后调用 doInvoke() 方法发起远程调用(该方法由 AbstractInvoker 的子类具体实现),
     * 最后得到 AsyncRpcResult 对象返回
     */
    @Override
    public Result invoke(Invocation inv) throws RpcException {
        // if invoker is destroyed due to address refresh from registry, let's allow the current invoke to proceed
        if (destroyed.get()) {
            logger.warn("Invoker for service " + this + " on consumer " + NetUtils.getLocalHost() + " is destroyed, "
                    + ", dubbo version is " + Version.getVersion() + ", this invoker should not be used any longer");
        }
        // 首先将传入的Invocation转换为RpcInvocation
        RpcInvocation invocation = (RpcInvocation) inv;
        invocation.setInvoker(this);
        // 将前文介绍的attachment集合添加为Invocation的附加信息
        if (CollectionUtils.isNotEmptyMap(attachment)) {
            invocation.addObjectAttachmentsIfAbsent(attachment);
        }

        // 将RpcContext的附加信息添加为Invocation的附加信息, 分析下RpcContext
        Map<String, Object> contextAttachments = RpcContext.getContext().getObjectAttachments();
        if (CollectionUtils.isNotEmptyMap(contextAttachments)) {
            /**
             * invocation.addAttachmentsIfAbsent(context){@link RpcInvocation#addAttachmentsIfAbsent(Map)}should not be used here,
             * because the {@link RpcContext#setAttachment(String, String)} is passed in the Filter when the call is triggered
             * by the built-in retry mechanism of the Dubbo. The attachment to update RpcContext will no longer work, which is
             * a mistake in most cases (for example, through Filter to RpcContext output traceId and spanId and other information).
             */
            invocation.addObjectAttachments(contextAttachments);
        }

        // 设置此次调用的模式, 异步还是同步
        invocation.setInvokeMode(RpcUtils.getInvokeMode(url, invocation));
        // 如果是异步调用, 给这次调用添加一个唯一ID
        RpcUtils.attachInvocationIdIfAsync(getUrl(), invocation);

        AsyncRpcResult asyncResult;
        try {
            // 调用子类实现的doInvoke()方法
            asyncResult = (AsyncRpcResult) doInvoke(invocation);
        } catch (InvocationTargetException e) { // biz exception
            Throwable te = e.getTargetException();
            if (te == null) {
                asyncResult = AsyncRpcResult.newDefaultAsyncResult(null, e, invocation);
            } else {
                if (te instanceof RpcException) {
                    ((RpcException) te).setCode(RpcException.BIZ_EXCEPTION);
                }
                asyncResult = AsyncRpcResult.newDefaultAsyncResult(null, te, invocation);
            }
        } catch (RpcException e) {
            if (e.isBiz()) {
                asyncResult = AsyncRpcResult.newDefaultAsyncResult(null, e, invocation);
            } else {
                throw e;
            }
        } catch (Throwable e) {
            asyncResult = AsyncRpcResult.newDefaultAsyncResult(null, e, invocation);
        }
        // 这里拿到的其实就是 AsyncRpcResult 中 responseFuture
        RpcContext.getContext().setFuture(new FutureAdapter(asyncResult.getResponseFuture()));
        return asyncResult;
    }

    /**
     * 根据不同的 InvokeMode 返回不同的线程池实现. 对于 SYNC 模式返回的线程池是
     * ThreadlessExecutor, 至于其他两种异步模式, 会根据 URL 选择对应的共享线程池
     */
    protected ExecutorService getCallbackExecutor(URL url, Invocation inv) {
        ExecutorService sharedExecutor = ExtensionLoader.getExtensionLoader(ExecutorRepository.class).getDefaultExtension().getExecutor(url);
        if (InvokeMode.SYNC == RpcUtils.getInvokeMode(getUrl(), inv)) {
            return new ThreadlessExecutor(sharedExecutor);
        } else {
            return sharedExecutor;
        }
    }

    protected abstract Result doInvoke(Invocation invocation) throws Throwable;

}
