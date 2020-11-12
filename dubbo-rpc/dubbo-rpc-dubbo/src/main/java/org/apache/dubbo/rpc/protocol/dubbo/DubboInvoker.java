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
package org.apache.dubbo.rpc.protocol.dubbo;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.config.ConfigurationUtils;
import org.apache.dubbo.common.utils.AtomicPositiveInteger;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.TimeoutException;
import org.apache.dubbo.remoting.exchange.ExchangeClient;
import org.apache.dubbo.rpc.AppResponse;
import org.apache.dubbo.rpc.AsyncRpcResult;
import org.apache.dubbo.rpc.FutureContext;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.Result;
import org.apache.dubbo.rpc.RpcContext;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.RpcInvocation;
import org.apache.dubbo.rpc.TimeoutCountDown;
import org.apache.dubbo.rpc.protocol.AbstractInvoker;
import org.apache.dubbo.rpc.support.RpcUtils;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static org.apache.dubbo.common.constants.CommonConstants.DEFAULT_TIMEOUT;
import static org.apache.dubbo.common.constants.CommonConstants.ENABLE_TIMEOUT_COUNTDOWN_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.GROUP_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.INTERFACE_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.PATH_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.TIMEOUT_ATTACHMENT_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.TIME_COUNTDOWN_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.VERSION_KEY;
import static org.apache.dubbo.rpc.Constants.TOKEN_KEY;

/**
 * DubboInvoker
 */
public class DubboInvoker<T> extends AbstractInvoker<T> {

    private final ExchangeClient[] clients;

    private final AtomicPositiveInteger index = new AtomicPositiveInteger();

    private final String version;

    private final ReentrantLock destroyLock = new ReentrantLock();

    private final Set<Invoker<?>> invokers;

    public DubboInvoker(Class<T> serviceType, URL url, ExchangeClient[] clients) {
        this(serviceType, url, clients, null);
    }

    public DubboInvoker(Class<T> serviceType, URL url, ExchangeClient[] clients, Set<Invoker<?>> invokers) {
        super(serviceType, url, new String[]{INTERFACE_KEY, GROUP_KEY, TOKEN_KEY});
        this.clients = clients;
        // get version.
        this.version = url.getParameter(VERSION_KEY, "0.0.0");
        this.invokers = invokers;
    }

    /**
     * 通过前面对 DubboProtocol 的分析我们知道, protocolBindingRefer() 方法会根据调用的业务接口类型以及
     * URL 创建底层的 ExchangeClient 集合, 然后封装成 DubboInvoker 对象返回. DubboInvoker 是 AbstractInvoker
     * 的实现类, 在其 doInvoke() 方法中首先会选择此次调用使用 ExchangeClient 对象, 然后确定此次调用是否需要
     * 返回值, 最后调用 ExchangeClient.request() 方法发送请求, 对返回的 Future 进行简单封装并返回
     * 
     * 在 Client 端发送请求时, 首先会创建对应的 DefaultFuture(其中记录了请求 ID 等信息), 然后依赖 Netty
     * 的异步发送特性将请求发送到 Server 端. 需要说明的是, 这整个发送过程是不会阻塞任何线程的. 之后, 将
     * DefaultFuture 返回给上层, 在这个返回过程中, DefaultFuture 会被封装成 AsyncRpcResult, 同时也可以添加回调函数.
     *
     * 当 Client 端接收到响应结果的时候, 会交给关联的线程池(ExecutorService)或是业务线程(使用 ThreadlessExecutor 场景)
     * 进行处理, 得到 Server 返回的真正结果. 拿到真正的返回结果后, 会将其设置到 DefaultFuture 中, 并调用 complete() 方法
     * 将其设置为完成状态. 此时, 就会触发前面注册在 DefaulFuture 上的回调函数, 执行回调逻辑
     *
     * https://s0.lgstatic.com/i/image/M00/64/40/Ciqc1F-X8WuACaAKAAEb-X6qf4Y710.png
     */
    @Override
    protected Result doInvoke(final Invocation invocation) throws Throwable {
        RpcInvocation inv = (RpcInvocation) invocation;
        // 此次调用的方法名称
        final String methodName = RpcUtils.getMethodName(invocation);
        // 向Invocation中添加附加信息, 这里将URL的path和version添加到附加信息中
        inv.setAttachment(PATH_KEY, getUrl().getPath());
        inv.setAttachment(VERSION_KEY, version);

        // 选择一个ExchangeClient实例
        ExchangeClient currentClient;
        if (clients.length == 1) {
            currentClient = clients[0];
        } else {
            currentClient = clients[index.getAndIncrement() % clients.length];
        }
        try {
            // 判断调用方式
            boolean isOneway = RpcUtils.isOneway(getUrl(), invocation);
            // 根据调用的方法名称和配置计算此次调用的超时时间
            int timeout = calculateTimeout(invocation, methodName);
            // 不需要关注返回值的请求
            if (isOneway) {
                boolean isSent = getUrl().getMethodParameter(methodName, Constants.SENT_KEY, false);
                currentClient.send(inv, isSent);
                // 在发送完oneway 请求之后, 会立即创建一个已完成状态的 AsyncRpcResult 对象(主要是其中的 responseFuture 是已完成状态)
                return AsyncRpcResult.newDefaultAsyncResult(invocation);
            } else {
                // 需要关注返回值的请求
                // 获取处理响应的线程池, 对于同步请求, 会使用ThreadlessExecutor. 对于异步请求, 则会使用共享的线程池
                ExecutorService executor = getCallbackExecutor(getUrl(), inv);
                // 使用上面选出的ExchangeClient执行request()方法, 将请求发送出去
                CompletableFuture<AppResponse> appResponseFuture =
                        currentClient.request(inv, timeout, executor).thenApply(obj -> (AppResponse) obj);
                // save for 2.6.x compatibility, for example, TraceFilter in Zipkin uses com.alibaba.xxx.FutureAdapter
                FutureContext.getContext().setCompatibleFuture(appResponseFuture);
                // 这里将AppResponse封装成AsyncRpcResult返回
                AsyncRpcResult result = new AsyncRpcResult(appResponseFuture, inv);
                result.setExecutor(executor);
                return result;
            }
        } catch (TimeoutException e) {
            throw new RpcException(RpcException.TIMEOUT_EXCEPTION, "Invoke remote method timeout. method: " + invocation.getMethodName() + ", provider: " + getUrl() + ", cause: " + e.getMessage(), e);
        } catch (RemotingException e) {
            throw new RpcException(RpcException.NETWORK_EXCEPTION, "Failed to invoke remote method: " + invocation.getMethodName() + ", provider: " + getUrl() + ", cause: " + e.getMessage(), e);
        }
    }

    @Override
    public boolean isAvailable() {
        if (!super.isAvailable()) {
            return false;
        }
        for (ExchangeClient client : clients) {
            if (client.isConnected() && !client.hasAttribute(Constants.CHANNEL_ATTRIBUTE_READONLY_KEY)) {
                //cannot write == not Available ?
                return true;
            }
        }
        return false;
    }

    @Override
    public void destroy() {
        // in order to avoid closing a client multiple times, a counter is used in case of connection per jvm, every
        // time when client.close() is called, counter counts down once, and when counter reaches zero, client will be
        // closed.
        if (super.isDestroyed()) {
            return;
        } else {
            // double check to avoid dup close
            destroyLock.lock();
            try {
                if (super.isDestroyed()) {
                    return;
                }
                super.destroy();
                if (invokers != null) {
                    invokers.remove(this);
                }
                for (ExchangeClient client : clients) {
                    try {
                        client.close(ConfigurationUtils.getServerShutdownTimeout());
                    } catch (Throwable t) {
                        logger.warn(t.getMessage(), t);
                    }
                }

            } finally {
                destroyLock.unlock();
            }
        }
    }

    private int calculateTimeout(Invocation invocation, String methodName) {
        Object countdown = RpcContext.getContext().get(TIME_COUNTDOWN_KEY);
        int timeout = DEFAULT_TIMEOUT;
        if (countdown == null) {
            timeout = (int) RpcUtils.getTimeout(getUrl(), methodName, RpcContext.getContext(), DEFAULT_TIMEOUT);
            if (getUrl().getParameter(ENABLE_TIMEOUT_COUNTDOWN_KEY, false)) {
                invocation.setObjectAttachment(TIMEOUT_ATTACHMENT_KEY, timeout); // pass timeout to remote server
            }
        } else {
            TimeoutCountDown timeoutCountDown = (TimeoutCountDown) countdown;
            timeout = (int) timeoutCountDown.timeRemaining(TimeUnit.MILLISECONDS);
            invocation.setObjectAttachment(TIMEOUT_ATTACHMENT_KEY, timeout);// pass timeout to remote server
        }
        return timeout;
    }
}
