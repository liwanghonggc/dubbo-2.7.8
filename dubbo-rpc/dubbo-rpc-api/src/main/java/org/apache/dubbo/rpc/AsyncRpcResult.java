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
package org.apache.dubbo.rpc;

import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.threadpool.ThreadlessExecutor;
import org.apache.dubbo.rpc.model.ConsumerMethodModel;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.BiConsumer;
import java.util.function.Function;

import static org.apache.dubbo.common.utils.ReflectUtils.defaultReturn;

/**
 * This class represents an unfinished RPC call, it will hold some context information for this call, for example RpcContext and Invocation,
 * so that when the call finishes and the result returns, it can guarantee all the contexts being recovered as the same as when the call was made
 * before any callback is invoked.
 * <p>
 * TODO if it's reasonable or even right to keep a reference to Invocation?
 * <p>
 * As {@link Result} implements CompletionStage, {@link AsyncRpcResult} allows you to easily build a async filter chain whose status will be
 * driven entirely by the state of the underlying RPC call.
 * <p>
 * AsyncRpcResult does not contain any concrete value (except the underlying value bring by CompletableFuture), consider it as a status transfer node.
 * {@link #getValue()} and {@link #getException()} are all inherited from {@link Result} interface, implementing them are mainly
 * for compatibility consideration. Because many legacy {@link Filter} implementation are most possibly to call getValue directly.
 * 
 * 在 DubboInvoker 中还有一个 AsyncRpcResult 类, 它表示的是一个异步的、未完成的 RPC 调用,
 * 其中会记录对应 RPC 调用的信息(例如, 关联的 RpcContext 和 Invocation 对象). RpcInvocation.InvokeMode
 * 字段中可以指定调用为 SYNC 模式, 也就是同步调用模式, 那 AsyncRpcResult 这种异步设计是如何支持同步调用的呢?
 * 在 AbstractProtocol.refer() 方法中, Dubbo 会将 DubboProtocol.protocolBindingRefer() 方法返回的 Invoker
 * 对象(即 DubboInvoker 对象)用 AsyncToSyncInvoker 封装一层
 */
public class AsyncRpcResult implements Result {
    private static final Logger logger = LoggerFactory.getLogger(AsyncRpcResult.class);

    /**
     * RpcContext may already have been changed when callback happens, it happens when the same thread is used to execute another RPC call.
     * So we should keep the reference of current RpcContext instance and restore it before callback being executed.
     * 
     * 用于存储相关的 RpcContext 对象。我们知道 RpcContext 是与线程绑定的, 而真正执行 AsyncRpcResult
     * 上添加的回调方法的线程可能先后处理过多个不同的 AsyncRpcResult, 所以我们需要传递并保存当前的 RpcContext
     */
    private RpcContext storedContext;
    private RpcContext storedServerContext;

    /**
     * 此次 RPC 调用关联的线程池
     */
    private Executor executor;

    /**
     * 此次 RPC 调用关联的 Invocation 对象
     */
    private Invocation invocation;

    /**
     * 这个 responseFuture 字段与前文提到的 DefaultFuture 有紧密的联系, 是 DefaultFuture 回调链上的
     * 一个 Future. 后面 AsyncRpcResult 之上添加的回调, 实际上都是添加到这个 Future 之上
     */
    private CompletableFuture<AppResponse> responseFuture;

    /**
     * 在 AsyncRpcResult 构造方法中, 除了接收发送请求返回的 CompletableFuture<AppResponse> 对象,
     * 还会将当前的 RpcContext 保存到 storedContext 和 storedServerContext 中
     */
    public AsyncRpcResult(CompletableFuture<AppResponse> future, Invocation invocation) {
        this.responseFuture = future;
        this.invocation = invocation;
        this.storedContext = RpcContext.getContext();
        this.storedServerContext = RpcContext.getServerContext();
    }

    /**
     * Notice the return type of {@link #getValue} is the actual type of the RPC method, not {@link AppResponse}
     *
     * @return
     */
    @Override
    public Object getValue() {
        return getAppResponse().getValue();
    }

    /**
     * CompletableFuture can only be completed once, so try to update the result of one completed CompletableFuture will
     * has no effect. To avoid this problem, we check the complete status of this future before update it's value.
     *
     * But notice that trying to give an uncompleted CompletableFuture a new specified value may face a race condition,
     * because the background thread watching the real result will also change the status of this CompletableFuture.
     * The result is you may lose the value you expected to set.
     *
     * @param value
     */
    @Override
    public void setValue(Object value) {
        try {
            if (responseFuture.isDone()) {
                responseFuture.get().setValue(value);
            } else {
                AppResponse appResponse = new AppResponse();
                appResponse.setValue(value);
                responseFuture.complete(appResponse);
            }
        } catch (Exception e) {
            // This should not happen in normal request process;
            logger.error("Got exception when trying to fetch the underlying result from AsyncRpcResult.");
            throw new RpcException(e);
        }
    }

    @Override
    public Throwable getException() {
        return getAppResponse().getException();
    }

    @Override
    public void setException(Throwable t) {
        try {
            if (responseFuture.isDone()) {
                responseFuture.get().setException(t);
            } else {
                AppResponse appResponse = new AppResponse();
                appResponse.setException(t);
                responseFuture.complete(appResponse);
            }
        } catch (Exception e) {
            // This should not happen in normal request process;
            logger.error("Got exception when trying to fetch the underlying result from AsyncRpcResult.");
            throw new RpcException(e);
        }
    }

    @Override
    public boolean hasException() {
        return getAppResponse().hasException();
    }

    public CompletableFuture<AppResponse> getResponseFuture() {
        return responseFuture;
    }

    public void setResponseFuture(CompletableFuture<AppResponse> responseFuture) {
        this.responseFuture = responseFuture;
    }

    public Result getAppResponse() {
        try {
            // 检测responseFuture是否已完成, 获取AppResponse
            if (responseFuture.isDone()) {
                return responseFuture.get();
            }
        } catch (Exception e) {
            // This should not happen in normal request process;
            logger.error("Got exception when trying to fetch the underlying result from AsyncRpcResult.");
            throw new RpcException(e);
        }

        // 根据调用方法的返回值, 生成默认值
        return createDefaultValue(invocation);
    }

    /**
     * This method will always return after a maximum 'timeout' waiting:
     * 1. if value returns before timeout, return normally.
     * 2. if no value returns after timeout, throw TimeoutException.
     *
     * @return
     * @throws InterruptedException
     * @throws ExecutionException
     */
    @Override
    public Result get() throws InterruptedException, ExecutionException {
        if (executor != null && executor instanceof ThreadlessExecutor) {
            ThreadlessExecutor threadlessExecutor = (ThreadlessExecutor) executor;
            threadlessExecutor.waitAndDrain();
        }
        return responseFuture.get();
    }

    @Override
    public Result get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
        if (executor != null && executor instanceof ThreadlessExecutor) {
            // 针对ThreadlessExecutor的特殊处理, 这里调用waitAndDrain()等待响应
            ThreadlessExecutor threadlessExecutor = (ThreadlessExecutor) executor;
            threadlessExecutor.waitAndDrain();
        }
        // 非ThreadlessExecutor线程池的场景中, 则直接调用Future(最底层是DefaultFuture)的get()方法阻塞
        return responseFuture.get(timeout, unit);
    }

    /**
     * AsyncRpcResult 对 Result 接口的实现, 例如, getValue() 方法、recreate() 方法、getAttachments() 方法等,
     * 都会先调用 getAppResponse() 方法从 responseFuture 中拿到 AppResponse 对象, 然后再调用其对应的方法.
     * 这里我们以 recreate() 方法为例, 简单分析一下. AsyncRpcResult 会对 FUTURE 特殊处理。如果服务接口定义的返回
     * 参数是 CompletableFuture, 则属于 FUTURE 模式, FUTURE 模式也属于 Dubbo 提供的一种异步调用方式, 只不过是
     * 服务端异步. FUTURE 模式下拿到的 CompletableFuture 对象其实是在 AbstractInvoker 中塞到 RpcContext 中的
     */
    @Override
    public Object recreate() throws Throwable {
        RpcInvocation rpcInvocation = (RpcInvocation) invocation;
        if (InvokeMode.FUTURE == rpcInvocation.getInvokeMode()) {
            return RpcContext.getContext().getFuture();
        }

        // 调用AppResponse.recreate()方法
        return getAppResponse().recreate();
    }

    /**
     * 通过 whenCompleteWithContext() 方法, 我们可以为 AsyncRpcResult 添加回调方法, 而这个回调方法会被
     * 包装一层并注册到 responseFuture 上. 这里的 beforeContext 首先会将当前线程的 RpcContext 记录到
     * tmpContext 中, 然后将构造函数中存储的 RpcContext 设置到当前线程中, 为后面的回调执行做准备.
     * 而 afterContext 则会恢复线程原有的 RpcContext. 这样, AsyncRpcResult 就可以处于不断地添加回调
     * 而不丢失 RpcContext 的状态. 总之, AsyncRpcResult 整个就是为异步请求设计的
     */
    public Result whenCompleteWithContext(BiConsumer<Result, Throwable> fn) {
        // 在responseFuture之上注册回调
        this.responseFuture = this.responseFuture.whenComplete((v, t) -> {
            beforeContext.accept(v, t);
            fn.accept(v, t);
            afterContext.accept(v, t);
        });
        return this;
    }

    @Override
    public <U> CompletableFuture<U> thenApply(Function<Result, ? extends U> fn) {
        return this.responseFuture.thenApply(fn);
    }

    @Override
    @Deprecated
    public Map<String, String> getAttachments() {
        return getAppResponse().getAttachments();
    }

    @Override
    public Map<String, Object> getObjectAttachments() {
        return getAppResponse().getObjectAttachments();
    }

    @Override
    public void setAttachments(Map<String, String> map) {
        getAppResponse().setAttachments(map);
    }

    @Override
    public void setObjectAttachments(Map<String, Object> map) {
        getAppResponse().setObjectAttachments(map);
    }

    @Deprecated
    @Override
    public void addAttachments(Map<String, String> map) {
        getAppResponse().addAttachments(map);
    }

    @Override
    public void addObjectAttachments(Map<String, Object> map) {
        getAppResponse().addObjectAttachments(map);
    }

    @Override
    public String getAttachment(String key) {
        return getAppResponse().getAttachment(key);
    }

    @Override
    public Object getObjectAttachment(String key) {
        return getAppResponse().getObjectAttachment(key);
    }

    @Override
    public String getAttachment(String key, String defaultValue) {
        return getAppResponse().getAttachment(key, defaultValue);
    }

    @Override
    public Object getObjectAttachment(String key, Object defaultValue) {
        return getAppResponse().getObjectAttachment(key, defaultValue);
    }

    @Override
    public void setAttachment(String key, String value) {
        setObjectAttachment(key, value);
    }

    @Override
    public void setAttachment(String key, Object value) {
        setObjectAttachment(key, value);
    }

    @Override
    public void setObjectAttachment(String key, Object value) {
        getAppResponse().setAttachment(key, value);
    }

    public Executor getExecutor() {
        return executor;
    }

    public void setExecutor(Executor executor) {
        this.executor = executor;
    }

    /**
     * tmp context to use when the thread switch to Dubbo thread.
     */
    private RpcContext tmpContext;

    private RpcContext tmpServerContext;
    private BiConsumer<Result, Throwable> beforeContext = (appResponse, t) -> {
        // 将当前线程的 RpcContext 记录到 tmpContext 中
        tmpContext = RpcContext.getContext();
        tmpServerContext = RpcContext.getServerContext();
        // 将构造函数中存储的 RpcContext 设置到当前线程中
        RpcContext.restoreContext(storedContext);
        RpcContext.restoreServerContext(storedServerContext);
    };

    private BiConsumer<Result, Throwable> afterContext = (appResponse, t) -> {
        // 将tmpContext中存储的RpcContext恢复到当前线程绑定的RpcContext
        RpcContext.restoreContext(tmpContext);
        RpcContext.restoreServerContext(tmpServerContext);
    };

    /**
     * Some utility methods used to quickly generate default AsyncRpcResult instance.
     */
    public static AsyncRpcResult newDefaultAsyncResult(AppResponse appResponse, Invocation invocation) {
        return new AsyncRpcResult(CompletableFuture.completedFuture(appResponse), invocation);
    }

    public static AsyncRpcResult newDefaultAsyncResult(Invocation invocation) {
        return newDefaultAsyncResult(null, null, invocation);
    }

    public static AsyncRpcResult newDefaultAsyncResult(Object value, Invocation invocation) {
        return newDefaultAsyncResult(value, null, invocation);
    }

    public static AsyncRpcResult newDefaultAsyncResult(Throwable t, Invocation invocation) {
        return newDefaultAsyncResult(null, t, invocation);
    }

    public static AsyncRpcResult newDefaultAsyncResult(Object value, Throwable t, Invocation invocation) {
        CompletableFuture<AppResponse> future = new CompletableFuture<>();
        AppResponse result = new AppResponse();
        if (t != null) {
            result.setException(t);
        } else {
            result.setValue(value);
        }
        future.complete(result);
        return new AsyncRpcResult(future, invocation);
    }

    private static Result createDefaultValue(Invocation invocation) {
        ConsumerMethodModel method = (ConsumerMethodModel) invocation.get(Constants.METHOD_MODEL);
        return method != null ? new AppResponse(defaultReturn(method.getReturnClass())) : new AppResponse();
    }
}

