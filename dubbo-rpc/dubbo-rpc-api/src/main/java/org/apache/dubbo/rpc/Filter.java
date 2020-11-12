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

import org.apache.dubbo.common.extension.SPI;

/**
 * Extension for intercepting the invocation for both service provider and consumer, furthermore, most of
 * functions in dubbo are implemented base on the same mechanism. Since every time when remote method is
 * invoked, the filter extensions will be executed too, the corresponding penalty should be considered before
 * more filters are added.
 * <pre>
 *  They way filter work from sequence point of view is
 *    <b>
 *    ...code before filter ...
 *          invoker.invoke(invocation) //filter work in a filter implementation class
 *          ...code after filter ...
 *    </b>
 *    Caching is implemented in dubbo using filter approach. If cache is configured for invocation then before
 *    remote call configured caching type's (e.g. Thread Local, JCache etc) implementation invoke method gets called.
 * </pre>
 * Filter. (SPI, Singleton, ThreadSafe)
 *
 * @see org.apache.dubbo.rpc.filter.GenericFilter
 * @see org.apache.dubbo.rpc.filter.EchoFilter
 * @see org.apache.dubbo.rpc.filter.TokenFilter
 * @see org.apache.dubbo.rpc.filter.TpsLimitFilter
 */
@SPI
public interface Filter {
    /**
     * Make sure call invoker.invoke() in your implementation.
     * 在 Dubbo 的 Filter 接口中, 定义了一个 invoke() 方法将请求传递给后续的 Invoker 进行处理
     * (后续的这个 Invoker 对象可能是一个 Filter 封装而成的)
     */
    Result invoke(Invoker<?> invoker, Invocation invocation) throws RpcException;

    /**
     * 在 Filter 接口内部还定义了一个 Listener 接口, 有一些 Filter 实现会同时实现这个内部 Listener 接口,
     * 当 invoke() 方法执行正常结束时, 会调用该 Listener 的 onResponse() 方法进行通知；当 invoke() 方法
     * 执行出现异常时, 会调用该 Listener 的 onError() 方法进行通知
     */
    interface Listener {

        void onResponse(Result appResponse, Invoker<?> invoker, Invocation invocation);

        void onError(Throwable t, Invoker<?> invoker, Invocation invocation);
    }

}