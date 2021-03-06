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
package org.apache.dubbo.remoting.transport.dispatcher.all;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.Dispatcher;

/**
 * AllDispatcher 创建的是 AllChannelHandler 对象, 它会将所有网络事件以及消息交给关联的线程池进行处理.
 * AllChannelHandler覆盖了 WrappedChannelHandler 中除了 sent() 方法之外的其他网络事件处理方法,
 * 将调用其底层的 ChannelHandler 的逻辑放到关联的线程池中执行
 * 
 * 如果你对线程模型不清楚, 可以依次回顾一下 Exchangers、HeaderExchanger、Transporters 三个门面类的 bind() 方法,
 * 以及 Dispatcher 各实现提供的线程模型, 搞清楚各个 ChannelHandler 是由哪个线程执行的, 这些知识点在前面课时都介绍过了,
 * 不再重复, 这里我们就直接以 AllDispatcher 实现为例给出结论
 *
 * 1) IO 线程内执行的 ChannelHandler 实现依次有: InternalEncoder、InternalDecoder(两者底层都是调用 DubboCodec)、
 * IdleStateHandler、MultiMessageHandler、HeartbeatHandler 和 NettyServerHandler
 *
 * 2) 在非 IO 线程内执行的 ChannelHandler 实现依次有: DecodeHandler、HeaderExchangeHandler 和 DubboProtocol$requestHandler
 */
public class AllDispatcher implements Dispatcher {

    public static final String NAME = "all";

    @Override
    public ChannelHandler dispatch(ChannelHandler handler, URL url) {
        return new AllChannelHandler(handler, url);
    }

}
