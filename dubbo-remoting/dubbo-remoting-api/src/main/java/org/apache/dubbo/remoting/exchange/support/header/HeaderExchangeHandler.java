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
package org.apache.dubbo.remoting.exchange.support.header;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.remoting.Channel;
import org.apache.dubbo.remoting.ChannelHandler;
import org.apache.dubbo.remoting.Constants;
import org.apache.dubbo.remoting.ExecutionException;
import org.apache.dubbo.remoting.RemotingException;
import org.apache.dubbo.remoting.exchange.ExchangeChannel;
import org.apache.dubbo.remoting.exchange.ExchangeHandler;
import org.apache.dubbo.remoting.exchange.Request;
import org.apache.dubbo.remoting.exchange.Response;
import org.apache.dubbo.remoting.exchange.support.DefaultFuture;
import org.apache.dubbo.remoting.transport.ChannelHandlerDelegate;

import java.net.InetSocketAddress;
import java.util.concurrent.CompletionStage;

import static org.apache.dubbo.common.constants.CommonConstants.READONLY_EVENT;


/**
 * HeaderExchangeHandler 是 ExchangeHandler 的装饰器, 其中维护了一个 ExchangeHandler 对象,
 * ExchangeHandler 接口是 Exchange 层与上层交互的接口之一, 上层调用方可以实现该接口完成自身的功能.
 * 然后再由 HeaderExchangeHandler 修饰, 具备 Exchange 层处理 Request-Response 的能力.
 * 最后再由 Transport ChannelHandler 修饰, 具备 Transport 层的能力
 *
 * HeaderExchangeHandler 作为一个装饰器, 其 connected()、disconnected()、sent()、received()、
 * caught() 方法最终都会转发给上层提供的 ExchangeHandler 进行处理. 这里我们需要聚焦的是 
 * HeaderExchangeHandler 本身对 Request 和 Response 的处理逻辑
 */
public class HeaderExchangeHandler implements ChannelHandlerDelegate {

    protected static final Logger logger = LoggerFactory.getLogger(HeaderExchangeHandler.class);

    private final ExchangeHandler handler;

    public HeaderExchangeHandler(ExchangeHandler handler) {
        if (handler == null) {
            throw new IllegalArgumentException("handler == null");
        }
        this.handler = handler;
    }

    static void handleResponse(Channel channel, Response response) throws RemotingException {
        if (response != null && !response.isHeartbeat()) {
            DefaultFuture.received(channel, response);
        }
    }

    private static boolean isClientSide(Channel channel) {
        InetSocketAddress address = channel.getRemoteAddress();
        URL url = channel.getUrl();
        return url.getPort() == address.getPort() &&
                NetUtils.filterLocalHost(url.getIp())
                        .equals(NetUtils.filterLocalHost(address.getAddress().getHostAddress()));
    }

    void handlerEvent(Channel channel, Request req) throws RemotingException {
        if (req.getData() != null && req.getData().equals(READONLY_EVENT)) {
            channel.setAttribute(Constants.CHANNEL_ATTRIBUTE_READONLY_KEY, Boolean.TRUE);
        }
    }

    void handleRequest(final ExchangeChannel channel, Request req) throws RemotingException {
        Response res = new Response(req.getId(), req.getVersion());
        // 请求解码失败
        if (req.isBroken()) {
            Object data = req.getData();

            String msg;
            if (data == null) {
                msg = null;
            } else if (data instanceof Throwable) {
                msg = StringUtils.toString((Throwable) data);
            } else {
                msg = data.toString();
            }
            // 设置异常信息和响应码
            res.setErrorMessage("Fail to decode request due to: " + msg);
            res.setStatus(Response.BAD_REQUEST);

            // 将异常响应返回给对端
            channel.send(res);
            return;
        }
        // find handler by message class.
        Object msg = req.getData();
        try {
            // 交给上层实现的ExchangeHandler进行处理
            CompletionStage<Object> future = handler.reply(channel, msg);
            // 处理结束后的回调
            future.whenComplete((appResult, t) -> {
                try {
                    // 返回正常响应
                    if (t == null) {
                        res.setStatus(Response.OK);
                        res.setResult(appResult);
                    } else {
                        // 处理过程发生异常, 设置异常信息和错误码
                        res.setStatus(Response.SERVICE_ERROR);
                        res.setErrorMessage(StringUtils.toString(t));
                    }
                    // 发送响应
                    channel.send(res);
                } catch (RemotingException e) {
                    logger.warn("Send result to consumer failed, channel is " + channel + ", msg is " + e);
                }
            });
        } catch (Throwable e) {
            res.setStatus(Response.SERVICE_ERROR);
            res.setErrorMessage(StringUtils.toString(e));
            channel.send(res);
        }
    }

    /**
     * 会为 Dubbo Channel 创建相应的 HeaderExchangeChannel, 并将两者绑定, 然后通知上层 ExchangeHandler 处理 connect 事件
     */
    @Override
    public void connected(Channel channel) throws RemotingException {
        ExchangeChannel exchangeChannel = HeaderExchangeChannel.getOrAddChannel(channel);
        handler.connected(exchangeChannel);
    }

    /**
     * 首先通知上层 ExchangeHandler 进行处理, 之后在 DefaultFuture.closeChannel() 通知 DefaultFuture
     * 连接断开(其实就是创建并传递一个 Response, 该 Response 的状态码为 CHANNEL_INACTIVE), 这样就不会
     * 继续阻塞业务线程了, 最后再将 HeaderExchangeChannel 与底层的 Dubbo Channel 解绑
     */
    @Override
    public void disconnected(Channel channel) throws RemotingException {
        ExchangeChannel exchangeChannel = HeaderExchangeChannel.getOrAddChannel(channel);
        try {
            handler.disconnected(exchangeChannel);
        } finally {
            DefaultFuture.closeChannel(channel);
            HeaderExchangeChannel.removeChannel(channel);
        }
    }

    /**
     * 该方法会通知上层 ExchangeHandler 实现的 sent() 方法, 同时还会针对 Request
     * 请求调用 DefaultFuture.sent() 方法记录请求的具体发送时间
     */
    @Override
    public void sent(Channel channel, Object message) throws RemotingException {
        Throwable exception = null;
        try {
            ExchangeChannel exchangeChannel = HeaderExchangeChannel.getOrAddChannel(channel);
            handler.sent(exchangeChannel, message);
        } catch (Throwable t) {
            exception = t;
            HeaderExchangeChannel.removeChannelIfDisconnected(channel);
        }
        if (message instanceof Request) {
            Request request = (Request) message;
            DefaultFuture.sent(channel, request);
        }
        if (exception != null) {
            if (exception instanceof RuntimeException) {
                throw (RuntimeException) exception;
            } else if (exception instanceof RemotingException) {
                throw (RemotingException) exception;
            } else {
                throw new RemotingException(channel.getLocalAddress(), channel.getRemoteAddress(),
                        exception.getMessage(), exception);
            }
        }
    }

    /**
     * 当响应传递到 HeaderExchangeHandler 的时候, 会通过调用 handleResponse() 方法进行处理,
     * 其中调用了 DefaultFuture.received() 方法, 该方法会找到响应关联的 DefaultFuture 对象
     * (根据请求 ID 从 FUTURES 集合查找)并调用 doReceived() 方法, 将 DefaultFuture 设置为完成状态
     */
    @Override
    public void received(Channel channel, Object message) throws RemotingException {
        final ExchangeChannel exchangeChannel = HeaderExchangeChannel.getOrAddChannel(channel);
        if (message instanceof Request) {
            // handle request.
            Request request = (Request) message;
            /**
             * 只读请求会由handlerEvent() 方法进行处理, 它会在 Channel 上设置 channel.readonly 标志, 后续介绍的上层调用中会读取该值
             */
            if (request.isEvent()) {
                handlerEvent(channel, request);
            } else {
                /**
                 * 双向请求由handleRequest() 方法进行处理, 会先对解码失败的请求进行处理, 返回异常响应；
                 * 然后将正常解码的请求交给上层实现的 ExchangeHandler 进行处理, 并添加回调.
                 * 上层 ExchangeHandler 处理完请求后, 会触发回调, 根据处理结果填充响应结果和响应码, 并向对端发送
                 */
                if (request.isTwoWay()) {
                    handleRequest(exchangeChannel, request);
                } else {
                    /**
                     * 单向请求直接委托给上层 ExchangeHandler 实现的 received() 方法进行处理,
                     * 由于不需要响应, HeaderExchangeHandler 不会关注处理结果
                     */
                    handler.received(exchangeChannel, request.getData());
                }
            }
        } else if (message instanceof Response) {
            // 对于 Response 的处理, HeaderExchangeHandler 会通过handleResponse()
            // 方法将关联的 DefaultFuture 设置为完成状态(或是异常完成状态)
            handleResponse(channel, (Response) message);
        } else if (message instanceof String) {
            // 对于 String 类型的消息, HeaderExchangeHandler 会根据当前服务的角色进行分类, 具体与 Dubbo 对 telnet 的支持相关
            if (isClientSide(channel)) {
                Exception e = new Exception("Dubbo client can not supported string message: " + message + " in channel: " + channel + ", url: " + channel.getUrl());
                logger.error(e.getMessage(), e);
            } else {
                String echo = handler.telnet(channel, (String) message);
                if (echo != null && echo.length() > 0) {
                    channel.send(echo);
                }
            }
        } else {
            handler.received(exchangeChannel, message);
        }
    }

    @Override
    public void caught(Channel channel, Throwable exception) throws RemotingException {
        if (exception instanceof ExecutionException) {
            ExecutionException e = (ExecutionException) exception;
            Object msg = e.getRequest();
            if (msg instanceof Request) {
                Request req = (Request) msg;
                if (req.isTwoWay() && !req.isHeartbeat()) {
                    Response res = new Response(req.getId(), req.getVersion());
                    res.setStatus(Response.SERVER_ERROR);
                    res.setErrorMessage(StringUtils.toString(e));
                    channel.send(res);
                    return;
                }
            }
        }
        ExchangeChannel exchangeChannel = HeaderExchangeChannel.getOrAddChannel(channel);
        try {
            handler.caught(exchangeChannel, exception);
        } finally {
            HeaderExchangeChannel.removeChannelIfDisconnected(channel);
        }
    }

    @Override
    public ChannelHandler getHandler() {
        if (handler instanceof ChannelHandlerDelegate) {
            return ((ChannelHandlerDelegate) handler).getHandler();
        } else {
            return handler;
        }
    }
}
