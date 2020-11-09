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
package org.apache.dubbo.remoting;

import org.apache.dubbo.common.extension.Adaptive;
import org.apache.dubbo.common.extension.SPI;
import org.apache.dubbo.remoting.buffer.ChannelBuffer;

import java.io.IOException;

/**
 * 这里需要关注的是 Codec2 接口被 @SPI 接口修饰了, 表示该接口是一个扩展接口, 同时其 encode() 方法和 decode()
 * 方法都被 @Adaptive 注解修饰, 也就会生成适配器类, 其中会根据 URL 中的 codec 值确定具体的扩展实现类
 */
@SPI
public interface Codec2 {

    @Adaptive({Constants.CODEC_KEY})
    void encode(Channel channel, ChannelBuffer buffer, Object message) throws IOException;

    @Adaptive({Constants.CODEC_KEY})
    Object decode(Channel channel, ChannelBuffer buffer) throws IOException;


    /**
     * DecodeResult 这个枚举是在处理 TCP 传输时粘包和拆包使用的, 当前能读取到的数据不足以构成一个消息时,
     * 就会使用 NEED_MORE_INPUT 这个枚举
     */
    enum DecodeResult {
        NEED_MORE_INPUT, SKIP_SOME_INPUT
    }

}

