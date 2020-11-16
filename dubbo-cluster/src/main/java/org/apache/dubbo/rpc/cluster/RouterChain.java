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
package org.apache.dubbo.rpc.cluster;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.extension.ExtensionLoader;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Router chain
 */
public class RouterChain<T> {

    /**
     * 当前 RouterChain 对象要过滤的 Invoker 集合. 我们可以看到, 在 StaticDirectory 中是通过 RouterChain.setInvokers() 方法进行设置的
     */
    private List<Invoker<T>> invokers = Collections.emptyList();


    /**
     * containing all routers, reconstruct every time 'route://' urls change.
     * 当前 RouterChain 中真正要使用的 Router 集合, 其中不仅包括了上面 builtinRouters 集合
     * 中全部的 Router 对象, 还包括通过 addRouters() 方法添加的 Router 对象
     */
    private volatile List<Router> routers = Collections.emptyList();

    // Fixed router instances: ConfigConditionRouter, TagRouter, e.g., the rule for each instance may change but the
    // instance will never delete or recreate.
    /**
     * 当前 RouterChain 激活的内置 Router 集合
     */
    private List<Router> builtinRouters = Collections.emptyList();

    public static <T> RouterChain<T> buildChain(URL url) {
        return new RouterChain<>(url);
    }

    /**
     * 在 RouterChain 的构造函数中, 会在传入的 URL 参数中查找 router 参数值, 并根据该值获取确定激活的 RouterFactory,
     * 之后通过 Dubbo SPI 机制加载这些激活的 RouterFactory 对象, 由 RouterFactory 创建当前激活的内置 Router 实例
     */
    private RouterChain(URL url) {
        // 通过ExtensionLoader加载激活的RouterFactory
        List<RouterFactory> extensionFactories = ExtensionLoader.getExtensionLoader(RouterFactory.class)
                .getActivateExtension(url, "router");

        // 遍历所有RouterFactory, 调用其getRouter()方法创建相应的Router对象
        List<Router> routers = extensionFactories.stream()
                .map(factory -> factory.getRouter(url))
                .collect(Collectors.toList());

        // 初始化buildinRouters字段以及routers字段
        initWithRouters(routers);
    }

    /**
     * the resident routers must being initialized before address notification.
     * FIXME: this method should not be public
     */
    public void initWithRouters(List<Router> builtinRouters) {
        this.builtinRouters = builtinRouters;
        this.routers = new ArrayList<>(builtinRouters);
        // 这里会对routers集合进行排序
        this.sort();
    }

    /**
     * If we use route:// protocol in version before 2.7.0, each URL will generate a Router instance, so we should
     * keep the routers up to date, that is, each time router URLs changes, we should update the routers list, only
     * keep the builtinRouters which are available all the time and the latest notified routers which are generated
     * from URLs.
     *
     * @param routers routers from 'router://' rules in 2.6.x or before.
     *                
     * 完成内置 Router 的初始化之后, 在 Directory 实现中还可以通过 addRouter() 方法添加新的 Router 实例到 routers 字段中
     */
    public void addRouters(List<Router> routers) {
        List<Router> newRouters = new ArrayList<>();
        // 添加builtinRouters集合
        newRouters.addAll(builtinRouters);
        // 添加传入的Router集合
        newRouters.addAll(routers);
        // 重新排序
        CollectionUtils.sort(newRouters);
        this.routers = newRouters;
    }

    private void sort() {
        Collections.sort(routers);
    }

    /**
     * RouterChain.route() 方法会遍历 routers 字段, 逐个调用 Router 对象的 route() 方法, 对 invokers 集合进行过滤
     */
    public List<Invoker<T>> route(URL url, Invocation invocation) {
        List<Invoker<T>> finalInvokers = invokers;
        // 遍历全部的Router对象
        for (Router router : routers) {
            finalInvokers = router.route(finalInvokers, url, invocation);
        }
        return finalInvokers;
    }

    /**
     * Notify router chain of the initial addresses from registry at the first time.
     * Notify whenever addresses in registry change.
     */
    public void setInvokers(List<Invoker<T>> invokers) {
        this.invokers = (invokers == null ? Collections.emptyList() : invokers);
        routers.forEach(router -> router.notify(this.invokers));
    }
}
