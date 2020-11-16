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
package org.apache.dubbo.rpc.cluster.router.condition;

import org.apache.dubbo.common.URL;
import org.apache.dubbo.common.logger.Logger;
import org.apache.dubbo.common.logger.LoggerFactory;
import org.apache.dubbo.common.utils.CollectionUtils;
import org.apache.dubbo.common.utils.NetUtils;
import org.apache.dubbo.common.utils.StringUtils;
import org.apache.dubbo.common.utils.UrlUtils;
import org.apache.dubbo.rpc.Invocation;
import org.apache.dubbo.rpc.Invoker;
import org.apache.dubbo.rpc.RpcException;
import org.apache.dubbo.rpc.cluster.router.AbstractRouter;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.dubbo.common.constants.CommonConstants.ENABLED_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.HOST_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.METHODS_KEY;
import static org.apache.dubbo.common.constants.CommonConstants.METHOD_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.ADDRESS_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.FORCE_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.PRIORITY_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.RULE_KEY;
import static org.apache.dubbo.rpc.cluster.Constants.RUNTIME_KEY;

/**
 * ConditionRouter 是基于条件表达式的路由实现类, 下面就是一条基于条件表达式的路由规则
 * host = 192.168.0.100 => host = 192.168.0.150
 * 
 * 在上述规则中, =>之前的为 Consumer 匹配的条件, 该条件中的所有参数会与 Consumer 的 URL 进行对比,
 * 当 Consumer 满足匹配条件时, 会对该 Consumer 的此次调用执行 => 后面的过滤规则
 *
 * => 之后为 Provider 地址列表的过滤条件, 该条件中的所有参数会和 Provider 的 URL 进行对比, Consumer 最终只拿到过滤后的地址列表
 *
 * 1) 如果 Consumer 匹配条件为空, 表示 => 之后的过滤条件对所有 Consumer 生效, 例如: => host != 192.168.0.150,
 *    含义是所有 Consumer 都不能请求 192.168.0.150 这个 Provider 节点
 *
 * 2) 如果 Provider 过滤条件为空, 表示禁止访问所有 Provider, 例如：host = 192.168.0.100 =>,
 *    含义是 192.168.0.100 这个 Consumer 不能访问任何 Provider 节点
 */
public class ConditionRouter extends AbstractRouter {
    public static final String NAME = "condition";

    private static final Logger logger = LoggerFactory.getLogger(ConditionRouter.class);

    /**
     * 用于切分路由规则的正则表达式
     */
    protected static final Pattern ROUTE_PATTERN = Pattern.compile("([&!=,]*)\\s*([^&!=,\\s]+)");

    /**
     * Consumer 匹配的条件集合, 通过解析条件表达式 rule 的 => 之前半部分, 可以得到该集合中的内容
     * 
     * whenCondition 和 thenCondition 两个集合中, Key 是条件表达式中指定的参数名称
     * (例如 host = 192.168.0.150 这个表达式中的 host). ConditionRouter 支持三类参数
     *
     * 1) 服务调用信息, 例如, method、argument 等
     *
     * 2) URL 本身的字段, 例如, protocol、host、port 等
     *
     * 3) URL 上的所有参数, 例如, application 等
     * 
     * Value 是 MatchPair 对象, 包含两个 Set 类型的集合—— matches 和 mismatches
     */
    protected Map<String, MatchPair> whenCondition;

    /**
     * Provider 匹配的条件集合, 通过解析条件表达式 rule 的 => 之后半部分, 可以得到该集合中的内容
     */
    protected Map<String, MatchPair> thenCondition;

    private boolean enabled;

    public ConditionRouter(String rule, boolean force, boolean enabled) {
        this.force = force;
        this.enabled = enabled;
        this.init(rule);
    }

    /**
     * 在 ConditionRouter 的构造方法中, 会根据 URL 中携带的相应参数初始化 priority、force、enable 等字段,
     * 然后从 URL 的 rule 参数中获取路由规则进行解析, 具体的解析逻辑是在 init() 方法中实现的
     */
    public ConditionRouter(URL url) {
        this.url = url;
        this.priority = url.getParameter(PRIORITY_KEY, 0);
        this.force = url.getParameter(FORCE_KEY, false);
        this.enabled = url.getParameter(ENABLED_KEY, true);
        init(url.getParameterAndDecoded(RULE_KEY));
    }

    public void init(String rule) {
        try {
            if (rule == null || rule.trim().length() == 0) {
                throw new IllegalArgumentException("Illegal route rule!");
            }
            // 将路由规则中的"consumer."和"provider."字符串清理掉
            rule = rule.replace("consumer.", "").replace("provider.", "");
            // 按照"=>"字符串进行分割, 得到whenRule和thenRule两部分
            int i = rule.indexOf("=>");
            String whenRule = i < 0 ? null : rule.substring(0, i).trim();
            String thenRule = i < 0 ? rule.trim() : rule.substring(i + 2).trim();
            // 解析whenRule和thenRule, 得到whenCondition和thenCondition两个条件集合
            Map<String, MatchPair> when = StringUtils.isBlank(whenRule) || "true".equals(whenRule) ? new HashMap<String, MatchPair>() : parseRule(whenRule);
            Map<String, MatchPair> then = StringUtils.isBlank(thenRule) || "false".equals(thenRule) ? null : parseRule(thenRule);
            // NOTE: It should be determined on the business level whether the `When condition` can be empty or not.
            this.whenCondition = when;
            this.thenCondition = then;
        } catch (ParseException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    /**
     * 了解了每个 MatchPair 的匹配流程之后, 我们来看parseRule() 方法是如何解析一条完整的条件表达式, 生成对应 MatchPair 的
     *
     * host = 2.2.2.2,1.1.1.1,3.3.3.3 & method !=get => host = 1.2.3.4
     * 
     * 我们先来看 => 之前的 Consumer 匹配规则的处理. 
     *
     * 分组 1 中, separator 为空字符串, content 为 host 字符串. 此时会进入上面示例代码展示的
     * parseRule() 方法中(1)处的分支, 创建 MatchPair 对象, 并以 host 为 Key 记录到 condition 集合中.
     *
     * 分组 2 中, separator 为 "=" 空字符串, content 为 "2.2.2.2" 字符串. 处理该分组时,
     * 会进入 parseRule() 方法中(2) 处的分支, 在 MatchPair 的 matches 集合中添加 "2.2.2.2" 字符串.
     *
     * 分组 3 中, separator 为 "," 字符串, content 为 "3.3.3.3" 字符串. 处理该分组时,
     * 会进入 parseRule() 方法中(3)处的分支, 继续向 MatchPair 的 matches 集合中添加 "3.3.3.3" 字符串.
     *
     * 分组 4 中, separator 为 "&" 字符串, content 为 "method" 字符串. 处理该分组时, 会进入
     * parseRule() 方法中(4)处的分支, 创建新的 MatchPair 对象, 并以 method 为 Key 记录到 condition 集合中.
     *
     * 分组 5 中, separator 为 "!=" 字符串, content 为 "get" 字符串. 处理该分组时, 会进入 parseRule()
     * 方法中(5)处的分支, 向步骤 4 新建的 MatchPair 对象中的 mismatches 集合添加 "get" 字符串.
     *
     * https://s0.lgstatic.com/i/image/M00/6D/8B/Ciqc1F-uM-OABiPoAADt1lcbl7U975.png
     *
     * https://s0.lgstatic.com/i/image/M00/6D/97/CgqCHl-uM-6AXnrOAAB6hJLFL50095.png
     */
    private static Map<String, MatchPair> parseRule(String rule)
            throws ParseException {
        Map<String, MatchPair> condition = new HashMap<String, MatchPair>();
        if (StringUtils.isBlank(rule)) {
            return condition;
        }
        // Key-Value pair, stores both match and mismatch conditions
        MatchPair pair = null;
        // Multiple values
        Set<String> values = null;
        // 首先, 按照ROUTE_PATTERN指定的正则表达式匹配整个条件表达式
        final Matcher matcher = ROUTE_PATTERN.matcher(rule);
        // 遍历匹配的结果
        while (matcher.find()) { // Try to match one by one
            // 每个匹配结果有两部分(分组), 第一部分是分隔符, 第二部分是内容
            String separator = matcher.group(1);
            String content = matcher.group(2);
            // Start part of the condition expression.
            // ---(1) 没有分隔符, content即为参数名称
            if (StringUtils.isEmpty(separator)) {
                pair = new MatchPair();
                // 初始化MatchPair对象, 并将其与对应的Key(即content)记录到condition集合中
                condition.put(content, pair);
            }
            // The KV part of the condition expression
            else if ("&".equals(separator)) {
                // &分隔符表示多个表达式, 会创建多个MatchPair对象
                if (condition.get(content) == null) {
                    pair = new MatchPair();
                    condition.put(content, pair);
                } else {
                    pair = condition.get(content);
                }
            }
            // The Value in the KV part.
            // =以及!=两个分隔符表示KV的分界线
            else if ("=".equals(separator)) {
                if (pair == null) {
                    throw new ParseException("Illegal route rule \""
                            + rule + "\", The error char '" + separator
                            + "' at index " + matcher.start() + " before \""
                            + content + "\".", matcher.start());
                }

                values = pair.matches;
                values.add(content);
            }
            // The Value in the KV part.
            else if ("!=".equals(separator)) {
                if (pair == null) {
                    throw new ParseException("Illegal route rule \""
                            + rule + "\", The error char '" + separator
                            + "' at index " + matcher.start() + " before \""
                            + content + "\".", matcher.start());
                }

                values = pair.mismatches;
                values.add(content);
            }
            // The Value in the KV part, if Value have more than one items.
            // 逗号分隔符表示有多个Value值
            else if (",".equals(separator)) { // Should be separated by ','
                if (values == null || values.isEmpty()) {
                    throw new ParseException("Illegal route rule \""
                            + rule + "\", The error char '" + separator
                            + "' at index " + matcher.start() + " before \""
                            + content + "\".", matcher.start());
                }
                values.add(content);
            } else {
                throw new ParseException("Illegal route rule \"" + rule
                        + "\", The error char '" + separator + "' at index "
                        + matcher.start() + " before \"" + content + "\".", matcher.start());
            }
        }
        return condition;
    }

    /**
     * 首先会尝试前面创建的 whenCondition 集合, 判断此次发起调用的 Consumer 是否符合表达式中 => 之前的 Consumer 过滤条件,
     * 若不符合, 直接返回整个 invokers 集合. 若符合, 则通过 thenCondition 集合对 invokers 集合进行过滤, 得到符合 Provider
     * 过滤条件的 Invoker 集合, 然后返回给上层调用方
     */
    @Override
    public <T> List<Invoker<T>> route(List<Invoker<T>> invokers, URL url, Invocation invocation)
            throws RpcException {
        if (!enabled) {
            return invokers;
        }

        if (CollectionUtils.isEmpty(invokers)) {
            return invokers;
        }
        try {
            // 匹配发起请求的Consumer是否符合表达式中=>之前的过滤条件
            if (!matchWhen(url, invocation)) {
                return invokers;
            }
            List<Invoker<T>> result = new ArrayList<Invoker<T>>();
            // 判断=>之后是否存在Provider过滤条件, 若不存在则直接返回空集合, 表示无Provider可用
            if (thenCondition == null) {
                logger.warn("The current consumer in the service blacklist. consumer: " + NetUtils.getLocalHost() + ", service: " + url.getServiceKey());
                return result;
            }
            // 逐个判断Invoker是否符合表达式中=>之后的过滤条件
            for (Invoker<T> invoker : invokers) {
                // 记录符合条件的Invoker
                if (matchThen(invoker.getUrl(), url)) {
                    result.add(invoker);
                }
            }
            if (!result.isEmpty()) {
                return result;
            }
            // 在无Invoker符合条件时, 根据force决定是返回空集合还是返回全部Invoker
            else if (force) {
                logger.warn("The route result is empty and force execute. consumer: " + NetUtils.getLocalHost() + ", service: " + url.getServiceKey() + ", router: " + url.getParameterAndDecoded(RULE_KEY));
                return result;
            }
        } catch (Throwable t) {
            logger.error("Failed to execute condition router rule: " + getUrl() + ", invokers: " + invokers + ", cause: " + t.getMessage(), t);
        }
        return invokers;
    }

    @Override
    public boolean isRuntime() {
        // We always return true for previously defined Router, that is, old Router doesn't support cache anymore.
//        return true;
        return this.url.getParameter(RUNTIME_KEY, false);
    }

    @Override
    public URL getUrl() {
        return url;
    }

    boolean matchWhen(URL url, Invocation invocation) {
        return CollectionUtils.isEmptyMap(whenCondition) || matchCondition(whenCondition, url, null, invocation);
    }

    private boolean matchThen(URL url, URL param) {
        return CollectionUtils.isNotEmptyMap(thenCondition) && matchCondition(thenCondition, url, param, null);
    }

    private boolean matchCondition(Map<String, MatchPair> condition, URL url, URL param, Invocation invocation) {
        Map<String, String> sample = url.toMap();
        boolean result = false;
        for (Map.Entry<String, MatchPair> matchPair : condition.entrySet()) {
            String key = matchPair.getKey();
            String sampleValue;
            //get real invoked method name from invocation
            if (invocation != null && (METHOD_KEY.equals(key) || METHODS_KEY.equals(key))) {
                sampleValue = invocation.getMethodName();
            } else if (ADDRESS_KEY.equals(key)) {
                sampleValue = url.getAddress();
            } else if (HOST_KEY.equals(key)) {
                sampleValue = url.getHost();
            } else {
                sampleValue = sample.get(key);
                if (sampleValue == null) {
                    sampleValue = sample.get(key);
                }
            }
            if (sampleValue != null) {
                if (!matchPair.getValue().isMatch(sampleValue, param)) {
                    return false;
                } else {
                    result = true;
                }
            } else {
                //not pass the condition
                if (!matchPair.getValue().matches.isEmpty()) {
                    return false;
                } else {
                    result = true;
                }
            }
        }
        return result;
    }

    protected static final class MatchPair {
        final Set<String> matches = new HashSet<String>();
        final Set<String> mismatches = new HashSet<String>();

        /**
         * 在使用 MatchPair 进行过滤的时候, 会按照下面四条规则执行
         *
         * 1) 当 mismatches 集合为空的时候, 会逐个遍历 matches 集合中的匹配条件, 匹配成功任意一条即会返回 true.
         *    这里具体的匹配逻辑以及后续 mismatches 集合中条件的匹配逻辑, 都是在 UrlUtils.isMatchGlobPattern()
         *    方法中实现, 其中完成了如下操作：如果匹配条件以 "$" 符号开头, 则从 URL 中获取相应的参数值进行匹配.
         *    当遇到 "" 通配符的时候, 会处理""通配符在匹配条件开头、中间以及末尾三种情况
         *
         * 2) 当 matches 集合为空的时候, 会逐个遍历 mismatches 集合中的匹配条件, 匹配成功任意一条即会返回 false
         *
         * 3) 当 matches 集合和 mismatches 集合同时不为空时, 会优先匹配 mismatches 集合中的条件, 成功匹配任意一条规则,
         *    就会返回 false；若 mismatches 中的条件全部匹配失败, 才会开始匹配 matches 集合, 成功匹配任意一条规则, 就会返回 true
         *
         * 4) 当上述三个步骤都没有成功匹配时, 直接返回 false
         */
        private boolean isMatch(String value, URL param) {
            if (!matches.isEmpty() && mismatches.isEmpty()) {
                for (String match : matches) {
                    if (UrlUtils.isMatchGlobPattern(match, value, param)) {
                        return true;
                    }
                }
                return false;
            }

            if (!mismatches.isEmpty() && matches.isEmpty()) {
                for (String mismatch : mismatches) {
                    if (UrlUtils.isMatchGlobPattern(mismatch, value, param)) {
                        return false;
                    }
                }
                return true;
            }

            if (!matches.isEmpty() && !mismatches.isEmpty()) {
                //when both mismatches and matches contain the same value, then using mismatches first
                for (String mismatch : mismatches) {
                    if (UrlUtils.isMatchGlobPattern(mismatch, value, param)) {
                        return false;
                    }
                }
                for (String match : matches) {
                    if (UrlUtils.isMatchGlobPattern(match, value, param)) {
                        return true;
                    }
                }
                return false;
            }
            return false;
        }
    }
}
