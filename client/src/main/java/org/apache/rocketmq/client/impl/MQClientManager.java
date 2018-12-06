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
package org.apache.rocketmq.client.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.rocketmq.client.ClientConfig;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.remoting.RPCHook;

//mq client 管理器 这个对象下面管理了 所有的 client 实例对象
public class MQClientManager {
    private final static InternalLogger log = ClientLogger.getLog();
    //单例模式
    private static MQClientManager instance = new MQClientManager();
    //工厂 id 生成器
    private AtomicInteger factoryIndexGenerator = new AtomicInteger();
    //将 client id 与 实例 对应起来  这个client 管理者中包含了所有的 client实例 每个client实例与一个client对象对应 那么 里面的队列是 什么意思
    // ? 或者说 多个 producer 都可以是 一个client ???  producer 具备client的职能然后根据特定规则从client中获取一个producer 处理实际逻辑
    private ConcurrentMap<String/* clientId */, MQClientInstance> factoryTable =
        new ConcurrentHashMap<String, MQClientInstance>();

    private MQClientManager() {

    }

    public static MQClientManager getInstance() {
        return instance;
    }

    public MQClientInstance getAndCreateMQClientInstance(final ClientConfig clientConfig) {
        return getAndCreateMQClientInstance(clientConfig, null);
    }

    //每次都会 生成一个新的mqclientinstance
    public MQClientInstance getAndCreateMQClientInstance(final ClientConfig clientConfig, RPCHook rpcHook) {
        //一个 client 对应一个 clientInstance
        String clientId = clientConfig.buildMQClientId();
        //通过id 获取到对应的实例
        MQClientInstance instance = this.factoryTable.get(clientId);
        if (null == instance) {
            instance =
                    //初始化实例对象  每次这个index 都会增加 然后又因为是单例整个程序 共用这个原子变量
                new MQClientInstance(clientConfig.cloneClientConfig(),
                    this.factoryIndexGenerator.getAndIncrement(), clientId, rpcHook);
            MQClientInstance prev = this.factoryTable.putIfAbsent(clientId, instance);
            if (prev != null) {
                instance = prev;
                log.warn("Returned Previous MQClientInstance for clientId:[{}]", clientId);
            } else {
                log.info("Created new MQClientInstance for clientId:[{}]", clientId);
            }
        }

        return instance;
    }

    public void removeClientFactory(final String clientId) {
        this.factoryTable.remove(clientId);
    }
}
