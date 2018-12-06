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
package org.apache.rocketmq.broker.client;

import io.netty.channel.Channel;

import java.util.Collection;
import java.util.List;

import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

//消费者id 修改的 监听器  就是在 注册和注销的时候 创建 和移除对应的拦截器对象
public class DefaultConsumerIdsChangeListener implements ConsumerIdsChangeListener {
    //关联的控制中心
    private final BrokerController brokerController;

    public DefaultConsumerIdsChangeListener(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    //事件类型 注册 注销 修改
    @Override
    public void handle(ConsumerGroupEvent event, String group, Object... args) {
        if (event == null) {
            return;
        }
        switch (event) {
            case CHANGE:
                if (args == null || args.length < 1) {
                    return;
                }
                //第一个参数 是 channel 列表
                List<Channel> channels = (List<Channel>) args[0];
                //当消费者 id 修改时要通知的 标识为true
                if (channels != null && brokerController.getBrokerConfig().isNotifyConsumerIdsChangedEnable()) {
                    for (Channel chl : channels) {
                        //这里是 从broker 回传给client 信息发生变化
                        //当 一个 client信息发生变化时 会发送心跳检测到broker 然后修改broker 配置 需要通知到同组的 其他client
                        this.brokerController.getBroker2Client().notifyConsumerIdsChanged(chl, group);
                    }
                }
                break;
            case UNREGISTER:
                //委托 执行 注销操作
                this.brokerController.getConsumerFilterManager().unRegister(group);
                break;
            case REGISTER:
                if (args == null || args.length < 1) {
                    return;
                }
                //如果是 注册 就是消费者新发布的订阅信息
                Collection<SubscriptionData> subscriptionDataList = (Collection<SubscriptionData>) args[0];
                //委托 注册  就是创建拦截器对象
                this.brokerController.getConsumerFilterManager().register(group, subscriptionDataList);
                break;
            default:
                throw new RuntimeException("Unknown event " + event);
        }
    }
}
