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
package org.apache.rocketmq.client.impl.consumer;

import java.util.Set;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;

/**
 * Consumer inner interface
 */
//消费者接口
public interface MQConsumerInner {
    //这个消费者属于哪个组
    String groupName();

    //消息属于 集群还是 广播
    MessageModel messageModel();

    //push/ pull
    ConsumeType consumeType();

    //每次有新的 mq 添加时 从哪里开始消费
    ConsumeFromWhere consumeFromWhere();

    //获取订阅消息 这个应该是 push关联的 会主动推送消息
    Set<SubscriptionData> subscriptions();

    //负载操作
    void doRebalance();

    //持久化
    void persistConsumerOffset();

    //更新订阅消息
    void updateTopicSubscribeInfo(final String topic, final Set<MessageQueue> info);

    boolean isSubscribeTopicNeedUpdate(final String topic);

    boolean isUnitMode();

    //消费者的运行信息
    ConsumerRunningInfo consumerRunningInfo();
}
