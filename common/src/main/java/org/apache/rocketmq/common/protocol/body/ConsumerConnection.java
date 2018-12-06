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

package org.apache.rocketmq.common.protocol.body;

import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.protocol.heartbeat.ConsumeType;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;

//消费者组的连接信息
public class ConsumerConnection extends RemotingSerializable {
    //维护客户端地址 和id
    private HashSet<Connection> connectionSet = new HashSet<Connection>();
    //订阅的概念还没理解 应该是订阅了 哪些topic 然后会push消息
    private ConcurrentMap<String/* Topic */, SubscriptionData> subscriptionTable =
        new ConcurrentHashMap<String, SubscriptionData>();
    //push/pull
    private ConsumeType consumeType;
    //广播 还是集群
    private MessageModel messageModel;
    //从 偏移量最后开始
    private ConsumeFromWhere consumeFromWhere;

    public int computeMinVersion() {
        int minVersion = Integer.MAX_VALUE;
        for (Connection c : this.connectionSet) {
            if (c.getVersion() < minVersion) {
                minVersion = c.getVersion();
            }
        }

        return minVersion;
    }

    public HashSet<Connection> getConnectionSet() {
        return connectionSet;
    }

    public void setConnectionSet(HashSet<Connection> connectionSet) {
        this.connectionSet = connectionSet;
    }

    public ConcurrentMap<String, SubscriptionData> getSubscriptionTable() {
        return subscriptionTable;
    }

    public void setSubscriptionTable(ConcurrentHashMap<String, SubscriptionData> subscriptionTable) {
        this.subscriptionTable = subscriptionTable;
    }

    public ConsumeType getConsumeType() {
        return consumeType;
    }

    public void setConsumeType(ConsumeType consumeType) {
        this.consumeType = consumeType;
    }

    public MessageModel getMessageModel() {
        return messageModel;
    }

    public void setMessageModel(MessageModel messageModel) {
        this.messageModel = messageModel;
    }

    public ConsumeFromWhere getConsumeFromWhere() {
        return consumeFromWhere;
    }

    public void setConsumeFromWhere(ConsumeFromWhere consumeFromWhere) {
        this.consumeFromWhere = consumeFromWhere;
    }
}
