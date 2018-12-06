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
package org.apache.rocketmq.client.impl.producer;

import java.util.Set;
import org.apache.rocketmq.client.producer.TransactionCheckListener;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;

//mq实现类的 上级接口
public interface MQProducerInner {
    //produce可以获取当前所有的topic 列表  每次发送消息的时候都要定位 topic
    Set<String> getPublishTopicList();

    //传入 指定的topic 来判断 这个topic 是否需要更新
    boolean isPublishTopicNeedUpdate(final String topic);

    //获取本事务的 监听器
    TransactionCheckListener checkListener();
    TransactionListener getCheckListener();

    //检查事务状态
    void checkTransactionState(
        final String addr,
        final MessageExt msg,
        final CheckTransactionStateRequestHeader checkRequestHeader);

    //将 指定topic 的 信息 修改
    void updateTopicPublishInfo(final String topic, final TopicPublishInfo info);

    //是否是 单元模式 这里是 对应 producer 的 unit 这样再创建的时候每个id 都会跟这个挂钩
    boolean isUnitMode();
}
