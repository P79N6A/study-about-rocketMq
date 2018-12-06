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
package org.apache.rocketmq.broker.client.net;

import io.netty.channel.Channel;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.message.MessageQueueForC;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.GetConsumerStatusBody;
import org.apache.rocketmq.common.protocol.body.ResetOffsetBody;
import org.apache.rocketmq.common.protocol.body.ResetOffsetBodyForC;
import org.apache.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerStatusRequestHeader;
import org.apache.rocketmq.common.protocol.header.NotifyConsumerIdsChangedRequestHeader;
import org.apache.rocketmq.common.protocol.header.ResetOffsetRequestHeader;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingSendRequestException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

//broker 发起的请求到client
public class Broker2Client {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final BrokerController brokerController;

    public Broker2Client(BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    //检测生产者 事务状态
    public void checkProducerTransactionState(
        final String group,
        final Channel channel,
        final CheckTransactionStateRequestHeader requestHeader,
        final MessageExt messageExt) throws Exception {
        //创建检查事务 状态的 请求
        RemotingCommand request =
            RemotingCommand.createRequestCommand(RequestCode.CHECK_TRANSACTION_STATE, requestHeader);
        request.setBody(MessageDecoder.encode(messageExt, false));
        try {
            //调用服务 因为是单次调用所以不需要返回数据 写入请求后 写入到pipe 在handler 中才是处理请求对象
            //这个服务端抽象出来是具有发送消息的能力的
            this.brokerController.getRemotingServer().invokeOneway(channel, request, 10);
        } catch (Exception e) {
            log.error("Check transaction failed because invoke producer exception. group={}, msgId={}", group, messageExt.getMsgId(), e.getMessage());
        }
    }

    //通知client 这里直接传入请求对象然后调用通信框架
    public RemotingCommand callClient(final Channel channel,
                                      final RemotingCommand request
    ) throws RemotingSendRequestException, RemotingTimeoutException, InterruptedException {
        return this.brokerController.getRemotingServer().invokeSync(channel, request, 10000);
    }

    //当某个消费者 组中的某个消费者 针对topic 的订阅信息 或者这个消费者组的 消息模式 消费模式 发生变化 要更新到同组的其他消费者
    public void notifyConsumerIdsChanged(
        final Channel channel,
        final String consumerGroup) {
        if (null == consumerGroup) {
            log.error("notifyConsumerIdsChanged consumerGroup is null");
            return;
        }

        NotifyConsumerIdsChangedRequestHeader requestHeader = new NotifyConsumerIdsChangedRequestHeader();
        requestHeader.setConsumerGroup(consumerGroup);
        RemotingCommand request =
            RemotingCommand.createRequestCommand(RequestCode.NOTIFY_CONSUMER_IDS_CHANGED, requestHeader);

        try {
            //委托服务端 发送请求对象 第一阶段都是写入数据 在handler 中才是处理请求
            this.brokerController.getRemotingServer().invokeOneway(channel, request, 10);
        } catch (Exception e) {
            log.error("notifyConsumerIdsChanged exception, " + consumerGroup, e.getMessage());
        }
    }

    //这里是创建修改偏移量请求对象 并自行创建response 对象返回
    public RemotingCommand resetOffset(String topic, String group, long timeStamp, boolean isForce) {
        return resetOffset(topic, group, timeStamp, isForce, false);
    }

    //重置偏移量
    public RemotingCommand resetOffset(String topic, String group, long timeStamp, boolean isForce,
                                       boolean isC) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        //获取不到配置信息 就返回错误
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
        if (null == topicConfig) {
            log.error("[reset-offset] reset offset failed, no topic in this broker. topic={}", topic);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("[reset-offset] reset offset failed, no topic in this broker. topic=" + topic);
            return response;
        }

        Map<MessageQueue, Long> offsetTable = new HashMap<MessageQueue, Long>();

        //根据可写队列 来创建mq对象 队列id 就是下标
        for (int i = 0; i < topicConfig.getWriteQueueNums(); i++) {
            MessageQueue mq = new MessageQueue();
            mq.setBrokerName(this.brokerController.getBrokerConfig().getBrokerName());
            mq.setTopic(topic);
            mq.setQueueId(i);

            //获取 偏移量信息
            long consumerOffset =
                this.brokerController.getConsumerOffsetManager().queryOffset(group, topic, i);
            if (-1 == consumerOffset) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark(String.format("THe consumer group <%s> not exist", group));
                return response;
            }

            long timeStampOffset;
            if (timeStamp == -1) {

                //获取在队列中的偏移量 就是/ 单位长度
                timeStampOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, i);
            } else {
                //如果带上时间戳条件
                timeStampOffset = this.brokerController.getMessageStore().getOffsetInQueueByTime(topic, i, timeStamp);
            }

            if (timeStampOffset < 0) {
                log.warn("reset offset is invalid. topic={}, queueId={}, timeStampOffset={}", topic, i, timeStampOffset);
                timeStampOffset = 0;
            }

            if (isForce || timeStampOffset < consumerOffset) {
                offsetTable.put(mq, timeStampOffset);
            } else {
                offsetTable.put(mq, consumerOffset);
            }
        }

        //设置 重置偏移量的请求
        ResetOffsetRequestHeader requestHeader = new ResetOffsetRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setGroup(group);
        requestHeader.setTimestamp(timeStamp);
        RemotingCommand request =
            RemotingCommand.createRequestCommand(RequestCode.RESET_CONSUMER_CLIENT_OFFSET, requestHeader);
        if (isC) {
            // c++ language
            //C 不看
            ResetOffsetBodyForC body = new ResetOffsetBodyForC();
            List<MessageQueueForC> offsetList = convertOffsetTable2OffsetList(offsetTable);
            body.setOffsetTable(offsetList);
            request.setBody(body.encode());
        } else {
            // other language
            ResetOffsetBody body = new ResetOffsetBody();
            body.setOffsetTable(offsetTable);
            request.setBody(body.encode());
        }

        ConsumerGroupInfo consumerGroupInfo =
            this.brokerController.getConsumerManager().getConsumerGroupInfo(group);

        if (consumerGroupInfo != null && !consumerGroupInfo.getAllChannel().isEmpty()) {
            ConcurrentMap<Channel, ClientChannelInfo> channelInfoTable =
                consumerGroupInfo.getChannelInfoTable();
            for (Map.Entry<Channel, ClientChannelInfo> entry : channelInfoTable.entrySet()) {
                int version = entry.getValue().getVersion();
                //根据版本执行旧版本不支持
                if (version >= MQVersion.Version.V3_0_7_SNAPSHOT.ordinal()) {
                    try {
                        this.brokerController.getRemotingServer().invokeOneway(entry.getKey(), request, 5000);
                        log.info("[reset-offset] reset offset success. topic={}, group={}, clientId={}",
                            topic, group, entry.getValue().getClientId());
                    } catch (Exception e) {
                        log.error("[reset-offset] reset offset exception. topic={}, group={}",
                            new Object[] {topic, group}, e);
                    }
                } else {
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("the client does not support this feature. version="
                        + MQVersion.getVersionDesc(version));
                    log.warn("[reset-offset] the client does not support this feature. version={}",
                        RemotingHelper.parseChannelRemoteAddr(entry.getKey()), MQVersion.getVersionDesc(version));
                    return response;
                }
            }
        } else {
            //消费者信息不存在 异常情况
            String errorInfo =
                String.format("Consumer not online, so can not reset offset, Group: %s Topic: %s Timestamp: %d",
                    requestHeader.getGroup(),
                    requestHeader.getTopic(),
                    requestHeader.getTimestamp());
            log.error(errorInfo);
            response.setCode(ResponseCode.CONSUMER_NOT_ONLINE);
            response.setRemark(errorInfo);
            return response;
        }
        response.setCode(ResponseCode.SUCCESS);
        ResetOffsetBody resBody = new ResetOffsetBody();
        resBody.setOffsetTable(offsetTable);
        response.setBody(resBody.encode());
        return response;
    }

    //将偏移量 表 转换为 list
    private List<MessageQueueForC> convertOffsetTable2OffsetList(Map<MessageQueue, Long> table) {
        List<MessageQueueForC> list = new ArrayList<>();
        for (Entry<MessageQueue, Long> entry : table.entrySet()) {
            MessageQueue mq = entry.getKey();
            MessageQueueForC tmp =
                new MessageQueueForC(mq.getTopic(), mq.getBrokerName(), mq.getQueueId(), entry.getValue());
            list.add(tmp);
        }
        return list;
    }

    //获取消费者状态
    public RemotingCommand getConsumeStatus(String topic, String group, String originClientId) {
        final RemotingCommand result = RemotingCommand.createResponseCommand(null);

        GetConsumerStatusRequestHeader requestHeader = new GetConsumerStatusRequestHeader();
        requestHeader.setTopic(topic);
        requestHeader.setGroup(group);
        RemotingCommand request =
            RemotingCommand.createRequestCommand(RequestCode.GET_CONSUMER_STATUS_FROM_CLIENT,
                requestHeader);

        Map<String, Map<MessageQueue, Long>> consumerStatusTable =
            new HashMap<String, Map<MessageQueue, Long>>();
        //获取不到信息 异常情况
        ConcurrentMap<Channel, ClientChannelInfo> channelInfoTable =
            this.brokerController.getConsumerManager().getConsumerGroupInfo(group).getChannelInfoTable();
        if (null == channelInfoTable || channelInfoTable.isEmpty()) {
            result.setCode(ResponseCode.SYSTEM_ERROR);
            result.setRemark(String.format("No Any Consumer online in the consumer group: [%s]", group));
            return result;
        }

        //通知 同一group 下的所有消费者  channel 不同
        for (Map.Entry<Channel, ClientChannelInfo> entry : channelInfoTable.entrySet()) {
            int version = entry.getValue().getVersion();
            String clientId = entry.getValue().getClientId();
            if (version < MQVersion.Version.V3_0_7_SNAPSHOT.ordinal()) {
                result.setCode(ResponseCode.SYSTEM_ERROR);
                result.setRemark("the client does not support this feature. version="
                    + MQVersion.getVersionDesc(version));
                log.warn("[get-consumer-status] the client does not support this feature. version={}",
                    RemotingHelper.parseChannelRemoteAddr(entry.getKey()), MQVersion.getVersionDesc(version));
                return result;
            } else if (UtilAll.isBlank(originClientId) || originClientId.equals(clientId)) {
                try {
                    RemotingCommand response =
                        this.brokerController.getRemotingServer().invokeSync(entry.getKey(), request, 5000);
                    assert response != null;
                    switch (response.getCode()) {
                        case ResponseCode.SUCCESS: {
                            if (response.getBody() != null) {
                                GetConsumerStatusBody body =
                                    GetConsumerStatusBody.decode(response.getBody(),
                                        GetConsumerStatusBody.class);

                                consumerStatusTable.put(clientId, body.getMessageQueueTable());
                                log.info(
                                    "[get-consumer-status] get consumer status success. topic={}, group={}, channelRemoteAddr={}",
                                    topic, group, clientId);
                            }
                        }
                        default:
                            break;
                    }
                } catch (Exception e) {
                    log.error(
                        "[get-consumer-status] get consumer status exception. topic={}, group={}, offset={}",
                        new Object[] {topic, group}, e);
                }

                if (!UtilAll.isBlank(originClientId) && originClientId.equals(clientId)) {
                    break;
                }
            }
        }

        result.setCode(ResponseCode.SUCCESS);
        GetConsumerStatusBody resBody = new GetConsumerStatusBody();
        resBody.setConsumerTable(consumerStatusTable);
        result.setBody(resBody.encode());
        return result;
    }
}
