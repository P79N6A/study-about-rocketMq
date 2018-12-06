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

import io.netty.channel.ChannelHandlerContext;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import org.apache.rocketmq.client.impl.factory.MQClientInstance;
import org.apache.rocketmq.client.impl.producer.MQProducerInner;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.ConsumeMessageDirectlyResult;
import org.apache.rocketmq.common.protocol.body.ConsumerRunningInfo;
import org.apache.rocketmq.common.protocol.body.GetConsumerStatusBody;
import org.apache.rocketmq.common.protocol.body.ResetOffsetBody;
import org.apache.rocketmq.common.protocol.header.CheckTransactionStateRequestHeader;
import org.apache.rocketmq.common.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerRunningInfoRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerStatusRequestHeader;
import org.apache.rocketmq.common.protocol.header.NotifyConsumerIdsChangedRequestHeader;
import org.apache.rocketmq.common.protocol.header.ResetOffsetRequestHeader;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;

//实际处理请求  这里处理的是 request的 而不是response的
public class ClientRemotingProcessor implements NettyRequestProcessor {
    private final InternalLogger log = ClientLogger.getLog();
    //这个 处理器是针对哪个client实例的
    private final MQClientInstance mqClientFactory;

    public ClientRemotingProcessor(final MQClientInstance mqClientFactory) {
        this.mqClientFactory = mqClientFactory;
    }

    //这里根据获取到的请求类型 进行不同的处理  这应该是在自定义的handler 中通过操作channelhandlercontext
    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        switch (request.getCode()) {
            //如果是查询事务的状态
            case RequestCode.CHECK_TRANSACTION_STATE:
                return this.checkTransactionState(ctx, request);
            //broker通知 client 某个组下的 消费者 发生变化
            case RequestCode.NOTIFY_CONSUMER_IDS_CHANGED:
                return this.notifyConsumerIdsChanged(ctx, request);
            //重置消费者偏移量
            case RequestCode.RESET_CONSUMER_CLIENT_OFFSET:
                return this.resetOffset(ctx, request);
            //获取消费者状态
            case RequestCode.GET_CONSUMER_STATUS_FROM_CLIENT:
                return this.getConsumeStatus(ctx, request);

                //获取消费者运行状态
            case RequestCode.GET_CONSUMER_RUNNING_INFO:
                return this.getConsumerRunningInfo(ctx, request);

            case RequestCode.CONSUME_MESSAGE_DIRECTLY:
                return this.consumeMessageDirectly(ctx, request);
            default:
                break;
        }
        return null;
    }

    //默认不拒绝请求
    @Override
    public boolean rejectRequest() {
        return false;
    }

    //检查事务的状态
    public RemotingCommand checkTransactionState(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        //解析 请求头
        final CheckTransactionStateRequestHeader requestHeader =
            (CheckTransactionStateRequestHeader) request.decodeCommandCustomHeader(CheckTransactionStateRequestHeader.class);
        //通过一个byte数组 初始化bytebuffe对象
        final ByteBuffer byteBuffer = ByteBuffer.wrap(request.getBody());
        //这里就是解析 request的数据 并设置到mesageExt中
        //就是说 message 的数据还分了2 层 一层是基本数据 一层是 额外数据
        final MessageExt messageExt = MessageDecoder.decode(byteBuffer);
        if (messageExt != null) {
            //把 容器里的 transactionid 取出来
            String transactionId = messageExt.getProperty(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
            if (null != transactionId && !"".equals(transactionId)) {
                messageExt.setTransactionId(transactionId);
            }
            //获取生产者组  查询的事务是哪一个生产者组
            final String group = messageExt.getProperty(MessageConst.PROPERTY_PRODUCER_GROUP);
            if (group != null) {
                //从这个客户端上 选择对应的 消费者对象
                MQProducerInner producer = this.mqClientFactory.selectProducer(group);
                if (producer != null) {
                    //从 ctx 中获取到 地址
                    final String addr = RemotingHelper.parseChannelRemoteAddr(ctx.channel());
                    //实际的 检查事务状态是 委托给producer实现的
                    //也就是 要查询 什么事务 要通过给定的 prodcuer 队列来实现 producer 又委托给 transactionListener对象
                    producer.checkTransactionState(addr, messageExt, requestHeader);
                } else {
                    log.debug("checkTransactionState, pick producer by group[{}] failed", group);
                }
            } else {
                log.warn("checkTransactionState, pick producer group failed");
            }
        } else {
            log.warn("checkTransactionState, decode message failed");
        }

        return null;
    }

    //当某个组上的某个消费者 mq 或者订阅消息 消息模式 消费模式 发生变化  通知到broker 后 再由broker通知 管理的 同组group 下的所有 消费者 修改 负载策略 使得 mq 的消费 合理
    public RemotingCommand notifyConsumerIdsChanged(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        try {
            final NotifyConsumerIdsChangedRequestHeader requestHeader =
                (NotifyConsumerIdsChangedRequestHeader) request.decodeCommandCustomHeader(NotifyConsumerIdsChangedRequestHeader.class);
            log.info("receive broker's notification[{}], the consumer group: {} changed, rebalance immediately",
                RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                requestHeader.getConsumerGroup());
            this.mqClientFactory.rebalanceImmediately();
        } catch (Exception e) {
            log.error("notifyConsumerIdsChanged exception", RemotingHelper.exceptionSimpleDesc(e));
        }
        return null;
    }

    public RemotingCommand resetOffset(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final ResetOffsetRequestHeader requestHeader =
            (ResetOffsetRequestHeader) request.decodeCommandCustomHeader(ResetOffsetRequestHeader.class);
        log.info("invoke reset offset operation from broker. brokerAddr={}, topic={}, group={}, timestamp={}",
            RemotingHelper.parseChannelRemoteAddr(ctx.channel()), requestHeader.getTopic(), requestHeader.getGroup(),
            requestHeader.getTimestamp());
        Map<MessageQueue, Long> offsetTable = new HashMap<MessageQueue, Long>();
        if (request.getBody() != null) {
            ResetOffsetBody body = ResetOffsetBody.decode(request.getBody(), ResetOffsetBody.class);
            offsetTable = body.getOffsetTable();
        }
        this.mqClientFactory.resetOffset(requestHeader.getTopic(), requestHeader.getGroup(), offsetTable);
        return null;
    }

    @Deprecated
    public RemotingCommand getConsumeStatus(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetConsumerStatusRequestHeader requestHeader =
            (GetConsumerStatusRequestHeader) request.decodeCommandCustomHeader(GetConsumerStatusRequestHeader.class);

        Map<MessageQueue, Long> offsetTable = this.mqClientFactory.getConsumerStatus(requestHeader.getTopic(), requestHeader.getGroup());
        GetConsumerStatusBody body = new GetConsumerStatusBody();
        body.setMessageQueueTable(offsetTable);
        response.setBody(body.encode());
        response.setCode(ResponseCode.SUCCESS);
        return response;
    }

    private RemotingCommand getConsumerRunningInfo(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetConsumerRunningInfoRequestHeader requestHeader =
            (GetConsumerRunningInfoRequestHeader) request.decodeCommandCustomHeader(GetConsumerRunningInfoRequestHeader.class);

        ConsumerRunningInfo consumerRunningInfo = this.mqClientFactory.consumerRunningInfo(requestHeader.getConsumerGroup());
        if (null != consumerRunningInfo) {
            if (requestHeader.isJstackEnable()) {
                Map<Thread, StackTraceElement[]> map = Thread.getAllStackTraces();
                String jstack = UtilAll.jstack(map);
                consumerRunningInfo.setJstack(jstack);
            }

            response.setCode(ResponseCode.SUCCESS);
            response.setBody(consumerRunningInfo.encode());
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("The Consumer Group <%s> not exist in this consumer", requestHeader.getConsumerGroup()));
        }

        return response;
    }

    private RemotingCommand consumeMessageDirectly(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final ConsumeMessageDirectlyResultRequestHeader requestHeader =
            (ConsumeMessageDirectlyResultRequestHeader) request
                .decodeCommandCustomHeader(ConsumeMessageDirectlyResultRequestHeader.class);

        final MessageExt msg = MessageDecoder.decode(ByteBuffer.wrap(request.getBody()));

        ConsumeMessageDirectlyResult result =
            this.mqClientFactory.consumeMessageDirectly(msg, requestHeader.getConsumerGroup(), requestHeader.getBrokerName());

        if (null != result) {
            response.setCode(ResponseCode.SUCCESS);
            response.setBody(result.encode());
        } else {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("The Consumer Group <%s> not exist in this consumer", requestHeader.getConsumerGroup()));
        }

        return response;
    }
}
