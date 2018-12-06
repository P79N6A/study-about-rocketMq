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
package org.apache.rocketmq.broker.processor;

import com.alibaba.fastjson.JSON;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.client.ClientChannelInfo;
import org.apache.rocketmq.broker.client.ConsumerGroupInfo;
import org.apache.rocketmq.broker.filter.ConsumerFilterData;
import org.apache.rocketmq.broker.filter.ExpressionMessageFilter;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.admin.ConsumeStats;
import org.apache.rocketmq.common.admin.OffsetWrapper;
import org.apache.rocketmq.common.admin.TopicOffset;
import org.apache.rocketmq.common.admin.TopicStatsTable;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageId;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.BrokerStatsData;
import org.apache.rocketmq.common.protocol.body.BrokerStatsItem;
import org.apache.rocketmq.common.protocol.body.Connection;
import org.apache.rocketmq.common.protocol.body.ConsumeQueueData;
import org.apache.rocketmq.common.protocol.body.ConsumeStatsList;
import org.apache.rocketmq.common.protocol.body.ConsumerConnection;
import org.apache.rocketmq.common.protocol.body.GroupList;
import org.apache.rocketmq.common.protocol.body.KVTable;
import org.apache.rocketmq.common.protocol.body.LockBatchRequestBody;
import org.apache.rocketmq.common.protocol.body.LockBatchResponseBody;
import org.apache.rocketmq.common.protocol.body.ProducerConnection;
import org.apache.rocketmq.common.protocol.body.QueryConsumeQueueResponseBody;
import org.apache.rocketmq.common.protocol.body.QueryConsumeTimeSpanBody;
import org.apache.rocketmq.common.protocol.body.QueryCorrectionOffsetBody;
import org.apache.rocketmq.common.protocol.body.QueueTimeSpan;
import org.apache.rocketmq.common.protocol.body.TopicList;
import org.apache.rocketmq.common.protocol.body.UnlockBatchRequestBody;
import org.apache.rocketmq.common.protocol.header.CloneGroupOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.ConsumeMessageDirectlyResultRequestHeader;
import org.apache.rocketmq.common.protocol.header.CreateTopicRequestHeader;
import org.apache.rocketmq.common.protocol.header.DeleteSubscriptionGroupRequestHeader;
import org.apache.rocketmq.common.protocol.header.DeleteTopicRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetAllTopicConfigResponseHeader;
import org.apache.rocketmq.common.protocol.header.GetBrokerConfigResponseHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumeStatsInBrokerHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumeStatsRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerConnectionListRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerRunningInfoRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetConsumerStatusRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetEarliestMsgStoretimeRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetEarliestMsgStoretimeResponseHeader;
import org.apache.rocketmq.common.protocol.header.GetMaxOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetMaxOffsetResponseHeader;
import org.apache.rocketmq.common.protocol.header.GetMinOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetMinOffsetResponseHeader;
import org.apache.rocketmq.common.protocol.header.GetProducerConnectionListRequestHeader;
import org.apache.rocketmq.common.protocol.header.GetTopicStatsInfoRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryConsumeQueueRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryConsumeTimeSpanRequestHeader;
import org.apache.rocketmq.common.protocol.header.QueryCorrectionOffsetHeader;
import org.apache.rocketmq.common.protocol.header.QueryTopicConsumeByWhoRequestHeader;
import org.apache.rocketmq.common.protocol.header.ResetOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.SearchOffsetRequestHeader;
import org.apache.rocketmq.common.protocol.header.SearchOffsetResponseHeader;
import org.apache.rocketmq.common.protocol.header.ViewBrokerStatsDataRequestHeader;
import org.apache.rocketmq.common.protocol.header.filtersrv.RegisterFilterServerRequestHeader;
import org.apache.rocketmq.common.protocol.header.filtersrv.RegisterFilterServerResponseHeader;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.common.stats.StatsItem;
import org.apache.rocketmq.common.stats.StatsSnapshot;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.filter.util.BitsArray;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.exception.RemotingTimeoutException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.LanguageCode;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.remoting.protocol.RemotingSerializable;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageFilter;
import org.apache.rocketmq.store.SelectMappedBufferResult;

//跟broker相关的请求处理器
public class AdminBrokerProcessor implements NettyRequestProcessor {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);
    private final BrokerController brokerController;

    public AdminBrokerProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
    }

    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        switch (request.getCode()) {
            case RequestCode.UPDATE_AND_CREATE_TOPIC:
                return this.updateAndCreateTopic(ctx, request);
            case RequestCode.DELETE_TOPIC_IN_BROKER:
                return this.deleteTopic(ctx, request);
            case RequestCode.GET_ALL_TOPIC_CONFIG:
                return this.getAllTopicConfig(ctx, request);
            case RequestCode.UPDATE_BROKER_CONFIG:
                return this.updateBrokerConfig(ctx, request);
            case RequestCode.GET_BROKER_CONFIG:
                return this.getBrokerConfig(ctx, request);
            case RequestCode.SEARCH_OFFSET_BY_TIMESTAMP:
                return this.searchOffsetByTimestamp(ctx, request);
            case RequestCode.GET_MAX_OFFSET:
                return this.getMaxOffset(ctx, request);
            case RequestCode.GET_MIN_OFFSET:
                return this.getMinOffset(ctx, request);
            case RequestCode.GET_EARLIEST_MSG_STORETIME:
                return this.getEarliestMsgStoretime(ctx, request);
            case RequestCode.GET_BROKER_RUNTIME_INFO:
                return this.getBrokerRuntimeInfo(ctx, request);
                //为指定client 下 增加一个mq 对象这样其他client 不能获取这个mq
            case RequestCode.LOCK_BATCH_MQ:
                return this.lockBatchMQ(ctx, request);
                //为指定client 下 的mq 解锁 其他client可以获取这个mq
            case RequestCode.UNLOCK_BATCH_MQ:
                return this.unlockBatchMQ(ctx, request);
            case RequestCode.UPDATE_AND_CREATE_SUBSCRIPTIONGROUP:
                return this.updateAndCreateSubscriptionGroup(ctx, request);
            case RequestCode.GET_ALL_SUBSCRIPTIONGROUP_CONFIG:
                return this.getAllSubscriptionGroup(ctx, request);
            case RequestCode.DELETE_SUBSCRIPTIONGROUP:
                return this.deleteSubscriptionGroup(ctx, request);
            case RequestCode.GET_TOPIC_STATS_INFO:
                return this.getTopicStatsInfo(ctx, request);
            case RequestCode.GET_CONSUMER_CONNECTION_LIST:
                return this.getConsumerConnectionList(ctx, request);
            case RequestCode.GET_PRODUCER_CONNECTION_LIST:
                return this.getProducerConnectionList(ctx, request);
            case RequestCode.GET_CONSUME_STATS:
                return this.getConsumeStats(ctx, request);
            case RequestCode.GET_ALL_CONSUMER_OFFSET:
                return this.getAllConsumerOffset(ctx, request);
            case RequestCode.GET_ALL_DELAY_OFFSET:
                return this.getAllDelayOffset(ctx, request);
            case RequestCode.INVOKE_BROKER_TO_RESET_OFFSET:
                return this.resetOffset(ctx, request);
            case RequestCode.INVOKE_BROKER_TO_GET_CONSUMER_STATUS:
                return this.getConsumerStatus(ctx, request);
            case RequestCode.QUERY_TOPIC_CONSUME_BY_WHO:
                return this.queryTopicConsumeByWho(ctx, request);
            case RequestCode.REGISTER_FILTER_SERVER:
                return this.registerFilterServer(ctx, request);
            case RequestCode.QUERY_CONSUME_TIME_SPAN:
                return this.queryConsumeTimeSpan(ctx, request);
            case RequestCode.GET_SYSTEM_TOPIC_LIST_FROM_BROKER:
                return this.getSystemTopicListFromBroker(ctx, request);
            case RequestCode.CLEAN_EXPIRED_CONSUMEQUEUE:
                return this.cleanExpiredConsumeQueue();
            case RequestCode.CLEAN_UNUSED_TOPIC:
                return this.cleanUnusedTopic();
            case RequestCode.GET_CONSUMER_RUNNING_INFO:
                return this.getConsumerRunningInfo(ctx, request);
            case RequestCode.QUERY_CORRECTION_OFFSET:
                return this.queryCorrectionOffset(ctx, request);
            case RequestCode.CONSUME_MESSAGE_DIRECTLY:
                return this.consumeMessageDirectly(ctx, request);
            case RequestCode.CLONE_GROUP_OFFSET:
                return this.cloneGroupOffset(ctx, request);
            case RequestCode.VIEW_BROKER_STATS_DATA:
                return ViewBrokerStatsData(ctx, request);
            case RequestCode.GET_BROKER_CONSUME_STATS:
                return fetchAllConsumeStatsInBroker(ctx, request);
            case RequestCode.QUERY_CONSUME_QUEUE:
                return queryConsumeQueue(ctx, request);
            default:
                break;
        }

        return null;
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }

    //创建指定topic
    private synchronized RemotingCommand updateAndCreateTopic(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final CreateTopicRequestHeader requestHeader =
            (CreateTopicRequestHeader) request.decodeCommandCustomHeader(CreateTopicRequestHeader.class);
        log.info("updateAndCreateTopic called by {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

        //如果topic 跟集群名一致返回错误消息
        if (requestHeader.getTopic().equals(this.brokerController.getBrokerConfig().getBrokerClusterName())) {
            String errorMsg = "the topic[" + requestHeader.getTopic() + "] is conflict with system reserved words.";
            log.warn(errorMsg);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(errorMsg);
            return response;
        }

        try {
            //将结果写入 ctx
            response.setCode(ResponseCode.SUCCESS);
            response.setOpaque(request.getOpaque());
            response.markResponseType();
            response.setRemark(null);
            ctx.writeAndFlush(response);
        } catch (Exception e) {
            log.error("Failed to produce a proper response", e);
        }

        TopicConfig topicConfig = new TopicConfig(requestHeader.getTopic());
        topicConfig.setReadQueueNums(requestHeader.getReadQueueNums());
        topicConfig.setWriteQueueNums(requestHeader.getWriteQueueNums());
        topicConfig.setTopicFilterType(requestHeader.getTopicFilterTypeEnum());
        topicConfig.setPerm(requestHeader.getPerm());
        topicConfig.setTopicSysFlag(requestHeader.getTopicSysFlag() == null ? 0 : requestHeader.getTopicSysFlag());

        //创建配置信息 并更新 每次更新都会进行持久化 并更新版本
        this.brokerController.getTopicConfigManager().updateTopicConfig(topicConfig);

        //将更新的信息 发布 也就是 注册到 nsv上
        this.brokerController.registerIncrementBrokerData(topicConfig,this.brokerController.getTopicConfigManager().getDataVersion());

        return null;
    }

    //删除 topic
    private synchronized RemotingCommand deleteTopic(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        DeleteTopicRequestHeader requestHeader =
            (DeleteTopicRequestHeader) request.decodeCommandCustomHeader(DeleteTopicRequestHeader.class);

        log.info("deleteTopic called by {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

        //委托删除topic 同时持久化
        this.brokerController.getTopicConfigManager().deleteTopicConfig(requestHeader.getTopic());
        //获取当前的 topicconfig 集合 并且 如果topic不存在就删除该topic对应的消费者队列
        this.brokerController.getMessageStore()
            .cleanUnusedTopic(this.brokerController.getTopicConfigManager().getTopicConfigTable().keySet());

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    //获取所有topic配置 信息
    private RemotingCommand getAllTopicConfig(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(GetAllTopicConfigResponseHeader.class);
        // final GetAllTopicConfigResponseHeader responseHeader =
        // (GetAllTopicConfigResponseHeader) response.readCustomHeader();

        //将 topicconfig 信息 编码成json 格式并返回
        String content = this.brokerController.getTopicConfigManager().encode();
        if (content != null && content.length() > 0) {
            try {
                //设置结果
                response.setBody(content.getBytes(MixAll.DEFAULT_CHARSET));
            } catch (UnsupportedEncodingException e) {
                log.error("", e);

                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        } else {
            log.error("No topic in this broker, client: {}", ctx.channel().remoteAddress());
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("No topic in this broker");
            return response;
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);

        return response;
    }

    //更新broker配置信息
    private synchronized RemotingCommand updateBrokerConfig(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        log.info("updateBrokerConfig called by {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

        byte[] body = request.getBody();
        if (body != null) {
            try {
                String bodyStr = new String(body, MixAll.DEFAULT_CHARSET);
                //将请求体解析成属性对象
                Properties properties = MixAll.string2Properties(bodyStr);
                if (properties != null) {
                    log.info("updateBrokerConfig, new config: [{}] client: {} ", properties, ctx.channel().remoteAddress());
                    //更新配置这里会 利用反射修改属性
                    this.brokerController.getConfiguration().update(properties);
                    //如果权限发生改动 要通知到 nameservice 将本broker中所有topicconfig的权限都修改
                    if (properties.containsKey("brokerPermission")) {
                        this.brokerController.getTopicConfigManager().getDataVersion().nextVersion();
                        this.brokerController.registerBrokerAll(false, false, true);
                    }
                } else {
                    log.error("string2Properties error");
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("string2Properties error");
                    return response;
                }
            } catch (UnsupportedEncodingException e) {
                log.error("", e);
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    //获取broker信息
    private RemotingCommand getBrokerConfig(ChannelHandlerContext ctx, RemotingCommand request) {

        final RemotingCommand response = RemotingCommand.createResponseCommand(GetBrokerConfigResponseHeader.class);
        final GetBrokerConfigResponseHeader responseHeader = (GetBrokerConfigResponseHeader) response.readCustomHeader();

        //将所有配置信息 以string 格式输出
        String content = this.brokerController.getConfiguration().getAllConfigsFormatString();
        if (content != null && content.length() > 0) {
            try {
                //将结果写入
                response.setBody(content.getBytes(MixAll.DEFAULT_CHARSET));
            } catch (UnsupportedEncodingException e) {
                log.error("", e);

                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        }

        //获取当前设置的版本并发送
        responseHeader.setVersion(this.brokerController.getConfiguration().getDataVersionJson());

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    //根据时间戳查询数据
    private RemotingCommand searchOffsetByTimestamp(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(SearchOffsetResponseHeader.class);
        final SearchOffsetResponseHeader responseHeader = (SearchOffsetResponseHeader) response.readCustomHeader();
        final SearchOffsetRequestHeader requestHeader =
            (SearchOffsetRequestHeader) request.decodeCommandCustomHeader(SearchOffsetRequestHeader.class);

        //从请求头中获取数据并委托 查询偏移量 这个偏移量是 /单位长度后的 实际获取数据还要* 单位长度
        long offset = this.brokerController.getMessageStore().getOffsetInQueueByTime(requestHeader.getTopic(), requestHeader.getQueueId(),
            requestHeader.getTimestamp());

        responseHeader.setOffset(offset);

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    //获取最大偏移量这个 也是/ 单位长度后的
    private RemotingCommand getMaxOffset(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(GetMaxOffsetResponseHeader.class);
        final GetMaxOffsetResponseHeader responseHeader = (GetMaxOffsetResponseHeader) response.readCustomHeader();
        final GetMaxOffsetRequestHeader requestHeader =
            (GetMaxOffsetRequestHeader) request.decodeCommandCustomHeader(GetMaxOffsetRequestHeader.class);

        long offset = this.brokerController.getMessageStore().getMaxOffsetInQueue(requestHeader.getTopic(), requestHeader.getQueueId());

        responseHeader.setOffset(offset);

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    //同上
    private RemotingCommand getMinOffset(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(GetMinOffsetResponseHeader.class);
        final GetMinOffsetResponseHeader responseHeader = (GetMinOffsetResponseHeader) response.readCustomHeader();
        final GetMinOffsetRequestHeader requestHeader =
            (GetMinOffsetRequestHeader) request.decodeCommandCustomHeader(GetMinOffsetRequestHeader.class);

        long offset = this.brokerController.getMessageStore().getMinOffsetInQueue(requestHeader.getTopic(), requestHeader.getQueueId());

        responseHeader.setOffset(offset);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    //获取最早的消息 保存时间
    private RemotingCommand getEarliestMsgStoretime(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(GetEarliestMsgStoretimeResponseHeader.class);
        final GetEarliestMsgStoretimeResponseHeader responseHeader = (GetEarliestMsgStoretimeResponseHeader) response.readCustomHeader();
        final GetEarliestMsgStoretimeRequestHeader requestHeader =
            (GetEarliestMsgStoretimeRequestHeader) request.decodeCommandCustomHeader(GetEarliestMsgStoretimeRequestHeader.class);

        long timestamp =
            this.brokerController.getMessageStore().getEarliestMessageTime(requestHeader.getTopic(), requestHeader.getQueueId());

        responseHeader.setTimestamp(timestamp);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    //获取 broker 的运行时数据
    private RemotingCommand getBrokerRuntimeInfo(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        //获取 运行时信息
        HashMap<String, String> runtimeInfo = this.prepareRuntimeInfo();
        //就是一般的 map 但是继承一个可以序列化的对象 这样就可以序列化子类的属性
        KVTable kvTable = new KVTable();
        kvTable.setTable(runtimeInfo);

        //序列化
        byte[] body = kvTable.encode();
        response.setBody(body);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    //将mq 绑定到 group 中的 某个成员 这样 组中其他成员就无法获取到这个mq对象了
    private RemotingCommand lockBatchMQ(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        LockBatchRequestBody requestBody = LockBatchRequestBody.decode(request.getBody(), LockBatchRequestBody.class);

        //这里其实就是在 该对象的容器中增加一对键值对  代表这个client实体被锁住了
        Set<MessageQueue> lockOKMQSet = this.brokerController.getRebalanceLockManager().tryLockBatch(
            requestBody.getConsumerGroup(),
            requestBody.getMqSet(),
            requestBody.getClientId());

        LockBatchResponseBody responseBody = new LockBatchResponseBody();
        responseBody.setLockOKMQSet(lockOKMQSet);

        response.setBody(responseBody.encode());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    //解锁 就是从容器中移除对应的键值对
    private RemotingCommand unlockBatchMQ(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        UnlockBatchRequestBody requestBody = UnlockBatchRequestBody.decode(request.getBody(), UnlockBatchRequestBody.class);

        this.brokerController.getRebalanceLockManager().unlockBatch(
            requestBody.getConsumerGroup(),
            requestBody.getMqSet(),
            requestBody.getClientId());

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    //更新and 创建订阅组信息
    private RemotingCommand updateAndCreateSubscriptionGroup(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        log.info("updateAndCreateSubscriptionGroup called by {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

        SubscriptionGroupConfig config = RemotingSerializable.decode(request.getBody(), SubscriptionGroupConfig.class);
        if (config != null) {
            //将订阅信息保存到 容器中
            this.brokerController.getSubscriptionGroupManager().updateSubscriptionGroupConfig(config);
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    //获取所有订阅消息
    private RemotingCommand getAllSubscriptionGroup(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        //将当前所有 订阅信息 编码成字符串
        String content = this.brokerController.getSubscriptionGroupManager().encode();
        if (content != null && content.length() > 0) {
            try {
                response.setBody(content.getBytes(MixAll.DEFAULT_CHARSET));
            } catch (UnsupportedEncodingException e) {
                log.error("", e);

                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        } else {
            log.error("No subscription group in this broker, client:{} ", ctx.channel().remoteAddress());
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("No subscription group in this broker");
            return response;
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);

        return response;
    }

    //删除订阅信息组
    private RemotingCommand deleteSubscriptionGroup(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        DeleteSubscriptionGroupRequestHeader requestHeader =
            (DeleteSubscriptionGroupRequestHeader) request.decodeCommandCustomHeader(DeleteSubscriptionGroupRequestHeader.class);

        log.info("deleteSubscriptionGroup called by {}", RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

        //删除订阅数据 也就是从容器中移除
        this.brokerController.getSubscriptionGroupManager().deleteSubscriptionGroupConfig(requestHeader.getGroupName());

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    //获取 指定topic 的 统计信息
    private RemotingCommand getTopicStatsInfo(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetTopicStatsInfoRequestHeader requestHeader =
            (GetTopicStatsInfoRequestHeader) request.decodeCommandCustomHeader(GetTopicStatsInfoRequestHeader.class);

        final String topic = requestHeader.getTopic();
        //获取 topicconfig
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
        if (null == topicConfig) {
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark("topic[" + topic + "] not exist");
            return response;
        }

        //创建统计对象  根据 写队列的 数量创建同等的数量并保存
        TopicStatsTable topicStatsTable = new TopicStatsTable();
        for (int i = 0; i < topicConfig.getWriteQueueNums(); i++) {
            MessageQueue mq = new MessageQueue();
            mq.setTopic(topic);
            mq.setBrokerName(this.brokerController.getBrokerConfig().getBrokerName());
            mq.setQueueId(i);

            TopicOffset topicOffset = new TopicOffset();
            long min = this.brokerController.getMessageStore().getMinOffsetInQueue(topic, i);
            if (min < 0)
                min = 0;

            long max = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, i);
            if (max < 0)
                max = 0;

            long timestamp = 0;
            if (max > 0) {
                timestamp = this.brokerController.getMessageStore().getMessageStoreTimeStamp(topic, i, max - 1);
            }

            topicOffset.setMinOffset(min);
            topicOffset.setMaxOffset(max);
            topicOffset.setLastUpdateTimestamp(timestamp);

            topicStatsTable.getOffsetTable().put(mq, topicOffset);
        }

        byte[] body = topicStatsTable.encode();
        response.setBody(body);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    //获取消费者连接对象 以group为条件
    private RemotingCommand getConsumerConnectionList(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetConsumerConnectionListRequestHeader requestHeader =
            (GetConsumerConnectionListRequestHeader) request.decodeCommandCustomHeader(GetConsumerConnectionListRequestHeader.class);

        //根据group 获取信息
        ConsumerGroupInfo consumerGroupInfo =
            this.brokerController.getConsumerManager().getConsumerGroupInfo(requestHeader.getConsumerGroup());
        if (consumerGroupInfo != null) {
            //通过消费信息 构建 消费者连接信息
            //这个ConsumerConnection 是以group 为单位记录这个group 下所有的消费者的连接对象
            ConsumerConnection bodydata = new ConsumerConnection();
            bodydata.setConsumeFromWhere(consumerGroupInfo.getConsumeFromWhere());
            bodydata.setConsumeType(consumerGroupInfo.getConsumeType());
            bodydata.setMessageModel(consumerGroupInfo.getMessageModel());
            bodydata.getSubscriptionTable().putAll(consumerGroupInfo.getSubscriptionTable());

            //channel 代表连接数
            Iterator<Map.Entry<Channel, ClientChannelInfo>> it = consumerGroupInfo.getChannelInfoTable().entrySet().iterator();
            while (it.hasNext()) {
                //为每个 clientInfo 都创建一个 connection 对象
                ClientChannelInfo info = it.next().getValue();
                Connection connection = new Connection();
                connection.setClientId(info.getClientId());
                connection.setLanguage(info.getLanguage());
                connection.setVersion(info.getVersion());
                connection.setClientAddr(RemotingHelper.parseChannelRemoteAddr(info.getChannel()));

                bodydata.getConnectionSet().add(connection);
            }

            byte[] body = bodydata.encode();
            response.setBody(body);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);

            return response;
        }

        response.setCode(ResponseCode.CONSUMER_NOT_ONLINE);
        response.setRemark("the consumer group[" + requestHeader.getConsumerGroup() + "] not online");
        return response;
    }

    //获取生产者组的连接对象
    private RemotingCommand getProducerConnectionList(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetProducerConnectionListRequestHeader requestHeader =
            (GetProducerConnectionListRequestHeader) request.decodeCommandCustomHeader(GetProducerConnectionListRequestHeader.class);

        //生产者组的连接对象
        ProducerConnection bodydata = new ProducerConnection();
        HashMap<Channel, ClientChannelInfo> channelInfoHashMap =
            this.brokerController.getProducerManager().getGroupChannelTable().get(requestHeader.getProducerGroup());
        if (channelInfoHashMap != null) {
            Iterator<Map.Entry<Channel, ClientChannelInfo>> it = channelInfoHashMap.entrySet().iterator();
            while (it.hasNext()) {
                ClientChannelInfo info = it.next().getValue();
                Connection connection = new Connection();
                connection.setClientId(info.getClientId());
                connection.setLanguage(info.getLanguage());
                connection.setVersion(info.getVersion());
                connection.setClientAddr(RemotingHelper.parseChannelRemoteAddr(info.getChannel()));

                bodydata.getConnectionSet().add(connection);
            }

            byte[] body = bodydata.encode();
            response.setBody(body);
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }

        response.setCode(ResponseCode.SYSTEM_ERROR);
        response.setRemark("the producer group[" + requestHeader.getProducerGroup() + "] not exist");
        return response;
    }

    //获取消费者统计消息
    private RemotingCommand getConsumeStats(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        final GetConsumeStatsRequestHeader requestHeader =
            (GetConsumeStatsRequestHeader) request.decodeCommandCustomHeader(GetConsumeStatsRequestHeader.class);

        //创建消费者统计对象
        ConsumeStats consumeStats = new ConsumeStats();

        Set<String> topics = new HashSet<String>();
        //如果 topic 为空 就获取 该组下的所有topic
        if (UtilAll.isBlank(requestHeader.getTopic())) {
            //根据group 获取 消费者 订阅的 topic
            topics = this.brokerController.getConsumerOffsetManager().whichTopicByConsumer(requestHeader.getConsumerGroup());
        } else {
            //否则 设置指定的topic
            topics.add(requestHeader.getTopic());
        }

        //根据topic 获取配置信息
        for (String topic : topics) {
            //如果 该topic 的配置信息不存在
            TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
            if (null == topicConfig) {
                log.warn("consumeStats, topic config not exist, {}", topic);
                continue;
            }

            {
                //通过group 和 topic 定位到订阅信息 如果订阅信息不存在
                SubscriptionData findSubscriptionData =
                    this.brokerController.getConsumerManager().findSubscriptionData(requestHeader.getConsumerGroup(), topic);

                if (null == findSubscriptionData
                    && this.brokerController.getConsumerManager().findSubscriptionDataCount(requestHeader.getConsumerGroup()) > 0) {
                    log.warn("consumeStats, the consumer group[{}], topic[{}] not exist", requestHeader.getConsumerGroup(), topic);
                    continue;
                }
            }

            //根据 读队列数量创建实体
            for (int i = 0; i < topicConfig.getReadQueueNums(); i++) {
                MessageQueue mq = new MessageQueue();
                mq.setTopic(topic);
                mq.setBrokerName(this.brokerController.getBrokerConfig().getBrokerName());
                mq.setQueueId(i);

                OffsetWrapper offsetWrapper = new OffsetWrapper();

                long brokerOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, i);
                if (brokerOffset < 0)
                    brokerOffset = 0;

                long consumerOffset = this.brokerController.getConsumerOffsetManager().queryOffset(
                    requestHeader.getConsumerGroup(),
                    topic,
                    i);
                if (consumerOffset < 0)
                    consumerOffset = 0;

                offsetWrapper.setBrokerOffset(brokerOffset);
                offsetWrapper.setConsumerOffset(consumerOffset);

                long timeOffset = consumerOffset - 1;
                if (timeOffset >= 0) {
                    long lastTimestamp = this.brokerController.getMessageStore().getMessageStoreTimeStamp(topic, i, timeOffset);
                    if (lastTimestamp > 0) {
                        offsetWrapper.setLastTimestamp(lastTimestamp);
                    }
                }

                //设置结果
                consumeStats.getOffsetTable().put(mq, offsetWrapper);
            }

            double consumeTps = this.brokerController.getBrokerStatsManager().tpsGroupGetNums(requestHeader.getConsumerGroup(), topic);

            //记录tps数据
            consumeTps += consumeStats.getConsumeTps();
            consumeStats.setConsumeTps(consumeTps);
        }

        byte[] body = consumeStats.encode();
        response.setBody(body);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    //获取 所有消费者的偏移量
    private RemotingCommand getAllConsumerOffset(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        //将对象转换成 json
        String content = this.brokerController.getConsumerOffsetManager().encode();
        if (content != null && content.length() > 0) {
            try {
                response.setBody(content.getBytes(MixAll.DEFAULT_CHARSET));
            } catch (UnsupportedEncodingException e) {
                log.error("get all consumer offset from master error.", e);

                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        } else {
            log.error("No consumer offset in this broker, client: {} ", ctx.channel().remoteAddress());
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("No consumer offset in this broker");
            return response;
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);

        return response;
    }

    //获取延迟偏移量
    private RemotingCommand getAllDelayOffset(ChannelHandlerContext ctx, RemotingCommand request) {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        String content = ((DefaultMessageStore) this.brokerController.getMessageStore()).getScheduleMessageService().encode();
        if (content != null && content.length() > 0) {
            try {
                response.setBody(content.getBytes(MixAll.DEFAULT_CHARSET));
            } catch (UnsupportedEncodingException e) {
                log.error("get all delay offset from master error.", e);

                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UnsupportedEncodingException " + e);
                return response;
            }
        } else {
            log.error("No delay offset in this broker, client: {} ", ctx.channel().remoteAddress());
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("No delay offset in this broker");
            return response;
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);

        return response;
    }

    //重置偏移量  这里将请求转发到了别处 也就是可能 a的功能是借由转发到b实现的 从外部看来就是a 具有该功能
    public RemotingCommand resetOffset(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final ResetOffsetRequestHeader requestHeader =
            (ResetOffsetRequestHeader) request.decodeCommandCustomHeader(ResetOffsetRequestHeader.class);
        log.info("[reset-offset] reset offset started by {}. topic={}, group={}, timestamp={}, isForce={}",
            RemotingHelper.parseChannelRemoteAddr(ctx.channel()), requestHeader.getTopic(), requestHeader.getGroup(),
            requestHeader.getTimestamp(), requestHeader.isForce());
        boolean isC = false;
        LanguageCode language = request.getLanguage();
        switch (language) {
            case CPP:
                isC = true;
                break;
        }
        return this.brokerController.getBroker2Client().resetOffset(requestHeader.getTopic(), requestHeader.getGroup(),
            requestHeader.getTimestamp(), requestHeader.isForce(), isC);
    }

    //获取消费者状态 也是转发
    public RemotingCommand getConsumerStatus(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final GetConsumerStatusRequestHeader requestHeader =
            (GetConsumerStatusRequestHeader) request.decodeCommandCustomHeader(GetConsumerStatusRequestHeader.class);

        log.info("[get-consumer-status] get consumer status by {}. topic={}, group={}",
            RemotingHelper.parseChannelRemoteAddr(ctx.channel()), requestHeader.getTopic(), requestHeader.getGroup());

        return this.brokerController.getBroker2Client().getConsumeStatus(requestHeader.getTopic(), requestHeader.getGroup(),
            requestHeader.getClientAddr());
    }

    //查询 topic
    private RemotingCommand queryTopicConsumeByWho(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        QueryTopicConsumeByWhoRequestHeader requestHeader =
            (QueryTopicConsumeByWhoRequestHeader) request.decodeCommandCustomHeader(QueryTopicConsumeByWhoRequestHeader.class);

        //查询哪个 group 订阅了这个topic
        HashSet<String> groups = this.brokerController.getConsumerManager().queryTopicConsumeByWho(requestHeader.getTopic());

        //在这个对象中 找到与 topic关联的 group信息
        Set<String> groupInOffset = this.brokerController.getConsumerOffsetManager().whichGroupByTopic(requestHeader.getTopic());
        if (groupInOffset != null && !groupInOffset.isEmpty()) {
            groups.addAll(groupInOffset);
        }

        GroupList groupList = new GroupList();
        groupList.setGroupList(groups);
        byte[] body = groupList.encode();

        response.setBody(body);
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    //注册拦截对象
    private RemotingCommand registerFilterServer(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(RegisterFilterServerResponseHeader.class);
        final RegisterFilterServerResponseHeader responseHeader = (RegisterFilterServerResponseHeader) response.readCustomHeader();
        final RegisterFilterServerRequestHeader requestHeader =
            (RegisterFilterServerRequestHeader) request.decodeCommandCustomHeader(RegisterFilterServerRequestHeader.class);

        this.brokerController.getFilterServerManager().registerFilterServer(ctx.channel(), requestHeader.getFilterServerAddr());

        responseHeader.setBrokerId(this.brokerController.getBrokerConfig().getBrokerId());
        responseHeader.setBrokerName(this.brokerController.getBrokerConfig().getBrokerName());

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    //查询消费时间间隔
    private RemotingCommand queryConsumeTimeSpan(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        QueryConsumeTimeSpanRequestHeader requestHeader =
            (QueryConsumeTimeSpanRequestHeader) request.decodeCommandCustomHeader(QueryConsumeTimeSpanRequestHeader.class);

        final String topic = requestHeader.getTopic();
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
        if (null == topicConfig) {
            response.setCode(ResponseCode.TOPIC_NOT_EXIST);
            response.setRemark("topic[" + topic + "] not exist");
            return response;
        }

        List<QueueTimeSpan> timeSpanSet = new ArrayList<QueueTimeSpan>();
        //每个可写队列都有一个 消费时间间隔
        for (int i = 0; i < topicConfig.getWriteQueueNums(); i++) {
            QueueTimeSpan timeSpan = new QueueTimeSpan();
            MessageQueue mq = new MessageQueue();
            mq.setTopic(topic);
            mq.setBrokerName(this.brokerController.getBrokerConfig().getBrokerName());
            mq.setQueueId(i);
            timeSpan.setMessageQueue(mq);

            long minTime = this.brokerController.getMessageStore().getEarliestMessageTime(topic, i);
            timeSpan.setMinTimeStamp(minTime);

            long max = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, i);
            long maxTime = this.brokerController.getMessageStore().getMessageStoreTimeStamp(topic, i, max - 1);
            timeSpan.setMaxTimeStamp(maxTime);

            long consumeTime;
            long consumerOffset = this.brokerController.getConsumerOffsetManager().queryOffset(
                requestHeader.getGroup(), topic, i);
            if (consumerOffset > 0) {
                consumeTime = this.brokerController.getMessageStore().getMessageStoreTimeStamp(topic, i, consumerOffset - 1);
            } else {
                consumeTime = minTime;
            }
            timeSpan.setConsumeTimeStamp(consumeTime);

            long maxBrokerOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(requestHeader.getTopic(), i);
            if (consumerOffset < maxBrokerOffset) {
                long nextTime = this.brokerController.getMessageStore().getMessageStoreTimeStamp(topic, i, consumerOffset);
                timeSpan.setDelayTime(System.currentTimeMillis() - nextTime);
            }
            timeSpanSet.add(timeSpan);
        }

        QueryConsumeTimeSpanBody queryConsumeTimeSpanBody = new QueryConsumeTimeSpanBody();
        queryConsumeTimeSpanBody.setConsumeTimeSpanSet(timeSpanSet);
        response.setBody(queryConsumeTimeSpanBody.encode());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    //从broker 获取系统topic
    private RemotingCommand getSystemTopicListFromBroker(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);

        Set<String> topics = this.brokerController.getTopicConfigManager().getSystemTopic();
        TopicList topicList = new TopicList();
        topicList.setTopicList(topics);
        response.setBody(topicList.encode());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    //清除过期队列
    public RemotingCommand cleanExpiredConsumeQueue() {
        log.warn("invoke cleanExpiredConsumeQueue start.");
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        brokerController.getMessageStore().cleanExpiredConsumerQueue();
        log.warn("invoke cleanExpiredConsumeQueue end.");
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    //清除不使用的topic
    public RemotingCommand cleanUnusedTopic() {
        log.warn("invoke cleanUnusedTopic start.");
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        //跟 topicConfig中 topic 对不上的都清除
        brokerController.getMessageStore().cleanUnusedTopic(brokerController.getTopicConfigManager().getTopicConfigTable().keySet());
        log.warn("invoke cleanUnusedTopic end.");
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    //获取消费者的运行时信息
    private RemotingCommand getConsumerRunningInfo(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final GetConsumerRunningInfoRequestHeader requestHeader =
            (GetConsumerRunningInfoRequestHeader) request.decodeCommandCustomHeader(GetConsumerRunningInfoRequestHeader.class);

        //也是通过 brokerToClient 转发实现
        return this.callConsumer(RequestCode.GET_CONSUMER_RUNNING_INFO, request, requestHeader.getConsumerGroup(),
            requestHeader.getClientId());
    }

    //查询正确的偏移量 同时删除掉 无效的topic
    private RemotingCommand queryCorrectionOffset(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        QueryCorrectionOffsetHeader requestHeader =
            (QueryCorrectionOffsetHeader) request.decodeCommandCustomHeader(QueryCorrectionOffsetHeader.class);

        //通过拦截对象清除 无效的topic 获取 topic下的 最小偏移量
        Map<Integer, Long> correctionOffset = this.brokerController.getConsumerOffsetManager()
            .queryMinOffsetInAllGroup(requestHeader.getTopic(), requestHeader.getFilterGroups());

        //获取正常的偏移量
        Map<Integer, Long> compareOffset =
            this.brokerController.getConsumerOffsetManager().queryOffset(requestHeader.getTopic(), requestHeader.getCompareGroup());

        if (compareOffset != null && !compareOffset.isEmpty()) {
            for (Map.Entry<Integer, Long> entry : compareOffset.entrySet()) {
                Integer queueId = entry.getKey();
                //设置成正确的 偏移量
                correctionOffset.put(queueId,
                    correctionOffset.get(queueId) > entry.getValue() ? Long.MAX_VALUE : correctionOffset.get(queueId));
            }
        }

        QueryCorrectionOffsetBody body = new QueryCorrectionOffsetBody();
        body.setCorrectionOffsets(correctionOffset);
        response.setBody(body.encode());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    //直接消费信息 这里是转发给 consumer来处理
    private RemotingCommand consumeMessageDirectly(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final ConsumeMessageDirectlyResultRequestHeader requestHeader = (ConsumeMessageDirectlyResultRequestHeader) request
            .decodeCommandCustomHeader(ConsumeMessageDirectlyResultRequestHeader.class);

        request.getExtFields().put("brokerName", this.brokerController.getBrokerConfig().getBrokerName());
        SelectMappedBufferResult selectMappedBufferResult = null;
        try {
            MessageId messageId = MessageDecoder.decodeMessageId(requestHeader.getMsgId());
            selectMappedBufferResult = this.brokerController.getMessageStore().selectOneMessageByOffset(messageId.getOffset());

            byte[] body = new byte[selectMappedBufferResult.getSize()];
            selectMappedBufferResult.getByteBuffer().get(body);
            request.setBody(body);
        } catch (UnknownHostException e) {
        } finally {
            if (selectMappedBufferResult != null) {
                selectMappedBufferResult.release();
            }
        }

        //转发
        return this.callConsumer(RequestCode.CONSUME_MESSAGE_DIRECTLY, request, requestHeader.getConsumerGroup(),
            requestHeader.getClientId());
    }

    //拷贝 组偏移量
    private RemotingCommand cloneGroupOffset(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        CloneGroupOffsetRequestHeader requestHeader =
            (CloneGroupOffsetRequestHeader) request.decodeCommandCustomHeader(CloneGroupOffsetRequestHeader.class);

        Set<String> topics;
        if (UtilAll.isBlank(requestHeader.getTopic())) {
            //获取该 group 订阅的 topic
            topics = this.brokerController.getConsumerOffsetManager().whichTopicByConsumer(requestHeader.getSrcGroup());
        } else {
            topics = new HashSet<String>();
            topics.add(requestHeader.getTopic());
        }

        //遍历每个 topic
        for (String topic : topics) {
            TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
            if (null == topicConfig) {
                log.warn("[cloneGroupOffset], topic config not exist, {}", topic);
                continue;
            }

            //请求头没有下线
            if (!requestHeader.isOffline()) {

                SubscriptionData findSubscriptionData =
                    this.brokerController.getConsumerManager().findSubscriptionData(requestHeader.getSrcGroup(), topic);
                if (this.brokerController.getConsumerManager().findSubscriptionDataCount(requestHeader.getSrcGroup()) > 0
                    && findSubscriptionData == null) {
                    log.warn("[cloneGroupOffset], the consumer group[{}], topic[{}] not exist", requestHeader.getSrcGroup(), topic);
                    continue;
                }
            }

            //委托实现
            this.brokerController.getConsumerOffsetManager().cloneOffset(requestHeader.getSrcGroup(), requestHeader.getDestGroup(),
                requestHeader.getTopic());
        }

        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    //查看 broker 统计信息
    private RemotingCommand ViewBrokerStatsData(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        final ViewBrokerStatsDataRequestHeader requestHeader =
            (ViewBrokerStatsDataRequestHeader) request.decodeCommandCustomHeader(ViewBrokerStatsDataRequestHeader.class);
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        DefaultMessageStore messageStore = (DefaultMessageStore) this.brokerController.getMessageStore();

        //获取统计信息
        StatsItem statsItem = messageStore.getBrokerStatsManager().getStatsItem(requestHeader.getStatsName(), requestHeader.getStatsKey());
        if (null == statsItem) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("The stats <%s> <%s> not exist", requestHeader.getStatsName(), requestHeader.getStatsKey()));
            return response;
        }

        //包含不同单位时间的信息
        BrokerStatsData brokerStatsData = new BrokerStatsData();

        {
            BrokerStatsItem it = new BrokerStatsItem();
            StatsSnapshot ss = statsItem.getStatsDataInMinute();
            it.setSum(ss.getSum());
            it.setTps(ss.getTps());
            it.setAvgpt(ss.getAvgpt());
            brokerStatsData.setStatsMinute(it);
        }

        {
            BrokerStatsItem it = new BrokerStatsItem();
            StatsSnapshot ss = statsItem.getStatsDataInHour();
            it.setSum(ss.getSum());
            it.setTps(ss.getTps());
            it.setAvgpt(ss.getAvgpt());
            brokerStatsData.setStatsHour(it);
        }

        {
            BrokerStatsItem it = new BrokerStatsItem();
            StatsSnapshot ss = statsItem.getStatsDataInDay();
            it.setSum(ss.getSum());
            it.setTps(ss.getTps());
            it.setAvgpt(ss.getAvgpt());
            brokerStatsData.setStatsDay(it);
        }

        response.setBody(brokerStatsData.encode());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    //从broker 中获取所有消费者信息
    private RemotingCommand fetchAllConsumeStatsInBroker(ChannelHandlerContext ctx, RemotingCommand request)
        throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        GetConsumeStatsInBrokerHeader requestHeader =
            (GetConsumeStatsInBrokerHeader) request.decodeCommandCustomHeader(GetConsumeStatsInBrokerHeader.class);
        boolean isOrder = requestHeader.isOrder();
        //group 与 订阅消息的 容器
        ConcurrentMap<String, SubscriptionGroupConfig> subscriptionGroups =
            brokerController.getSubscriptionGroupManager().getSubscriptionGroupTable();

        List<Map<String/* subscriptionGroupName */, List<ConsumeStats>>> brokerConsumeStatsList =
            new ArrayList<Map<String, List<ConsumeStats>>>();

        long totalDiff = 0L;

        for (String group : subscriptionGroups.keySet()) {
            Map<String, List<ConsumeStats>> subscripTopicConsumeMap = new HashMap<String, List<ConsumeStats>>();
            //获取 group下 归属的 topic
            Set<String> topics = this.brokerController.getConsumerOffsetManager().whichTopicByConsumer(group);
            List<ConsumeStats> consumeStatsList = new ArrayList<ConsumeStats>();
            for (String topic : topics) {
                ConsumeStats consumeStats = new ConsumeStats();
                TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(topic);
                if (null == topicConfig) {
                    log.warn("consumeStats, topic config not exist, {}", topic);
                    continue;
                }

                if (isOrder && !topicConfig.isOrder()) {
                    continue;
                }

                {
                    SubscriptionData findSubscriptionData = this.brokerController.getConsumerManager().findSubscriptionData(group, topic);

                    if (null == findSubscriptionData
                        && this.brokerController.getConsumerManager().findSubscriptionDataCount(group) > 0) {
                        log.warn("consumeStats, the consumer group[{}], topic[{}] not exist", group, topic);
                        continue;
                    }
                }

                //根据写队列数量创建
                for (int i = 0; i < topicConfig.getWriteQueueNums(); i++) {
                    MessageQueue mq = new MessageQueue();
                    mq.setTopic(topic);
                    mq.setBrokerName(this.brokerController.getBrokerConfig().getBrokerName());
                    mq.setQueueId(i);
                    OffsetWrapper offsetWrapper = new OffsetWrapper();
                    long brokerOffset = this.brokerController.getMessageStore().getMaxOffsetInQueue(topic, i);
                    if (brokerOffset < 0)
                        brokerOffset = 0;
                    long consumerOffset = this.brokerController.getConsumerOffsetManager().queryOffset(
                        group,
                        topic,
                        i);
                    if (consumerOffset < 0)
                        consumerOffset = 0;

                    offsetWrapper.setBrokerOffset(brokerOffset);
                    offsetWrapper.setConsumerOffset(consumerOffset);

                    long timeOffset = consumerOffset - 1;
                    if (timeOffset >= 0) {
                        long lastTimestamp = this.brokerController.getMessageStore().getMessageStoreTimeStamp(topic, i, timeOffset);
                        if (lastTimestamp > 0) {
                            offsetWrapper.setLastTimestamp(lastTimestamp);
                        }
                    }
                    consumeStats.getOffsetTable().put(mq, offsetWrapper);
                }
                double consumeTps = this.brokerController.getBrokerStatsManager().tpsGroupGetNums(group, topic);
                consumeTps += consumeStats.getConsumeTps();
                consumeStats.setConsumeTps(consumeTps);
                totalDiff += consumeStats.computeTotalDiff();
                consumeStatsList.add(consumeStats);
            }
            //获取统计信息
            subscripTopicConsumeMap.put(group, consumeStatsList);
            brokerConsumeStatsList.add(subscripTopicConsumeMap);
        }
        ConsumeStatsList consumeStats = new ConsumeStatsList();
        consumeStats.setBrokerAddr(brokerController.getBrokerAddr());
        consumeStats.setConsumeStatsList(brokerConsumeStatsList);
        consumeStats.setTotalDiff(totalDiff);
        response.setBody(consumeStats.encode());
        response.setCode(ResponseCode.SUCCESS);
        response.setRemark(null);
        return response;
    }

    //获取运行时信息
    private HashMap<String, String> prepareRuntimeInfo() {
        HashMap<String, String> runtimeInfo = this.brokerController.getMessageStore().getRuntimeInfo();
        runtimeInfo.put("brokerVersionDesc", MQVersion.getVersionDesc(MQVersion.CURRENT_VERSION));
        runtimeInfo.put("brokerVersion", String.valueOf(MQVersion.CURRENT_VERSION));

        runtimeInfo.put("msgPutTotalYesterdayMorning",
            String.valueOf(this.brokerController.getBrokerStats().getMsgPutTotalYesterdayMorning()));
        runtimeInfo.put("msgPutTotalTodayMorning", String.valueOf(this.brokerController.getBrokerStats().getMsgPutTotalTodayMorning()));
        runtimeInfo.put("msgPutTotalTodayNow", String.valueOf(this.brokerController.getBrokerStats().getMsgPutTotalTodayNow()));

        runtimeInfo.put("msgGetTotalYesterdayMorning",
            String.valueOf(this.brokerController.getBrokerStats().getMsgGetTotalYesterdayMorning()));
        runtimeInfo.put("msgGetTotalTodayMorning", String.valueOf(this.brokerController.getBrokerStats().getMsgGetTotalTodayMorning()));
        runtimeInfo.put("msgGetTotalTodayNow", String.valueOf(this.brokerController.getBrokerStats().getMsgGetTotalTodayNow()));

        runtimeInfo.put("sendThreadPoolQueueSize", String.valueOf(this.brokerController.getSendThreadPoolQueue().size()));

        runtimeInfo.put("sendThreadPoolQueueCapacity",
            String.valueOf(this.brokerController.getBrokerConfig().getSendThreadPoolQueueCapacity()));

        runtimeInfo.put("pullThreadPoolQueueSize", String.valueOf(this.brokerController.getPullThreadPoolQueue().size()));
        runtimeInfo.put("pullThreadPoolQueueCapacity",
            String.valueOf(this.brokerController.getBrokerConfig().getPullThreadPoolQueueCapacity()));

        runtimeInfo.put("queryThreadPoolQueueSize", String.valueOf(this.brokerController.getQueryThreadPoolQueue().size()));
        runtimeInfo.put("queryThreadPoolQueueCapacity",
            String.valueOf(this.brokerController.getBrokerConfig().getQueryThreadPoolQueueCapacity()));

        runtimeInfo.put("EndTransactionQueueSize", String.valueOf(this.brokerController.getEndTransactionThreadPoolQueue().size()));
        runtimeInfo.put("EndTransactionThreadPoolQueueCapacity",
            String.valueOf(this.brokerController.getBrokerConfig().getEndTransactionPoolQueueCapacity()));

        runtimeInfo.put("dispatchBehindBytes", String.valueOf(this.brokerController.getMessageStore().dispatchBehindBytes()));
        runtimeInfo.put("pageCacheLockTimeMills", String.valueOf(this.brokerController.getMessageStore().lockTimeMills()));

        runtimeInfo.put("sendThreadPoolQueueHeadWaitTimeMills", String.valueOf(this.brokerController.headSlowTimeMills4SendThreadPoolQueue()));
        runtimeInfo.put("pullThreadPoolQueueHeadWaitTimeMills", String.valueOf(this.brokerController.headSlowTimeMills4PullThreadPoolQueue()));
        runtimeInfo.put("queryThreadPoolQueueHeadWaitTimeMills", String.valueOf(this.brokerController.headSlowTimeMills4QueryThreadPoolQueue()));

        runtimeInfo.put("earliestMessageTimeStamp", String.valueOf(this.brokerController.getMessageStore().getEarliestMessageTime()));
        runtimeInfo.put("startAcceptSendRequestTimeStamp", String.valueOf(this.brokerController.getBrokerConfig().getStartAcceptSendRequestTimeStamp()));
        if (this.brokerController.getMessageStore() instanceof DefaultMessageStore) {
            DefaultMessageStore defaultMessageStore = (DefaultMessageStore) this.brokerController.getMessageStore();
            runtimeInfo.put("remainTransientStoreBufferNumbs", String.valueOf(defaultMessageStore.remainTransientStoreBufferNumbs()));
            if (defaultMessageStore.getMessageStoreConfig().isTransientStorePoolEnable()) {
                runtimeInfo.put("remainHowManyDataToCommit", MixAll.humanReadableByteCount(defaultMessageStore.getCommitLog().remainHowManyDataToCommit(), false));
            }
            runtimeInfo.put("remainHowManyDataToFlush", MixAll.humanReadableByteCount(defaultMessageStore.getCommitLog().remainHowManyDataToFlush(), false));
        }

        java.io.File commitLogDir = new java.io.File(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
        if (commitLogDir.exists()) {
            runtimeInfo.put("commitLogDirCapacity", String.format("Total : %s, Free : %s.", MixAll.humanReadableByteCount(commitLogDir.getTotalSpace(), false), MixAll.humanReadableByteCount(commitLogDir.getFreeSpace(), false)));
        }

        return runtimeInfo;
    }

    //访问 消费者 根据请求类型
    private RemotingCommand callConsumer(
        final int requestCode,
        final RemotingCommand request,
        final String consumerGroup,
        final String clientId) throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        ClientChannelInfo clientChannelInfo = this.brokerController.getConsumerManager().findChannel(consumerGroup, clientId);

        //没有数据
        if (null == clientChannelInfo) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("The Consumer <%s> <%s> not online", consumerGroup, clientId));
            return response;
        }

        //低于某个版本 不具备该功能
        if (clientChannelInfo.getVersion() < MQVersion.Version.V3_1_8_SNAPSHOT.ordinal()) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("The Consumer <%s> Version <%s> too low to finish, please upgrade it to V3_1_8_SNAPSHOT",
                clientId,
                MQVersion.getVersionDesc(clientChannelInfo.getVersion())));
            return response;
        }

        try {
            RemotingCommand newRequest = RemotingCommand.createRequestCommand(requestCode, null);
            newRequest.setExtFields(request.getExtFields());
            newRequest.setBody(request.getBody());

            return this.brokerController.getBroker2Client().callClient(clientChannelInfo.getChannel(), newRequest);
        } catch (RemotingTimeoutException e) {
            response.setCode(ResponseCode.CONSUME_MSG_TIMEOUT);
            response
                .setRemark(String.format("consumer <%s> <%s> Timeout: %s", consumerGroup, clientId, RemotingHelper.exceptionSimpleDesc(e)));
            return response;
        } catch (Exception e) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(
                String.format("invoke consumer <%s> <%s> Exception: %s", consumerGroup, clientId, RemotingHelper.exceptionSimpleDesc(e)));
            return response;
        }
    }

    //查询消费者队列
    private RemotingCommand queryConsumeQueue(ChannelHandlerContext ctx,
        RemotingCommand request) throws RemotingCommandException {
        QueryConsumeQueueRequestHeader requestHeader =
            (QueryConsumeQueueRequestHeader) request.decodeCommandCustomHeader(QueryConsumeQueueRequestHeader.class);

        RemotingCommand response = RemotingCommand.createResponseCommand(null);

        ConsumeQueue consumeQueue = this.brokerController.getMessageStore().getConsumeQueue(requestHeader.getTopic(),
            requestHeader.getQueueId());
        if (consumeQueue == null) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("%d@%s is not exist!", requestHeader.getQueueId(), requestHeader.getTopic()));
            return response;
        }

        QueryConsumeQueueResponseBody body = new QueryConsumeQueueResponseBody();
        response.setCode(ResponseCode.SUCCESS);
        response.setBody(body.encode());

        body.setMaxQueueIndex(consumeQueue.getMaxOffsetInQueue());
        body.setMinQueueIndex(consumeQueue.getMinOffsetInQueue());

        MessageFilter messageFilter = null;
        if (requestHeader.getConsumerGroup() != null) {
            SubscriptionData subscriptionData = this.brokerController.getConsumerManager().findSubscriptionData(
                requestHeader.getConsumerGroup(), requestHeader.getTopic()
            );
            body.setSubscriptionData(subscriptionData);
            if (subscriptionData == null) {
                body.setFilterData(String.format("%s@%s is not online!", requestHeader.getConsumerGroup(), requestHeader.getTopic()));
            } else {
                //获取对应的拦截对象
                ConsumerFilterData filterData = this.brokerController.getConsumerFilterManager()
                    .get(requestHeader.getTopic(), requestHeader.getConsumerGroup());
                //设置拦截数据
                body.setFilterData(JSON.toJSONString(filterData, true));

                //初始化拦截器
                messageFilter = new ExpressionMessageFilter(subscriptionData, filterData,
                    this.brokerController.getConsumerFilterManager());
            }
        }

        SelectMappedBufferResult result = consumeQueue.getIndexBuffer(requestHeader.getIndex());
        if (result == null) {
            response.setRemark(String.format("Index %d of %d@%s is not exist!", requestHeader.getIndex(), requestHeader.getQueueId(), requestHeader.getTopic()));
            return response;
        }
        try {
            //创建单元数据列表
            List<ConsumeQueueData> queues = new ArrayList<>();
            for (int i = 0; i < result.getSize() && i < requestHeader.getCount() * ConsumeQueue.CQ_STORE_UNIT_SIZE; i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                //获取单元数据
                ConsumeQueueData one = new ConsumeQueueData();
                one.setPhysicOffset(result.getByteBuffer().getLong());
                one.setPhysicSize(result.getByteBuffer().getInt());
                one.setTagsCode(result.getByteBuffer().getLong());

                //检测 这个tagscode是不是地址
                if (!consumeQueue.isExtAddr(one.getTagsCode())) {
                    queues.add(one);
                    continue;
                }

                //将地址兑换成偏移量并设置到对象中
                ConsumeQueueExt.CqExtUnit cqExtUnit = consumeQueue.getExt(one.getTagsCode());
                if (cqExtUnit != null) {
                    one.setExtendDataJson(JSON.toJSONString(cqExtUnit));
                    if (cqExtUnit.getFilterBitMap() != null) {
                        one.setBitMap(BitsArray.create(cqExtUnit.getFilterBitMap()).toString());
                    }
                    if (messageFilter != null) {
                        //这里代表是否通过拦截
                        one.setEval(messageFilter.isMatchedByConsumeQueue(cqExtUnit.getTagsCode(), cqExtUnit));
                    }
                } else {
                    one.setMsg("Cq extend not exist!addr: " + one.getTagsCode());
                }

                queues.add(one);
            }
            body.setQueueData(queues);
        } finally {
            result.release();
        }

        return response;
    }
}
