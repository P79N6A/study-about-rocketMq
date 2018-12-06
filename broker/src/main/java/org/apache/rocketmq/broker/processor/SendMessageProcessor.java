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

import io.netty.channel.ChannelHandlerContext;
import org.apache.rocketmq.broker.BrokerController;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageContext;
import org.apache.rocketmq.broker.mqtrace.ConsumeMessageHook;
import org.apache.rocketmq.broker.mqtrace.SendMessageContext;
import org.apache.rocketmq.common.MQVersion;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.UtilAll;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageExtBatch;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.ConsumerSendMsgBackRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.common.subscription.SubscriptionGroupConfig;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.config.StorePathConfigHelper;
import org.apache.rocketmq.store.stats.BrokerStatsManager;

import java.net.SocketAddress;
import java.util.List;
import java.util.Map;

//发送消息的处理器
public class SendMessageProcessor extends AbstractSendMessageProcessor implements NettyRequestProcessor {

    //一组钩子对象
    private List<ConsumeMessageHook> consumeMessageHookList;

    public SendMessageProcessor(final BrokerController brokerController) {
        super(brokerController);
    }

    //处理请求对象
    @Override
    public RemotingCommand processRequest(ChannelHandlerContext ctx,
                                          RemotingCommand request) throws RemotingCommandException {
        SendMessageContext mqtraceContext;
        switch (request.getCode()) {
            //将之前的消息重新写入
            case RequestCode.CONSUMER_SEND_MSG_BACK:
                return this.consumerSendMsgBack(ctx, request);
            default:
                //创建请求头对象
                SendMessageRequestHeader requestHeader = parseRequestHeader(request);
                if (requestHeader == null) {
                    return null;
                }

                //构建上下文对象
                mqtraceContext = buildMsgContext(ctx, requestHeader);
                this.executeSendMessageHookBefore(ctx, request, mqtraceContext);

                RemotingCommand response;
                //是否是批量消息
                if (requestHeader.isBatch()) {
                    //处理批量消息  批量消息 是不支持重读的
                    response = this.sendBatchMessage(ctx, request, mqtraceContext, requestHeader);
                } else {
                    //处理单条消息
                    response = this.sendMessage(ctx, request, mqtraceContext, requestHeader);
                }

                //处理后的钩子
                this.executeSendMessageHookAfter(response, mqtraceContext);
                return response;
        }
    }

    @Override
    public boolean rejectRequest() {
        return this.brokerController.getMessageStore().isOSPageCacheBusy() ||
            this.brokerController.getMessageStore().isTransientStorePoolDeficient();
    }

    //将之前的消息重写
    private RemotingCommand consumerSendMsgBack(final ChannelHandlerContext ctx, final RemotingCommand request)
        throws RemotingCommandException {
        final RemotingCommand response = RemotingCommand.createResponseCommand(null);
        //将请求 解析成对应的 参数对象
        final ConsumerSendMsgBackRequestHeader requestHeader =
            (ConsumerSendMsgBackRequestHeader)request.decodeCommandCustomHeader(ConsumerSendMsgBackRequestHeader.class);

        //如果有消息钩子 且原始消息id 不为空 原始消息 是之前拉取消息时 设置的消息id
        if (this.hasConsumeMessageHook() && !UtilAll.isBlank(requestHeader.getOriginMsgId())) {

            //创建上下文对象 执行回调
            ConsumeMessageContext context = new ConsumeMessageContext();
            context.setConsumerGroup(requestHeader.getGroup());
            context.setTopic(requestHeader.getOriginTopic());
            //交易类型为 再次消费申请
            context.setCommercialRcvStats(BrokerStatsManager.StatsType.SEND_BACK);
            context.setCommercialRcvTimes(1);
            //设置 交易所有者？？？ 这个值应该没有设置吧
            context.setCommercialOwner(request.getExtFields().get(BrokerStatsManager.COMMERCIAL_OWNER));

            this.executeConsumeMessageHookAfter(context);
        }

        //通过组名找到 订阅组配置  找不到订阅信息就返回错误提示
        SubscriptionGroupConfig subscriptionGroupConfig =
            this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(requestHeader.getGroup());
        if (null == subscriptionGroupConfig) {
            response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
            response.setRemark("subscription group not exist, " + requestHeader.getGroup() + " "
                + FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST));
            return response;
        }

        //获取 broker 权限 如果没有可写权限就 错误 在发送这个请求时  要求了 broker地址是 master
        if (!PermName.isWriteable(this.brokerController.getBrokerConfig().getBrokerPermission())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1() + "] sending message is forbidden");
            return response;
        }

        //如果 没有重试队列数 那请求应该就结束了
        if (subscriptionGroupConfig.getRetryQueueNums() <= 0) {
            response.setCode(ResponseCode.SUCCESS);
            response.setRemark(null);
            return response;
        }

        //创建重读的 topic  根据重读的队列数生成一个 id  重读 的 topic 是基于 group 的 而不是 基于 topic的
        String newTopic = MixAll.getRetryTopic(requestHeader.getGroup());
        //获取到 可重读的 队列数 并随机获取一个
        int queueIdInt = Math.abs(this.random.nextInt() % 99999999) % subscriptionGroupConfig.getRetryQueueNums();

        //这个标识是什么意思???
        int topicSysFlag = 0;
        if (requestHeader.isUnitMode()) {
            topicSysFlag = TopicSysFlag.buildSysFlag(false, true);
        }

        //创建新的主题配置对象  这里会同步到nsv 上 并将创建的 topicConfig 返回
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(
            newTopic,
            subscriptionGroupConfig.getRetryQueueNums(),
            PermName.PERM_WRITE | PermName.PERM_READ, topicSysFlag);
        //没有生成合适的 topicconfig 就返回错误
        if (null == topicConfig) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("topic[" + newTopic + "] not exist");
            return response;
        }

        //不具备 写权限 提示权限不够  创建的参数 都是带读写 了 这个一般不会出问题  这个 应该是 本 broker上的 关于 本 topic 的 消息 是否具备写权限 如果不能写 就不能满足重读
        //看来 重读的 时候必须要把重读的信息 再次写入到commitLog 中 这样才好追踪哪个消息是重写过的
        if (!PermName.isWriteable(topicConfig.getPerm())) {
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark(String.format("the topic[%s] sending message is forbidden", newTopic));
            return response;
        }

        //根据 请求头的offset 获取消息对象 消息读取不到也是异常 因为发送消息的时候在commitLog 上记录了消息长度
        //这里获取分为2步  第一步 从消息头上 获取这段消息的长度 第二步 根据长度 获取完整数据
        //这里是在 确认 之前的消息是否 真的写入了
        MessageExt msgExt = this.brokerController.getMessageStore().lookMessageByOffset(requestHeader.getOffset());
        if (null == msgExt) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("look message by offset failed, " + requestHeader.getOffset());
            return response;
        }

        //给新消息 设置 一个 重读的标识 代表这个消息 是重写入的  retry-topic作为一个 消息的属性 而 retrygroup 才是真正的topic
        final String retryTopic = msgExt.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
        if (null == retryTopic) {
            MessageAccessor.putProperty(msgExt, MessageConst.PROPERTY_RETRY_TOPIC, msgExt.getTopic());
        }
        //不等待消息 存储完成 那就是 异步写入
        msgExt.setWaitStoreMsgOK(false);

        //延迟 频率 就是 用户 在 处理消息时 设置的  默认是0
        int delayLevel = requestHeader.getDelayLevel();

        //获取 最大的重读次数  现在还不确定这个订阅组配置是 broker 设置的 还是 从nsv上获取的
        int maxReconsumeTimes = subscriptionGroupConfig.getRetryMaxTimes();
        if (request.getVersion() >= MQVersion.Version.V3_4_9.ordinal()) {
            //新版本 重试次数 是 consumer 上设置的 从那里取过来
            maxReconsumeTimes = requestHeader.getMaxReconsumeTimes();
        }

        //已经超过了最大重试次数 或者 是 -1  -1 就是 直接加入 死信队列
        if (msgExt.getReconsumeTimes() >= maxReconsumeTimes
            || delayLevel < 0) {
            //将topic 设置成 死信
            newTopic = MixAll.getDLQTopic(requestHeader.getGroup());
            queueIdInt = Math.abs(this.random.nextInt() % 99999999) % DLQ_NUMS_PER_GROUP;

            //创建死信的 topic 信息
            topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(newTopic,
                DLQ_NUMS_PER_GROUP,
                PermName.PERM_WRITE, 0
            );
            if (null == topicConfig) {
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("topic[" + newTopic + "] not exist");
                return response;
            }
        } else {
            //当延迟级别 是0 的时候 就是由broker来控制 延迟级别
            if (0 == delayLevel) {
                //根据重试次数 + 3 获取延迟级别
                delayLevel = 3 + msgExt.getReconsumeTimes();
            }

            msgExt.setDelayTimeLevel(delayLevel);
        }

        //创建 broker内部自用的 消息对象
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        //使用新的topic 写入消息
        msgInner.setTopic(newTopic);
        msgInner.setBody(msgExt.getBody());
        msgInner.setFlag(msgExt.getFlag());
        MessageAccessor.setProperties(msgInner, msgExt.getProperties());
        msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));
        msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(null, msgExt.getTags()));

        //跟这条消息对应的 mq id
        msgInner.setQueueId(queueIdInt);
        msgInner.setSysFlag(msgExt.getSysFlag());
        msgInner.setBornTimestamp(msgExt.getBornTimestamp());
        msgInner.setBornHost(msgExt.getBornHost());
        msgInner.setStoreHost(this.getStoreHost());
        msgInner.setReconsumeTimes(msgExt.getReconsumeTimes() + 1);

        String originMsgId = MessageAccessor.getOriginMessageId(msgExt);
        MessageAccessor.setOriginMessageId(msgInner, UtilAll.isBlank(originMsgId) ? msgExt.getMsgId() : originMsgId);

        //将消息保存  这里将 消费者消费的消息 又重新保存到 broker 了 就是为了被重试队列再次消费
        PutMessageResult putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
        if (putMessageResult != null) {
            switch (putMessageResult.getPutMessageStatus()) {
                case PUT_OK:
                    //成功保存
                    String backTopic = msgExt.getTopic();
                    String correctTopic = msgExt.getProperty(MessageConst.PROPERTY_RETRY_TOPIC);
                    if (correctTopic != null) {
                        backTopic = correctTopic;
                    }

                    //记录 指定 topic 下的 重写入次数
                    this.brokerController.getBrokerStatsManager().incSendBackNums(requestHeader.getGroup(), backTopic);

                    response.setCode(ResponseCode.SUCCESS);
                    response.setRemark(null);

                    return response;
                default:
                    break;
            }

            //失败
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(putMessageResult.getPutMessageStatus().name());
            return response;
        }

        response.setCode(ResponseCode.SYSTEM_ERROR);
        response.setRemark("putMessageResult is null");
        return response;
    }

    //处理死信 和 retry 消息  这里是生产者第一次发来的消息 是怎么处理retry 消息呢  之前对retry的认知是 重复消费消息 当消费成功后 在处理back请求时 再次加入到 broker中 现在看看怎么处理
    private boolean handleRetryAndDLQ(SendMessageRequestHeader requestHeader, RemotingCommand response,
                                      RemotingCommand request,
                                      MessageExt msg, TopicConfig topicConfig) {
        //获取主题
        String newTopic = requestHeader.getTopic();
        //以 retry开头的主题
        if (null != newTopic && newTopic.startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
            //获取真正的组名 等下 topic 和 组名 不是对应关系吧  可能 组名 和 topic 就是一样的
            String groupName = newTopic.substring(MixAll.RETRY_GROUP_TOPIC_PREFIX.length());
            //根据topic 获取订阅消息
            SubscriptionGroupConfig subscriptionGroupConfig =
                this.brokerController.getSubscriptionGroupManager().findSubscriptionGroupConfig(groupName);

            //订阅消息配置不存在 不能处理
            if (null == subscriptionGroupConfig) {
                response.setCode(ResponseCode.SUBSCRIPTION_GROUP_NOT_EXIST);
                response.setRemark(
                    "subscription group not exist, " + groupName + " " + FAQUrl.suggestTodo(FAQUrl.SUBSCRIPTION_GROUP_NOT_EXIST));
                return false;
            }

            //重新消费次数
            int maxReconsumeTimes = subscriptionGroupConfig.getRetryMaxTimes();
            //高于3.4.9版本 使用请求头中的 重试次数
            if (request.getVersion() >= MQVersion.Version.V3_4_9.ordinal()) {
                maxReconsumeTimes = requestHeader.getMaxReconsumeTimes();
            }
            //获取当前重新消费次数
            int reconsumeTimes = requestHeader.getReconsumeTimes() == null ? 0 : requestHeader.getReconsumeTimes();
            //超过了 最大的消费次数 那么这条消息就被看做是 死信
            if (reconsumeTimes >= maxReconsumeTimes) {
                //创建 死信主题
                newTopic = MixAll.getDLQTopic(groupName);
                //生成随机的id 这个id 不超过 每个死信组的 最大队列数量  id到底怎么用 以及 为什么是这样的限制???
                int queueIdInt = Math.abs(this.random.nextInt() % 99999999) % DLQ_NUMS_PER_GROUP;
                //创建指定的 topic
                topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(newTopic,
                    DLQ_NUMS_PER_GROUP,
                    PermName.PERM_WRITE, 0
                );
                //将消息设置成死信
                msg.setTopic(newTopic);
                msg.setQueueId(queueIdInt);
                //创建 topicConfig 失败 就是系统异常
                if (null == topicConfig) {
                    response.setCode(ResponseCode.SYSTEM_ERROR);
                    response.setRemark("topic[" + newTopic + "] not exist");
                    return false;
                }
            }
        }
        int sysFlag = requestHeader.getSysFlag();
        if (TopicFilterType.MULTI_TAG == topicConfig.getTopicFilterType()) {
            sysFlag |= MessageSysFlag.MULTI_TAGS_FLAG;
        }
        msg.setSysFlag(sysFlag);
        return true;
    }

    //处理单条消息的逻辑
    private RemotingCommand sendMessage(final ChannelHandlerContext ctx,
                                        final RemotingCommand request,
                                        final SendMessageContext sendMessageContext,
                                        final SendMessageRequestHeader requestHeader) throws RemotingCommandException {

        //创建并获取 请求头对象
        final RemotingCommand response = RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);
        final SendMessageResponseHeader responseHeader = (SendMessageResponseHeader)response.readCustomHeader();

        //设置唯一标识
        response.setOpaque(request.getOpaque());

        response.addExtField(MessageConst.PROPERTY_MSG_REGION, this.brokerController.getBrokerConfig().getRegionId());
        response.addExtField(MessageConst.PROPERTY_TRACE_SWITCH, String.valueOf(this.brokerController.getBrokerConfig().isTraceOn()));

        log.debug("receive SendMessage request command, {}", request);

        final long startTimstamp = this.brokerController.getBrokerConfig().getStartAcceptSendRequestTimeStamp();
        if (this.brokerController.getMessageStore().now() < startTimstamp) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("broker unable to service, until %s", UtilAll.timeMillisToHumanString2(startTimstamp)));
            return response;
        }

        response.setCode(-1);
        super.msgCheck(ctx, requestHeader, response);
        if (response.getCode() != -1) {
            return response;
        }

        final byte[] body = request.getBody();

        int queueIdInt = requestHeader.getQueueId();
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());

        if (queueIdInt < 0) {
            queueIdInt = Math.abs(this.random.nextInt() % 99999999) % topicConfig.getWriteQueueNums();
        }

        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(requestHeader.getTopic());
        msgInner.setQueueId(queueIdInt);

        //到上面为止 都和 处理批数据一样

        //如果处理 重复消息 和 死信 失败 返回结果  这里是看消息是否是 retry 消息 然后 如果超过最大的消费次数 就标记成死信
        //消息 从生产者那里创建的时候就可以确实 是否是 可重复消费的消息了 然后消费者消费后 发送back 到broker 如果发现是retry消息 又保存到broker 为下一次被消费做准备
        if (!handleRetryAndDLQ(requestHeader, response, request, msgInner, topicConfig)) {
            //当处理失败的时候在 response 中以及设置了 异常原因了 这里可以直接返回
            return response;
        }

        //继续设置 消息体对象
        msgInner.setBody(body);
        msgInner.setFlag(requestHeader.getFlag());
        MessageAccessor.setProperties(msgInner, MessageDecoder.string2messageProperties(requestHeader.getProperties()));
        msgInner.setPropertiesString(requestHeader.getProperties());
        msgInner.setBornTimestamp(requestHeader.getBornTimestamp());
        msgInner.setBornHost(ctx.channel().remoteAddress());
        msgInner.setStoreHost(this.getStoreHost());
        msgInner.setReconsumeTimes(requestHeader.getReconsumeTimes() == null ? 0 : requestHeader.getReconsumeTimes());
        PutMessageResult putMessageResult = null;
        //获取 请求头中的属性
        Map<String, String> oriProps = MessageDecoder.string2messageProperties(requestHeader.getProperties());
        String traFlag = oriProps.get(MessageConst.PROPERTY_TRANSACTION_PREPARED);
        if (traFlag != null && Boolean.parseBoolean(traFlag)) {
            //这里 有可能 会拒绝事务消息
            if (this.brokerController.getBrokerConfig().isRejectTransactionMessage()) {
                response.setCode(ResponseCode.NO_PERMISSION);
                response.setRemark(
                    "the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1()
                        + "] sending transaction message is forbidden");
                return response;
            }
            //针对死信 和 retry 的消息处理也在里面
            //是事务消息 采用特殊的 处理方式
            putMessageResult = this.brokerController.getTransactionalMessageService().prepareMessage(msgInner);
        } else {
            //普通消息
            putMessageResult = this.brokerController.getMessageStore().putMessage(msgInner);
        }

        //一样要处理 结果 触发钩子 给response 设置参数
        return handlePutMessageResult(putMessageResult, response, request, msgInner, responseHeader, sendMessageContext, ctx, queueIdInt);

    }

    //处理 putmessage 的结果
    private RemotingCommand handlePutMessageResult(PutMessageResult putMessageResult, RemotingCommand response,
                                                   RemotingCommand request, MessageExt msg,
                                                   SendMessageResponseHeader responseHeader, SendMessageContext sendMessageContext, ChannelHandlerContext ctx,
                                                   int queueIdInt) {
        //如果传入的 结果为null 就是系统异常
        if (putMessageResult == null) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark("store putMessage return null");
            return response;
        }
        boolean sendOK = false;

        //根据 结果的不同 执行不同的逻辑
        switch (putMessageResult.getPutMessageStatus()) {
            // Success
            case PUT_OK:
                sendOK = true;
                response.setCode(ResponseCode.SUCCESS);
                break;
            case FLUSH_DISK_TIMEOUT:
                //刷盘超时
                response.setCode(ResponseCode.FLUSH_DISK_TIMEOUT);
                sendOK = true;
                break;
            case FLUSH_SLAVE_TIMEOUT:
                //slave 不是不能写入吗???
                response.setCode(ResponseCode.FLUSH_SLAVE_TIMEOUT);
                sendOK = true;
                break;
            case SLAVE_NOT_AVAILABLE:
                response.setCode(ResponseCode.SLAVE_NOT_AVAILABLE);
                sendOK = true;
                break;

            // Failed
            //创建映射文件失败
            case CREATE_MAPEDFILE_FAILED:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("create mapped file failed, server is busy or broken.");
                break;
            case MESSAGE_ILLEGAL:
                //属性过长
            case PROPERTIES_SIZE_EXCEEDED:
                response.setCode(ResponseCode.MESSAGE_ILLEGAL);
                response.setRemark(
                    "the message is illegal, maybe msg body or properties length not matched. msg body length limit 128k, msg properties length limit 32k.");
                break;
            case SERVICE_NOT_AVAILABLE:
                response.setCode(ResponseCode.SERVICE_NOT_AVAILABLE);
                response.setRemark(
                    "service not available now, maybe disk full, " + diskUtil() + ", maybe your broker machine memory too small.");
                break;
                //操作系统繁忙???
            case OS_PAGECACHE_BUSY:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("[PC_SYNCHRONIZED]broker busy, start flow control for a while");
                break;
            case UNKNOWN_ERROR:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UNKNOWN_ERROR");
                break;
            default:
                response.setCode(ResponseCode.SYSTEM_ERROR);
                response.setRemark("UNKNOWN_ERROR DEFAULT");
                break;
        }

        //设置交易所有者
        String owner = request.getExtFields().get(BrokerStatsManager.COMMERCIAL_OWNER);
        if (sendOK) {

            //给状态 管理器 增加属性
            this.brokerController.getBrokerStatsManager().incTopicPutNums(msg.getTopic(), putMessageResult.getAppendMessageResult().getMsgNum(), 1);
            this.brokerController.getBrokerStatsManager().incTopicPutSize(msg.getTopic(),
                putMessageResult.getAppendMessageResult().getWroteBytes());
            this.brokerController.getBrokerStatsManager().incBrokerPutNums(putMessageResult.getAppendMessageResult().getMsgNum());

            //给 响应结果设置
            response.setRemark(null);

            responseHeader.setMsgId(putMessageResult.getAppendMessageResult().getMsgId());
            responseHeader.setQueueId(queueIdInt);
            responseHeader.setQueueOffset(putMessageResult.getAppendMessageResult().getLogicsOffset());

            //发送响应结果  这里就是交给 netty  pipeline 的下一个节点
            doResponse(ctx, request, response);

            if (hasSendMessageHook()) {
                sendMessageContext.setMsgId(responseHeader.getMsgId());
                sendMessageContext.setQueueId(responseHeader.getQueueId());
                sendMessageContext.setQueueOffset(responseHeader.getQueueOffset());

                //这里设置的值 还不理解???
                //获取交易数量
                int commercialBaseCount = brokerController.getBrokerConfig().getCommercialBaseCount();
                //获取 写了多少字节
                int wroteSize = putMessageResult.getAppendMessageResult().getWroteBytes();
                //size-per-count 应该是一个单位 把写入的字节数换算成另一个东西  在乘上 交易数量
                int incValue = (int)Math.ceil(wroteSize / BrokerStatsManager.SIZE_PER_COUNT) * commercialBaseCount;

                sendMessageContext.setCommercialSendStats(BrokerStatsManager.StatsType.SEND_SUCCESS);
                sendMessageContext.setCommercialSendTimes(incValue);
                sendMessageContext.setCommercialSendSize(wroteSize);
                sendMessageContext.setCommercialOwner(owner);
            }
            return null;
        } else {
            if (hasSendMessageHook()) {
                int wroteSize = request.getBody().length;
                int incValue = (int)Math.ceil(wroteSize / BrokerStatsManager.SIZE_PER_COUNT);

                sendMessageContext.setCommercialSendStats(BrokerStatsManager.StatsType.SEND_FAILURE);
                sendMessageContext.setCommercialSendTimes(incValue);
                sendMessageContext.setCommercialSendSize(wroteSize);
                sendMessageContext.setCommercialOwner(owner);
            }
        }
        return response;
    }

    //处理 收到生产者批量消息的请求
    private RemotingCommand sendBatchMessage(final ChannelHandlerContext ctx,
                                             final RemotingCommand request,
                                             final SendMessageContext sendMessageContext,
                                             final SendMessageRequestHeader requestHeader) throws RemotingCommandException {

        //创建 响应对象
        final RemotingCommand response = RemotingCommand.createResponseCommand(SendMessageResponseHeader.class);
        //获取消息头对象
        final SendMessageResponseHeader responseHeader = (SendMessageResponseHeader)response.readCustomHeader();

        //请求的标识
        response.setOpaque(request.getOpaque());

        //设置消息的区域和 trace  这个还不知道什么意思？？？
        response.addExtField(MessageConst.PROPERTY_MSG_REGION, this.brokerController.getBrokerConfig().getRegionId());
        response.addExtField(MessageConst.PROPERTY_TRACE_SWITCH, String.valueOf(this.brokerController.getBrokerConfig().isTraceOn()));

        log.debug("Receive SendMessage request command {}", request);

        //开始接受消息的时间戳  这个是不是在哪里要设置啊
        final long startTimstamp = this.brokerController.getBrokerConfig().getStartAcceptSendRequestTimeStamp();
        //返回当前时间
        if (this.brokerController.getMessageStore().now() < startTimstamp) {
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(String.format("broker unable to service, until %s", UtilAll.timeMillisToHumanString2(startTimstamp)));
            return response;
        }

        response.setCode(-1);
        //交由父类检查数据
        super.msgCheck(ctx, requestHeader, response);
        if (response.getCode() != -1) {
            return response;
        }

        int queueIdInt = requestHeader.getQueueId();
        //获取 主题配置
        TopicConfig topicConfig = this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());

        //设置成 可写队列的数量
        if (queueIdInt < 0) {
            queueIdInt = Math.abs(this.random.nextInt() % 99999999) % topicConfig.getWriteQueueNums();
        }

        //标题过长
        if (requestHeader.getTopic().length() > Byte.MAX_VALUE) {
            response.setCode(ResponseCode.MESSAGE_ILLEGAL);
            response.setRemark("message topic length too long " + requestHeader.getTopic().length());
            return response;
        }

        //如果是 retry 的 就是错误的  批量消息 不支持 retry
        if (requestHeader.getTopic() != null && requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
            response.setCode(ResponseCode.MESSAGE_ILLEGAL);
            response.setRemark("batch request does not support retry group " + requestHeader.getTopic());
            return response;
        }
        //创建批量消息体
        MessageExtBatch messageExtBatch = new MessageExtBatch();
        messageExtBatch.setTopic(requestHeader.getTopic());
        messageExtBatch.setQueueId(queueIdInt);

        int sysFlag = requestHeader.getSysFlag();
        //如果有 多个标签就设置
        if (TopicFilterType.MULTI_TAG == topicConfig.getTopicFilterType()) {
            sysFlag |= MessageSysFlag.MULTI_TAGS_FLAG;
        }
        messageExtBatch.setSysFlag(sysFlag);

        messageExtBatch.setFlag(requestHeader.getFlag());
        //从请求头中 获取属性 到消息组中
        MessageAccessor.setProperties(messageExtBatch, MessageDecoder.string2messageProperties(requestHeader.getProperties()));
        messageExtBatch.setBody(request.getBody());
        messageExtBatch.setBornTimestamp(requestHeader.getBornTimestamp());
        //将生产者的地址设置到 发送地址
        messageExtBatch.setBornHost(ctx.channel().remoteAddress());
        messageExtBatch.setStoreHost(this.getStoreHost());
        //第几次消费
        messageExtBatch.setReconsumeTimes(requestHeader.getReconsumeTimes() == null ? 0 : requestHeader.getReconsumeTimes());

        //将消息保存到 broker中
        PutMessageResult putMessageResult = this.brokerController.getMessageStore().putMessages(messageExtBatch);

        //处理结果  就是在broker中心的 属性中 修改一些参数 并且 设置一下 response
        return handlePutMessageResult(putMessageResult, response, request, messageExtBatch, responseHeader, sendMessageContext, ctx, queueIdInt);
    }

    public boolean hasConsumeMessageHook() {
        return consumeMessageHookList != null && !this.consumeMessageHookList.isEmpty();
    }

    public void executeConsumeMessageHookAfter(final ConsumeMessageContext context) {
        if (hasConsumeMessageHook()) {
            for (ConsumeMessageHook hook : this.consumeMessageHookList) {
                try {
                    hook.consumeMessageAfter(context);
                } catch (Throwable e) {
                    // Ignore
                }
            }
        }
    }

    public SocketAddress getStoreHost() {
        return storeHost;
    }

    //就是打印3个 使用率
    private String diskUtil() {
        //获取 物理地址？？？
        String storePathPhysic = this.brokerController.getMessageStoreConfig().getStorePathCommitLog();
        //获取 物理 使用率 就是当前使用空间/ 总可用空间
        double physicRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathPhysic);

        //获取日志路径
        String storePathLogis =
            StorePathConfigHelper.getStorePathConsumeQueue(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
        //获取物理使用率
        double logisRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathLogis);

        String storePathIndex =
            StorePathConfigHelper.getStorePathIndex(this.brokerController.getMessageStoreConfig().getStorePathRootDir());
        double indexRatio = UtilAll.getDiskPartitionSpaceUsedPercent(storePathIndex);

        return String.format("CL: %5.2f CQ: %5.2f INDEX: %5.2f", physicRatio, logisRatio, indexRatio);
    }

    public void registerConsumeMessageHook(List<ConsumeMessageHook> consumeMessageHookList) {
        this.consumeMessageHookList = consumeMessageHookList;
    }
}
