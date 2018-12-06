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
import org.apache.rocketmq.broker.mqtrace.SendMessageContext;
import org.apache.rocketmq.broker.mqtrace.SendMessageHook;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.DBMsgConstants;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.help.FAQUrl;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.protocol.RequestCode;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeader;
import org.apache.rocketmq.common.protocol.header.SendMessageRequestHeaderV2;
import org.apache.rocketmq.common.protocol.header.SendMessageResponseHeader;
import org.apache.rocketmq.common.sysflag.MessageSysFlag;
import org.apache.rocketmq.common.sysflag.TopicSysFlag;
import org.apache.rocketmq.common.utils.ChannelUtil;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingCommandException;
import org.apache.rocketmq.remoting.netty.NettyRequestProcessor;
import org.apache.rocketmq.remoting.protocol.RemotingCommand;
import org.apache.rocketmq.store.MessageExtBrokerInner;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.List;
import java.util.Map;
import java.util.Random;

//默认的 远程请求 处理对象
public abstract class AbstractSendMessageProcessor implements NettyRequestProcessor {
    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.BROKER_LOGGER_NAME);

    protected final static int DLQ_NUMS_PER_GROUP = 1;
    //组合一个 broker 控制器
    protected final BrokerController brokerController;
    protected final Random random = new Random(System.currentTimeMillis());
    //仓库地址???
    protected final SocketAddress storeHost;
    //发送消息的 钩子
    private List<SendMessageHook> sendMessageHookList;

    public AbstractSendMessageProcessor(final BrokerController brokerController) {
        this.brokerController = brokerController;
        this.storeHost =
            new InetSocketAddress(brokerController.getBrokerConfig().getBrokerIP1(), brokerController
                .getNettyServerConfig().getListenPort());
    }

    //创建消息上下文对象
    protected SendMessageContext buildMsgContext(ChannelHandlerContext ctx,
        SendMessageRequestHeader requestHeader) {
        //没有钩子就不需要构建 被处理的对象了
        if (!this.hasSendMessageHook()) {
            return null;
        }
        SendMessageContext mqtraceContext;
        mqtraceContext = new SendMessageContext();
        //从请求头中获取消息
        mqtraceContext.setProducerGroup(requestHeader.getProducerGroup());
        mqtraceContext.setTopic(requestHeader.getTopic());
        mqtraceContext.setMsgProps(requestHeader.getProperties());
        //解析 channel 中的地址  在初始化 channel 的时候 就保存了jdk 的channel 里面有地址
        mqtraceContext.setBornHost(RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
        //broker 是从broker 控制器获得的
        mqtraceContext.setBrokerAddr(this.brokerController.getBrokerAddr());
        //获取 地区信息
        mqtraceContext.setBrokerRegionId(this.brokerController.getBrokerConfig().getRegionId());
        mqtraceContext.setBornTimeStamp(requestHeader.getBornTimestamp());

        //解析请求头 中的属性
        Map<String, String> properties = MessageDecoder.string2messageProperties(requestHeader.getProperties());
        String uniqueKey = properties.get(MessageConst.PROPERTY_UNIQ_CLIENT_MESSAGE_ID_KEYIDX);
        //增加2个属性
        properties.put(MessageConst.PROPERTY_MSG_REGION, this.brokerController.getBrokerConfig().getRegionId());
        properties.put(MessageConst.PROPERTY_TRACE_SWITCH, String.valueOf(this.brokerController.getBrokerConfig().isTraceOn()));
        requestHeader.setProperties(MessageDecoder.messageProperties2String(properties));

        if (uniqueKey == null) {
            uniqueKey = "";
        }
        mqtraceContext.setMsgUniqueKey(uniqueKey);
        return mqtraceContext;
    }

    public boolean hasSendMessageHook() {
        return sendMessageHookList != null && !this.sendMessageHookList.isEmpty();
    }

    //创建内部消息
    protected MessageExtBrokerInner buildInnerMsg(final ChannelHandlerContext ctx,
        final SendMessageRequestHeader requestHeader, final byte[] body, TopicConfig topicConfig) {
        //从请求头中获取 队列id
        int queueIdInt = requestHeader.getQueueId();
        if (queueIdInt < 0) {
            queueIdInt = Math.abs(this.random.nextInt() % 99999999) % topicConfig.getWriteQueueNums();
        }
        int sysFlag = requestHeader.getSysFlag();

        //增加 标识
        if (TopicFilterType.MULTI_TAG == topicConfig.getTopicFilterType()) {
            sysFlag |= MessageSysFlag.MULTI_TAGS_FLAG;
        }

        //创建broker 内部传递的消息体对象
        MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
        msgInner.setTopic(requestHeader.getTopic());
        msgInner.setBody(body);
        msgInner.setFlag(requestHeader.getFlag());
        MessageAccessor.setProperties(msgInner,
            MessageDecoder.string2messageProperties(requestHeader.getProperties()));
        msgInner.setPropertiesString(requestHeader.getProperties());
        msgInner.setTagsCode(MessageExtBrokerInner.tagsString2tagsCode(topicConfig.getTopicFilterType(),
            msgInner.getTags()));

        msgInner.setQueueId(queueIdInt);
        msgInner.setSysFlag(sysFlag);
        msgInner.setBornTimestamp(requestHeader.getBornTimestamp());
        msgInner.setBornHost(ctx.channel().remoteAddress());
        msgInner.setStoreHost(this.getStoreHost());
        msgInner.setReconsumeTimes(requestHeader.getReconsumeTimes() == null ? 0 : requestHeader
            .getReconsumeTimes());
        return msgInner;
    }

    public SocketAddress getStoreHost() {
        return storeHost;
    }

    //检查消息内容 这里怎么同时传入了 request和 response
    protected RemotingCommand msgContentCheck(final ChannelHandlerContext ctx,
        final SendMessageRequestHeader requestHeader, RemotingCommand request,
        final RemotingCommand response) {
        //主题 过大 导致的 非法
        if (requestHeader.getTopic().length() > Byte.MAX_VALUE) {
            log.warn("putMessage message topic length too long {}", requestHeader.getTopic().length());
            response.setCode(ResponseCode.MESSAGE_ILLEGAL);
            return response;
        }
        //属性过大
        if (requestHeader.getProperties() != null && requestHeader.getProperties().length() > Short.MAX_VALUE) {
            log.warn("putMessage message properties length too long {}", requestHeader.getProperties().length());
            response.setCode(ResponseCode.MESSAGE_ILLEGAL);
            return response;
        }
        //消息过大
        if (request.getBody().length > DBMsgConstants.MAX_BODY_SIZE) {
            log.warn(" topic {}  msg body size {}  from {}", requestHeader.getTopic(),
                request.getBody().length, ChannelUtil.getRemoteIp(ctx.channel()));
            response.setRemark("msg body must be less 64KB");
            response.setCode(ResponseCode.MESSAGE_ILLEGAL);
            return response;
        }
        return response;
    }

    //检查消息对象
    protected RemotingCommand msgCheck(final ChannelHandlerContext ctx,
        final SendMessageRequestHeader requestHeader, final RemotingCommand response) {
        //是否具备 写 权限  slave 的节点是 只读节点  且请求头的 主题是 顺序主题
        if (!PermName.isWriteable(this.brokerController.getBrokerConfig().getBrokerPermission())
            && this.brokerController.getTopicConfigManager().isOrderTopic(requestHeader.getTopic())) {
            //无权限
            response.setCode(ResponseCode.NO_PERMISSION);
            response.setRemark("the broker[" + this.brokerController.getBrokerConfig().getBrokerIP1()
                + "] sending message is forbidden");
            return response;
        }
        //这个topic 能否发送消息
        if (!this.brokerController.getTopicConfigManager().isTopicCanSendMessage(requestHeader.getTopic())) {
            String errorMsg = "the topic[" + requestHeader.getTopic() + "] is conflict with system reserved words.";
            log.warn(errorMsg);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(errorMsg);
            return response;
        }

        //根据 topic 返回对应的 topicconfig
        TopicConfig topicConfig =
            this.brokerController.getTopicConfigManager().selectTopicConfig(requestHeader.getTopic());
        if (null == topicConfig) {
            int topicSysFlag = 0;
            if (requestHeader.isUnitMode()) {
                //这个标识还看不懂???
                if (requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    topicSysFlag = TopicSysFlag.buildSysFlag(false, true);
                } else {
                    topicSysFlag = TopicSysFlag.buildSysFlag(true, false);
                }
            }

            log.warn("the topic {} not exist, producer: {}", requestHeader.getTopic(), ctx.channel().remoteAddress());
            //创建新的config
            topicConfig = this.brokerController.getTopicConfigManager().createTopicInSendMessageMethod(
                requestHeader.getTopic(),
                requestHeader.getDefaultTopic(),
                RemotingHelper.parseChannelRemoteAddr(ctx.channel()),
                requestHeader.getDefaultTopicQueueNums(), topicSysFlag);

            //如果创建失败了
            if (null == topicConfig) {
                if (requestHeader.getTopic().startsWith(MixAll.RETRY_GROUP_TOPIC_PREFIX)) {
                    //调用下面的方法创建一个新的config
                    topicConfig =
                        this.brokerController.getTopicConfigManager().createTopicInSendMessageBackMethod(
                            requestHeader.getTopic(), 1, PermName.PERM_WRITE | PermName.PERM_READ,
                            topicSysFlag);
                }
            }

            //如果还是失败就代表异常了
            if (null == topicConfig) {
                response.setCode(ResponseCode.TOPIC_NOT_EXIST);
                response.setRemark("topic[" + requestHeader.getTopic() + "] not exist, apply first please!"
                    + FAQUrl.suggestTodo(FAQUrl.APPLY_TOPIC_URL));
                return response;
            }
        }

        int queueIdInt = requestHeader.getQueueId();
        int idValid = Math.max(topicConfig.getWriteQueueNums(), topicConfig.getReadQueueNums());
        if (queueIdInt >= idValid) {
            String errorInfo = String.format("request queueId[%d] is illegal, %s Producer: %s",
                queueIdInt,
                topicConfig.toString(),
                RemotingHelper.parseChannelRemoteAddr(ctx.channel()));

            log.warn(errorInfo);
            response.setCode(ResponseCode.SYSTEM_ERROR);
            response.setRemark(errorInfo);

            return response;
        }
        return response;
    }

    public void registerSendMessageHook(List<SendMessageHook> sendMessageHookList) {
        this.sendMessageHookList = sendMessageHookList;
    }

    //处理结果
    protected void doResponse(ChannelHandlerContext ctx, RemotingCommand request,
        final RemotingCommand response) {
        //不是一次调用的情况
        if (!request.isOnewayRPC()) {
            try {
                //将结果 写入到netty的pipeline 中 让下个节点处理
                ctx.writeAndFlush(response);
            } catch (Throwable e) {
                log.error("SendMessageProcessor process request over, but response failed", e);
                log.error(request.toString());
                log.error(response.toString());
            }
        }
    }

    public void executeSendMessageHookBefore(final ChannelHandlerContext ctx, final RemotingCommand request,
        SendMessageContext context) {
        if (hasSendMessageHook()) {
            for (SendMessageHook hook : this.sendMessageHookList) {
                try {
                    //反序列化成 请求头对象
                    final SendMessageRequestHeader requestHeader = parseRequestHeader(request);

                    if (null != requestHeader) {
                        context.setProducerGroup(requestHeader.getProducerGroup());
                        context.setTopic(requestHeader.getTopic());
                        context.setBodyLength(request.getBody().length);
                        context.setMsgProps(requestHeader.getProperties());
                        context.setBornHost(RemotingHelper.parseChannelRemoteAddr(ctx.channel()));
                        context.setBrokerAddr(this.brokerController.getBrokerAddr());
                        context.setQueueId(requestHeader.getQueueId());
                    }

                    hook.sendMessageBefore(context);
                    if (requestHeader != null) {
                        requestHeader.setProperties(context.getMsgProps());
                    }
                } catch (Throwable e) {
                    // Ignore
                }
            }
        }
    }

    //创建请求头对象
    protected SendMessageRequestHeader parseRequestHeader(RemotingCommand request)
        throws RemotingCommandException {

        //简易的请求头 如果是 特殊消息就是序列化成这种
        SendMessageRequestHeaderV2 requestHeaderV2 = null;
        SendMessageRequestHeader requestHeader = null;
        switch (request.getCode()) {
            case RequestCode.SEND_BATCH_MESSAGE:
            case RequestCode.SEND_MESSAGE_V2:
                requestHeaderV2 =
                    (SendMessageRequestHeaderV2) request
                        .decodeCommandCustomHeader(SendMessageRequestHeaderV2.class);
            case RequestCode.SEND_MESSAGE:
                if (null == requestHeaderV2) {
                    requestHeader =
                        (SendMessageRequestHeader) request
                            .decodeCommandCustomHeader(SendMessageRequestHeader.class);
                } else {
                    //通过v2 创建v1
                    requestHeader = SendMessageRequestHeaderV2.createSendMessageRequestHeaderV1(requestHeaderV2);
                }
            default:
                break;
        }
        return requestHeader;
    }

    public void executeSendMessageHookAfter(final RemotingCommand response, final SendMessageContext context) {
        if (hasSendMessageHook()) {
            for (SendMessageHook hook : this.sendMessageHookList) {
                try {
                    if (response != null) {
                        final SendMessageResponseHeader responseHeader =
                            (SendMessageResponseHeader) response.readCustomHeader();
                        context.setMsgId(responseHeader.getMsgId());
                        context.setQueueId(responseHeader.getQueueId());
                        context.setQueueOffset(responseHeader.getQueueOffset());
                        context.setCode(response.getCode());
                        context.setErrorMsg(response.getRemark());
                    }
                    hook.sendMessageAfter(context);
                } catch (Throwable e) {
                    // Ignore
                }
            }
        }
    }

    @Override
    public boolean rejectRequest() {
        return false;
    }
}
