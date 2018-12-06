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
package org.apache.rocketmq.client.consumer;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.log.ClientLogger;
import org.apache.rocketmq.common.ThreadFactoryImpl;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

/**
 * Schedule service for pull consumer
 */
//拉消息 的 定时服务
public class MQPullConsumerScheduleService {
    private final InternalLogger log = ClientLogger.getLog();
    //监听器 实现类
    private final MessageQueueListener messageQueueListener = new MessageQueueListenerImpl();
    //队列 和 任务的关系
    private final ConcurrentMap<MessageQueue, PullTaskImpl> taskTable =
        new ConcurrentHashMap<MessageQueue, PullTaskImpl>();
    private DefaultMQPullConsumer defaultMQPullConsumer;
    private int pullThreadNums = 20;
    //回调函数 每个 topic 对应一个
    private ConcurrentMap<String /* topic */, PullTaskCallback> callbackTable =
        new ConcurrentHashMap<String, PullTaskCallback>();
    private ScheduledThreadPoolExecutor scheduledThreadPoolExecutor;

    public MQPullConsumerScheduleService(final String consumerGroup) {
        this.defaultMQPullConsumer = new DefaultMQPullConsumer(consumerGroup);
        //默认是集群模式
        this.defaultMQPullConsumer.setMessageModel(MessageModel.CLUSTERING);
    }

    //设置任务
    public void putTask(String topic, Set<MessageQueue> mqNewSet) {
        Iterator<Entry<MessageQueue, PullTaskImpl>> it = this.taskTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<MessageQueue, PullTaskImpl> next = it.next();
            if (next.getKey().getTopic().equals(topic)) {
                if (!mqNewSet.contains(next.getKey())) {
                    //如果 找不到对应的mq  就关闭这个mq
                    next.getValue().setCancelled(true);
                    it.remove();
                }
            }
        }

        for (MessageQueue mq : mqNewSet) {
            //为每个 mq 设置一个 任务 并保存到容器中
            if (!this.taskTable.containsKey(mq)) {
                PullTaskImpl command = new PullTaskImpl(mq);
                this.taskTable.put(mq, command);
                //启动定时任务
                this.scheduledThreadPoolExecutor.schedule(command, 0, TimeUnit.MILLISECONDS);

            }
        }
    }

    //启动这个对象
    public void start() throws MQClientException {
        final String group = this.defaultMQPullConsumer.getConsumerGroup();
        //根据指定线程数 初始化这个 定时任务对象
        this.scheduledThreadPoolExecutor = new ScheduledThreadPoolExecutor(
            this.pullThreadNums,
            new ThreadFactoryImpl("PullMsgThread-" + group)
        );

        //设置线程处理对象
        this.defaultMQPullConsumer.setMessageQueueListener(this.messageQueueListener);

        this.defaultMQPullConsumer.start();

        log.info("MQPullConsumerScheduleService start OK, {} {}",
            this.defaultMQPullConsumer.getConsumerGroup(), this.callbackTable);
    }

    //设置回调对象
    public void registerPullTaskCallback(final String topic, final PullTaskCallback callback) {
        this.callbackTable.put(topic, callback);
        //如果 listener 为null 就是不修改值 而不是设置为null
        this.defaultMQPullConsumer.registerMessageQueueListener(topic, null);
    }

    public void shutdown() {
        if (this.scheduledThreadPoolExecutor != null) {
            this.scheduledThreadPoolExecutor.shutdown();
        }

        if (this.defaultMQPullConsumer != null) {
            this.defaultMQPullConsumer.shutdown();
        }
    }

    public ConcurrentMap<String, PullTaskCallback> getCallbackTable() {
        return callbackTable;
    }

    public void setCallbackTable(ConcurrentHashMap<String, PullTaskCallback> callbackTable) {
        this.callbackTable = callbackTable;
    }

    public int getPullThreadNums() {
        return pullThreadNums;
    }

    public void setPullThreadNums(int pullThreadNums) {
        this.pullThreadNums = pullThreadNums;
    }

    public DefaultMQPullConsumer getDefaultMQPullConsumer() {
        return defaultMQPullConsumer;
    }

    public void setDefaultMQPullConsumer(DefaultMQPullConsumer defaultMQPullConsumer) {
        this.defaultMQPullConsumer = defaultMQPullConsumer;
    }

    public MessageModel getMessageModel() {
        return this.defaultMQPullConsumer.getMessageModel();
    }

    public void setMessageModel(MessageModel messageModel) {
        this.defaultMQPullConsumer.setMessageModel(messageModel);
    }

    //当消息发生改变时  这里主要看传入的参数
    class MessageQueueListenerImpl implements MessageQueueListener {
        @Override
        public void messageQueueChanged(String topic, Set<MessageQueue> mqAll, Set<MessageQueue> mqDivided) {
            MessageModel messageModel =
                MQPullConsumerScheduleService.this.defaultMQPullConsumer.getMessageModel();
            switch (messageModel) {
                case BROADCASTING:
                    //全部设置到 广播
                    MQPullConsumerScheduleService.this.putTask(topic, mqAll);
                    break;
                case CLUSTERING:
                    //拆分的设置到 集群
                    MQPullConsumerScheduleService.this.putTask(topic, mqDivided);
                    break;
                default:
                    break;
            }
        }
    }

    //拉取任务
    class PullTaskImpl implements Runnable {
        //这是针对哪个队列
        private final MessageQueue messageQueue;
        //这个任务是否被关闭
        private volatile boolean cancelled = false;

        public PullTaskImpl(final MessageQueue messageQueue) {
            this.messageQueue = messageQueue;
        }

        @Override
        public void run() {
            String topic = this.messageQueue.getTopic();
            //如果任务没有关闭
            if (!this.isCancelled()) {
                //获取对应的 回调对象
                PullTaskCallback pullTaskCallback =
                    MQPullConsumerScheduleService.this.callbackTable.get(topic);
                if (pullTaskCallback != null) {
                    //创建用于回调的 上下文对象
                    final PullTaskContext context = new PullTaskContext();
                    context.setPullConsumer(MQPullConsumerScheduleService.this.defaultMQPullConsumer);
                    try {
                        //执行回调任务  也就是处理任务的实际逻辑
                        pullTaskCallback.doPullTask(this.messageQueue, context);
                    } catch (Throwable e) {
                        //在延迟指定时间后应该是要继续执行  默认是200
                        context.setPullNextDelayTimeMillis(1000);
                        log.error("doPullTask Exception", e);
                    }

                    //正常情况应该是在执行后任务被关闭了 不然 就在一定延迟后继续执行这个任务
                    if (!this.isCancelled()) {
                        MQPullConsumerScheduleService.this.scheduledThreadPoolExecutor.schedule(this,
                            context.getPullNextDelayTimeMillis(), TimeUnit.MILLISECONDS);
                    } else {
                        log.warn("The Pull Task is cancelled after doPullTask, {}", messageQueue);
                    }
                } else {
                    log.warn("Pull Task Callback not exist , {}", topic);
                }
            } else {
                log.warn("The Pull Task is cancelled, {}", messageQueue);
            }
        }

        public boolean isCancelled() {
            return cancelled;
        }

        public void setCancelled(boolean cancelled) {
            this.cancelled = cancelled;
        }

        public MessageQueue getMessageQueue() {
            return messageQueue;
        }
    }
}
