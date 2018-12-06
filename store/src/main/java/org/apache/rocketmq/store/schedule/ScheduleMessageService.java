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
package org.apache.rocketmq.store.schedule;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.apache.rocketmq.common.ConfigManager;
import org.apache.rocketmq.common.TopicFilterType;
import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageAccessor;
import org.apache.rocketmq.common.message.MessageConst;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.running.RunningStats;
import org.apache.rocketmq.store.ConsumeQueue;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.apache.rocketmq.store.DefaultMessageStore;
import org.apache.rocketmq.store.MessageExtBrokerInner;
import org.apache.rocketmq.store.PutMessageResult;
import org.apache.rocketmq.store.PutMessageStatus;
import org.apache.rocketmq.store.SelectMappedBufferResult;
import org.apache.rocketmq.store.config.StorePathConfigHelper;

//定时消息管理
public class ScheduleMessageService extends ConfigManager {
    private static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.STORE_LOGGER_NAME);

    //只有topic 跟这个一样的 才能 使用它的功能
    public static final String SCHEDULE_TOPIC = "SCHEDULE_TOPIC_XXXX";
    private static final long FIRST_DELAY_TIME = 1000L;
    private static final long DELAY_FOR_A_WHILE = 100L;
    private static final long DELAY_FOR_A_PERIOD = 10000L;

    //级别 对应延迟时间
    private final ConcurrentMap<Integer /* level */, Long/* delay timeMillis */> delayLevelTable =
        new ConcurrentHashMap<Integer, Long>(32);

    //级别对应消息的 偏移量起始位置
    private final ConcurrentMap<Integer /* level */, Long/* offset */> offsetTable =
        new ConcurrentHashMap<Integer, Long>(32);

    //使用jdk自带的 timer对象
    private final Timer timer = new Timer("ScheduleMessageTimerThread", true);

    private final DefaultMessageStore defaultMessageStore;

    private int maxDelayLevel;

    public ScheduleMessageService(final DefaultMessageStore defaultMessageStore) {
        this.defaultMessageStore = defaultMessageStore;
    }

    public static int queueId2DelayLevel(final int queueId) {
        return queueId + 1;
    }

    public static int delayLevel2QueueId(final int delayLevel) {
        return delayLevel - 1;
    }

    //构建运行状态
    public void buildRunningStats(HashMap<String, String> stats) {
        Iterator<Entry<Integer, Long>> it = this.offsetTable.entrySet().iterator();
        while (it.hasNext()) {
            Entry<Integer, Long> next = it.next();
            //将 延迟级别转换成 队列id  id 就是 延迟级别-1
            int queueId = delayLevel2QueueId(next.getKey());
            //延迟级别对应的偏移量
            long delayOffset = next.getValue();
            //就是说这个topic 应该是保留的 一开始就有对应的 偏移量 一旦对应到这个消费队列 就可以直接拿到这个maxOffset  这个偏移量对应映射文件队列最大偏移量/单元长度
            long maxOffset = this.defaultMessageStore.getMaxOffsetInQueue(SCHEDULE_TOPIC, queueId);
            String value = String.format("%d,%d", delayOffset, maxOffset);
            String key = String.format("%s_%d", RunningStats.scheduleMessageOffset.name(), next.getKey());
            //将 延迟等级 偏移量 都设置到这个map中
            stats.put(key, value);
        }
    }

    //更新偏移量
    private void updateOffset(int delayLevel, long offset) {
        this.offsetTable.put(delayLevel, offset);
    }

    public long computeDeliverTimestamp(final int delayLevel, final long storeTimestamp) {
        //延迟等级能 获取到延迟时间 用这个加上起始时间获得 预测时间
        Long time = this.delayLevelTable.get(delayLevel);
        if (time != null) {
            return time + storeTimestamp;
        }

        return storeTimestamp + 1000;
    }

    //启动
    public void start() {

        for (Map.Entry<Integer, Long> entry : this.delayLevelTable.entrySet()) {
            Integer level = entry.getKey();
            Long timeDelay = entry.getValue();
            Long offset = this.offsetTable.get(level);
            if (null == offset) {
                offset = 0L;
            }

            if (timeDelay != null) {
                //根据延时级别和偏移量 开始第一次 拉取消息
                this.timer.schedule(new DeliverDelayedMessageTimerTask(level, offset), FIRST_DELAY_TIME);
            }
        }

        //以固定时间 进行 持久化
        this.timer.scheduleAtFixedRate(new TimerTask() {

            @Override
            public void run() {
                try {
                    ScheduleMessageService.this.persist();
                } catch (Throwable e) {
                    log.error("scheduleAtFixedRate flush exception", e);
                }
            }
        }, 10000, this.defaultMessageStore.getMessageStoreConfig().getFlushDelayOffsetInterval());
    }

    //关闭定时器
    public void shutdown() {
        this.timer.cancel();
    }

    public int getMaxDelayLevel() {
        return maxDelayLevel;
    }

    public String encode() {
        return this.encode(false);
    }

    //从指定文件路径 加载数据 并decode 这个方法由子类实现
    public boolean load() {
        boolean result = super.load();
        //解析延迟数据 也要成功
        result = result && this.parseDelayLevel();
        return result;
    }

    //增加指定前后缀 后 返回路径
    @Override
    public String configFilePath() {
        return StorePathConfigHelper.getDelayOffsetStorePath(this.defaultMessageStore.getMessageStoreConfig()
            .getStorePathRootDir());
    }

    //load 方法后调用的 解码
    @Override
    public void decode(String jsonString) {
        if (jsonString != null) {
            //将数据映射成这个对象 并全部设置到容器中
            DelayOffsetSerializeWrapper delayOffsetSerializeWrapper =
                DelayOffsetSerializeWrapper.fromJson(jsonString, DelayOffsetSerializeWrapper.class);
            if (delayOffsetSerializeWrapper != null) {
                this.offsetTable.putAll(delayOffsetSerializeWrapper.getOffsetTable());
            }
        }
    }

    //将数据解析 成 json格式
    public String encode(final boolean prettyFormat) {
        DelayOffsetSerializeWrapper delayOffsetSerializeWrapper = new DelayOffsetSerializeWrapper();
        delayOffsetSerializeWrapper.setOffsetTable(this.offsetTable);
        return delayOffsetSerializeWrapper.toJson(prettyFormat);
    }

    //解析 延迟级别
    public boolean parseDelayLevel() {
        //设置常量
        HashMap<String, Long> timeUnitTable = new HashMap<String, Long>();
        timeUnitTable.put("s", 1000L);
        timeUnitTable.put("m", 1000L * 60);
        timeUnitTable.put("h", 1000L * 60 * 60);
        timeUnitTable.put("d", 1000L * 60 * 60 * 24);

        //获取 时间延迟 数据
        //样式"1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h"
        String levelString = this.defaultMessageStore.getMessageStoreConfig().getMessageDelayLevel();
        try {
            String[] levelArray = levelString.split(" ");
            for (int i = 0; i < levelArray.length; i++) {
                String value = levelArray[i];
                //解析出后面的时间单位
                String ch = value.substring(value.length() - 1);
                //根据单位获得 时间
                Long tu = timeUnitTable.get(ch);

                //最多不能超过给定的 级别  这个级别是在哪里设置的???
                int level = i + 1;
                if (level > this.maxDelayLevel) {
                    this.maxDelayLevel = level;
                }
                //获得 单位前的 数字
                long num = Long.parseLong(value.substring(0, value.length() - 1));
                //获得  延迟时间
                long delayTimeMillis = tu * num;
                this.delayLevelTable.put(level, delayTimeMillis);
            }
        } catch (Exception e) {
            log.error("parseDelayLevel exception", e);
            log.info("levelString String = {}", levelString);
            return false;
        }

        return true;
    }

    //定时器 只能执行 定时任务这个类是拓展 jdk的 定时任务的
    class DeliverDelayedMessageTimerTask extends TimerTask {
        //延迟级别 偏移量
        private final int delayLevel;
        private final long offset;

        public DeliverDelayedMessageTimerTask(int delayLevel, long offset) {
            this.delayLevel = delayLevel;
            this.offset = offset;
        }

        //Timer 就是启用一个线程去 直接 task的 run方法
        @Override
        public void run() {
            try {
                this.executeOnTimeup();
            } catch (Exception e) {
                // XXX: warn and notify me
                log.error("ScheduleMessageService, executeOnTimeup exception", e);
                //异常 就重新设置一个新的任务
                ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(
                    this.delayLevel, this.offset), DELAY_FOR_A_PERIOD);
            }
        }

        /**
         * @return
         */
        //正确的 送达时间
        private long correctDeliverTimestamp(final long now, final long deliverTimestamp) {

            //这是传入的 预测的 送达时间
            long result = deliverTimestamp;

            //这什么意思???  将now 当作送达时间 而不是maxTimestamp  可能需要返回的是 未延迟的真正时间
            long maxTimestamp = now + ScheduleMessageService.this.delayLevelTable.get(this.delayLevel);
            if (deliverTimestamp > maxTimestamp) {
                result = now;
            }

            return result;
        }

        //timer 任务的 核心逻辑
        //这个任务就是不断的 从 定时topic 队列中获取在指定时间内更新的 消息 并写入到  bytebuffer中 同时 还会进行刷盘工作 写入到物理内存
        public void executeOnTimeup() {
            //查询到对应的 消费者队列  这个特殊的 topic 下每个队列都对应一个 延迟级别
            //然后 非事务消息 和 提交的事务消息 都在里面
            //消费队列对象不同于 普通的 mq对象  通过topic 和 队列 id 获取到唯一的 cq
            ConsumeQueue cq =
                ScheduleMessageService.this.defaultMessageStore.findConsumeQueue(SCHEDULE_TOPIC,
                    delayLevel2QueueId(delayLevel));

            //应该是 回滚的偏移量吧  每个任务都有一个起始的偏移量 这里应该是 进行 例如 定时刷盘之类的任务 就是从 这个偏移量 开始 应该是要* 单位长度才能映射到实际文件的 偏移量
            long failScheduleOffset = offset;

            if (cq != null) {
                //这个 offset 需要* 单位长度 获取真正的 偏移量 然后再去 获取对应映射文件的数据
                SelectMappedBufferResult bufferCQ = cq.getIndexBuffer(this.offset);
                if (bufferCQ != null) {
                    try {
                        //这个 应该是类似 起点一样的东西 每次循环这个值 都会改变 代表下次从哪里开始
                        long nextOffset = offset;
                        int i = 0;
                        //创建 cq 对象
                        ConsumeQueueExt.CqExtUnit cqExtUnit = new ConsumeQueueExt.CqExtUnit();
                        //每次前进单位长度 然后读取映射文件的数据 里面的偏移量对应的是实际文件的信息
                        for (; i < bufferCQ.getSize(); i += ConsumeQueue.CQ_STORE_UNIT_SIZE) {
                            //从 给定位置获取数据
                            long offsetPy = bufferCQ.getByteBuffer().getLong();
                            int sizePy = bufferCQ.getByteBuffer().getInt();
                            long tagsCode = bufferCQ.getByteBuffer().getLong();

                            //解析 该消息的最晚时间
                            if (cq.isExtAddr(tagsCode)) {
                                if (cq.getExt(tagsCode, cqExtUnit)) {
                                    tagsCode = cqExtUnit.getTagsCode();
                                } else {
                                    //can't find ext content.So re compute tags code.
                                    log.error("[BUG] can't find consume queue extend file content!addr={}, offsetPy={}, sizePy={}",
                                        tagsCode, offsetPy, sizePy);
                                    //计算 tagsCode
                                    long msgStoreTime = defaultMessageStore.getCommitLog().pickupStoreTimestamp(offsetPy, sizePy);
                                    tagsCode = computeDeliverTimestamp(delayLevel, msgStoreTime);
                                }
                            }

                            long now = System.currentTimeMillis();
                            //应该是 now 加上 延迟的时间  每个 消息 都会自带一个 最晚的时间
                            long deliverTimestamp = this.correctDeliverTimestamp(now, tagsCode);

                            //每次 都是 增加单位长度  换句话说 每次循环 这个 offset 都是没有直接被改变的而是修改的 nextoffset 可能结束这个方法的时候会改变
                            nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);

                            //延迟的 时间
                            long countdown = deliverTimestamp - now;

                            //这里是2种情况 如果时间 超过了 预定的 最大延时时间就是这个任务预期会超时完成 那么就直接 写入 如果 不会超过 就在 规定时间后 再执行
                            //如果 还有多余时间 就是将任务延迟到那个点再执行
                            //这里是超过了 最大时间  的特殊情况  也就是超时
                            if (countdown <= 0) {
                                //从映射文件中 获取 数据 并 解析成messageExt 对象
                                MessageExt msgExt =
                                    ScheduleMessageService.this.defaultMessageStore.lookMessageByOffset(
                                        offsetPy, sizePy);

                                if (msgExt != null) {
                                    try {
                                        //解析数据
                                        MessageExtBrokerInner msgInner = this.messageTimeup(msgExt);
                                        //将数据保存  这里 保存是 委托到 commit 进行的
                                        PutMessageResult putMessageResult =
                                            ScheduleMessageService.this.defaultMessageStore
                                                .putMessage(msgInner);

                                        if (putMessageResult != null
                                            && putMessageResult.getPutMessageStatus() == PutMessageStatus.PUT_OK) {
                                            continue;
                                        } else {
                                            // XXX: warn and notify me
                                            log.error(
                                                "ScheduleMessageService, a message time up, but reput it failed, topic: {} msgId {}",
                                                msgExt.getTopic(), msgExt.getMsgId());
                                            //在 下一个偏移量的位置写入数据
                                            ScheduleMessageService.this.timer.schedule(
                                                new DeliverDelayedMessageTimerTask(this.delayLevel,
                                                    nextOffset), DELAY_FOR_A_PERIOD);
                                            //将偏移量更新
                                            ScheduleMessageService.this.updateOffset(this.delayLevel,
                                                nextOffset);
                                            return;
                                        }
                                    } catch (Exception e) {
                                        /*
                                         * XXX: warn and notify me



                                         */
                                        log.error(
                                            "ScheduleMessageService, messageTimeup execute error, drop it. msgExt="
                                                + msgExt + ", nextOffset=" + nextOffset + ",offsetPy="
                                                + offsetPy + ",sizePy=" + sizePy, e);
                                    }
                                }
                            } else {
                                //没超时的 正常情况  根据 预期时间 来执行定时任务
                                ScheduleMessageService.this.timer.schedule(
                                    new DeliverDelayedMessageTimerTask(this.delayLevel, nextOffset),
                                    countdown);
                                ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);
                                return;
                            }
                        } // end of for

                        //这里 是 一次更新 nextOffset  这里最后个任务 好像会执行2次
                        nextOffset = offset + (i / ConsumeQueue.CQ_STORE_UNIT_SIZE);
                        ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(
                            this.delayLevel, nextOffset), DELAY_FOR_A_WHILE);
                        ScheduleMessageService.this.updateOffset(this.delayLevel, nextOffset);
                        return;
                    } finally {

                        //反射 获取 clean 对象 释放 物理内存
                        bufferCQ.release();
                    }
                } // end of if (bufferCQ != null)
                else {

                    //对应的mq 没有找到 bytebuffer对象的话  就是偏移量 小于 最小偏移量
                    long cqMinOffset = cq.getMinOffsetInQueue();
                    if (offset < cqMinOffset) {
                        //将 回滚的 偏移量设置成 这个最小偏移量
                        failScheduleOffset = cqMinOffset;
                        log.error("schedule CQ offset invalid. offset=" + offset + ", cqMinOffset="
                            + cqMinOffset + ", queueId=" + cq.getQueueId());
                    }
                }
            } // end of if (cq != null)

            //从给定偏移量 开始重新进行任务
            ScheduleMessageService.this.timer.schedule(new DeliverDelayedMessageTimerTask(this.delayLevel,
                failScheduleOffset), DELAY_FOR_A_WHILE);
        }

        //将消息 从 延时消息 还原成 普通消息
        private MessageExtBrokerInner messageTimeup(MessageExt msgExt) {
            //将 messageExt 数据 映射到 brokerInner中
            MessageExtBrokerInner msgInner = new MessageExtBrokerInner();
            msgInner.setBody(msgExt.getBody());
            msgInner.setFlag(msgExt.getFlag());
            MessageAccessor.setProperties(msgInner, msgExt.getProperties());

            //是 单个 拦截 还是多个拦截
            TopicFilterType topicFilterType = MessageExt.parseTopicFilterType(msgInner.getSysFlag());
            //解析 tags 的数据  就是 返回 tags 的 hashcode
            long tagsCodeValue =
                MessageExtBrokerInner.tagsString2tagsCode(topicFilterType, msgInner.getTags());
            //设置数据
            msgInner.setTagsCode(tagsCodeValue);
            msgInner.setPropertiesString(MessageDecoder.messageProperties2String(msgExt.getProperties()));

            msgInner.setSysFlag(msgExt.getSysFlag());
            msgInner.setBornTimestamp(msgExt.getBornTimestamp());
            msgInner.setBornHost(msgExt.getBornHost());
            msgInner.setStoreHost(msgExt.getStoreHost());
            msgInner.setReconsumeTimes(msgExt.getReconsumeTimes());

            //异步的意思???
            msgInner.setWaitStoreMsgOK(false);
            //把延迟属性去除
            MessageAccessor.clearProperty(msgInner, MessageConst.PROPERTY_DELAY_TIME_LEVEL);

            //还原真正的 topic
            msgInner.setTopic(msgInner.getProperty(MessageConst.PROPERTY_REAL_TOPIC));

            String queueIdStr = msgInner.getProperty(MessageConst.PROPERTY_REAL_QUEUE_ID);
            int queueId = Integer.parseInt(queueIdStr);
            msgInner.setQueueId(queueId);

            return msgInner;
        }
    }
}
