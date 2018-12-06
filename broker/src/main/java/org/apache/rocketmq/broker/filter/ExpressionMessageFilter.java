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

package org.apache.rocketmq.broker.filter;

import org.apache.rocketmq.common.constant.LoggerName;
import org.apache.rocketmq.common.filter.ExpressionType;
import org.apache.rocketmq.logging.InternalLogger;
import org.apache.rocketmq.logging.InternalLoggerFactory;
import org.apache.rocketmq.common.message.MessageDecoder;
import org.apache.rocketmq.common.protocol.heartbeat.SubscriptionData;
import org.apache.rocketmq.filter.util.BitsArray;
import org.apache.rocketmq.filter.util.BloomFilter;
import org.apache.rocketmq.store.ConsumeQueueExt;
import org.apache.rocketmq.store.MessageFilter;

import java.nio.ByteBuffer;
import java.util.Map;

//单次的消息拦截对象 针对 拉取消息时 处理消息
public class ExpressionMessageFilter implements MessageFilter {

    protected static final InternalLogger log = InternalLoggerFactory.getLogger(LoggerName.FILTER_LOGGER_NAME);

    //订阅的数据体
    protected final SubscriptionData subscriptionData;
    //消费者的拦截对象  null 就代表不是 sql92 模式  是 tag 模式
    protected final ConsumerFilterData consumerFilterData;
    //拦截器管理对象
    protected final ConsumerFilterManager consumerFilterManager;
    protected final boolean bloomDataValid;

    public ExpressionMessageFilter(SubscriptionData subscriptionData, ConsumerFilterData consumerFilterData,
        ConsumerFilterManager consumerFilterManager) {
        this.subscriptionData = subscriptionData;
        this.consumerFilterData = consumerFilterData;
        this.consumerFilterManager = consumerFilterManager;
        if (consumerFilterData == null) {
            bloomDataValid = false;
            return;
        }
        BloomFilter bloomFilter = this.consumerFilterManager.getBloomFilter();
        if (bloomFilter != null && bloomFilter.isValid(consumerFilterData.getBloomFilterData())) {
            bloomDataValid = true;
        } else {
            bloomDataValid = false;
        }
    }

    //拦截工作  针对 cq 的拦截
    @Override
    public boolean isMatchedByConsumeQueue(Long tagsCode, ConsumeQueueExt.CqExtUnit cqExtUnit) {
        //为空代表是 class拦截模式
        if (null == subscriptionData) {
            return true;
        }

        //如果是针对  class拦截就直接通过 这里不进行处理
        if (subscriptionData.isClassFilterMode()) {
            return true;
        }

        // by tags code.
        //是 tag 类型的拦截
        if (ExpressionType.isTagType(subscriptionData.getExpressionType())) {

            if (tagsCode == null) {
                return true;
            }

            if (subscriptionData.getSubString().equals(SubscriptionData.SUB_ALL)) {
                return true;
            }

            //对比hash值 存在就可以获取到该数据  cq 只保存了  tag 如果 是 sql92模式 要去 cqExt 对象中拿
            return subscriptionData.getCodeSet().contains(tagsCode.intValue());
        } else {
            // no expression or no bloom
            // sql92类型  没有 就不用拦截
            if (consumerFilterData == null || consumerFilterData.getExpression() == null
                || consumerFilterData.getCompiledExpression() == null || consumerFilterData.getBloomFilterData() == null) {
                return true;
            }

            // message is before consumer
            if (cqExtUnit == null || !consumerFilterData.isMsgInLive(cqExtUnit.getMsgStoreTime())) {
                log.debug("Pull matched because not in live: {}, {}", consumerFilterData, cqExtUnit);
                return true;
            }

            //获取 拦截数据
            byte[] filterBitMap = cqExtUnit.getFilterBitMap();
            BloomFilter bloomFilter = this.consumerFilterManager.getBloomFilter();
            if (filterBitMap == null || !this.bloomDataValid
                || filterBitMap.length * Byte.SIZE != consumerFilterData.getBloomFilterData().getBitNum()) {
                return true;
            }

            BitsArray bitsArray = null;
            try {
                //将拦截数据 构造成 bit数组
                bitsArray = BitsArray.create(filterBitMap);
                //一个拦截判断 还不明白
                boolean ret = bloomFilter.isHit(consumerFilterData.getBloomFilterData(), bitsArray);
                log.debug("Pull {} by bit map:{}, {}, {}", ret, consumerFilterData, bitsArray, cqExtUnit);
                return ret;
            } catch (Throwable e) {
                log.error("bloom filter error, sub=" + subscriptionData
                    + ", filter=" + consumerFilterData + ", bitMap=" + bitsArray, e);
            }
        }

        return true;
    }

    //在 commitLog 中过滤消息
    @Override
    public boolean isMatchedByCommitLog(ByteBuffer msgBuffer, Map<String, String> properties) {
        //数据为空 就是 class拦截模式
        if (subscriptionData == null) {
            return true;
        }

        //class拦截模式 的话 这里就不做处理了
        if (subscriptionData.isClassFilterMode()) {
            return true;
        }

        //commitLog 不做 tag模式过滤
        if (ExpressionType.isTagType(subscriptionData.getExpressionType())) {
            return true;
        }

        ConsumerFilterData realFilterData = this.consumerFilterData;
        Map<String, String> tempProperties = properties;

        // no expression
        if (realFilterData == null || realFilterData.getExpression() == null
            || realFilterData.getCompiledExpression() == null) {
            return true;
        }

        if (tempProperties == null && msgBuffer != null) {
            //从 buffer 中解析出属性
            tempProperties = MessageDecoder.decodeProperties(msgBuffer);
        }

        Object ret = null;
        try {
            //创建 待解析对象
            MessageEvaluationContext context = new MessageEvaluationContext(tempProperties);

            //检测结果
            ret = realFilterData.getCompiledExpression().evaluate(context);
        } catch (Throwable e) {
            log.error("Message Filter error, " + realFilterData + ", " + tempProperties, e);
        }

        log.debug("Pull eval result: {}, {}, {}", ret, realFilterData, tempProperties);

        if (ret == null || !(ret instanceof Boolean)) {
            return false;
        }

        return (Boolean) ret;
    }

}
