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
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.rocketmq.test.listener;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.apache.rocketmq.client.consumer.listener.MessageListener;
import org.apache.rocketmq.test.clientinterface.MQCollector;
import org.apache.rocketmq.test.util.TestUtil;

//抽象的 消息处理器对象 同时具备了 容器的智能
public class AbstractListener extends MQCollector implements MessageListener {
    public static Logger logger = Logger.getLogger(AbstractListener.class);
    protected boolean isDebug = false;
    protected String listenerName = null;
    protected Collection<Object> allSendMsgs = null;

    public AbstractListener() {
        super();
    }

    public AbstractListener(String listenerName) {
        super();
        this.listenerName = listenerName;
    }

    public AbstractListener(String originMsgCollector, String msgBodyCollector) {
        super(originMsgCollector, msgBodyCollector);
    }

    public boolean isDebug() {
        return isDebug;
    }

    public void setDebug(boolean debug) {
        isDebug = debug;
    }

    //沉睡指定时间
    public void waitForMessageConsume(int timeoutMills) {
        TestUtil.waitForMonment(timeoutMills);
    }

    //禁止添加数据
    public void stopRecv() {
        super.lockCollectors();
    }

    //消耗数据
    public Collection<Object> waitForMessageConsume(Collection<Object> allSendMsgs,
        int timeoutMills) {
        this.allSendMsgs = allSendMsgs;
        List<Object> sendMsgs = new ArrayList<Object>();
        //将要消耗的数据存放到 这个容器中
        sendMsgs.addAll(allSendMsgs);

        long curTime = System.currentTimeMillis();
        while (!sendMsgs.isEmpty()) {
            Iterator<Object> iter = sendMsgs.iterator();
            while (iter.hasNext()) {
                Object msg = iter.next();
                //如果在 msgbody 中存在 就移除这个对象
                if (msgBodys.getAllData().contains(msg)) {
                    iter.remove();
                }
            }
            if (sendMsgs.isEmpty()) {
                break;
            } else {
                if (System.currentTimeMillis() - curTime >= timeoutMills) {
                    logger.error(String.format("timeout but  [%s]  not recv all send messages!",
                        listenerName));
                    break;
                } else {
                    //等待一段时间 继续下一次移除操作 直到超时 或者数据全部被移除
                    logger.info(String.format("[%s] still [%s] msg not recv!", listenerName,
                        sendMsgs.size()));
                    TestUtil.waitForMonment(500);
                }
            }
        }

        return sendMsgs;
    }

    //等待数据消费
    public long waitForMessageConsume(int size,
        int timeoutMills) {

        long curTime = System.currentTimeMillis();
        while (true) {
            //这是什么意思 数据数量过多就返回？？？  应该是等待 里面的数据 不断增加直到超过size
            if (msgBodys.getDataSize() >= size) {
                break;
            }
            if (System.currentTimeMillis() - curTime >= timeoutMills) {
                logger.error(String.format("timeout but  [%s]  not recv all send messages!",
                    listenerName));
                break;
            } else {
                logger.info(String.format("[%s] still [%s] msg not recv!", listenerName,
                    size - msgBodys.getDataSize()));
                TestUtil.waitForMonment(500);
            }
        }

        return msgBodys.getDataSize();
    }

    public void waitForMessageConsume(Map<Object, Object> sendMsgIndex, int timeoutMills) {
        Collection<Object> notRecvMsgs = waitForMessageConsume(sendMsgIndex.keySet(), timeoutMills);
        for (Object object : notRecvMsgs) {
            logger.info(sendMsgIndex.get(object));
        }
    }
}
